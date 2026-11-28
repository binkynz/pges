//! Reading a page of the log for one or several categories, merged at a cut no category can skip, or for
//! every category of the store.

use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use sqlx::PgPool;
use uuid::Uuid;

use pges_core::{category, commit, event, frame, high_water, position, store, stream, version};

use crate::{batch, ident, sql, sql::statement, subscription};

/// A category the subscription reads, with its statements and its event type names.
#[derive(Debug, Clone)]
pub(crate) struct Watched {
    pub(crate) id: category::CategoryId,
    pub(crate) name: category::CategoryName,
    fetch: sql::FetchCommits,
    types: HashMap<u32, Arc<str>>,
}

impl Watched {
    pub(crate) fn new(
        store: &store::StoreName,
        id: category::CategoryId,
        name: category::CategoryName,
        sharding: sql::Sharding,
    ) -> Self {
        Self {
            id,
            name,
            fetch: sql::FetchCommits::new(store, name, sharding),
            types: HashMap::new(),
        }
    }
}

/// Which categories a subscription reads.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Scope {
    /// The categories it was given, merged at a cut.
    Listed,
    /// Every category of the store, including those registered later, read from the log alone.
    All,
}

/// How a [`Reader`] reads, besides its categories.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Reading {
    pub(crate) scope: Scope,
    pub(crate) limit: u32,
    pub(crate) on_unknown: event::OnUnknown,
    /// The number of shards; 1 reads every stream.
    pub(crate) shards: i16,
}

impl Reading {
    pub(crate) fn sharding(&self) -> sql::Sharding {
        if self.shards > 1 {
            sql::Sharding::Sharded
        } else {
            sql::Sharding::Whole
        }
    }
}

/// One merged page.
#[derive(Debug)]
pub(crate) struct Page {
    /// The records at or below `cut`, in position order.
    pub(crate) batch: batch::Batch,
    /// `H`, the store's newest position when the page was read.
    pub(crate) head: position::Position,
}

impl Page {
    /// Whether the page reached `H`: every watched event up to the store's newest position was read.
    pub(crate) fn is_caught_up(&self) -> bool {
        self.batch.last_position() >= self.head
    }
}

/// Reads pages for a set of categories, or for the whole store, for every stream or one shard's.
#[derive(Debug, Clone)]
pub(crate) struct Reader {
    pool: PgPool,
    sql: Arc<sql::Statements>,
    store: store::StoreName,
    categories: Vec<Watched>,
    /// The categories a listed reader was given, for [`batch::Batch::decode`]'s check; `None` for the whole
    /// store.
    listed: Option<Arc<[category::CategoryName]>>,
    reading: Reading,
    /// The shard read, below `reading.shards`.
    shard: i16,
    /// A category whose retention horizon is at or below this is read after its start position, and the
    /// commits retention dropped from it are skipped; a category whose horizon is above it is read after
    /// the cursor only if the cursor is not behind the horizon, or the read fails with
    /// [`subscription::SubscribeError::CursorBeforeHorizon`]. `BEGINNING` for a subscription, which must
    /// not skip events it has not handled; the head at creation for one started at
    /// [`subscription::Start::Retained`]; `MAX` for a backfill.
    skip_through: position::Position,
}

impl Reader {
    pub(crate) fn new(
        pool: PgPool,
        sql: Arc<sql::Statements>,
        store: &store::StoreName,
        categories: Vec<Watched>,
        reading: Reading,
    ) -> Self {
        let listed = match reading.scope {
            Scope::Listed => Some(categories.iter().map(|c| c.name).collect()),
            Scope::All => None,
        };

        Self {
            pool,
            sql,
            store: store.clone(),
            categories,
            listed,
            reading,
            shard: 0,
            skip_through: position::Position::BEGINNING,
        }
    }

    /// Skips the commits retention dropped from categories whose horizon is at or below `through` (see
    /// `Reader::skip_through`).
    pub(crate) fn skip_dropped_through(&mut self, through: position::Position) {
        self.skip_through = through;
    }

    /// The same reader, for one shard's streams.
    pub(crate) fn for_shard(&self, shard: i16) -> Self {
        Self {
            shard,
            ..self.clone()
        }
    }

    /// The categories a listed reader was given; `None` for the whole store.
    pub(crate) fn listed(&self) -> Option<Arc<[category::CategoryName]>> {
        self.listed.clone()
    }

    /// The categories a waker watches: the listed ones, or none for the whole store.
    pub(crate) fn watched_ids(&self) -> Vec<category::CategoryId> {
        match self.reading.scope {
            Scope::Listed => self.categories.iter().map(|c| c.id).collect(),
            Scope::All => Vec::new(),
        }
    }

    /// What a waker is given once the reader has read up to `cut`: the store and every watched category at
    /// `cut`.
    pub(crate) fn known(&self, cut: position::Position) -> high_water::HighWater {
        high_water::HighWater::new(
            cut,
            self.watched_ids().into_iter().map(|id| (id, cut)).collect(),
            None,
        )
    }

    /// Loads every event type name of the watched categories.
    pub(crate) async fn load_types(&mut self) -> Result<(), subscription::SubscribeError> {
        let ids: Vec<i32> = self.categories.iter().map(|c| c.id.get()).collect();
        let rows = self
            .sql
            .types
            .bind(statement::event_types::Params { category_ids: &ids })
            .fetch_all(&self.pool)
            .await?;
        for row in rows {
            let Ok(type_id) = u32::try_from(row.event_type_id) else {
                continue;
            };
            if let Some(watched) = self
                .categories
                .iter_mut()
                .find(|c| c.id.get() == row.category_id)
            {
                watched.types.insert(type_id, row.name.into());
            }
        }

        Ok(())
    }

    /// The page after `cursor`.
    ///
    /// 1. Read `H`, the store's newest position. The sequencer commits log rows in position order, one
    ///    transaction at a time, so every position up to `H` is visible to the statements that follow.
    /// 2. Read the rows after `cursor`, up to `H` ([`Reader::read_listed`], [`Reader::read_all`]).
    pub(crate) async fn read(
        &mut self,
        cursor: position::Position,
    ) -> Result<Page, subscription::SubscribeError> {
        let pool = self.pool.clone();

        self.read_from(&pool, cursor).await
    }

    /// [`Reader::read`] on `pool`, such as a replica's.
    ///
    /// A replica replays the primary's commits in order, so what it shows is a prefix of the log and the
    /// merge cut holds there too. Every page statement, for listed categories, the whole store or a shard,
    /// runs on `pool`; horizon checks, category lookups, event type names and the deleted commits lookup
    /// still use the primary.
    pub(crate) async fn read_from(
        &mut self,
        pool: &PgPool,
        cursor: position::Position,
    ) -> Result<Page, subscription::SubscribeError> {
        let head = self
            .sql
            .head
            .bind(statement::head::Params {})
            .fetch_one(pool)
            .await?
            .head;
        let head = to_position(head)?;

        self.read_through(pool, cursor, head).await
    }

    /// [`Reader::read_from`] with `head` as `H`, without reading the store's newest position. Every position
    /// up to `head` must already be visible on `pool`: `head` was read from it earlier, or is below such a
    /// position. Visibility only grows, so a backfill reads up to its end, which it checked against `H`.
    pub(crate) async fn read_through(
        &mut self,
        pool: &PgPool,
        cursor: position::Position,
        head: position::Position,
    ) -> Result<Page, subscription::SubscribeError> {
        if head <= cursor {
            return Ok(Page {
                batch: batch::Batch::new(
                    Vec::new(),
                    cursor,
                    self.reading.on_unknown,
                    self.listed(),
                ),
                head,
            });
        }

        match self.reading.scope {
            Scope::Listed => self.read_listed(pool, cursor, head).await,
            Scope::All => self.read_all(pool, cursor, head).await,
        }
    }

    /// 1. Read each category's next log rows after `cursor` (for a category whose dropped commits the reader
    ///    skips, after its start position when that is higher), up to `head`, at most `limit` each, with
    ///    its horizon or start checked in the statement.
    /// 2. The cut is the lowest last position of the categories that returned a full page of log rows, or
    ///    `head` if none did: no category can have an unread row at or below it.
    /// 3. Fetch the commits of the rows at or below the cut by id, and return them in position order. Rows
    ///    above the cut are read again by the next page, so their commits are not fetched. For a shard,
    ///    other shards' commits come without payload and are skipped.
    async fn read_listed(
        &mut self,
        pool: &PgPool,
        cursor: position::Position,
        head: position::Position,
    ) -> Result<Page, subscription::SubscribeError> {
        loop {
            // At or past `skip_through`, every skipped category's start is at or below the cursor.
            let floors = if cursor < self.skip_through {
                self.start_floors(cursor).await?
            } else {
                vec![cursor; self.categories.len()]
            };
            match self.read_listed_from(pool, cursor, &floors, head).await? {
                Some(page) => return Ok(page),
                // Retention raised a skipped category's start between the statements; read again from it.
                None => continue,
            }
        }
    }

    /// [`Reader::read_listed`] with each category read after its floor: the cursor, or for a category whose
    /// dropped commits the reader skips, its start position when that is higher. `None` when such a
    /// category's start passed its floor meanwhile.
    async fn read_listed_from(
        &mut self,
        pool: &PgPool,
        cursor: position::Position,
        floors: &[position::Position],
        head: position::Position,
    ) -> Result<Option<Page>, subscription::SubscribeError> {
        let mut logs = Vec::with_capacity(self.categories.len());
        for (category, floor) in self.categories.iter().zip(floors) {
            logs.push(self.read_category_log(pool, category, *floor, head).await?);
        }

        let limit = usize::try_from(self.reading.limit).unwrap_or(usize::MAX);
        let cut = cut(
            head,
            limit,
            logs.iter()
                .map(|log| (log.len(), log.last().map(|row| row.last_position))),
        );

        let mut pages = Vec::with_capacity(self.categories.len());
        for (category, log) in self.categories.iter().zip(&logs) {
            pages.push(self.fetch_category(pool, category, log, cut).await?);
        }

        // A category is checked against its horizon when its log page is empty, which the statement also
        // returns below the horizon, or when a commit of its page is missing: retention may have raised the
        // horizon and dropped the commit between the two statements. A commit missing without that, such as
        // one `delete_stream` deleted, or one retention dropped from a category the reader skips, is
        // skipped.
        let suspect: Vec<(category::CategoryId, position::Position)> = self
            .categories
            .iter()
            .zip(floors)
            .zip(logs.iter().zip(&pages))
            .filter(|(_, (log, page))| log.is_empty() || page.missing == Missing::Some)
            .map(|((category, floor), _)| (category.id, *floor))
            .collect();
        match self.passed_horizon(&suspect).await? {
            Some(Passed::Behind(error)) => return Err(error),
            Some(Passed::Skipped) => return Ok(None),
            None => {}
        }

        let mut commits: Vec<(usize, CommitRow)> = pages
            .into_iter()
            .enumerate()
            .flat_map(|(index, page)| page.commits.into_iter().map(move |row| (index, row)))
            .collect();
        commits.sort_unstable_by_key(|(_, row)| row.last_position);

        let mut records = Vec::new();
        for (index, row) in commits {
            self.decode(index, row, cursor, &mut records).await?;
        }

        Ok(Some(Page {
            batch: batch::Batch::new(records, cut, self.reading.on_unknown, self.listed()),
            head,
        }))
    }

    /// One category's next log rows after `floor`, up to `head`, in position order.
    async fn read_category_log(
        &self,
        pool: &PgPool,
        category: &Watched,
        floor: position::Position,
        head: position::Position,
    ) -> Result<Vec<LogRow>, subscription::SubscribeError> {
        self.sql
            .read_category_log
            .bind(statement::read_category_log::Params {
                category_id: category.id.get(),
                after: to_i64(floor),
                head: to_i64(head),
                limit: i64::from(self.reading.limit),
                skip_through: to_i64(self.skip_through),
            })
            .fetch_all(pool)
            .await?
            .into_iter()
            .map(|row| {
                Ok(LogRow {
                    last_position: to_position(row.last_position)?,
                    commit_id: row.commit_id,
                    category_id: category.id,
                    event_count: row.event_count,
                })
            })
            .collect()
    }

    /// The commits of one category's log rows at or below `cut`, by id from the category's partition. A
    /// statement planned for the ids prunes the category's time leaves to the ones the page touches, so its
    /// cost does not grow with the leaves.
    async fn fetch_category(
        &self,
        pool: &PgPool,
        category: &Watched,
        log: &[LogRow],
        cut: position::Position,
    ) -> Result<CategoryPage, subscription::SubscribeError> {
        let below = log.partition_point(|row| row.last_position <= cut);
        let log = &log[..below];
        if log.is_empty() {
            return Ok(CategoryPage {
                commits: Vec::new(),
                missing: Missing::None,
            });
        }

        let commit_ids: Vec<Uuid> = log.iter().map(|row| row.commit_id).collect();
        let mut fetched = self
            .fetch_commits(pool, &category.fetch, &commit_ids)
            .await?;

        let mut missing = Missing::None;
        let mut commits = Vec::with_capacity(log.len());
        for row in log {
            let Some(data) = fetched.remove(&row.commit_id) else {
                missing = Missing::Some;
                continue;
            };
            let Some(payload) = data.payload else {
                continue;
            };
            commits.push(CommitRow {
                last_position: row.last_position,
                event_count: row.event_count,
                stream_id: data.stream_id,
                last_version: data.last_version,
                commit_id: data.commit_id,
                format: data.format,
                payload,
            });
        }

        Ok(CategoryPage { commits, missing })
    }

    /// The commits of one category by id, on `pool`; for a shard, other shards' commits without payload.
    async fn fetch_commits(
        &self,
        pool: &PgPool,
        fetch: &sql::FetchCommits,
        commit_ids: &[Uuid],
    ) -> Result<HashMap<Uuid, CommitData>, subscription::SubscribeError> {
        let data: Vec<CommitData> = match fetch {
            sql::FetchCommits::Whole(fetch) => fetch
                .bind(statement::fetch_commits::Params { commit_ids })
                .fetch_all(pool)
                .await?
                .into_iter()
                .map(CommitData::from_whole)
                .collect(),
            sql::FetchCommits::Sharded(fetch) => fetch
                .bind(statement::fetch_commits_sharded::Params {
                    commit_ids,
                    shards: i64::from(self.reading.shards),
                    shard: i64::from(self.shard),
                })
                .fetch_all(pool)
                .await?
                .into_iter()
                .map(CommitData::from_sharded)
                .collect(),
        };

        Ok(data
            .into_iter()
            .map(|data| (data.commit_id, data))
            .collect())
    }

    /// 1. Read the next `limit` rows of the log after `cursor`, up to `head`, checked against the highest
    ///    retention horizon of any category. The cut is the last row's position for a full page, otherwise
    ///    `head`: one statement saw every row up to `head`, so no merge is needed.
    /// 2. Fetch the commits of each category in the page by id, loading categories registered since the
    ///    last read. For a shard, other shards' commits come without payload and are skipped.
    /// 3. Skip the log rows of commits `delete_stream` deleted, and of commits retention dropped from a
    ///    category whose dropped commits the reader skips. Any other missing commit was dropped by retention,
    ///    which the highest horizon shows, or the log is corrupt.
    async fn read_all(
        &mut self,
        pool: &PgPool,
        cursor: position::Position,
        head: position::Position,
    ) -> Result<Page, subscription::SubscribeError> {
        let log = self
            .sql
            .read_log
            .bind(statement::read_log::Params {
                after: to_i64(cursor),
                head: to_i64(head),
                limit: i64::from(self.reading.limit),
                skip_through: to_i64(self.skip_through),
            })
            .fetch_all(pool)
            .await?
            .into_iter()
            .map(LogRow::from_row)
            .collect::<Result<Vec<_>, _>>()?;
        if log.is_empty() {
            self.check_highest_horizon(cursor).await?;
        }

        let limit = usize::try_from(self.reading.limit).unwrap_or(usize::MAX);
        let cut = match log.last() {
            Some(last) if log.len() >= limit => last.last_position,
            _ => head,
        };

        let mut by_category: Vec<(category::CategoryId, Vec<Uuid>)> = Vec::new();
        for row in &log {
            match by_category
                .iter_mut()
                .find(|(id, _)| *id == row.category_id)
            {
                Some((_, ids)) => ids.push(row.commit_id),
                None => by_category.push((row.category_id, vec![row.commit_id])),
            }
        }
        self.load_categories(by_category.iter().map(|(id, _)| *id))
            .await?;

        let mut fetched: HashMap<(category::CategoryId, Uuid), CommitData> =
            HashMap::with_capacity(log.len());
        for (id, commit_ids) in &by_category {
            let index = self.index_of(*id, cut)?;
            let data = self
                .fetch_commits(pool, &self.categories[index].fetch, commit_ids)
                .await?;
            for (commit_id, data) in data {
                fetched.insert((*id, commit_id), data);
            }
        }

        let deleted = self.deleted_commits(&log, &fetched).await?;
        let dropped = self.dropped_commits(cursor, &log, &fetched).await?;

        let mut records = Vec::new();
        for row in log {
            let Some(data) = fetched.remove(&(row.category_id, row.commit_id)) else {
                if deleted.contains(&(row.category_id, row.commit_id))
                    || dropped.contains(&(row.category_id, row.commit_id))
                {
                    continue;
                }
                self.check_highest_horizon(cursor).await?;
                return Err(subscription::SubscribeError::Corrupt {
                    position: row.last_position,
                    reason: "a log row without its commit",
                });
            };
            let Some(payload) = data.payload else {
                continue;
            };
            let index = self.index_of(row.category_id, row.last_position)?;
            let commit = CommitRow {
                last_position: row.last_position,
                event_count: row.event_count,
                stream_id: data.stream_id,
                last_version: data.last_version,
                commit_id: data.commit_id,
                format: data.format,
                payload,
            };
            self.decode(index, commit, cursor, &mut records).await?;
        }

        Ok(Page {
            batch: batch::Batch::new(records, cut, self.reading.on_unknown, self.listed()),
            head,
        })
    }

    /// Adds the categories among `ids` that are not watched yet, with their event types: categories
    /// registered after the subscription started.
    async fn load_categories(
        &mut self,
        ids: impl Iterator<Item = category::CategoryId>,
    ) -> Result<(), subscription::SubscribeError> {
        let missing: Vec<i32> = ids
            .filter(|id| !self.categories.iter().any(|c| c.id == *id))
            .map(|id| id.get())
            .collect();
        if missing.is_empty() {
            return Ok(());
        }

        let rows = self
            .sql
            .categories_by_id
            .bind(statement::categories_by_id::Params {
                category_ids: &missing,
            })
            .fetch_all(&self.pool)
            .await?;
        for row in rows {
            let id = category::CategoryId::new(row.category_id);
            let interned = ident::intern(&row.name).ok_or(INVALID_CATEGORY_NAME)?;
            self.categories.push(Watched::new(
                &self.store,
                id,
                interned,
                self.reading.sharding(),
            ));
        }

        self.load_types().await
    }

    /// The log rows of `log` whose commit is missing from `fetched` because `delete_stream` deleted it, as
    /// category and commit id. Read on the primary: a commit missing from a replica's fetch was deleted
    /// there by a transaction the primary already shows.
    async fn deleted_commits(
        &self,
        log: &[LogRow],
        fetched: &HashMap<(category::CategoryId, Uuid), CommitData>,
    ) -> Result<HashSet<(category::CategoryId, Uuid)>, subscription::SubscribeError> {
        let (category_ids, commit_ids): (Vec<i32>, Vec<Uuid>) = log
            .iter()
            .filter(|row| !fetched.contains_key(&(row.category_id, row.commit_id)))
            .map(|row| (row.category_id.get(), row.commit_id))
            .unzip();
        if commit_ids.is_empty() {
            return Ok(HashSet::new());
        }

        let rows = self
            .sql
            .deleted_commits
            .bind(statement::deleted_commits::Params {
                category_ids: &category_ids,
                commit_ids: &commit_ids,
            })
            .fetch_all(&self.pool)
            .await?;

        Ok(rows
            .into_iter()
            .map(|row| (category::CategoryId::new(row.category_id), row.commit_id))
            .collect())
    }

    /// The log rows of `log` whose commit is missing from `fetched` because retention dropped it from a
    /// category whose dropped commits the reader skips: the row is at or below the category's horizon, and
    /// the horizon at or below `skip_through`. Read after the commits: retention raises the horizon before
    /// it drops them.
    async fn dropped_commits(
        &self,
        cursor: position::Position,
        log: &[LogRow],
        fetched: &HashMap<(category::CategoryId, Uuid), CommitData>,
    ) -> Result<HashSet<(category::CategoryId, Uuid)>, subscription::SubscribeError> {
        // Past `skip_through`, every row is above the horizon of each skipped category.
        if cursor >= self.skip_through {
            return Ok(HashSet::new());
        }
        let missing: Vec<&LogRow> = log
            .iter()
            .filter(|row| !fetched.contains_key(&(row.category_id, row.commit_id)))
            .collect();
        if missing.is_empty() {
            return Ok(HashSet::new());
        }

        let mut ids: Vec<i32> = missing.iter().map(|row| row.category_id.get()).collect();
        ids.sort_unstable();
        ids.dedup();
        let retained = self.retained(&ids).await?;

        Ok(missing
            .into_iter()
            .filter(|row| {
                retained.get(&row.category_id).is_some_and(|retained| {
                    self.skips(retained.horizon) && row.last_position <= retained.horizon
                })
            })
            .map(|row| (row.category_id, row.commit_id))
            .collect())
    }

    fn index_of(
        &self,
        id: category::CategoryId,
        position: position::Position,
    ) -> Result<usize, subscription::SubscribeError> {
        self.categories.iter().position(|c| c.id == id).ok_or(
            subscription::SubscribeError::Corrupt {
                position,
                reason: "a log row of an unknown category",
            },
        )
    }

    /// The log statement of the whole store returns no rows for a cursor behind the highest horizon of any
    /// category it does not skip; tell that apart from being caught up.
    async fn check_highest_horizon(
        &self,
        cursor: position::Position,
    ) -> Result<(), subscription::SubscribeError> {
        let row = self
            .sql
            .highest_horizon
            .bind(statement::highest_horizon::Params {
                skip_through: to_i64(self.skip_through),
            })
            .fetch_optional(&self.pool)
            .await?;
        let Some(row) = row else {
            return Ok(());
        };

        let horizon = to_position(row.horizon_position)?;
        if horizon > cursor {
            return Err(subscription::SubscribeError::CursorBeforeHorizon {
                category: category::CategoryId::new(row.category_id),
                cursor,
                horizon,
            });
        }

        Ok(())
    }

    /// The read statement returns no rows for a cursor behind a category's horizon, or for a category whose
    /// dropped commits the reader skips, behind its start. For the categories that returned none or missed
    /// a commit, each with the position it was read after, tell that apart from being caught up: the first
    /// whose horizon is above that position, as
    /// [`CursorBeforeHorizon`](subscription::SubscribeError::CursorBeforeHorizon), or else
    /// [`Passed::Skipped`] if a skipped category's start is.
    async fn passed_horizon(
        &self,
        empty: &[(category::CategoryId, position::Position)],
    ) -> Result<Option<Passed>, subscription::SubscribeError> {
        if empty.is_empty() {
            return Ok(None);
        }

        let ids: Vec<i32> = empty.iter().map(|(id, _)| id.get()).collect();
        let retained = self.retained(&ids).await?;
        let mut passed = None;
        for (id, read_after) in empty {
            let Some(retained) = retained.get(id) else {
                continue;
            };
            if !self.skips(retained.horizon) {
                if retained.horizon > *read_after {
                    return Ok(Some(Passed::Behind(
                        subscription::SubscribeError::CursorBeforeHorizon {
                            category: *id,
                            cursor: *read_after,
                            horizon: retained.horizon,
                        },
                    )));
                }
            } else if retained.start > *read_after {
                passed = Some(Passed::Skipped);
            }
        }

        Ok(passed)
    }

    /// For each category, in order, `cursor`, or for a category whose dropped commits the reader skips, its
    /// start position when that is higher.
    async fn start_floors(
        &self,
        cursor: position::Position,
    ) -> Result<Vec<position::Position>, subscription::SubscribeError> {
        let ids: Vec<i32> = self.categories.iter().map(|c| c.id.get()).collect();
        let retained = self.retained(&ids).await?;

        Ok(self
            .categories
            .iter()
            .map(|category| match retained.get(&category.id) {
                Some(retained) if self.skips(retained.horizon) => cursor.max(retained.start),
                _ => cursor,
            })
            .collect())
    }

    /// The retention horizon and start position of the categories `ids`.
    async fn retained(
        &self,
        ids: &[i32],
    ) -> Result<HashMap<category::CategoryId, Retained>, subscription::SubscribeError> {
        let rows = self
            .sql
            .horizons
            .bind(statement::horizons::Params { category_ids: ids })
            .fetch_all(&self.pool)
            .await?;
        let mut retained = HashMap::with_capacity(rows.len());
        for row in rows {
            retained.insert(
                category::CategoryId::new(row.category_id),
                Retained {
                    horizon: to_position(row.horizon_position)?,
                    start: to_position(row.start_position)?,
                },
            );
        }

        Ok(retained)
    }

    /// Whether the reader skips the commits retention dropped from a category with this horizon.
    fn skips(&self, horizon: position::Position) -> bool {
        horizon <= self.skip_through
    }

    async fn decode(
        &mut self,
        index: usize,
        row: CommitRow,
        cursor: position::Position,
        out: &mut Vec<batch::RawRecorded>,
    ) -> Result<(), subscription::SubscribeError> {
        let at = row.last_position;
        let corrupt = move |reason| subscription::SubscribeError::Corrupt {
            position: at,
            reason,
        };
        let commit_id = commit::CommitId::from_uuid(row.commit_id)
            .ok_or(corrupt("commit id is not a UUIDv7"))?;
        let event_count =
            u16::try_from(row.event_count).map_err(|_| corrupt("negative event_count"))?;
        let format = frame::Format::from_code(row.format).map_err(|source| {
            subscription::SubscribeError::Frame {
                position: at,
                source,
            }
        })?;
        let frame = frame::decode(format, &row.payload, event_count).map_err(|source| {
            subscription::SubscribeError::Frame {
                position: at,
                source,
            }
        })?;
        let metadata = Arc::new(frame.metadata);
        let stream_id = stream::StreamId::from_uuid(row.stream_id);
        let recorded_at = commit_id.recorded_at();
        let count = u64::from(event_count);
        let first_position = row
            .last_position
            .get()
            .checked_sub(count - 1)
            .filter(|first| *first >= 1)
            .ok_or(corrupt("event_count larger than last_position"))?;
        let first_version = row
            .last_version
            .checked_sub(i64::from(event_count) - 1)
            .and_then(|first| u64::try_from(first).ok())
            .ok_or(corrupt("event_count larger than last_version"))?;

        for (offset, stored) in (0_u64..).zip(&frame.events) {
            let position = position::Position::new(first_position + offset)
                .ok_or(corrupt("position above the maximum"))?;
            if position <= cursor {
                continue;
            }
            let version =
                version::Version::new(first_version + offset).ok_or(corrupt("version below 1"))?;

            let category = &self.categories[index];
            if !category.types.contains_key(&stored.type_id.get()) {
                self.load_types().await?;
            }
            let category = &self.categories[index];
            let Some(type_name) = category.types.get(&stored.type_id.get()) else {
                return Err(subscription::SubscribeError::UnknownEventType {
                    position,
                    category: category.id,
                    type_id: stored.type_id,
                });
            };

            if event::is_system_type(type_name) {
                out.push(batch::RawRecorded::System(system_event(
                    type_name,
                    position,
                    stream_id,
                    category.name,
                )));
                continue;
            }

            out.push(batch::RawRecorded::Event(batch::RawEvent {
                position,
                stream_id,
                category: category.name,
                category_id: category.id,
                version,
                commit_id,
                recorded_at,
                metadata: metadata.clone(),
                type_name: type_name.clone(),
                schema_version: stored.schema_version,
                data: stored.data.to_vec(),
            }));
        }

        Ok(())
    }
}

/// The merge cut: the lowest last position among the categories whose page was full (`rows == limit`),
/// or `head` when no page was full. Each item is one category's row count and last position.
pub(crate) fn cut(
    head: position::Position,
    limit: usize,
    pages: impl IntoIterator<Item = (usize, Option<position::Position>)>,
) -> position::Position {
    pages
        .into_iter()
        .filter(|(rows, _)| *rows >= limit)
        .filter_map(|(_, last)| last)
        .min()
        .unwrap_or(head)
}

/// The categories `names`, sorted by id and without duplicates, ready for a [`Reader`].
/// [`subscription::SubscribeError::UnknownCategory`] names the first that is not registered.
pub(crate) async fn resolve(
    pool: &PgPool,
    sql: &sql::Statements,
    store: &store::StoreName,
    names: &[category::CategoryName],
    sharding: sql::Sharding,
) -> Result<Vec<Watched>, subscription::SubscribeError> {
    let bound: Vec<&str> = names.iter().map(|name| name.as_str()).collect();
    let rows = sql
        .categories
        .bind(statement::categories::Params { names: &bound })
        .fetch_all(pool)
        .await?;
    let mut found: Vec<(category::CategoryId, String)> = rows
        .into_iter()
        .map(|row| (category::CategoryId::new(row.category_id), row.name))
        .collect();
    if let Some(missing) = names
        .iter()
        .find(|name| !found.iter().any(|(_, found)| found == name.as_str()))
    {
        return Err(subscription::SubscribeError::UnknownCategory(*missing));
    }
    found.sort_unstable();
    found.dedup();

    found
        .into_iter()
        .map(|(id, name)| {
            let interned = ident::intern(&name).ok_or(INVALID_CATEGORY_NAME)?;
            Ok(Watched::new(store, id, interned, sharding))
        })
        .collect()
}

/// A category name read from the database breaks the naming rule, which the `categories` CHECK makes
/// impossible.
const INVALID_CATEGORY_NAME: subscription::SubscribeError = subscription::SubscribeError::Corrupt {
    position: position::Position::BEGINNING,
    reason: "a stored category name breaks the naming rule",
};

pub(crate) fn to_i64(position: position::Position) -> i64 {
    position.to_i64()
}

pub(crate) fn to_position(
    position: i64,
) -> Result<position::Position, subscription::SubscribeError> {
    position::Position::from_i64(position).ok_or(subscription::SubscribeError::Corrupt {
        position: position::Position::BEGINNING,
        reason: "negative position",
    })
}

fn system_event(
    type_name: &str,
    position: position::Position,
    stream_id: stream::StreamId,
    category: category::CategoryName,
) -> batch::SystemEvent {
    if type_name == event::STREAM_DELETED {
        return batch::SystemEvent::StreamDeleted {
            position,
            stream_id,
            category,
        };
    }

    batch::SystemEvent::Unrecognised {
        position,
        type_name: type_name.to_owned(),
        stream_id,
    }
}

/// One row of a category read.
#[derive(Debug)]
struct CommitRow {
    last_position: position::Position,
    event_count: i16,
    stream_id: Uuid,
    last_version: i64,
    commit_id: Uuid,
    format: i16,
    payload: Vec<u8>,
}

/// The commits of one category's log rows at or below the cut, for a listed read.
#[derive(Debug)]
struct CategoryPage {
    /// In position order, without other shards' commits and missing ones.
    commits: Vec<CommitRow>,
    missing: Missing,
}

/// A category's retention horizon and start position.
#[derive(Debug, Clone, Copy)]
struct Retained {
    horizon: position::Position,
    start: position::Position,
}

/// How a read's cursor stands against a category's retention.
#[derive(Debug)]
enum Passed {
    /// Behind the horizon of a category whose dropped commits the reader does not skip.
    Behind(subscription::SubscribeError),
    /// Behind the start of a category whose dropped commits the reader skips: read again from it.
    Skipped,
}

/// Whether a log row's commit was not found.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Missing {
    None,
    Some,
}

/// One row of a page of the log: the whole store's, or one category's.
#[derive(Debug)]
struct LogRow {
    last_position: position::Position,
    commit_id: Uuid,
    category_id: category::CategoryId,
    event_count: i16,
}

impl LogRow {
    fn from_row(row: statement::read_log::Row) -> Result<Self, subscription::SubscribeError> {
        Ok(Self {
            last_position: to_position(row.last_position)?,
            commit_id: row.commit_id,
            category_id: category::CategoryId::new(row.category_id),
            event_count: row.event_count,
        })
    }
}

/// A commit fetched by id for a page of the whole store's log. `payload` is `None` for another shard's
/// commit.
#[derive(Debug)]
struct CommitData {
    commit_id: Uuid,
    stream_id: Uuid,
    last_version: i64,
    format: i16,
    payload: Option<Vec<u8>>,
}

impl CommitData {
    fn from_whole(row: statement::fetch_commits::Row) -> Self {
        Self {
            commit_id: row.commit_id,
            stream_id: row.stream_id,
            last_version: row.last_version,
            format: row.format,
            payload: Some(row.payload),
        }
    }

    fn from_sharded(row: statement::fetch_commits_sharded::Row) -> Self {
        Self {
            commit_id: row.commit_id,
            stream_id: row.stream_id,
            last_version: row.last_version,
            format: row.format,
            payload: row.payload,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use proptest::prelude::*;

    use super::*;

    fn at(n: u64) -> position::Position {
        position::Position::new(n).unwrap()
    }

    #[test]
    fn cuts_at_the_lowest_full_page() {
        assert_eq!(
            cut(
                at(50),
                2,
                [(2, Some(at(9))), (2, Some(at(7))), (1, Some(at(3)))]
            ),
            at(7)
        );
        assert_eq!(cut(at(50), 2, [(1, Some(at(9))), (0, None)]), at(50));
        assert_eq!(cut(at(50), 2, []), at(50));
    }

    /// A log: one category per position (single-event commits keep the model small).
    fn log() -> impl Strategy<Value = Vec<u8>> {
        prop::collection::vec(0..4_u8, 0..60)
    }

    /// Simulates `Reader::read` over a log: each watched category returns its first `limit` positions in
    /// `(cursor, head]`, the cut is applied, and the rows at or below it are emitted.
    fn read(log: &[u8], watched: &[u8], cursor: u64, head: u64, limit: usize) -> (Vec<u64>, u64) {
        let pages: BTreeMap<u8, Vec<u64>> = watched
            .iter()
            .map(|c| {
                let rows = (cursor + 1..=head)
                    .filter(|p| log[*p as usize - 1] == *c)
                    .take(limit)
                    .collect();
                (*c, rows)
            })
            .collect();
        let cut = cut(
            at(head),
            limit,
            pages
                .values()
                .map(|rows| (rows.len(), rows.last().map(|p| at(*p)))),
        )
        .get();
        let mut emitted: Vec<u64> = pages
            .values()
            .flatten()
            .copied()
            .filter(|p| *p <= cut)
            .collect();
        emitted.sort_unstable();

        (emitted, cut)
    }

    proptest! {
        /// Reading page after page, while the log grows between pages, emits every watched position exactly
        /// once, in order, and the cursor never moves backwards or past `H`.
        #[test]
        fn pages_cover_every_watched_position_once_in_order(
            log in log(),
            watched in prop::collection::btree_set(0..4_u8, 1..4),
            limit in 1..5_usize,
            growth in prop::collection::vec(0..6_usize, 1..40),
        ) {
            let watched: Vec<u8> = watched.into_iter().collect();
            let mut cursor = 0_u64;
            let mut visible = 0_usize;
            let mut emitted = Vec::new();
            for grow in growth.iter().chain(std::iter::repeat_n(&usize::MAX, log.len() + 1)) {
                visible = visible.saturating_add(*grow).min(log.len());
                let head = visible as u64;
                if head <= cursor {
                    continue;
                }
                let (rows, cut) = read(&log, &watched, cursor, head, limit);
                prop_assert!(cut > cursor);
                prop_assert!(cut <= head);
                prop_assert!(rows.iter().all(|p| *p > cursor && *p <= cut));
                emitted.extend(rows);
                cursor = cut;
            }

            let expected: Vec<u64> = (1..=log.len() as u64)
                .filter(|p| watched.contains(&log[*p as usize - 1]))
                .collect();
            prop_assert_eq!(emitted, expected);
            prop_assert_eq!(cursor, log.len() as u64);
        }
    }
}
