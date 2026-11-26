//! Each store's log as the leader sees it: the next positions, the coarse high-water marks, and the ids of
//! recently sequenced commits for deduplication.

use std::collections::{BTreeMap, BTreeSet, HashSet, VecDeque};

use sqlx::PgConnection;
use uuid::Uuid;

use pges_core::{category, commit, high_water, lsn, position, store};
use pges_postgres::sql::{
    template,
    typed::{self, Query as _},
};

use crate::{sequencer, source, sql::statement};

/// Every store of the database, loaded when a leadership term starts.
pub(crate) struct Logs {
    stores: BTreeMap<store::StoreId, StoreLog>,
    dedupe_tail: usize,
}

impl Logs {
    /// Reads every store's newest positions from its log, and seeds the dedupe window.
    pub(crate) async fn load(
        conn: &mut PgConnection,
        dedupe_tail: usize,
    ) -> Result<Self, sequencer::RunError> {
        let rows = statement::log_stores::Query::fixed()
            .bind(statement::log_stores::Params {})
            .fetch_all(&mut *conn)
            .await?;

        let mut logs = Self {
            stores: BTreeMap::new(),
            dedupe_tail,
        };
        for row in rows {
            let id = store::StoreId::new(row.store_id);
            logs.add(conn, id, row.schema_name, row.last_position)
                .await?;
        }

        Ok(logs)
    }

    /// The stores whose log this term has seen.
    pub(crate) fn store_ids(&self) -> Vec<store::StoreId> {
        self.stores.keys().copied().collect()
    }

    /// Loads stores created after the term started, if `commits` name any.
    pub(crate) async fn load_missing(
        &mut self,
        conn: &mut PgConnection,
        commits: &[source::CommitRef],
    ) -> Result<(), sequencer::RunError> {
        let missing: BTreeSet<store::StoreId> = commits
            .iter()
            .map(|commit| commit.store)
            .filter(|id| !self.stores.contains_key(id))
            .collect();

        for id in missing {
            let row = statement::log_store::Query::fixed()
                .bind(statement::log_store::Params { store_id: id.get() })
                .fetch_optional(&mut *conn)
                .await?;
            let Some(row) = row else {
                return Err(sequencer::RunError::UnknownStore(id));
            };
            self.add(conn, id, row.schema_name, row.last_position)
                .await?;
        }

        Ok(())
    }

    /// Assigns positions to `commits` in order, skipping commits already sequenced, and writes their log
    /// rows. The positions take effect in memory at once: if the transaction does not commit, the caller
    /// discards this `Logs` and loads it again. Returns the stores that got rows, with the categories that
    /// did.
    pub(crate) async fn write(
        &mut self,
        tx: &mut PgConnection,
        commits: &[source::CommitRef],
    ) -> Result<Touched, sequencer::RunError> {
        let mut rows: BTreeMap<store::StoreId, Rows> = BTreeMap::new();

        for commit in commits {
            let Some(log) = self.stores.get_mut(&commit.store) else {
                return Err(sequencer::RunError::UnknownStore(commit.store));
            };
            if !log.window.insert(commit.commit_id) {
                continue;
            }

            let last = log
                .position
                .checked_add(u64::from(commit.event_count.get()))
                .ok_or(sequencer::RunError::PositionOverflow(commit.store))?;
            log.position = last;
            log.categories.insert(commit.category, last);
            log.dirty_categories.insert(commit.category);

            let batch = rows.entry(commit.store).or_default();
            batch.categories.insert(commit.category);
            batch.last_positions.push(last.to_i64());
            batch.commit_ids.push(commit.commit_id.as_uuid());
            batch.category_ids.push(commit.category.get());
            batch.event_counts.push(commit.event_count.to_i16());
        }

        for (id, batch) in &rows {
            let Some(log) = self.stores.get(id) else {
                continue;
            };
            log.sql
                .insert
                .bind(statement::log_insert::Params {
                    last_positions: &batch.last_positions,
                    commit_ids: &batch.commit_ids,
                    category_ids: &batch.category_ids,
                    event_counts: &batch.event_counts,
                })
                .execute(&mut *tx)
                .await?;
        }

        Ok(rows
            .into_iter()
            .map(|(id, batch)| (id, batch.categories))
            .collect())
    }

    /// Whether any high-water mark moved since the last [`Logs::persist`].
    pub(crate) fn is_dirty(&self) -> bool {
        self.stores
            .values()
            .any(|log| log.position != log.persisted || !log.dirty_categories.is_empty())
    }

    /// Writes the coarse high-water marks of the stores that moved, in the caller's fenced transaction.
    pub(crate) async fn persist(&self, tx: &mut PgConnection) -> Result<(), sqlx::Error> {
        for (id, log) in &self.stores {
            if !log.dirty_categories.is_empty() {
                let (ids, positions): (Vec<i32>, Vec<i64>) = log
                    .dirty_categories
                    .iter()
                    .map(|category| {
                        let at = log
                            .categories
                            .get(category)
                            .copied()
                            .unwrap_or(position::Position::BEGINNING);
                        (category.get(), at.to_i64())
                    })
                    .unzip();
                log.sql
                    .categories
                    .bind(statement::log_set_category_positions::Params {
                        category_ids: &ids,
                        positions: &positions,
                    })
                    .execute(&mut *tx)
                    .await?;
            }
            if log.position != log.persisted {
                statement::log_set_store_position::Query::fixed()
                    .bind(statement::log_set_store_position::Params {
                        last_position: log.position.to_i64(),
                        store_id: id.get(),
                    })
                    .execute(&mut *tx)
                    .await?;
            }
        }

        Ok(())
    }

    /// Records that the transaction holding [`Logs::persist`] committed. Replays can now only start after
    /// the persisted token, so the dedupe window shrinks to its tail.
    pub(crate) fn persisted(&mut self) {
        for log in self.stores.values_mut() {
            log.persisted = log.position;
            log.dirty_categories.clear();
            log.window.trim();
        }
    }

    /// The store's schema name.
    #[cfg(feature = "watch-notify")]
    pub(crate) fn schema(&self, id: store::StoreId) -> Option<&str> {
        self.stores.get(&id).map(|log| log.schema.as_str())
    }

    /// The store's high-water mark.
    pub(crate) fn high_water(
        &self,
        id: store::StoreId,
        lsn: Option<lsn::Lsn>,
    ) -> Option<high_water::HighWater> {
        let log = self.stores.get(&id)?;

        Some(high_water::HighWater::new(
            log.position,
            log.categories
                .iter()
                .map(|(category, at)| (*category, *at))
                .collect(),
            lsn,
        ))
    }

    async fn add(
        &mut self,
        conn: &mut PgConnection,
        id: store::StoreId,
        schema: String,
        persisted: i64,
    ) -> Result<(), sequencer::RunError> {
        let Ok(name) = store::StoreName::parse(schema.as_str()) else {
            return Err(sequencer::RunError::InvalidStoreName(schema));
        };
        let sql = StoreSql::new(&name);

        let newest = sql
            .newest
            .bind(statement::log_newest::Params {})
            .fetch_optional(&mut *conn)
            .await?
            .map(|row| row.last_position);
        let mut categories = BTreeMap::new();
        for row in sql
            .category_newest
            .bind(statement::log_category_newest::Params {})
            .fetch_all(&mut *conn)
            .await?
        {
            let category = category::CategoryId::new(row.category_id);
            categories.insert(category, to_position(row.last_position));
        }

        // Replays after the durable token can only cover log rows above the coarse store position, plus
        // whatever a source replays beyond it (the tail).
        let mut window = Window::new(self.dedupe_tail);
        let mut recent: Vec<Uuid> = Vec::new();
        if self.dedupe_tail > 0 {
            let tail = i64::try_from(self.dedupe_tail).unwrap_or(i64::MAX);
            let mut rows: Vec<Uuid> = sql
                .tail
                .bind(statement::log_tail::Params { limit: tail })
                .fetch_all(&mut *conn)
                .await?
                .into_iter()
                .map(|row| row.commit_id)
                .collect();
            rows.reverse();
            recent.extend(rows);
        }
        for row in sql
            .above
            .bind(statement::log_above::Params { after: persisted })
            .fetch_all(&mut *conn)
            .await?
        {
            recent.push(row.commit_id);
        }
        for id_in_log in recent {
            if let Some(commit_id) = commit::CommitId::from_uuid(id_in_log) {
                window.insert(commit_id);
            }
        }

        let position = newest
            .map(to_position)
            .unwrap_or(position::Position::BEGINNING);
        self.stores.insert(
            id,
            StoreLog {
                schema,
                sql,
                position,
                persisted: to_position(persisted).min(position),
                categories,
                dirty_categories: BTreeSet::new(),
                window,
            },
        );

        Ok(())
    }
}

/// The stores a batch wrote to, with the categories it touched in each.
pub(crate) type Touched = BTreeMap<store::StoreId, BTreeSet<category::CategoryId>>;

struct StoreLog {
    #[cfg_attr(not(feature = "watch-notify"), allow(dead_code))]
    schema: String,
    sql: StoreSql,
    /// The newest position written.
    position: position::Position,
    /// `pges.stores.last_position` as this term last wrote or read it.
    persisted: position::Position,
    categories: BTreeMap<category::CategoryId, position::Position>,
    /// Categories whose `categories.last_position` is behind.
    dirty_categories: BTreeSet<category::CategoryId>,
    window: Window,
}

struct StoreSql {
    insert: typed::Rendered<statement::log_insert::Query>,
    categories: typed::Rendered<statement::log_set_category_positions::Query>,
    newest: typed::Rendered<statement::log_newest::Query>,
    category_newest: typed::Rendered<statement::log_category_newest::Query>,
    tail: typed::Rendered<statement::log_tail::Query>,
    above: typed::Rendered<statement::log_above::Query>,
}

impl StoreSql {
    fn new(store: &store::StoreName) -> Self {
        let values = template::Values::new().store(store);

        Self {
            insert: statement::log_insert::Query::render(&values),
            categories: statement::log_set_category_positions::Query::render(&values),
            newest: statement::log_newest::Query::render(&values),
            category_newest: statement::log_category_newest::Query::render(&values),
            tail: statement::log_tail::Query::render(&values),
            above: statement::log_above::Query::render(&values),
        }
    }
}

#[derive(Default)]
struct Rows {
    categories: BTreeSet<category::CategoryId>,
    last_positions: Vec<i64>,
    commit_ids: Vec<Uuid>,
    category_ids: Vec<i32>,
    event_counts: Vec<i16>,
}

/// Ids of a store's commits sequenced since the last durable token, plus the `tail` before it.
struct Window {
    order: VecDeque<commit::CommitId>,
    ids: HashSet<commit::CommitId>,
    tail: usize,
}

impl Window {
    fn new(tail: usize) -> Self {
        Self {
            order: VecDeque::new(),
            ids: HashSet::new(),
            tail,
        }
    }

    /// `false` if the commit is already in the window.
    fn insert(&mut self, commit_id: commit::CommitId) -> bool {
        if !self.ids.insert(commit_id) {
            return false;
        }

        self.order.push_back(commit_id);
        true
    }

    fn trim(&mut self) {
        while self.order.len() > self.tail {
            if let Some(oldest) = self.order.pop_front() {
                self.ids.remove(&oldest);
            }
        }
    }
}

fn to_position(position: i64) -> position::Position {
    position::Position::from_i64(position).unwrap_or(position::Position::BEGINNING)
}

/// An `int2` event count read from a source's table.
pub(crate) fn event_count_from_sql(count: i16) -> Option<commit::EventCount> {
    u16::try_from(count).ok().and_then(commit::EventCount::new)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn id() -> commit::CommitId {
        commit::CommitId::from_uuid(Uuid::now_v7()).unwrap()
    }

    #[test]
    fn the_window_drops_repeats_and_keeps_its_tail_after_a_trim() {
        let mut window = Window::new(2);
        let ids: Vec<_> = (0..4).map(|_| id()).collect();
        for commit_id in &ids {
            assert!(window.insert(*commit_id));
        }
        assert!(!window.insert(ids[0]));

        window.trim();
        assert!(window.insert(ids[0]));
        assert!(window.insert(ids[1]));
        assert!(!window.insert(ids[2]));
        assert!(!window.insert(ids[3]));
    }
}
