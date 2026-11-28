//! Recovery after a halt of the logical source: a new slot, and log rows for the commits the old slot never
//! delivered, found in the snapshot the new slot exports.

use std::{
    collections::{BTreeMap, HashMap},
    future::Future,
    time::Duration,
};

use sqlx::{
    Connection, Executor, PgConnection, PgPool, Postgres, pool::PoolConnection,
    postgres::types::PgInterval,
};
use uuid::Uuid;

use pges_core::{category, commit, lsn, position, store};
use pges_postgres::{
    limit,
    sql::{template, typed::Query as _},
};
use pges_replication::{connection, error, name};

use crate::{lease, log, logical, sequencer, source, sql::statement};

/// Log rows written per statement.
const CHUNK: usize = 10_000;

pub(super) async fn run(
    source: &mut logical::LogicalSource,
    config: &logical::ResequenceConfig,
) -> Result<logical::Resequenced, logical::ResequenceError> {
    let ttl = sequencer::pg_interval(config.lease.ttl());
    let renew_every = config.lease.renew();

    let mut conn = source.pool.acquire().await?;
    check_writer_roles(&mut conn, &config.writer_roles).await?;
    let fence = match lease::acquire_halted(&mut conn, ttl).await? {
        lease::HaltedLease::Taken(fence) => fence,
        lease::HaltedLease::NotHalted => return Err(logical::ResequenceError::NotHalted),
        lease::HaltedLease::Held => return Err(logical::ResequenceError::LeaseHeld),
    };
    tracing::info!(%fence, "took the lease to resequence");

    let found = under_lease(
        &mut conn,
        fence,
        ttl,
        renew_every,
        source.replace_slot_and_scan(),
    )
    .await;
    let found = match found {
        Ok(found) => found,
        Err(logical::ResequenceError::LeaseLost) => {
            return Err(logical::ResequenceError::LeaseLost);
        }
        Err(error) => {
            release(source, &mut conn, fence).await;
            return Err(error);
        }
    };

    let written = write(&mut conn, fence, ttl, &found).await;
    match written {
        Ok(stores) => Ok(logical::Resequenced {
            slot: found.slot,
            consistent_point: found.consistent_point,
            stores,
        }),
        Err(Written::Unknown(error)) => Err(logical::ResequenceError::CommitUnknown(error.into())),
        // The slot may already be the one of a resequence that took the lease over.
        Err(Written::RolledBack(logical::ResequenceError::LeaseLost)) => {
            Err(logical::ResequenceError::LeaseLost)
        }
        Err(Written::RolledBack(error)) => {
            source.drop_new_slot(&found.slot).await;
            release(source, &mut conn, fence).await;
            Err(error)
        }
    }
}

/// The new slot, and the commits without a log row as of its consistent point.
struct Found {
    slot: name::SlotName,
    consistent_point: lsn::Lsn,
    stores: Vec<StoreOrphans>,
}

struct StoreOrphans {
    store: store::StoreId,
    schema: store::StoreName,
    /// In commit id order.
    commits: Vec<Orphan>,
}

struct Orphan {
    commit_id: commit::CommitId,
    category: category::CategoryId,
    event_count: commit::EventCount,
}

enum Written {
    /// Nothing was written.
    RolledBack(logical::ResequenceError),
    /// The commit failed: it may have been applied.
    Unknown(sqlx::Error),
}

impl logical::LogicalSource {
    /// Replaces the slot and finds the orphans in the snapshot exported at the new slot's consistent point.
    /// The new slot is dropped again if the search fails.
    async fn replace_slot_and_scan(&mut self) -> Result<Found, logical::ResequenceError> {
        let prepared = self.prepare().await?;
        if self
            .routes
            .values()
            .any(|route| *route == logical::Route::Skipped)
            && !self
                .routes
                .values()
                .any(|route| matches!(route, logical::Route::Store(_)))
        {
            return Err(source::SourceError::Invalid(
                "the database's stores use the outbox; there is no slot to resequence".to_owned(),
            )
            .into());
        }
        let slot = prepared.slot;
        let mut failover = connection::Failover::Disabled;
        if let Some(state) = &prepared.existing {
            if state.database.as_deref() != Some(prepared.database.name.as_str())
                || state.plugin.as_deref() != Some("pgoutput")
            {
                return Err(source::SourceError::Invalid(format!(
                    "replication slot {slot} belongs to database {:?} with plugin {:?}; it is not this database's slot",
                    state.database, state.plugin
                ))
                .into());
            }
            if state.activity == logical::SlotActivity::Active {
                return Err(logical::ResequenceError::SlotInUse {
                    slot: slot.to_string(),
                });
            }
            failover = state.failover;
        }
        if let logical::SlotFailover::Set(set) = self.config.failover {
            failover = set;
        }

        let mut replication = self.connect_checked(&prepared.database).await?;
        if prepared.existing.is_some() {
            tracing::warn!(%slot, "dropping the old replication slot");
            match replication
                .drop_slot(&slot, connection::DropWait::NoWait)
                .await
            {
                Ok(()) => {}
                Err(error::CommandError::Server(server))
                    if server.code == error::SqlState::UNDEFINED_OBJECT => {}
                Err(error::CommandError::Server(server))
                    if server.code == error::SqlState::OBJECT_IN_USE =>
                {
                    return Err(logical::ResequenceError::SlotInUse {
                        slot: slot.to_string(),
                    });
                }
                Err(error) => return Err(logical::transient(error).into()),
            }
        }
        let created = replication
            .create_slot_with(&slot, failover, connection::SlotSnapshot::Export)
            .await
            .map_err(logical::transient)?;
        tracing::info!(%slot, consistent_point = %created.consistent_point, "created a new replication slot");

        let scanned = match &created.snapshot {
            Some(snapshot) => scan(&self.pool, snapshot, &self.routes).await,
            None => Err(logical::transient_message("the new slot exported no snapshot").into()),
        };
        // The exported snapshot ends with the next command on this connection, so the drop comes after the
        // search.
        if scanned.is_err()
            && let Err(error) = replication
                .drop_slot(&slot, connection::DropWait::NoWait)
                .await
        {
            tracing::warn!(%slot, %error, "could not drop the new replication slot; resequence drops it when run again");
        }
        let _ = replication.close().await;

        Ok(Found {
            slot,
            consistent_point: created.consistent_point,
            stores: scanned?,
        })
    }

    /// Drops the slot created by a resequence that then failed, so a sequencer cannot stream from it.
    async fn drop_new_slot(&self, slot: &name::SlotName) {
        let dropped = async {
            let mut replication = logical::connect(&self.replication).await?;
            let dropped = replication
                .drop_slot(slot, connection::DropWait::NoWait)
                .await
                .map_err(logical::transient);
            let _ = replication.close().await;
            dropped
        }
        .await;
        if let Err(error) = dropped {
            tracing::warn!(%slot, %error, "could not drop the new replication slot; resequence drops it when run again");
        }
    }
}

/// Runs `work` while renewing the lease on `conn`. A lost lease abandons `work` with
/// [`logical::ResequenceError::LeaseLost`]: the process that took the lease over may be replacing the slot
/// itself, so this one must not touch the slot any more. A slot `work` left behind is dropped by the next
/// resequence.
async fn under_lease<T>(
    conn: &mut PgConnection,
    fence: sequencer::Fence,
    ttl: PgInterval,
    every: Duration,
    work: impl Future<Output = Result<T, logical::ResequenceError>>,
) -> Result<T, logical::ResequenceError> {
    tokio::pin!(work);
    let mut renewal = tokio::time::interval(every);
    renewal.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    renewal.tick().await;

    loop {
        tokio::select! {
            result = &mut work => return result,
            _ = renewal.tick() => {
                match lease::renew(conn, fence, ttl).await {
                    Ok(true) => {}
                    Ok(false) => {
                        tracing::warn!(%fence, "lost the lease while resequencing");
                        return Err(logical::ResequenceError::LeaseLost);
                    }
                    Err(error) => tracing::warn!(%error, "renewing the lease failed"),
                }
            }
        }
    }
}

/// Refuses unless every writer role caps its transactions at the shortest `max_txn_age` of the stores the
/// logical source sequences: the search window relies on it.
async fn check_writer_roles(
    conn: &mut PgConnection,
    writer_roles: &limit::WriterRoles,
) -> Result<(), logical::ResequenceError> {
    let shortest = statement::resequence_shortest_max_txn_age::Query::fixed()
        .bind(statement::resequence_shortest_max_txn_age::Params {})
        .fetch_one(&mut *conn)
        .await?
        .micros;
    let Some(shortest) = shortest else {
        return Ok(());
    };

    let max_txn_age = Duration::from_micros(u64::try_from(shortest).unwrap_or(0));
    limit::check_writer_roles(conn, writer_roles, max_txn_age)
        .await
        .map_err(logical::ResequenceError::WriterRole)
}

/// The orphans of every store the logical source sequences, read in the exported snapshot.
async fn scan(
    pool: &PgPool,
    snapshot: &name::SnapshotName,
    routes: &HashMap<String, logical::Route>,
) -> Result<Vec<StoreOrphans>, logical::ResequenceError> {
    let mut stores: BTreeMap<store::StoreId, store::StoreName> = BTreeMap::new();
    for (schema, route) in routes {
        if let logical::Route::Store(store) = route {
            stores.insert(*store, logical::store_name(schema)?);
        }
    }

    let mut conn = pool.acquire().await?;
    let mut tx = conn.begin().await?;
    logical::import_snapshot(&mut tx, snapshot).await?;

    let mut found = Vec::with_capacity(stores.len());
    for (store, schema) in stores {
        let commits = store_orphans(&mut tx, store, &schema).await?;
        found.push(StoreOrphans {
            store,
            schema,
            commits,
        });
    }
    tx.rollback().await?;

    Ok(found)
}

/// One store's commits without a log row, in commit id order. A commit that committed after the store's
/// newest sequenced commit has an id at most a window (the store's `max_txn_age + 2 × max_clock_skew`)
/// older than it, and every log row of such an id lies after the newest log row with an id one more window
/// older. Rows an earlier resequence wrote are in commit id order, not commit order, so they do not count as
/// that newest row.
async fn store_orphans(
    tx: &mut PgConnection,
    store: store::StoreId,
    schema: &store::StoreName,
) -> Result<Vec<Orphan>, logical::ResequenceError> {
    let values = template::Values::new().store(schema);
    let after_sql = statement::resequence_after::Query::render(&values);
    let orphans_sql = statement::resequence_orphans::Query::render(&values);

    let window = statement::resequence_window::Query::fixed()
        .bind(statement::resequence_window::Params {
            store_id: store.get(),
        })
        .fetch_one(&mut *tx)
        .await?
        .window;

    let bounds: Option<(Uuid, Uuid)> = statement::resequence_bounds::Query::render(&values)
        .bind(statement::resequence_bounds::Params { window })
        .fetch_optional(&mut *tx)
        .await?
        .map(|row| (row.floor, row.stop));

    let categories = statement::resequence_categories::Query::render(&values)
        .bind(statement::resequence_categories::Params {})
        .fetch_all(&mut *tx)
        .await?;

    let mut orphans = Vec::new();
    for statement::resequence_categories::Row {
        category_id: category,
        resequenced_from,
        resequenced_through,
    } in categories
    {
        let (floor, after) = match bounds {
            Some((floor, stop)) => {
                let after = after_sql
                    .bind(statement::resequence_after::Params {
                        category_id: category,
                        stop,
                        resequenced_from,
                        resequenced_through,
                    })
                    .fetch_optional(&mut *tx)
                    .await?
                    .map(|row| row.last_position);
                (floor, after.unwrap_or(0))
            }
            None => (Uuid::nil(), 0),
        };

        let rows = orphans_sql
            .bind(statement::resequence_orphans::Params {
                category_id: category,
                floor,
                after,
            })
            .fetch_all(&mut *tx)
            .await?;
        for row in rows {
            let statement::resequence_orphans::Row {
                commit_id: id,
                event_count: events,
            } = row;
            let commit_id = commit::CommitId::from_uuid(id).ok_or_else(|| {
                source::SourceError::Invalid(format!("{schema}.commits: invalid commit_id {id}"))
            })?;
            let event_count = log::event_count_from_sql(events).ok_or_else(|| {
                source::SourceError::Invalid(format!(
                    "{schema}.commits: commit {id} has {events} events"
                ))
            })?;
            orphans.push(Orphan {
                commit_id,
                category: category::CategoryId::new(category),
                event_count,
            });
        }
    }
    orphans.sort_by_key(|orphan| orphan.commit_id.as_uuid());

    Ok(orphans)
}

/// Writes the log rows, stores the consistent point as the token, clears `halted` and releases the lease, in
/// one transaction under the fence.
async fn write(
    conn: &mut PoolConnection<Postgres>,
    fence: sequencer::Fence,
    ttl: PgInterval,
    found: &Found,
) -> Result<Vec<logical::Recovered>, Written> {
    let rolled_back = |error: logical::ResequenceError| Written::RolledBack(error);

    // Renewals stop here: one after this transaction would take the lease again after it was released.
    match lease::renew(conn, fence, ttl).await {
        Ok(true) => {}
        Ok(false) => return Err(rolled_back(logical::ResequenceError::LeaseLost)),
        Err(error) => return Err(rolled_back(error.into())),
    }

    let mut tx = conn.begin().await.map_err(|e| rolled_back(e.into()))?;
    let held = statement::resequence_hold_fence::Query::fixed()
        .bind(statement::resequence_hold_fence::Params { fence: fence.sql() })
        .fetch_optional(&mut *tx)
        .await
        .map_err(|e| rolled_back(e.into()))?;
    if held.is_none() {
        return Err(rolled_back(logical::ResequenceError::LeaseLost));
    }

    let mut recovered = Vec::with_capacity(found.stores.len());
    for store in &found.stores {
        let last = insert(&mut tx, store).await.map_err(rolled_back)?;
        recovered.push(logical::Recovered {
            store: store.store,
            commits: store.commits.len(),
            last_position: last,
        });
    }

    tx.execute(statement::SYNCHRONOUS_COMMIT.sql())
        .await
        .map_err(|e| rolled_back(e.into()))?;
    let token = logical::encode_token(found.consistent_point);
    let done = statement::resequence_finish::Query::fixed()
        .bind(statement::resequence_finish::Params {
            fence: fence.sql(),
            source: logical::KIND.as_str(),
            token: &token,
        })
        .execute(&mut *tx)
        .await
        .map_err(|e| rolled_back(e.into()))?;
    if done.rows_affected() != 1 {
        return Err(rolled_back(logical::ResequenceError::LeaseLost));
    }
    tx.commit().await.map_err(Written::Unknown)?;

    for store in &recovered {
        tracing::info!(store = %store.store, commits = store.commits, last_position = %store.last_position, "resequenced");
    }
    Ok(recovered)
}

/// Writes one store's log rows after its newest position, and records the positions each category got, so
/// retention scans them whole. Returns the newest position afterwards.
async fn insert(
    tx: &mut PgConnection,
    store: &StoreOrphans,
) -> Result<position::Position, logical::ResequenceError> {
    let values = template::Values::new().store(&store.schema);
    let insert = statement::log_insert::Query::render(&values);
    let record_ranges = statement::resequence_record_ranges::Query::render(&values);

    let newest = statement::log_newest::Query::render(&values)
        .bind(statement::log_newest::Params {})
        .fetch_optional(&mut *tx)
        .await?
        .map(|row| row.last_position);
    // Never below the coarse mark, as a sequencer's term: `prune_log` can delete the newest rows.
    let marked = statement::log_store::Query::fixed()
        .bind(statement::log_store::Params {
            store_id: store.store.get(),
        })
        .fetch_optional(&mut *tx)
        .await?
        .map(|row| row.last_position);
    let mut position = newest
        .max(marked)
        .and_then(position::Position::from_i64)
        .unwrap_or(position::Position::BEGINNING);

    let mut ranges: BTreeMap<i32, (i64, i64)> = BTreeMap::new();
    for chunk in store.commits.chunks(CHUNK) {
        let mut last_positions = Vec::with_capacity(chunk.len());
        let mut commit_ids = Vec::with_capacity(chunk.len());
        let mut category_ids = Vec::with_capacity(chunk.len());
        let mut event_counts = Vec::with_capacity(chunk.len());
        for orphan in chunk {
            position = position
                .checked_add(u64::from(orphan.event_count.get()))
                .ok_or(logical::ResequenceError::PositionOverflow(store.store))?;
            let last = position.to_i64();
            last_positions.push(last);
            ranges
                .entry(orphan.category.get())
                .and_modify(|range| range.1 = last)
                .or_insert((last, last));
            commit_ids.push(orphan.commit_id.as_uuid());
            category_ids.push(orphan.category.get());
            event_counts.push(orphan.event_count.to_i16());
        }

        insert
            .bind(statement::log_insert::Params {
                last_positions: &last_positions,
                commit_ids: &commit_ids,
                category_ids: &category_ids,
                event_counts: &event_counts,
            })
            .execute(&mut *tx)
            .await?;
    }

    if !ranges.is_empty() {
        let categories: Vec<i32> = ranges.keys().copied().collect();
        let (firsts, lasts): (Vec<i64>, Vec<i64>) = ranges.values().copied().unzip();
        record_ranges
            .bind(statement::resequence_record_ranges::Params {
                category_ids: &categories,
                firsts: &firsts,
                lasts: &lasts,
            })
            .execute(&mut *tx)
            .await?;
    }

    Ok(position)
}

/// Gives the lease up after a failure, on `conn` or, if that fails, on a fresh connection.
async fn release(
    source: &logical::LogicalSource,
    conn: &mut PgConnection,
    fence: sequencer::Fence,
) {
    if lease::release(conn, fence).await.is_ok() {
        return;
    }
    let released = match source.pool.acquire().await {
        Ok(mut fresh) => lease::release(&mut fresh, fence).await,
        Err(error) => Err(error),
    };
    if let Err(error) = released {
        tracing::warn!(%error, "could not release the lease; it expires by itself");
    }
}
