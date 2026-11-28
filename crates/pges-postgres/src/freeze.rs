//! Freezing closed partitions, and how far each partition of a store is from transaction id wraparound.
//!
//! A time leaf stops receiving appends once its time span is past, and a log partition once the log has
//! filled it. Leaves and log partitions are created with `autovacuum_freeze_min_age = 0`, so autovacuum
//! usually freezes them while they are written. [`admin::Admin::freeze_closed_partitions`] freezes the rest
//! once, so anti-wraparound vacuum later finds them all frozen and costs milliseconds instead of a full read
//! and rewrite. [`admin::Admin::partition_ages`] reports what to alert on.

use std::time::Duration;

use sqlx::{Executor, PgConnection, SqlStr};

use crate::{
    admin, error, lock, retention,
    sql::{statement, template, typed::Query as _},
};

impl admin::Admin {
    /// Freezes the store's closed partitions that are not all frozen yet: time leaves in name order, then
    /// log partitions oldest first.
    ///
    /// Closed partitions are the time leaves no append can still write to (their upper bound more than the
    /// store's `max_txn_age` plus twice its `max_clock_skew` in the past, as for retention) and the full log
    /// partitions other than the newest. A partition is frozen with `VACUUM (FREEZE, ANALYZE)` only when its
    /// all-frozen pages, as the last vacuum recorded them, are fewer than its pages, or when it holds rows
    /// deleted since its last vacuum (by [`store::Store::delete_stream`](crate::store::Store::delete_stream));
    /// the vacuum then skips the pages already frozen. A partition that is all frozen costs nothing.
    ///
    /// Freezing a partition autovacuum missed reads it whole and writes about half its size in WAL, so run
    /// this off-peak, on its own schedule. The maintenance lock is taken for one partition at a time, so
    /// [`admin::Admin::maintain`] and [`admin::Admin::apply_retention`] wait for one partition at most, not
    /// for the whole run. A partition whose lock stays taken (by an anti-wraparound autovacuum) is reported
    /// in [`FreezeReport::busy`] and tried again on the next run.
    ///
    /// Fails with [`FreezeError::StoreNotFound`] if the store does not exist.
    pub async fn freeze_closed_partitions(&self) -> Result<FreezeReport, FreezeError> {
        let mut conn = self.session().await?;
        let candidates = closed_unfrozen(&mut conn, self).await;
        self.end_session(conn).await;

        let mut report = FreezeReport::default();
        for candidate in candidates? {
            let mut conn = self.session().await?;
            let vacuumed = freeze(&mut conn, self, &candidate).await;
            self.end_session(conn).await;

            let partition = retention::PartitionName::of(&candidate.name);
            match vacuumed? {
                Vacuum::Done => report.frozen.push(FrozenPartition {
                    partition,
                    pages: candidate.pages,
                    unfrozen_pages: candidate.unfrozen_pages,
                    dead_rows: candidate.dead_rows,
                }),
                Vacuum::Busy => report.busy.push(partition),
                Vacuum::Gone => {}
            }
        }

        Ok(report)
    }

    /// Reads, for every partition of the store (time leaves, log partitions, and the partitions of
    /// `streams` and `snapshots`), its pages, how many of them are all frozen, and the age of its oldest
    /// unfrozen transaction id and multixact id, in name order.
    ///
    /// Alert well before an age reaches `autovacuum_freeze_max_age` (200 million by default) on a closed
    /// partition that is not all frozen: anti-wraparound vacuum would then read and rewrite it.
    ///
    /// It reads the catalog only and does not take the maintenance lock, so it answers while maintenance,
    /// retention or a freeze runs. A store that does not exist has no partitions.
    pub async fn partition_ages(&self) -> Result<Vec<PartitionAge>, PartitionAgesError> {
        let mut conn = self.pool().acquire().await?;

        Ok(partition_ages(&mut conn, self.store()).await?)
    }
}

/// What one [`admin::Admin::freeze_closed_partitions`] run did.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
#[non_exhaustive]
pub struct FreezeReport {
    /// The closed partitions frozen, in the order they were frozen.
    pub frozen: Vec<FrozenPartition>,
    /// Closed partitions that needed freezing but whose lock another vacuum kept; the next run tries again.
    pub busy: Vec<retention::PartitionName>,
}

/// A closed partition [`admin::Admin::freeze_closed_partitions`] froze.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct FrozenPartition {
    /// The partition's table name.
    pub partition: retention::PartitionName,
    /// Its pages, its TOAST table's included, before the run.
    pub pages: u64,
    /// The pages that were not recorded as all frozen before the run.
    pub unfrozen_pages: u64,
    /// The rows deleted or updated since its last vacuum, before the run.
    pub dead_rows: u64,
}

/// How far one partition is from transaction id wraparound.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct PartitionAge {
    /// The partition's table name.
    pub partition: retention::PartitionName,
    /// Its pages, its TOAST table's included.
    pub pages: u64,
    /// The pages the last vacuum or analyze recorded as all frozen.
    pub frozen_pages: u64,
    /// Rows deleted or updated that vacuum has not removed yet, from the cumulative statistics (reset by a
    /// crash). Their pages are no longer all frozen, whatever [`PartitionAge::frozen_pages`] says.
    pub dead_rows: u64,
    /// Transactions since the oldest transaction id that may be unfrozen in it (`age(relfrozenxid)`).
    pub xid_age: u32,
    /// Multixacts since the oldest multixact id that may remain in it (`mxid_age(relminmxid)`).
    pub multixact_age: u32,
}

/// Closed partitions could not be frozen. Partitions frozen before the error stay frozen.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum FreezeError {
    /// No store with this name exists.
    #[error("store {0} does not exist")]
    StoreNotFound(pges_core::store::StoreName),
    /// A database error.
    #[error(transparent)]
    Database(#[from] error::DbError),
    /// The store's maintenance lock stayed taken by another session for longer than the lock wait
    /// ([`admin::Admin::with_lock_wait`]). Partitions frozen before stay frozen; the next run continues.
    #[error(transparent)]
    LockTimeout(lock::LockTimeout),
}

/// The partitions' ages could not be read.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum PartitionAgesError {
    /// A database error.
    #[error(transparent)]
    Database(#[from] error::DbError),
}

impl From<sqlx::Error> for FreezeError {
    fn from(source: sqlx::Error) -> Self {
        Self::Database(source.into())
    }
}

impl From<sqlx::Error> for PartitionAgesError {
    fn from(source: sqlx::Error) -> Self {
        Self::Database(source.into())
    }
}

/// A closed partition and how much of it is not frozen.
struct Candidate {
    name: template::Ident,
    pages: u64,
    unfrozen_pages: u64,
    dead_rows: u64,
}

enum Vacuum {
    Done,
    Busy,
    Gone,
}

/// The store's closed partitions that are not all frozen: leaves in name order, then log partitions oldest
/// first.
async fn closed_unfrozen(
    conn: &mut PgConnection,
    admin: &admin::Admin,
) -> Result<Vec<Candidate>, FreezeError> {
    let store = admin.store_name();
    let limits = statement::writer_limits::Query::fixed()
        .bind(statement::writer_limits::Params {
            store: store.as_str(),
        })
        .fetch_optional(&mut *conn)
        .await?;
    let Some(limits) = limits else {
        return Err(FreezeError::StoreNotFound(store.clone()));
    };

    let mut closed = statement::closed_leaves::Query::fixed()
        .bind(statement::closed_leaves::Params {
            commits: &format!("{store}.commits"),
            max_txn_age: limits.max_txn_age,
            max_clock_skew: limits.max_clock_skew,
        })
        .fetch_all(&mut *conn)
        .await?
        .into_iter()
        .map(|row| row.leaf)
        .collect::<Vec<_>>();
    closed.extend(retention::full_log_partitions(conn, admin).await?);

    let ages = partition_ages(conn, store.as_str()).await?;

    Ok(closed
        .into_iter()
        .filter_map(|name| {
            let age = ages.iter().find(|age| age.partition == name.as_str())?;
            let unfrozen_pages = age.pages.saturating_sub(age.frozen_pages);
            (unfrozen_pages > 0 || age.dead_rows > 0).then_some(Candidate {
                name: template::Ident::new(name)?,
                pages: age.pages,
                unfrozen_pages,
                dead_rows: age.dead_rows,
            })
        })
        .collect())
}

/// Freezes one partition, unless retention dropped it since it was listed.
async fn freeze(
    conn: &mut PgConnection,
    admin: &admin::Admin,
    candidate: &Candidate,
) -> Result<Vacuum, sqlx::Error> {
    let store = admin.store_name();
    if !admin::relation_exists(conn, store.as_str(), candidate.name.as_str()).await? {
        return Ok(Vacuum::Gone);
    }

    let values = template::Values::new()
        .store(store)
        .ident(template::IdentPlaceholder::Table, &candidate.name);
    vacuum(conn, statement::FREEZE_PARTITION.render_text(&values)).await
}

async fn partition_ages(
    conn: &mut PgConnection,
    store: &str,
) -> Result<Vec<PartitionAge>, sqlx::Error> {
    let rows = statement::partition_ages::Query::fixed()
        .bind(statement::partition_ages::Params { store })
        .fetch_all(&mut *conn)
        .await?;

    Ok(rows
        .into_iter()
        .map(|row| PartitionAge {
            partition: retention::PartitionName::new(row.relname),
            pages: count(row.pages),
            frozen_pages: count(row.frozen_pages),
            dead_rows: count(row.dead_rows),
            xid_age: age(row.xid_age),
            multixact_age: age(row.mxid_age),
        })
        .collect())
}

/// Runs a vacuum, retrying with backoff while its lock is taken. VACUUM waits for a running autovacuum of
/// the table, which is cancelled for it unless it is an anti-wraparound one.
async fn vacuum(conn: &mut PgConnection, statement: SqlStr) -> Result<Vacuum, sqlx::Error> {
    let mut backoff = Duration::from_millis(50);
    let mut attempt = 1;

    loop {
        match conn.execute(statement.clone()).await {
            Ok(_) => return Ok(Vacuum::Done),
            Err(e) if error::sqlstate(&e).as_deref() == Some(admin::LOCK_NOT_AVAILABLE) => {
                if attempt == admin::DDL_ATTEMPTS {
                    return Ok(Vacuum::Busy);
                }
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(Duration::from_secs(2));
                attempt += 1;
            }
            Err(e) => return Err(e),
        }
    }
}

fn count(value: Option<i64>) -> u64 {
    value.and_then(|v| u64::try_from(v).ok()).unwrap_or(0)
}

fn age(value: Option<i32>) -> u32 {
    value.and_then(|v| u32::try_from(v).ok()).unwrap_or(0)
}
