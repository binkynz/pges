//! Reading a subscription's log pages from a streaming replica
//! ([`Builder::replica`](crate::subscription::Builder::replica)), and the cap on subscriptions that fall back
//! to the primary while a replica lags ([`FallbackLimit`]).
//!
//! Before each read from the replica, the subscription waits until the replica has replayed the WAL
//! position (LSN) that the waker reported with its high-water mark: the sequencer's WAL insert position after
//! the batch committed, so the replica then shows that batch's log rows and every commit they name. That LSN
//! is a hint: a `NOTIFY` can be sent by any role, so when the replica has not replayed it at the first check,
//! the subscription lowers it to the primary's current WAL insert position (one short statement) and never
//! waits past that. A forged LSN therefore neither stalls the subscription nor sends it to the fallback.
//!
//! A waker that carries no LSN (a poll waker, or no waker at all) makes the subscription read the primary's current
//! WAL insert position instead, one short statement on the store's pool. Checkpoints, leases and `run_pg`
//! transactions stay on the primary.
//!
//! Without a [fallback](Replica::fallback), the subscription waits for the replica however long it lags.
//! With one, once the replica has lagged longer than the given time, pages are read from the primary
//! instead, one page at a time, while a permit of the fallback's [`FallbackLimit`] is free; otherwise the
//! subscription keeps waiting for the replica. Share one limit between the subscriptions of a process, so a
//! lagging replica cannot send every subscription to the primary at once.

use std::{future::Future, sync::Arc, time::Duration};

use sqlx::PgPool;
use tokio::{
    sync::{OwnedSemaphorePermit, Semaphore},
    time::Instant,
};

use pges_core::{interval, lsn};
use pges_postgres::sql::typed::Query as _;

use crate::{sql, subscription};

/// A streaming replica of the store's database that a subscription reads its log pages from, and what it
/// does while the replica lags. A pool alone converts into a replica without a fallback.
///
/// ```no_run
/// # fn example(replica_pool: sqlx::PgPool) {
/// use pges_subscribe::replica;
///
/// // Shared by every subscription of the process.
/// let limit = replica::FallbackLimit::new(4);
/// let replica = replica::Replica::new(replica_pool).fallback(pges_core::interval!(2 s), limit.clone());
/// # }
/// ```
#[derive(Debug, Clone)]
pub struct Replica {
    pool: PgPool,
    fallback: Option<Fallback>,
}

impl Replica {
    /// Reads from `pool`, waiting for it however long it lags.
    pub fn new(pool: PgPool) -> Self {
        Self {
            pool,
            fallback: None,
        }
    }

    /// Reads from the primary, one page at a time, once the replica has lagged longer than `after`, while
    /// `limit` has a free permit. Errors reading the replica's position then count as lag.
    pub fn fallback(mut self, after: interval::Interval, limit: FallbackLimit) -> Self {
        self.fallback = Some(Fallback {
            after: after.get(),
            limit,
        });
        self
    }
}

impl From<PgPool> for Replica {
    fn from(pool: PgPool) -> Self {
        Self::new(pool)
    }
}

/// How many subscriptions may read from the primary at once because their replica lags. Clones share the
/// same permits, so give every subscription of a process a clone of one limit.
///
/// A permit is held only while one page is read from the primary, not while the handler runs.
#[derive(Debug, Clone)]
pub struct FallbackLimit {
    permits: Arc<Semaphore>,
}

impl FallbackLimit {
    /// A limit of `concurrent` reads from the primary at once. 0 never falls back.
    pub fn new(concurrent: usize) -> Self {
        Self {
            permits: Arc::new(Semaphore::new(concurrent.min(Semaphore::MAX_PERMITS))),
        }
    }
}

/// The first pause between checks of the replica's replay position; it doubles up to [`MAX_POLL`].
const FIRST_POLL: Duration = Duration::from_millis(2);
const MAX_POLL: Duration = Duration::from_millis(100);

/// Where the next page comes from.
pub(crate) enum Ready {
    /// The replica has replayed the target LSN.
    Replica,
    /// The replica lags past the fallback time: read one page from the primary, holding the permit.
    Primary(OwnedSemaphorePermit),
    /// Shutdown came while waiting.
    Shutdown,
}

/// Reads of one term loop from a [`Replica`]: its pool and fallback, and the WAL position its next read
/// needs.
pub(crate) struct Reader {
    pool: PgPool,
    fallback: Option<Fallback>,
    target: Target,
    /// When the subscription started waiting for a target the replica has not reached.
    lagging_since: Option<Instant>,
    /// Read from the replica on the first wait.
    layout: Option<WalLayout>,
}

#[derive(Debug, Clone)]
struct Fallback {
    after: Duration,
    limit: FallbackLimit,
}

/// The WAL position the replica must reach before the next read.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Target {
    /// Ask the primary for its WAL insert position.
    Primary,
    /// A waker's LSN. It is a claim (any role can send a `NOTIFY`), so a replica that has not replayed it
    /// waits only up to the primary's insert position, never past it.
    Hint(lsn::Lsn),
    /// A position the primary reported.
    At(lsn::Lsn),
    /// The replica has replayed everything the subscription was told about.
    Reached,
}

impl Reader {
    pub(crate) fn new(replica: Replica) -> Self {
        Self {
            pool: replica.pool,
            fallback: replica.fallback,
            target: Target::Primary,
            lagging_since: None,
            layout: None,
        }
    }

    pub(crate) fn pool(&self) -> &PgPool {
        &self.pool
    }

    /// The waker fired with `lsn`. Without one, the next read first asks the primary for its position.
    pub(crate) fn woken(&mut self, lsn: Option<lsn::Lsn>) {
        self.target = match (lsn, self.target) {
            (Some(lsn), Target::At(earlier)) if lsn <= earlier => Target::At(earlier),
            (Some(lsn), Target::At(earlier) | Target::Hint(earlier)) => {
                Target::Hint(lsn.max(earlier))
            }
            (Some(lsn), Target::Primary | Target::Reached) => Target::Hint(lsn),
            (None, _) => Target::Primary,
        };
    }

    /// After an error, the next read asks the primary for its position again, so a replica that went away
    /// counts as lagging.
    pub(crate) fn reset(&mut self) {
        self.target = Target::Primary;
    }

    /// Waits until the replica has replayed the target, or until the fallback may read from the primary.
    ///
    /// Errors reading the replica's position count as lag when a fallback is configured; without one they are
    /// returned. Errors from the primary are always returned.
    pub(crate) async fn ready(
        &mut self,
        primary: &PgPool,
        shutdown: &mut (impl Future<Output = ()> + Unpin),
    ) -> Result<Ready, subscription::SubscribeError> {
        let mut target = match self.target {
            Target::Reached => return Ok(Ready::Replica),
            Target::At(lsn) | Target::Hint(lsn) => lsn,
            Target::Primary => {
                let lsn = insert_lsn(primary).await?;
                self.target = Target::At(lsn);
                lsn
            }
        };
        let lagging_since = *self.lagging_since.get_or_insert_with(Instant::now);

        let mut pause = FIRST_POLL;
        loop {
            match self.replayed().await {
                Ok((Replayed::Primary, _)) => return Ok(self.reached()),
                Ok((Replayed::At(replayed), layout)) if replayed >= layout.record_end(target) => {
                    return Ok(self.reached());
                }
                // Checked only once the replica is behind it, so a real notice costs no round trip to the
                // primary when the replica has kept up.
                Ok((Replayed::At(_), _)) if matches!(self.target, Target::Hint(_)) => {
                    target = target.min(insert_lsn(primary).await?);
                    self.target = Target::At(target);
                    continue;
                }
                Ok((Replayed::At(_), _)) => {}
                Err(_) if self.fallback.is_some() => {}
                Err(error) => return Err(error.into()),
            }

            let mut wait = pause;
            if let Some(fallback) = &self.fallback {
                let due = lagging_since + fallback.after;
                if Instant::now() >= due {
                    if let Ok(permit) = fallback.limit.permits.clone().try_acquire_owned() {
                        return Ok(Ready::Primary(permit));
                    }
                } else {
                    wait = wait.min(due - Instant::now());
                }
            }

            tokio::select! {
                () = tokio::time::sleep(wait) => {}
                () = &mut *shutdown => return Ok(Ready::Shutdown),
            }
            pause = (pause * 2).min(MAX_POLL);
        }
    }

    fn reached(&mut self) -> Ready {
        self.target = Target::Reached;
        self.lagging_since = None;

        Ready::Replica
    }

    async fn replayed(&mut self) -> Result<(Replayed, WalLayout), sqlx::Error> {
        let layout = match self.layout {
            Some(layout) => layout,
            None => {
                let row = sql::statement::wal_layout::Query::fixed()
                    .bind(sql::statement::wal_layout::Params {})
                    .fetch_one(&self.pool)
                    .await?;
                let layout = WalLayout {
                    block: u64::try_from(row.block).unwrap_or(0),
                    segment: u64::try_from(row.segment).unwrap_or(0),
                };
                *self.layout.insert(layout)
            }
        };
        let row = sql::statement::replay_lsn::Query::fixed()
            .bind(sql::statement::replay_lsn::Params {})
            .fetch_one(&self.pool)
            .await?;

        let replayed = match (row.in_recovery, row.offset) {
            (false, _) => Replayed::Primary,
            (true, offset) => Replayed::At(lsn::Lsn::new(
                offset
                    .and_then(|offset| u64::try_from(offset).ok())
                    .unwrap_or(0),
            )),
        };

        Ok((replayed, layout))
    }
}

/// The primary's WAL insert position.
async fn insert_lsn(primary: &PgPool) -> Result<lsn::Lsn, sqlx::Error> {
    let offset = sql::statement::insert_lsn::Query::fixed()
        .bind(sql::statement::insert_lsn::Params {})
        .fetch_one(primary)
        .await?
        .offset;

    Ok(lsn::Lsn::new(u64::try_from(offset).unwrap_or(0)))
}

/// The WAL page and segment sizes of the replica's cluster (`wal_block_size`, `wal_segment_size`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct WalLayout {
    block: u64,
    segment: u64,
}

impl WalLayout {
    /// A target to compare with `pg_last_wal_replay_lsn()`, which is the end of the last replayed record.
    ///
    /// `pg_current_wal_insert_lsn()` is where the next record starts: right after the page header when the
    /// last record ended at a page boundary. A replica that replayed that record stops at the boundary, so
    /// on an idle server it would never reach the insert position. No record ends inside a page header, so
    /// lowering such a target to the page start waits for exactly the same records.
    fn record_end(self, target: lsn::Lsn) -> lsn::Lsn {
        let lsn = target.get();
        if self.block == 0 || self.segment == 0 {
            return target;
        }

        let header = if lsn % self.segment < self.block {
            LONG_PAGE_HEADER
        } else {
            SHORT_PAGE_HEADER
        };
        if lsn % self.block != header {
            return target;
        }

        lsn::Lsn::new(lsn - header)
    }
}

/// `SizeOfXLogShortPHD`: the header of a WAL page, aligned.
const SHORT_PAGE_HEADER: u64 = 24;
/// `SizeOfXLogLongPHD`: the header of the first page of a WAL segment, aligned.
const LONG_PAGE_HEADER: u64 = 40;

/// How far a replica pool has replayed.
enum Replayed {
    /// The pool's server is not in recovery (a primary, or a promoted replica): it is never behind itself.
    Primary,
    At(lsn::Lsn),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_insert_position_after_a_page_header_is_reached_at_the_page_start() {
        let layout = WalLayout {
            block: 8192,
            segment: 16 * 1024 * 1024,
        };
        let at = |lsn: u64| layout.record_end(lsn::Lsn::new(lsn)).get();

        assert_eq!(at(5 * 8192 + 24), 5 * 8192);
        assert_eq!(at(3 * 16 * 1024 * 1024 + 40), 3 * 16 * 1024 * 1024);
        assert_eq!(
            at(3 * 16 * 1024 * 1024 + 24),
            3 * 16 * 1024 * 1024 + 24,
            "a segment's first page has the long header"
        );
        assert_eq!(at(5 * 8192 + 40), 5 * 8192 + 40, "a record can end there");
        assert_eq!(at(5 * 8192 + 1000), 5 * 8192 + 1000);
        assert_eq!(at(5 * 8192), 5 * 8192);
    }

    #[test]
    fn a_zero_limit_has_no_permits() {
        let limit = FallbackLimit::new(0);

        assert!(limit.permits.clone().try_acquire_owned().is_err());
    }

    #[test]
    fn clones_share_permits() {
        let limit = FallbackLimit::new(1);
        let clone = limit.clone();

        let held = limit.permits.clone().try_acquire_owned().unwrap();
        assert!(clone.permits.clone().try_acquire_owned().is_err());
        drop(held);
        assert!(clone.permits.clone().try_acquire_owned().is_ok());
    }
}
