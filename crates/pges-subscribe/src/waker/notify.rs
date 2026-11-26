//! Waking from the sequencer's `NOTIFY pges_<store>`: no extra endpoint, one listening connection per
//! waker.

use std::{collections::BTreeMap, time::Duration};

use sqlx::{PgPool, postgres::PgListener};
use tokio::time::Instant;

use pges_core::{category, high_water, lsn, position};
use pges_postgres::store;

use crate::waker;

/// Listens on the store's channel, `pges_<store schema name>`, where the sequencer (with its `watch-notify`
/// feature) sends a [`Notice`](high_water::Notice) after each batch that wrote to the store's log.
///
/// The waker keeps the newest positions the notices carried, so it is level-triggered like the other
/// wakers: a notice that arrived while the subscriber was busy still wakes the next
/// [`Waker::changed`](waker::Waker::changed) whose `known` it has passed. Notices only ever raise positions.
///
/// It resolves at once, whatever `known` says:
///
/// - after the `LISTEN` connection was opened, the first time and after a lost connection. Notices sent
///   while nobody listened are gone, so the subscriber reads the log once to catch up;
/// - after a notice it cannot read (a format this crate does not know, or a malformed payload).
///
/// A notice whose categories were left out (`*`, when they did not fit the payload) counts as every
/// category having reached the notice's store position.
///
/// The waker holds one connection while it listens, opened lazily on the first call. A connection error
/// while listening is treated as a lost connection; [`WakeError::Database`](waker::WakeError::Database)
/// comes only from opening a new one. `LISTEN` needs a session of its own, so the pool must not sit
/// behind a pooler in transaction mode; [`NotifyWaker::with_separate_pool`] takes another pool for that case.
///
/// Reconnects back off. After a failed attempt to listen, or a connection lost within 5 s of opening, the
/// next attempt waits 100 ms, doubling after each further failure up to 5 s. A connection lost after it
/// stayed up for 5 s is replaced at once. The wait happens inside [`Waker::changed`](waker::Waker::changed),
/// so a subscriber that drops the call (to shut down or renew its lease) does not wait for it, and a later
/// call waits only for what is left of it.
///
/// The listening connection only reads, so one that dies silently (dropped by a firewall or a NAT without a
/// reset) may never be noticed, and the waker then stays quiet. Where that can happen, combine it with a
/// `PollWaker` (`wake-poll`) with a long `max_interval` through [`Wakers`](waker::Wakers), so a
/// missed notice delays the subscriber by at most that long.
pub struct NotifyWaker {
    pool: PgPool,
    pool_kind: PoolKind,
    channel: String,
    listener: Option<Listening>,
    seen: Seen,
    recheck: Recheck,
    backoff: Backoff,
}

impl NotifyWaker {
    /// Listens for `store`'s notices on a connection of the store's pool. A subscription counts that
    /// connection when it checks the pool's size ([`Waker::store_connections`](waker::Waker::store_connections)).
    pub fn new(store: &store::Store) -> Self {
        Self::listening(store, store.pool().clone(), PoolKind::Store)
    }

    /// Listens for `store`'s notices on a connection of `pool`, a pool of its own that reaches the store's
    /// database, for example one that bypasses a pooler in transaction mode. The listening connection does
    /// not count against the store's pool. Pass a separate pool, not a clone of [`store::Store::pool`]:
    /// [`NotifyWaker::new`] is for that.
    pub fn with_separate_pool(store: &store::Store, pool: PgPool) -> Self {
        Self::listening(store, pool, PoolKind::Separate)
    }

    fn listening(store: &store::Store, pool: PgPool, pool_kind: PoolKind) -> Self {
        Self {
            pool,
            pool_kind,
            channel: format!("pges_{}", store.name()),
            listener: None,
            seen: Seen::default(),
            recheck: Recheck::NotDue,
            backoff: Backoff::default(),
        }
    }

    async fn listen(&self) -> Result<PgListener, sqlx::Error> {
        let mut listener = PgListener::connect_with(&self.pool).await?;
        listener.eager_reconnect(false);
        listener.listen(&self.channel).await?;

        Ok(listener)
    }
}

impl waker::Waker for NotifyWaker {
    /// Resolves with the newest positions the notices carried. Categories not named by any notice are at
    /// [`position::Position::BEGINNING`], unless a notice left the categories out.
    async fn changed(
        &mut self,
        known: &high_water::HighWater,
    ) -> Result<high_water::HighWater, waker::WakeError> {
        loop {
            // Cancel safety: the listener and the recheck are stored with no await in between, so a call
            // dropped after opening the connection still fires on the next call. The backoff stores its
            // deadline, so a dropped call does not restart the wait.
            let listening = match &mut self.listener {
                Some(listening) => listening,
                None => {
                    if let Some(retry_at) = self.backoff.retry_at {
                        tokio::time::sleep_until(retry_at).await;
                    }
                    let listener = match self.listen().await {
                        Ok(listener) => listener,
                        Err(error) => {
                            self.backoff.failed(Instant::now());
                            return Err(error.into());
                        }
                    };
                    self.backoff.connected();
                    self.recheck = Recheck::Due;
                    self.listener.insert(Listening {
                        listener,
                        opened: Instant::now(),
                    })
                }
            };

            let high = self.seen.high_water(known);
            if self.recheck == Recheck::Due {
                self.recheck = Recheck::NotDue;
                return Ok(high);
            }
            if waker::has_passed(&high, known) {
                return Ok(high);
            }

            match listening.listener.try_recv().await {
                Ok(Some(notification)) => {
                    match high_water::Notice::decode(notification.payload()) {
                        Ok(notice) => self.seen.record(&notice),
                        Err(_) => self.recheck = Recheck::Due,
                    }
                }
                // The connection is gone (`None`), or broken in a way the listener does not recognise as
                // lost. Either way notices may have been missed: listen on a new one, then fire.
                Ok(None) | Err(_) => {
                    let opened = listening.opened;
                    self.listener = None;
                    self.backoff.lost(opened, Instant::now());
                }
            }
        }
    }

    fn store_connections(&self) -> u32 {
        match self.pool_kind {
            PoolKind::Store => 1,
            PoolKind::Separate => 0,
        }
    }
}

impl std::fmt::Debug for NotifyWaker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NotifyWaker")
            .field("channel", &self.channel)
            .field("listening", &self.listener.is_some())
            .finish_non_exhaustive()
    }
}

/// Whose pool a [`NotifyWaker`] listens on, so a subscription knows whether the listening connection is
/// taken from the store's pool.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PoolKind {
    Store,
    Separate,
}

/// The first wait before trying to listen again after a failure.
const MIN_BACKOFF: Duration = Duration::from_millis(100);

/// The longest wait before trying to listen again; also how long a connection must stay up to reset it.
const MAX_BACKOFF: Duration = Duration::from_secs(5);

/// An open `LISTEN` connection.
struct Listening {
    listener: PgListener,
    opened: Instant,
}

/// Whether the next call resolves at once.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Recheck {
    Due,
    NotDue,
}

/// When the waker may try to listen again.
#[derive(Debug, Default)]
struct Backoff {
    /// Failed attempts and short-lived connections since the last connection that stayed up.
    failures: u32,
    retry_at: Option<Instant>,
}

impl Backoff {
    fn failed(&mut self, now: Instant) {
        self.failures = self.failures.saturating_add(1);
        self.retry_at = Some(now + self.delay());
    }

    fn connected(&mut self) {
        self.retry_at = None;
    }

    fn lost(&mut self, opened: Instant, now: Instant) {
        if now.saturating_duration_since(opened) >= MAX_BACKOFF {
            self.failures = 0;
            self.retry_at = None;
            return;
        }

        self.failed(now);
    }

    fn delay(&self) -> Duration {
        let doublings = self.failures.saturating_sub(1).min(16);

        MIN_BACKOFF.saturating_mul(1 << doublings).min(MAX_BACKOFF)
    }
}

/// The newest positions the notices carried.
#[derive(Debug, Default)]
struct Seen {
    store: position::Position,
    categories: BTreeMap<category::CategoryId, position::Position>,
    /// The highest store position of a notice that left its categories out: any category may be there.
    omitted: position::Position,
    lsn: Option<lsn::Lsn>,
}

impl Seen {
    fn record(&mut self, notice: &high_water::Notice) {
        self.store = self.store.max(notice.store());
        self.lsn = self.lsn.max(notice.lsn());
        match notice.categories() {
            high_water::NoticeCategories::Listed(categories) => {
                for (id, at) in categories {
                    let newest = self.categories.entry(*id).or_default();
                    *newest = (*newest).max(*at);
                }
            }
            // `Omitted`, or a kind this crate does not know: any category may have moved.
            _ => self.omitted = self.omitted.max(notice.store()),
        }
    }

    /// The positions seen, with every category `known` watches raised to the last omitted notice.
    fn high_water(&self, known: &high_water::HighWater) -> high_water::HighWater {
        let mut categories = self.categories.clone();
        for (id, _) in known.categories() {
            let newest = categories.entry(*id).or_default();
            *newest = (*newest).max(self.omitted);
        }

        high_water::HighWater::new(self.store, categories.into_iter().collect(), self.lsn)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn at(n: u64) -> position::Position {
        position::Position::new(n).unwrap()
    }

    fn id(n: i32) -> category::CategoryId {
        category::CategoryId::new(n)
    }

    fn known(store: u64, categories: &[i32]) -> high_water::HighWater {
        high_water::HighWater::new(
            at(store),
            categories.iter().map(|c| (id(*c), at(store))).collect(),
            None,
        )
    }

    fn notice(payload: &str) -> high_water::Notice {
        high_water::Notice::decode(payload).unwrap()
    }

    #[test]
    fn keeps_the_newest_position_of_each_category() {
        let mut seen = Seen::default();

        seen.record(&notice("1;12;;1:12"));
        seen.record(&notice("1;9;;1:9,2:8"));

        let high = seen.high_water(&known(0, &[1, 2]));
        assert_eq!(high.store(), at(12));
        assert_eq!(high.category(id(1)), at(12));
        assert_eq!(high.category(id(2)), at(8));
        assert!(waker::has_passed(&high, &known(10, &[1])));
        assert!(!waker::has_passed(&high, &known(10, &[2])));
        assert!(!waker::has_passed(&high, &known(10, &[3])));
    }

    #[test]
    fn omitted_categories_count_as_reaching_the_store_position() {
        let mut seen = Seen::default();

        seen.record(&notice("1;20;;*"));
        seen.record(&notice("1;25;;1:25"));

        let high = seen.high_water(&known(0, &[1, 2]));
        assert_eq!(high.category(id(1)), at(25));
        assert_eq!(high.category(id(2)), at(20));
        assert!(waker::has_passed(&high, &known(19, &[2])));
        assert!(!waker::has_passed(&high, &known(20, &[2])));
        // The whole store.
        assert!(waker::has_passed(&high, &known(24, &[])));
        assert!(!waker::has_passed(&high, &known(25, &[])));
    }

    #[test]
    fn the_backoff_doubles_up_to_its_cap_and_resets_after_a_lasting_connection() {
        let now = Instant::now();
        let mut backoff = Backoff::default();

        let mut delays = Vec::new();
        for _ in 0..8 {
            backoff.failed(now);
            delays.push(backoff.retry_at.unwrap() - now);
        }
        let ms = |n| Duration::from_millis(n);
        assert_eq!(
            delays,
            [
                ms(100),
                ms(200),
                ms(400),
                ms(800),
                ms(1600),
                ms(3200),
                ms(5000),
                ms(5000)
            ]
        );

        backoff.connected();
        assert_eq!(backoff.retry_at, None);
        // Lost soon after opening: still failing.
        backoff.lost(now, now + ms(10));
        assert_eq!(backoff.retry_at, Some(now + ms(10) + MAX_BACKOFF));
        // Lost after staying up: reconnect at once, and start over.
        backoff.connected();
        backoff.lost(now, now + MAX_BACKOFF);
        assert_eq!(backoff.retry_at, None);
        backoff.failed(now);
        assert_eq!(backoff.retry_at, Some(now + MIN_BACKOFF));
    }

    #[test]
    fn keeps_the_newest_lsn() {
        let mut seen = Seen::default();

        seen.record(&notice("1;2;0/20;1:2"));
        seen.record(&notice("1;1;0/10;1:1"));

        assert_eq!(
            seen.high_water(&known(0, &[])).lsn(),
            notice("1;2;0/20;").lsn()
        );
    }
}
