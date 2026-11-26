//! Waking by polling the log: the fallback when nothing pushes high-water marks.

use std::time::Duration;

use sqlx::PgPool;

use pges_core::{category, high_water, interval, position};
use pges_postgres::{sql::typed::Rendered, store};

use crate::{sql, sql::statement, waker};

/// Reads the newest `log_by_category` entry of each watched category (or the newest log row, when the
/// subscriber watches the whole store) until one has passed what the subscriber knows.
///
/// The first read happens at once. While nothing changes, the wait between reads starts at
/// [`PollConfig::interval`] and doubles up to [`PollConfig::max_interval`]; it starts over after the waker
/// fires. Each read is one index lookup per category. `categories.last_position` is not used: the sequencer
/// writes it only about once a second.
#[derive(Debug, Clone)]
pub struct PollWaker {
    pool: PgPool,
    newest: Rendered<statement::head::Query>,
    newest_by_category: Rendered<statement::newest_by_category::Query>,
    config: PollConfig,
    wait: Duration,
}

impl PollWaker {
    /// Polls `store`'s log on the store's pool.
    pub fn new(store: &store::Store, config: PollConfig) -> Self {
        let name = pges_postgres::sql::store_name(store);

        Self {
            pool: store.pool().clone(),
            newest: sql::head(name),
            newest_by_category: sql::newest_by_category(name),
            wait: config.interval.get(),
            config,
        }
    }

    async fn read(
        &self,
        known: &high_water::HighWater,
    ) -> Result<high_water::HighWater, waker::WakeError> {
        if known.categories().is_empty() {
            let newest = self
                .newest
                .bind(statement::head::Params {})
                .fetch_one(&self.pool)
                .await?
                .head;
            return Ok(high_water::HighWater::new(
                to_position(newest),
                Vec::new(),
                None,
            ));
        }

        let ids: Vec<i32> = known.categories().iter().map(|(id, _)| id.get()).collect();
        let rows = self
            .newest_by_category
            .bind(statement::newest_by_category::Params { category_ids: &ids })
            .fetch_all(&self.pool)
            .await?;
        let categories: Vec<(category::CategoryId, position::Position)> = rows
            .into_iter()
            .map(|row| {
                (
                    category::CategoryId::new(row.category_id),
                    to_position(row.last_position),
                )
            })
            .collect();
        let store = categories
            .iter()
            .map(|(_, newest)| *newest)
            .max()
            .unwrap_or(position::Position::BEGINNING);

        Ok(high_water::HighWater::new(store, categories, None))
    }
}

impl waker::Waker for PollWaker {
    /// Resolves with the newest positions read. Without watched categories, `store` is the store's newest
    /// position; otherwise it is the highest of the watched categories' newest positions.
    async fn changed(
        &mut self,
        known: &high_water::HighWater,
    ) -> Result<high_water::HighWater, waker::WakeError> {
        loop {
            let high = self.read(known).await?;
            if waker::has_passed(&high, known) {
                self.wait = self.config.interval.get();
                return Ok(high);
            }

            tokio::time::sleep(self.wait).await;
            self.wait = (self.wait * 2).min(self.config.max_interval.get());
        }
    }
}

/// How often [`PollWaker`] reads the log: the first wait after a read that found nothing new, and the
/// longest wait, which is never shorter than the first.
///
/// ```
/// use pges_subscribe::waker::poll;
///
/// // In a `const`, `unwrap` runs at compile time: a `max_interval` shorter than `interval` does not compile.
/// const QUICK: poll::PollConfig =
///     poll::PollConfig::new(pges_core::interval!(10 ms), pges_core::interval!(1 s)).unwrap();
///
/// assert_eq!(QUICK.interval(), pges_core::interval!(10 ms));
/// assert!(poll::PollConfig::new(pges_core::interval!(1 s), pges_core::interval!(10 ms)).is_none());
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub struct PollConfig {
    interval: interval::Interval,
    max_interval: interval::Interval,
}

impl PollConfig {
    /// Waits `interval` after a read that found nothing new, doubling up to `max_interval`. `None` if
    /// `max_interval` is shorter than `interval`.
    pub const fn new(
        interval: interval::Interval,
        max_interval: interval::Interval,
    ) -> Option<Self> {
        if max_interval.get().as_nanos() < interval.get().as_nanos() {
            return None;
        }

        Some(Self {
            interval,
            max_interval,
        })
    }

    /// The first wait after a read that found nothing new. Default 100 ms.
    pub const fn interval(&self) -> interval::Interval {
        self.interval
    }

    /// The longest wait between reads. Default 5 s.
    pub const fn max_interval(&self) -> interval::Interval {
        self.max_interval
    }
}

impl Default for PollConfig {
    fn default() -> Self {
        Self {
            interval: pges_core::interval!(100 ms),
            max_interval: pges_core::interval!(5 s),
        }
    }
}

fn to_position(position: i64) -> position::Position {
    position::Position::from_i64(position).unwrap_or(position::Position::BEGINNING)
}
