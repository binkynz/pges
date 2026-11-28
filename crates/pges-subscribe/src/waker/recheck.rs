//! Waking on a timer: the waker of a subscription that was given none.

use tokio::time::Instant;

use pges_core::{high_water, interval};

use crate::waker;

/// Waits a fixed interval and reports nothing new, so the subscriber reads the log again. A subscription
/// given no waker uses `Recheck::default()`, every 500 ms.
///
/// Combine it with a push waker through [`Wakers`](waker::Wakers) to bound how long a missed wake-up delays
/// the subscriber.
///
/// The interval runs from the first call that waits, not from each call: a call dropped before it fires
/// (when the subscriber renews its lease, or another waker in [`Wakers`](waker::Wakers) fires first) leaves
/// the deadline for the next call. So within `Wakers` it still fires about once per interval while a push
/// waker fires more often: at most one extra log read per interval. A call made after the deadline passed,
/// such as the first one after the subscriber spent longer than the interval on a batch, returns at once.
#[derive(Debug, Clone, Copy)]
pub struct Recheck {
    every: interval::Interval,
    /// When the pending call fires. Kept across calls: the subscriber drops a call to renew its lease, which
    /// can be more often than the interval, and the next call must not start the interval again.
    due: Option<Instant>,
}

impl Recheck {
    /// Fires every `interval`.
    pub const fn every(interval: interval::Interval) -> Self {
        Self {
            every: interval,
            due: None,
        }
    }

    /// The interval.
    pub const fn interval(&self) -> interval::Interval {
        self.every
    }
}

impl PartialEq for Recheck {
    /// Rechecks are equal when their intervals are.
    fn eq(&self, other: &Self) -> bool {
        self.every == other.every
    }
}

impl Eq for Recheck {}

impl Default for Recheck {
    /// Every 500 ms.
    fn default() -> Self {
        Self::every(pges_core::interval!(500 ms))
    }
}

impl waker::Waker for Recheck {
    /// Resolves with `known` after the interval.
    async fn changed(
        &mut self,
        known: &high_water::HighWater,
    ) -> Result<high_water::HighWater, waker::WakeError> {
        let due = *self
            .due
            .get_or_insert_with(|| Instant::now() + self.every.get());
        tokio::time::sleep_until(due).await;
        self.due = None;

        Ok(known.clone())
    }
}
