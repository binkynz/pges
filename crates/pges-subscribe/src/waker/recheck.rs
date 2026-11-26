//! Waking on a timer: the waker of a subscription that was given none.

use pges_core::{high_water, interval};

use crate::waker;

/// Waits a fixed interval and reports nothing new, so the subscriber reads the log again. A subscription
/// given no waker uses `Recheck::default()`, every 500 ms.
///
/// Combine it with a push waker through [`Wakers`](waker::Wakers) to bound how long a missed wake-up delays
/// the subscriber.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Recheck {
    every: interval::Interval,
}

impl Recheck {
    /// Fires every `interval`.
    pub const fn every(interval: interval::Interval) -> Self {
        Self { every: interval }
    }

    /// The interval.
    pub const fn interval(&self) -> interval::Interval {
        self.every
    }
}

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
        tokio::time::sleep(self.every.get()).await;

        Ok(known.clone())
    }
}
