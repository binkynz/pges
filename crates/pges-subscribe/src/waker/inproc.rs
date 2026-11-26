//! Waking from a `tokio::sync::watch` channel of high-water marks, such as the one the sequencer's
//! in-process `Watch` hands out when it runs in the same binary.

use pges_core::high_water;

use crate::waker;

/// Wakes when the channel's high-water mark passes what the subscriber knows.
///
/// The channel is level-triggered: a waker sees the newest value, not every value, which is all a
/// subscriber needs. It does not depend on the sequencer crate; any sender of
/// [`HighWater`](high_water::HighWater) values for one store works. Once the sender is dropped and the last
/// value has not passed `known`, [`Waker::changed`](waker::Waker::changed) fails with
/// [`WakeError::Closed`](waker::WakeError::Closed).
#[derive(Debug, Clone)]
pub struct InprocWaker {
    receiver: tokio::sync::watch::Receiver<high_water::HighWater>,
}

impl InprocWaker {
    /// Wakes from `receiver`, for example `sequencer.watch().high_water(store.store_id())`.
    pub fn new(receiver: tokio::sync::watch::Receiver<high_water::HighWater>) -> Self {
        Self { receiver }
    }
}

impl waker::Waker for InprocWaker {
    async fn changed(
        &mut self,
        known: &high_water::HighWater,
    ) -> Result<high_water::HighWater, waker::WakeError> {
        loop {
            {
                let high = self.receiver.borrow_and_update();
                if waker::has_passed(&high, known) {
                    return Ok(high.clone());
                }
            }

            self.receiver
                .changed()
                .await
                .map_err(|_| waker::WakeError::Closed)?;
        }
    }
}
