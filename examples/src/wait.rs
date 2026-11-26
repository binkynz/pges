//! Waiting, with a time limit, for a sequencer or a subscription to get somewhere.

use std::time::Duration;

use anyhow::{Context, Result, bail};

use pges::core::{high_water, position};
use pges::sequencer::{sequencer, watch};
use pges::subscribe::subscription;

/// How long the examples wait for anything.
pub const LIMIT: Duration = Duration::from_secs(30);

/// `n` events into a store's log, as a position. Fails above [`position::Position::MAX`].
pub fn position(n: u64) -> Result<position::Position> {
    position::Position::new(n).context("past the largest position")
}

/// Waits until the sequencer leads. Fails if its run ends, or after [`LIMIT`].
pub async fn leading(watch: &watch::Watch) -> Result<sequencer::Fence> {
    let mut status = watch.status();
    let waited = tokio::time::timeout(
        LIMIT,
        status.wait_for(|status| {
            matches!(
                status,
                sequencer::Status::Leading(_)
                    | sequencer::Status::Halted(_)
                    | sequencer::Status::Stopped
                    | sequencer::Status::Abandoned
            )
        }),
    )
    .await;
    // `wait_for` hands out a read lock on the channel; it is released here, before the next await.
    let status = match waited {
        Ok(Ok(status)) => status.clone(),
        Ok(Err(_)) => bail!("the sequencer is gone"),
        Err(_) => bail!("the sequencer did not lead within {LIMIT:?}"),
    };

    match status {
        sequencer::Status::Leading(fence) => Ok(fence),
        other => bail!("the sequencer stopped: {other:?}"),
    }
}

/// Waits until the store's high-water mark reaches `position`, and returns it. Fails after [`LIMIT`].
pub async fn high_water(
    receiver: &mut tokio::sync::watch::Receiver<high_water::HighWater>,
    position: position::Position,
) -> Result<high_water::HighWater> {
    let waited = tokio::time::timeout(
        LIMIT,
        receiver.wait_for(|high_water| high_water.store() >= position),
    )
    .await;

    match waited {
        Ok(Ok(high_water)) => Ok(high_water.clone()),
        Ok(Err(_)) => bail!("the sequencer is gone"),
        Err(_) => bail!("the log did not reach position {position} within {LIMIT:?}"),
    }
}

/// Waits until the subscription has handled and checkpointed `position`. Fails if it stops, or after
/// [`LIMIT`].
pub async fn handled(
    handle: &subscription::SubscriptionHandle,
    position: position::Position,
) -> Result<subscription::Status> {
    let mut status = handle.status_changes();
    let waited = tokio::time::timeout(
        LIMIT,
        status.wait_for(|status| {
            status.position >= position || matches!(status.state, subscription::State::Stopped)
        }),
    )
    .await;
    let status = match waited {
        Ok(Ok(status)) => status.clone(),
        Ok(Err(_)) => bail!("the subscription is gone"),
        Err(_) => bail!("the subscription did not reach position {position} within {LIMIT:?}"),
    };

    if status.position < position {
        bail!("the subscription stopped at position {}", status.position);
    }
    Ok(status)
}
