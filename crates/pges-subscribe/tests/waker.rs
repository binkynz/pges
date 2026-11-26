//! Wakers: the in-process channel, polling with backoff, and several combined.

#![cfg(all(feature = "wake-inproc", feature = "wake-poll"))]
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

mod common;

use std::time::Duration;

use anyhow::Result;

use pges_core::{category, high_water, interval, non_empty, position, stream};
use pges_subscribe::{
    subscription::StoreExt,
    subscription_name,
    waker::{self, Waker, inproc, poll},
};

use common::{Cat, Fixture, Recorder};

fn at(n: u64) -> position::Position {
    pges_testing::position(n)
}

fn known(store: u64, categories: &[category::CategoryId]) -> high_water::HighWater {
    high_water::HighWater::new(
        at(store),
        categories.iter().map(|id| (*id, at(store))).collect(),
        None,
    )
}

async fn pending<W: Waker>(waker: &mut W, known: &high_water::HighWater, wait: Duration) -> bool {
    tokio::time::timeout(wait, waker.changed(known))
        .await
        .is_err()
}

#[tokio::test]
async fn inproc_wakes_only_for_watched_categories() -> Result<()> {
    let (jobs, orders) = (category::CategoryId::new(1), category::CategoryId::new(2));
    let (sender, receiver) = tokio::sync::watch::channel(high_water::HighWater::default());
    let mut waker = inproc::InprocWaker::new(receiver);
    let known = known(5, &[jobs]);

    sender.send_replace(high_water::HighWater::new(at(5), vec![(jobs, at(5))], None));
    assert!(pending(&mut waker, &known, Duration::from_millis(50)).await);

    // Another category and the store move: still nothing for jobs.
    sender.send_replace(high_water::HighWater::new(
        at(9),
        vec![(jobs, at(5)), (orders, at(9))],
        None,
    ));
    assert!(pending(&mut waker, &known, Duration::from_millis(50)).await);

    let changed = tokio::spawn(async move {
        let result = waker.changed(&known).await;
        (waker, result)
    });
    tokio::time::sleep(Duration::from_millis(20)).await;
    sender.send_replace(high_water::HighWater::new(
        at(10),
        vec![(jobs, at(10)), (orders, at(9))],
        None,
    ));
    let (mut waker, woken) = changed.await?;
    assert_eq!(woken?.category(jobs), at(10));

    // Level-triggered: a value already past `known` resolves at once.
    assert!(
        !pending(
            &mut waker,
            &self::known(5, &[jobs]),
            Duration::from_millis(50)
        )
        .await
    );

    // The whole store, when no category is watched.
    assert!(!pending(&mut waker, &self::known(9, &[]), Duration::from_millis(50)).await);
    assert!(pending(&mut waker, &self::known(10, &[]), Duration::from_millis(50)).await);

    drop(sender);
    assert!(matches!(
        waker.changed(&self::known(10, &[jobs])).await,
        Err(waker::WakeError::Closed)
    ));

    Ok(())
}

fn fast_poll() -> poll::PollConfig {
    poll::PollConfig::new(interval!(5 ms), interval!(40 ms)).unwrap()
}

#[tokio::test]
async fn poll_wakes_when_a_watched_category_gets_a_log_entry() -> Result<()> {
    let f = Fixture::new(pges_postgres::store::Outbox::Disabled).await?;
    let (job, part) = (stream::StreamId::new_v7(), stream::StreamId::new_v7());
    f.append(Cat::Jobs, job, 1).await?;
    f.sequence_all().await?;
    let mut waker = poll::PollWaker::new(&f.store, fast_poll());
    let jobs = f.id(Cat::Jobs);

    assert!(pending(&mut waker, &known(1, &[jobs]), Duration::from_millis(150)).await);

    f.append(Cat::Parts, part, 2).await?;
    f.sequence_all().await?;
    assert!(pending(&mut waker, &known(3, &[jobs]), Duration::from_millis(150)).await);
    // Unwatched categories still move the store.
    let store = waker.changed(&known(1, &[])).await?;
    assert_eq!(store.store(), at(3));

    let watching = tokio::spawn(async move {
        let result = waker.changed(&known(3, &[jobs])).await;
        (waker, result)
    });
    tokio::time::sleep(Duration::from_millis(100)).await;
    f.append(Cat::Jobs, job, 1).await?;
    f.sequence_all().await?;
    let (_, woken) = tokio::time::timeout(Duration::from_secs(5), watching).await??;
    assert_eq!(woken?.category(jobs), at(4));

    f.finish().await
}

#[tokio::test]
async fn any_keeps_waking_after_one_waker_failed() -> Result<()> {
    let f = Fixture::new(pges_postgres::store::Outbox::Disabled).await?;
    let job = stream::StreamId::new_v7();
    let (sender, receiver) = tokio::sync::watch::channel(high_water::HighWater::default());
    drop(sender);

    // The in-process channel is closed (its sequencer stopped); polling still wakes the subscription.
    let running = common::Running::spawn(
        f.store
            .subscription(subscription_name!("any"))
            .categories(non_empty![category!("jobs")])
            .waker(
                waker::Wakers::new(inproc::InprocWaker::new(receiver))
                    .or(poll::PollWaker::new(&f.store, fast_poll())),
            )
            .run_pg(Recorder::new("any")),
    );
    for expected in 1..=3_u64 {
        f.append(Cat::Jobs, job, 1).await?;
        f.sequence_all().await?;
        running.wait_position(expected).await?;
    }
    running.stop().await?;

    f.finish().await
}
