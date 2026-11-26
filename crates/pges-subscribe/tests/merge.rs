//! Several categories merged in position order: the cut rule, with log rows written by hand so the
//! interleaving of commits, sequencer batches and reads is under the test's control.
//!
//! The cut rule: a reader reads `H`, the store's newest position, then each category's page up to `H`. The cut
//! is the smallest last position over the categories whose page was full, or `H` if none was. Only rows at or
//! below the cut are delivered, and the cursor moves to the cut.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

mod common;

use std::{num::NonZeroU32, sync::Arc, time::Duration};

use anyhow::Result;
use proptest::prelude::*;

use pges_core::{category, interval, non_empty, stream};
use pges_subscribe::{
    subscription::{self, StoreExt},
    subscription_name,
    waker::recheck,
};

use common::{Cat, Fixture, Recorder};

fn subscription(f: &Fixture, name: &str, batch_size: u32) -> subscription::Subscription {
    subscription_on(&f.store, name, batch_size)
}

fn subscription_on(
    store: &pges_postgres::store::Store,
    name: &str,
    batch_size: u32,
) -> subscription::Subscription {
    store
        .subscription(subscription::SubscriptionName::parse(name).unwrap())
        .categories(non_empty![category!("jobs"), category!("orders")])
        .batch_size(NonZeroU32::new(batch_size).unwrap())
        .lease(interval!(400 ms))
        .acquire_interval(interval!(20 ms))
        .retry_interval(interval!(20 ms))
        .waker(recheck::Recheck::every(interval!(5 ms)))
        .run_pg(Recorder::new(name))
}

#[tokio::test]
async fn merges_categories_in_position_order_and_cuts_at_full_pages() -> Result<()> {
    let f = Fixture::new(pges_postgres::store::Outbox::Disabled).await?;
    let (job, order, part) = (
        stream::StreamId::new_v7(),
        stream::StreamId::new_v7(),
        stream::StreamId::new_v7(),
    );
    for (cat, stream_id, events) in [
        (Cat::Jobs, job, 1),
        (Cat::Parts, part, 3),
        (Cat::Orders, order, 2),
        (Cat::Jobs, job, 1),
        (Cat::Jobs, job, 2),
        (Cat::Jobs, job, 1),
        (Cat::Orders, order, 1),
        (Cat::Parts, part, 2),
    ] {
        f.append(cat, stream_id, events).await?;
    }
    f.sequence_all().await?;
    let head = common::head(&f.pool).await?;

    let recorder = Recorder::new("merge");
    let running = common::Running::spawn(
        f.store
            .subscription(subscription_name!("merge"))
            .categories_of::<(common::JobEvent, common::OrderEvent)>()
            .batch_size(NonZeroU32::new(2).unwrap())
            .waker(recheck::Recheck::every(interval!(5 ms)))
            .run_pg(recorder.clone()),
    );
    running.wait_position(head).await?;

    let expected = f.expected(&[f.id(Cat::Jobs), f.id(Cat::Orders)]).await?;
    common::check_delivery(&f.delivered("merge").await?, &expected)?;
    assert_eq!(f.cursor("merge").await?, Some(head as i64));
    let calls = recorder.calls();
    assert!(
        calls >= 3,
        "two commits per category per batch need several batches, got {calls}"
    );

    // Only an unsubscribed category moves: the cursor follows with empty batches, the handler is not called.
    f.append(Cat::Parts, part, 1).await?;
    f.sequence_all().await?;
    let head = common::head(&f.pool).await?;
    running.wait_position(head).await?;
    assert_eq!(f.cursor("merge").await?, Some(head as i64));
    assert_eq!(recorder.calls(), calls);

    running.stop().await?;
    f.finish().await
}

/// Sequencer batches commit between the reads of `H` and of each category, as fast as they can. Without the
/// `H` bound, a full page of one category could move the cut past rows another category's earlier read did
/// not see yet.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_sequencer_batches_never_skip_an_event() -> Result<()> {
    let f = Arc::new(Fixture::new(pges_postgres::store::Outbox::Disabled).await?);
    let streams: Vec<stream::StreamId> = (0..6).map(|_| stream::StreamId::new_v7()).collect();
    for i in 0..600 {
        // Mostly jobs-orders pairs, sequenced together below, so one batch lands a row of each category.
        let cat = match i % 5 {
            0 | 2 => Cat::Jobs,
            1 | 3 => Cat::Orders,
            _ => Cat::Parts,
        };
        f.append(
            cat,
            streams[cat as usize * 2 + (i / 5) % 2],
            1 + (i % 2) as u32,
        )
        .await?;
    }

    let running = common::Running::spawn(
        f.store
            .subscription(subscription_name!("race"))
            .categories(non_empty![category!("jobs"), category!("orders")])
            .batch_size(NonZeroU32::new(1).unwrap())
            .waker(recheck::Recheck::every(interval!(1 ms)))
            .run_pg(Recorder::new("race")),
    );
    let sequencer = {
        let f = f.clone();
        tokio::spawn(async move {
            // Paced so the subscriber is caught up, and racing the next batch, most of the time.
            let mut round = 0_u64;
            while f.sequence(2).await? > 0 {
                round += 1;
                tokio::time::sleep(Duration::from_micros(500 * (round % 7))).await;
            }
            anyhow::Ok(())
        })
    };
    sequencer.await??;
    let head = common::head(&f.pool).await?;
    running.wait_position(head).await?;
    running.stop().await?;

    let expected = f.expected(&[f.id(Cat::Jobs), f.id(Cat::Orders)]).await?;
    common::check_delivery(&f.delivered("race").await?, &expected)?;

    Arc::into_inner(f).unwrap().finish().await
}

#[derive(Debug, Clone)]
enum Op {
    /// An append of `events` events to stream `stream` of a category (jobs, orders, or unsubscribed parts).
    Append {
        cat: u8,
        stream: u8,
        events: u32,
    },
    /// A sequencer batch of up to `n` commits.
    Sequence(i64),
    /// Graceful shutdown, then a new instance resumes from the checkpoint.
    Restart,
    /// The subscription's task is dropped mid-flight; a new instance waits for the lease to lapse.
    Crash,
    Pause(u64),
}

fn op() -> impl Strategy<Value = Op> {
    prop_oneof![
        6 => (0..3_u8, 0..4_u8, 1..4_u32)
            .prop_map(|(cat, stream, events)| Op::Append { cat, stream, events }),
        4 => (1..6_i64).prop_map(Op::Sequence),
        1 => Just(Op::Restart),
        1 => Just(Op::Crash),
        1 => (0..20_u64).prop_map(Op::Pause),
    ]
}

enum Instance {
    Graceful(common::Running),
    Crashable(common::Crashable),
}

async fn run_case(ops: Vec<Op>, batch_size: u32) -> Result<()> {
    let f = Arc::new(Fixture::new(pges_postgres::store::Outbox::Disabled).await?);
    let streams: Vec<stream::StreamId> = (0..12).map(|_| stream::StreamId::new_v7()).collect();
    let name = "merge-property";
    let mut errors = Vec::new();

    let mut instance = Instance::Crashable(
        common::Crashable::spawn(&f, |store| subscription_on(store, name, batch_size)).await?,
    );
    for op in ops {
        match op {
            Op::Append {
                cat,
                stream,
                events,
            } => {
                let cat = [Cat::Jobs, Cat::Orders, Cat::Parts][cat as usize];
                let stream_id = streams[cat as usize * 4 + stream as usize];
                f.append(cat, stream_id, events).await?;
            }
            Op::Sequence(n) => {
                f.sequence(n).await?;
            }
            Op::Restart => {
                match instance {
                    Instance::Graceful(running) => {
                        errors.extend(running.errors());
                        running.handle.shutdown().await?;
                    }
                    Instance::Crashable(crashable) => {
                        errors.extend(crashable.crash().await);
                    }
                }
                instance =
                    Instance::Graceful(common::Running::spawn(subscription(&f, name, batch_size)));
            }
            Op::Crash => {
                match instance {
                    Instance::Graceful(running) => {
                        errors.extend(running.errors());
                        running.handle.shutdown().await?;
                    }
                    Instance::Crashable(crashable) => {
                        errors.extend(crashable.crash().await);
                    }
                }
                instance = Instance::Crashable(
                    common::Crashable::spawn(&f, |store| subscription_on(store, name, batch_size))
                        .await?,
                );
            }
            Op::Pause(ms) => tokio::time::sleep(Duration::from_millis(ms)).await,
        }
    }

    f.sequence_all().await?;
    let head = common::head(&f.pool).await?;
    // With nothing to read, position 0 is reached before the instance takes the lease and writes its cursor.
    let holds_lease = |s: &subscription::State| matches!(s, subscription::State::Running);
    match instance {
        Instance::Graceful(running) => {
            common::wait_state(running.handle.status_changes(), holds_lease).await?;
            running.wait_position(head).await?;
            errors.extend(running.errors());
            running.handle.shutdown().await?;
        }
        Instance::Crashable(crashable) => {
            common::wait_state(crashable.status.clone(), holds_lease).await?;
            common::wait_position(crashable.status.clone(), head).await?;
            errors.extend(crashable.crash().await);
        }
    }
    anyhow::ensure!(
        errors.is_empty(),
        "the subscription recovered from errors: {errors:?}"
    );

    let expected = f.expected(&[f.id(Cat::Jobs), f.id(Cat::Orders)]).await?;
    common::check_delivery(&f.delivered(name).await?, &expected)?;
    anyhow::ensure!(f.cursor(name).await? == Some(head as i64));

    Arc::into_inner(f).unwrap().finish().await
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 16, ..ProptestConfig::default() })]

    /// Any interleaving of commits across three categories, sequencer batches, and subscription restarts
    /// and crashes delivers every event of the two subscribed categories once, in position order.
    #[test]
    fn interleavings_deliver_every_event_once_in_position_order(
        ops in prop::collection::vec(op(), 1..40),
        batch_size in 1..4_u32,
    ) {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(4)
            .enable_all()
            .build()
            .unwrap();
        let result = runtime.block_on(run_case(ops, batch_size));
        prop_assert!(result.is_ok(), "{:?}", result);
    }
}
