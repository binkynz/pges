//! One category end to end with the real outbox sequencer: catch-up, live tail woken by the sequencer's
//! in-process watch, typed decoding, and events this code does not know.

#![cfg(feature = "wake-inproc")]
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

mod common;

use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

use anyhow::{Context, Result, ensure};
use serde::{Deserialize, Serialize};

use pges_core::{event, stream, version};
use pges_subscribe::{
    batch, handler,
    subscription::{self, StoreExt},
    subscription_name, waker,
};

use common::{Cat, Fixture, JobEvent};

/// A newer writer's event type in the `jobs` category, which `JobEvent` does not know.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, pges_derive::Event)]
#[event(category = "jobs")]
enum FutureJobEvent {
    #[event(name = "job.future")]
    Arrived { note: String },
}

type Seen = Arc<Mutex<Vec<(u64, stream::StreamId, u64, u32)>>>;

/// At-least-once handler collecting `(position, stream, version, n)` of every decoded job event.
struct Collect(Seen);

impl handler::Handler for Collect {
    async fn handle(&mut self, batch: &batch::Batch) -> Result<(), handler::HandlerError> {
        for event in batch.decode::<JobEvent>() {
            let event = event?;
            let JobEvent::Noted { n } = event.event;
            self.0.lock().unwrap().push((
                event.position.get(),
                event.stream_id,
                event.version.get(),
                n,
            ));
        }
        Ok(())
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn catches_up_then_follows_the_live_tail() -> Result<()> {
    let f = Fixture::new(pges_postgres::store::Outbox::Enabled).await?;
    let (a, b, part) = (
        stream::StreamId::new_v7(),
        stream::StreamId::new_v7(),
        stream::StreamId::new_v7(),
    );
    for round in 0..10 {
        f.append(Cat::Jobs, if round % 2 == 0 { a } else { b }, 2)
            .await?;
        f.append(Cat::Parts, part, 1).await?;
    }
    let sequencer = common::Sequencer::start(&f).await?;
    let store_id = f.store.store_id();

    let seen: Seen = Arc::default();
    let running = common::Running::spawn(
        f.store
            .subscription(subscription_name!("jobs-tail"))
            .category::<JobEvent>()
            .batch_size(std::num::NonZeroU32::new(3).unwrap())
            .waker(waker::inproc::InprocWaker::new(
                sequencer.watch.high_water(store_id),
            ))
            .run(Collect(seen.clone())),
    );

    tokio::time::timeout(Duration::from_secs(20), async {
        while seen.lock().unwrap().len() < 20 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .context("catch-up")?;

    // The live tail: no recheck interval is set and the waker is the only wake-up.
    for _ in 0..5 {
        f.append(Cat::Jobs, a, 1).await?;
        f.append(Cat::Parts, part, 1).await?;
    }
    tokio::time::timeout(Duration::from_secs(20), async {
        while seen.lock().unwrap().len() < 25 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .context("live tail")?;

    let seen = seen.lock().unwrap().clone();
    let expected = f.expected(&[f.id(Cat::Jobs)]).await?;
    ensure!(
        seen.iter().map(|s| s.0 as i64).collect::<Vec<_>>()
            == expected.iter().map(|e| e.0).collect::<Vec<_>>(),
        "positions differ: {seen:?} vs {expected:?}"
    );
    for id in [a, b] {
        let versions: Vec<u64> = seen.iter().filter(|s| s.1 == id).map(|s| s.2).collect();
        ensure!(
            versions == (1..=versions.len() as u64).collect::<Vec<_>>(),
            "versions of {id}: {versions:?}"
        );
    }
    // Two-event commits decode as n = 0, 1 in version order.
    let first_a: Vec<u32> = seen
        .iter()
        .filter(|s| s.1 == a)
        .take(2)
        .map(|s| s.3)
        .collect();
    assert_eq!(first_a, [0, 1]);

    running.stop().await?;
    sequencer.stop().await?;
    f.finish().await
}

#[tokio::test]
async fn unknown_events_are_skipped_or_stop_the_subscription() -> Result<()> {
    let f = Fixture::new(pges_postgres::store::Outbox::Disabled).await?;
    f.store.register::<FutureJobEvent>().await?;
    let id = stream::StreamId::new_v7();
    f.append(Cat::Jobs, id, 1).await?;
    f.store
        .append(
            id,
            version::ExpectedVersion::Any,
            &[FutureJobEvent::Arrived {
                note: "from a newer writer".into(),
            }],
        )
        .await?;
    f.append(Cat::Jobs, id, 1).await?;
    f.sequence_all().await?;

    let seen: Seen = Arc::default();
    let running = common::Running::spawn(
        f.store
            .subscription(subscription_name!("skips"))
            .category::<JobEvent>()
            .on_unknown(event::OnUnknown::Skip)
            .run(Collect(seen.clone())),
    );
    running.wait_position(3).await?;
    let positions: Vec<u64> = seen.lock().unwrap().iter().map(|s| s.0).collect();
    assert_eq!(positions, [1, 3]);
    running.stop().await?;

    let error = common::Running::spawn(
        f.store
            .subscription(subscription_name!("fails"))
            .category::<JobEvent>()
            .on_error(subscription::RetryPolicy::new(
                pges_core::interval!(1 ms),
                std::num::NonZeroU32::MIN,
            ))
            .run(Collect(Arc::default())),
    )
    .stopped()
    .await?;
    let subscription::SubscribeError::HandlerFailed { source, .. } = &error else {
        panic!("unexpected error {error:?}");
    };
    assert!(
        matches!(
            source.downcast_ref::<batch::DecodeError>(),
            Some(batch::DecodeError::UnknownEvent { type_name, .. }) if type_name == "job.future"
        ),
        "{source:?}"
    );
    assert_eq!(f.cursor("fails").await?, Some(0));

    f.finish().await
}
