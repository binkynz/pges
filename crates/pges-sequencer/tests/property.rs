//! Random interleavings of appends across categories with sequencer restarts and crashes, for the outbox and
//! the logical source: the log ends up gap-free and duplicate-free, holds every commit, and orders each
//! stream by version.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

mod common;

use std::{num::NonZeroUsize, time::Duration};

use anyhow::Result;
use proptest::prelude::*;

use pges_core::stream;
use pges_sequencer::sequencer;

use common::{Fixture, Kind, Target};

const STREAMS: usize = 6;

#[derive(Debug, Clone)]
enum Op {
    /// Appends run concurrently: (stream, events).
    Burst(Vec<(usize, u32)>),
    /// Graceful stop, then a new sequencer.
    Restart,
    /// The sequencer is dropped mid-flight, then a new one waits for the lease to expire.
    Crash,
    Pause(u64),
}

fn op() -> impl Strategy<Value = Op> {
    prop_oneof![
        6 => prop::collection::vec((0..STREAMS, 1..4_u32), 1..6).prop_map(Op::Burst),
        1 => Just(Op::Restart),
        1 => Just(Op::Crash),
        1 => (0..30_u64).prop_map(Op::Pause),
    ]
}

fn config(batch_size: usize) -> pges_sequencer::sequencer::SequencerConfig {
    let mut config = common::fast();
    config.lease = pges_sequencer::sequencer::Lease::with_renew(
        pges_core::interval!(400 ms),
        pges_core::interval!(100 ms),
    )
    .unwrap();
    config.acquire_interval = pges_core::interval!(20 ms);
    config.token_interval = sequencer::TokenInterval::Every(pges_core::interval!(30 ms));
    config.retry_interval = pges_core::interval!(30 ms);
    config.batch_size = NonZeroUsize::new(batch_size).unwrap();
    config
}

async fn run_case(kind: Kind, ops: Vec<Op>, batch_size: usize) -> Result<()> {
    let f = std::sync::Arc::new(Fixture::with(kind).await?);
    let streams: Vec<Target> = (0..STREAMS)
        .map(|i| {
            let id = stream::StreamId::new_v7();
            if i % 2 == 0 {
                Target::Job(id)
            } else {
                Target::Order(id)
            }
        })
        .collect();

    let mut running = Some(f.start(config(batch_size)).await?);
    // The logical source only sees commits once its slot exists.
    running.as_ref().unwrap().wait_leading().await?;
    for op in ops {
        match op {
            Op::Burst(appends) => {
                let mut tasks = Vec::new();
                for (stream, events) in appends {
                    let f = f.clone();
                    let target = streams[stream];
                    tasks.push(tokio::spawn(async move { f.append(target, events).await }));
                }
                for task in tasks {
                    task.await??;
                }
            }
            Op::Restart => {
                running.take().unwrap().stop().await?;
                running = Some(f.start(config(batch_size)).await?);
            }
            Op::Crash => {
                running.take().unwrap().kill().await;
                running = Some(f.start(config(batch_size)).await?);
            }
            Op::Pause(ms) => tokio::time::sleep(Duration::from_millis(ms)).await,
        }
    }

    let commits: i64 = sqlx::query_scalar("SELECT count(*) FROM es.commits")
        .fetch_one(&f.pool)
        .await?;
    common::wait_sequenced(&f.pool, commits as usize).await?;
    let log = common::check_log(&f.pool).await?;
    anyhow::ensure!(log.len() == commits as usize);

    running.take().unwrap().stop().await?;
    std::sync::Arc::into_inner(f).unwrap().finish().await
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 16, ..ProptestConfig::default() })]

    #[test]
    fn interleavings_give_a_gap_free_duplicate_free_log(
        ops in prop::collection::vec(op(), 1..10),
        batch_size in 1..6_usize,
    ) {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(4)
            .enable_all()
            .build()
            .unwrap();
        let result = runtime.block_on(run_case(Kind::Outbox, ops, batch_size));
        prop_assert!(result.is_ok(), "{:?}", result);
    }
}

#[cfg(feature = "source-logical")]
proptest! {
    #![proptest_config(ProptestConfig { cases: 12, ..ProptestConfig::default() })]

    #[test]
    fn logical_interleavings_give_a_gap_free_duplicate_free_log(
        ops in prop::collection::vec(op(), 1..10),
        batch_size in 1..6_usize,
    ) {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(4)
            .enable_all()
            .build()
            .unwrap();
        let result = runtime.block_on(run_case(Kind::Logical, ops, batch_size));
        prop_assert!(result.is_ok(), "{:?}", result);
    }
}
