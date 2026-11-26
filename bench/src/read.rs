//! Spike 3, in part: the rate at which a subscription catches up by reading the log joined with `commits`,
//! and `load` latency by stream length.
//!
//! Catch-up: `--events` single-event commits are appended and sequenced with the logical source, the
//! sequencer stops, the database is vacuumed and analysed, and a subscription starts from the beginning. It
//! decodes every event and counts it; its rate is events over the time until its checkpoint reaches the last
//! position.
//!
//! Load: for each length, `--load-streams` streams are written with one event per commit, then `load` reads
//! one of them, picked at random, per call for `--duration` (after `--warmup`), one call at a time.

use std::{
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use anyhow::{Context, Result, bail, ensure};
use serde_json::json;
use sqlx::Executor;
use tokio::{sync::oneshot, time::Instant};

use pges::core::{position, stream, version};
use pges::postgres::{append, store};
use pges::replication::config;
use pges::sequencer::{logical, sequencer};
use pges::subscribe::{
    batch, handler,
    subscription::{self, StoreExt},
};

use crate::{database, event, measure, report, sql};

/// Options of `pges-bench read`.
#[derive(Debug, Clone, clap::Args)]
pub struct Args {
    /// Events the catch-up subscription reads; 0 skips catch-up.
    #[arg(long, default_value_t = 100_000)]
    pub events: u64,
    /// Events per subscription batch.
    #[arg(long, default_value_t = 256)]
    pub batch_size: u32,
    /// Stream lengths to load, comma-separated; empty skips load.
    #[arg(long, value_delimiter = ',', default_value = "10,100,1000")]
    pub lengths: Vec<u64>,
    /// Streams written for each length.
    #[arg(long, default_value_t = 10)]
    pub load_streams: usize,
    /// How long loads run unmeasured for each length first.
    #[arg(long, default_value = "1s", value_parser = crate::parse_duration)]
    pub warmup: Duration,
    /// How long loads run for each length.
    #[arg(long, default_value = "3s", value_parser = crate::parse_duration)]
    pub duration: Duration,
    /// Bytes of payload per event.
    #[arg(long, default_value_t = 100)]
    pub payload: usize,
    /// How long to wait for sequencing or catch-up before giving up.
    #[arg(long, default_value = "300s", value_parser = crate::parse_duration)]
    pub limit: Duration,
}

/// Runs catch-up, then the loads.
pub async fn run(server: &database::Server, args: &Args) -> Result<report::Report> {
    let mut report = report::Report::new(
        "read",
        &["what", "events", "rate/s", "p50 ms", "p99 ms", "p99.9 ms"],
    );

    let scratch = server.scratch().await?;
    let result = measure_all(&scratch, args, &mut report).await;
    scratch.cleanup().await?;
    result?;

    Ok(report)
}

/// Counts and decodes what it is handed.
struct CountingHandler {
    events: Arc<AtomicU64>,
}

impl handler::Handler for CountingHandler {
    async fn handle(&mut self, batch: &batch::Batch) -> Result<(), handler::HandlerError> {
        let mut decoded = 0;
        for recorded in batch.decode::<event::BenchEvent>() {
            recorded?;
            decoded += 1;
        }
        self.events.fetch_add(decoded, Ordering::Relaxed);

        Ok(())
    }
}

/// One `load` of a stream of a known length.
#[derive(Clone)]
struct Load {
    store: store::Store,
    streams: Arc<[stream::StreamId]>,
    length: u64,
}

impl measure::Operation for Load {
    async fn call(&self, rng: &mut event::Rng) -> Result<u64> {
        let stream = self.streams[rng.below(self.streams.len())];
        let loaded = self.store.load::<event::Counter>(stream).await?;
        ensure!(
            loaded.state().events == self.length,
            "loaded {} events, expected {}",
            loaded.state().events,
            self.length
        );

        Ok(loaded.state().events)
    }
}

async fn measure_all(
    scratch: &database::Scratch,
    args: &Args,
    report: &mut report::Report,
) -> Result<()> {
    let pool = scratch.pool(9).await?;
    let store = database::open_store(
        pool.clone(),
        pges::core::store_name!("bench"),
        store::Outbox::Disabled,
    )
    .await?;

    if args.events > 0 {
        sequence(scratch, &store, args).await?;
        pool.execute(sql::statement::VACUUM_ANALYZE.sql()).await?;
        let (elapsed, events) = catch_up(
            &store,
            Scope::Category,
            args.events,
            args.batch_size,
            args.limit,
        )
        .await?;
        ensure!(
            events == args.events,
            "the catch-up subscription decoded {events} events, not {}",
            args.events
        );
        let rate = measure::per_second(events, elapsed);
        let record = json!({
            "bench": "read",
            "what": "catch-up",
            "events": events,
            "batch_size": args.batch_size,
            "payload_bytes": args.payload,
            "seconds": elapsed.as_secs_f64(),
            "events_per_s": rate,
        });
        report.push(
            record,
            vec![
                "catch-up".into(),
                events.to_string(),
                report::rate(rate),
                "-".into(),
                "-".into(),
                "-".into(),
            ],
        );
    }

    for length in &args.lengths {
        let streams = write_streams(&store, *length, args).await?;
        let operation = Load {
            store: store.clone(),
            streams,
            length: *length,
        };
        measure::warm_up(operation.clone(), 1, args.warmup).await?;
        let load = measure::drive(operation, 1, args.duration, measure::Pacing::Closed, 5).await?;
        let summary = load.latency.summary();
        let what = format!("load {length}");
        let record = json!({
            "bench": "read",
            "what": "load",
            "length": length,
            "streams": args.load_streams,
            "payload_bytes": args.payload,
            "seconds": load.elapsed.as_secs_f64(),
            "loads": load.calls,
            "loads_per_s": load.calls_per_second(),
            "events_per_s": load.events_per_second(),
            "latency": summary.json(),
            "errors": load.errors,
            "first_error": load.first_error,
        });
        let [p50, p99, p999] = report::latency_cells(&summary);
        report.push(
            record,
            vec![
                what,
                load.events.to_string(),
                report::rate(load.calls_per_second()),
                p50,
                p99,
                p999,
            ],
        );
    }

    pool.close().await;
    Ok(())
}

/// Appends `--events` single-event commits and sequences them with the logical source.
async fn sequence(scratch: &database::Scratch, store: &store::Store, args: &Args) -> Result<()> {
    const PER_CALL: u64 = 100;
    const WRITERS: u64 = 8;

    let sequencer_pool = scratch.pool(8).await?;
    let mut source = logical::LogicalSource::new(
        sequencer_pool.clone(),
        config::Config::from_url(scratch.url())?,
        logical::LogicalConfig::default(),
    );
    source.bootstrap().await.context("creating the slot")?;
    let sequencer = sequencer::Sequencer::new(
        sequencer_pool.clone(),
        source,
        sequencer::SequencerConfig::default(),
    );
    let mut high_water = sequencer.watch().high_water(store.store_id());
    let (stop, stopped) = oneshot::channel::<()>();
    let sequencing = tokio::spawn(sequencer.run(async { stopped.await.unwrap_or_default() }));

    let streams: Arc<[stream::StreamId]> = (0..1000).map(|_| stream::StreamId::new_v7()).collect();
    let events: Arc<[event::BenchEvent]> =
        event::BenchEvent::batch(64, args.payload, &mut event::Rng::new(6)).into();
    let calls = args.events.div_ceil(PER_CALL);
    let mut tasks = tokio::task::JoinSet::new();
    for writer in 0..WRITERS {
        let store = store.clone();
        let streams = streams.clone();
        let events = events.clone();
        let total = args.events;
        tasks.spawn(async move {
            for call in (writer..calls).step_by(WRITERS as usize) {
                let first = call * PER_CALL;
                let entries = (first..(first + PER_CALL).min(total)).map(|n| {
                    let n = n as usize;
                    append::AppendEntry::new(
                        streams[n % streams.len()],
                        version::ExpectedVersion::Any,
                        std::slice::from_ref(&events[n % events.len()]),
                    )
                });
                store.append_many(entries).await?;
            }
            anyhow::Ok(())
        });
    }
    let mut written = Ok(());
    while let Some(task) = tasks.join_next().await {
        if let Err(error) = task.map_err(anyhow::Error::from).and_then(|result| result) {
            written = Err(error);
        }
    }

    let reached = match written {
        Ok(()) => tokio::time::timeout(
            args.limit,
            high_water.wait_for(|high_water| high_water.store().get() >= args.events),
        )
        .await
        .map(|waited| waited.map(|_| ()))
        .map_err(|_| {
            anyhow::anyhow!(
                "the log did not reach {} within {:?}",
                args.events,
                args.limit
            )
        })
        .and_then(|waited| waited.context("the sequencer is gone")),
        Err(error) => Err(error),
    };

    let _ = stop.send(());
    let ended = sequencing.await;
    sequencer_pool.close().await;
    reached?;
    ended.context("the sequencer panicked")??;

    Ok(())
}

/// Runs a subscription over `scope` from the beginning until it has checkpointed `events`. Returns how long
/// it took and how many events the handler decoded.
pub(crate) async fn catch_up(
    store: &store::Store,
    scope: Scope,
    events: u64,
    batch_size: u32,
    limit: Duration,
) -> Result<(Duration, u64)> {
    let decoded = Arc::new(AtomicU64::new(0));
    let started = Instant::now();
    let builder = store
        .subscription(match scope {
            Scope::Category => pges::subscribe::subscription_name!("bench.catch-up"),
            Scope::Store => pges::subscribe::subscription_name!("bench.catch-up-all"),
        })
        .start(subscription::Start::Beginning)
        .batch_size(std::num::NonZeroU32::new(batch_size).context("--batch-size must not be 0")?);
    let handler = CountingHandler {
        events: decoded.clone(),
    };
    let handle = match scope {
        Scope::Category => builder.category::<event::BenchEvent>().run(handler),
        Scope::Store => builder.all_categories().run(handler),
    }
    .spawn();

    let target = position::Position::new(events).context("too many events for a position")?;
    let mut status = handle.status_changes();
    let waited = tokio::time::timeout(
        limit,
        status.wait_for(|status| {
            status.position >= target || matches!(status.state, subscription::State::Stopped)
        }),
    )
    .await;
    let elapsed = started.elapsed();
    // `wait_for` hands out a read lock on the channel; it is released here, before the next await.
    let reached = match waited {
        Ok(Ok(status)) => Ok(status.position),
        Ok(Err(_)) => Err(anyhow::anyhow!("the subscription is gone")),
        Err(_) => Err(anyhow::anyhow!(
            "the subscription did not reach {target} within {limit:?}"
        )),
    };
    handle.shutdown().await?;

    let reached = reached?;
    if reached < target {
        bail!("the subscription stopped at {reached}");
    }
    Ok((elapsed, decoded.load(Ordering::Relaxed)))
}

/// What a catch-up subscription reads.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Scope {
    /// The bench category, through the category read.
    Category,
    /// Every category, through the store-wide read.
    Store,
}

/// Writes `--load-streams` streams of `length` events, one event per commit.
async fn write_streams(
    store: &store::Store,
    length: u64,
    args: &Args,
) -> Result<Arc<[stream::StreamId]>> {
    let streams: Arc<[stream::StreamId]> = (0..args.load_streams.max(1))
        .map(|_| stream::StreamId::new_v7())
        .collect();
    let events: Arc<[event::BenchEvent]> =
        event::BenchEvent::batch(64, args.payload, &mut event::Rng::new(7)).into();

    let mut tasks = tokio::task::JoinSet::new();
    for id in streams.iter().copied() {
        let store = store.clone();
        let events = events.clone();
        tasks.spawn(async move {
            for n in 0..length {
                let event = &events[n as usize % events.len()];
                store
                    .append(
                        id,
                        version::ExpectedVersion::Any,
                        std::slice::from_ref(event),
                    )
                    .await?;
            }
            anyhow::Ok(())
        });
    }
    while let Some(written) = tasks.join_next().await {
        written??;
    }

    Ok(streams)
}
