//! Spike 1: append throughput and latency, one hot stream against many streams, single appends against
//! batches, and the WAL each event costs, with the full-page-image share.
//!
//! Each scenario runs in a scratch database of its own. The many-stream scenarios first write one event to
//! every stream, so the measured appends update existing streams. No sequencer runs.

use std::{sync::Arc, time::Duration};

use anyhow::{Result, bail};
use clap::ValueEnum;
use serde_json::json;
use sqlx::Executor as _;

use pges::core::{stream, version};
use pges::postgres::{append, store};

use crate::{database, event, measure, report, sql, wal};

/// Options of `pges-bench append`.
#[derive(Debug, Clone, clap::Args)]
pub struct Args {
    /// Scenarios to run, comma-separated.
    #[arg(
        long,
        value_enum,
        value_delimiter = ',',
        default_value = "hot-single,hot-batch,many-single,many-batch"
    )]
    pub scenarios: Vec<Scenario>,
    /// Streams the many-stream scenarios spread their appends over.
    #[arg(long, default_value_t = 100_000)]
    pub streams: usize,
    /// Concurrent appenders, each with a connection of its own.
    #[arg(long, default_value_t = 16)]
    pub concurrency: usize,
    /// How long each scenario runs.
    #[arg(long, default_value = "5s", value_parser = crate::parse_duration)]
    pub duration: Duration,
    /// Events per call in the batch scenarios.
    #[arg(long, default_value_t = 10)]
    pub batch: usize,
    /// Bytes of payload per event.
    #[arg(long, default_value_t = 100)]
    pub payload: usize,
    /// How long each scenario runs unmeasured first, after the prefill and before the checkpoint.
    #[arg(long, default_value = "1s", value_parser = crate::parse_duration)]
    pub warmup: Duration,
    /// Whether to run CHECKPOINT before each scenario.
    #[arg(long, value_enum, default_value = "before")]
    pub checkpoint: wal::Checkpoint,
    /// Whether the tables are analysed after the prefill.
    #[arg(long, value_enum, default_value = "analyze")]
    pub statistics: Statistics,
}

/// Whether the benchmark gathers planner statistics after the prefill.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum Statistics {
    /// `ANALYZE` after the prefill, as autovacuum would eventually: plans cached during the prefill are
    /// invalidated.
    Analyze,
    /// Only what autovacuum gathers during the run: the measured appends start with the plans the prefill's
    /// connections cached, as on a store that was just created and filled.
    Autovacuum,
}

/// What is appended, and where.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum Scenario {
    /// One event per call, all to one stream.
    HotSingle,
    /// `--batch` events per call, in one commit to one stream.
    HotBatch,
    /// One event per call, to a random stream of `--streams`.
    ManySingle,
    /// `append_many` of `--batch` streams per call, one event each, from a random offset of `--streams`.
    ManyBatch,
    /// `--batch` events per call, in one commit to a random stream of `--streams`.
    ManyEvents,
}

/// Runs the scenarios.
pub async fn run(server: &database::Server, args: &Args) -> Result<report::Report> {
    if args.batch == 0 || args.streams < args.batch {
        bail!("--batch must be at least 1 and at most --streams");
    }

    let mut report = report::Report::new(
        "append",
        &[
            "scenario",
            "conc",
            "calls/s",
            "events/s",
            "p50 ms",
            "p99 ms",
            "p99.9 ms",
            "WAL B/event",
            "FPI share",
            "errors",
        ],
    );
    for scenario in &args.scenarios {
        let scratch = server.scratch().await?;
        let result = measure_scenario(&scratch, *scenario, args).await;
        scratch.cleanup().await?;
        let (load, usage) = result?;

        let name = label(*scenario);
        let summary = load.latency.summary();
        let record = json!({
            "bench": "append",
            "scenario": name,
            "concurrency": args.concurrency,
            "streams": streams_for(*scenario, args),
            "batch": events_per_call(*scenario, args),
            "payload_bytes": args.payload,
            "checkpoint": format!("{:?}", args.checkpoint).to_lowercase(),
            "statistics": format!("{:?}", args.statistics).to_lowercase(),
            "seconds": load.elapsed.as_secs_f64(),
            "calls": load.calls,
            "events": load.events,
            "calls_per_s": load.calls_per_second(),
            "events_per_s": load.events_per_second(),
            "latency": summary.json(),
            "errors": load.errors,
            "first_error": load.first_error,
            "wal": usage.json(load.events),
        });
        let [p50, p99, p999] = report::latency_cells(&summary);
        let wal_per_event = if load.events > 0 {
            format!("{:.0}", usage.bytes as f64 / load.events as f64)
        } else {
            "-".into()
        };
        let fpi_share = usage
            .fpi_share()
            .map_or_else(|| "-".into(), |share| format!("{:.0}%", share * 100.0));
        report.push(
            record,
            vec![
                name,
                args.concurrency.to_string(),
                report::rate(load.calls_per_second()),
                report::rate(load.events_per_second()),
                p50,
                p99,
                p999,
                wal_per_event,
                fpi_share,
                load.errors.to_string(),
            ],
        );
    }

    Ok(report)
}

/// An append of one scenario, run by every worker.
#[derive(Clone)]
struct AppendOperation {
    store: store::Store,
    scenario: Scenario,
    streams: Arc<[stream::StreamId]>,
    events: Arc<[event::BenchEvent]>,
}

impl measure::Operation for AppendOperation {
    async fn call(&self, rng: &mut event::Rng) -> Result<u64> {
        let any = version::ExpectedVersion::Any;
        let events = &self.events[..];

        match self.scenario {
            Scenario::HotSingle => {
                self.store
                    .append(self.streams[0], any, &events[..1])
                    .await?;
                Ok(1)
            }
            Scenario::HotBatch => {
                self.store.append(self.streams[0], any, events).await?;
                Ok(events.len() as u64)
            }
            Scenario::ManySingle => {
                let stream = self.streams[rng.below(self.streams.len())];
                self.store.append(stream, any, &events[..1]).await?;
                Ok(1)
            }
            Scenario::ManyEvents => {
                let stream = self.streams[rng.below(self.streams.len())];
                self.store.append(stream, any, events).await?;
                Ok(events.len() as u64)
            }
            Scenario::ManyBatch => {
                let start = rng.below(self.streams.len());
                let entries = (0..events.len()).map(|n| {
                    let stream = self.streams[(start + n) % self.streams.len()];
                    append::AppendEntry::new(stream, any, &events[n..=n])
                });
                self.store.append_many(entries).await?;
                Ok(events.len() as u64)
            }
        }
    }
}

async fn measure_scenario(
    scratch: &database::Scratch,
    scenario: Scenario,
    args: &Args,
) -> Result<(measure::Load, wal::Usage)> {
    let marks = scratch.pool(1).await?;
    wal::prepare(&marks).await;
    let connections = connections(args.concurrency);
    let pool = scratch.pool(connections).await?;
    let store = database::open_store(
        pool.clone(),
        pges::core::store_name!("bench"),
        store::Outbox::Disabled,
    )
    .await?;

    let mut rng = event::Rng::new(1);
    let streams: Arc<[stream::StreamId]> = (0..streams_for(scenario, args))
        .map(|_| stream::StreamId::new_v7())
        .collect();
    prefill(&store, &streams, args.payload).await?;
    if args.statistics == Statistics::Analyze {
        marks.execute(sql::statement::ANALYZE.sql()).await?;
    }
    let operation = AppendOperation {
        store,
        scenario,
        streams,
        events: event::BenchEvent::batch(events_per_call(scenario, args), args.payload, &mut rng)
            .into(),
    };
    measure::warm_up(operation.clone(), args.concurrency, args.warmup).await?;
    wal::flush_stats(&pool, connections).await?;

    let start = wal::mark(&marks, args.checkpoint).await?;
    let load = measure::drive(
        operation,
        args.concurrency,
        args.duration,
        measure::Pacing::Closed,
        1,
    )
    .await?;
    pool.close().await;
    let usage = wal::usage(&marks, &start).await?;
    marks.close().await;

    Ok((load, usage))
}

/// Writes one event to each stream, so the measured appends update streams that exist.
async fn prefill(
    store: &store::Store,
    streams: &Arc<[stream::StreamId]>,
    payload: usize,
) -> Result<()> {
    const CHUNK: usize = 500;
    const WRITERS: usize = 8;

    let events = event::BenchEvent::batch(CHUNK, payload, &mut event::Rng::new(2));
    let events: Arc<[event::BenchEvent]> = events.into();
    let chunks = streams.len().div_ceil(CHUNK);
    let mut tasks = tokio::task::JoinSet::new();
    for writer in 0..WRITERS {
        let store = store.clone();
        let streams = streams.clone();
        let events = events.clone();
        tasks.spawn(async move {
            for chunk in (writer..chunks).step_by(WRITERS) {
                let ids = &streams[chunk * CHUNK..((chunk + 1) * CHUNK).min(streams.len())];
                let entries = ids.iter().zip(events.iter()).map(|(id, event)| {
                    append::AppendEntry::new(
                        *id,
                        version::ExpectedVersion::NoStream,
                        std::slice::from_ref(event),
                    )
                });
                store.append_many(entries).await?;
            }
            anyhow::Ok(())
        });
    }
    while let Some(written) = tasks.join_next().await {
        written??;
    }

    Ok(())
}

fn label(scenario: Scenario) -> String {
    scenario.to_possible_value().map_or_else(
        || format!("{scenario:?}"),
        |value| value.get_name().to_owned(),
    )
}

fn streams_for(scenario: Scenario, args: &Args) -> usize {
    match scenario {
        Scenario::HotSingle | Scenario::HotBatch => 1,
        Scenario::ManySingle | Scenario::ManyBatch | Scenario::ManyEvents => args.streams,
    }
}

fn events_per_call(scenario: Scenario, args: &Args) -> usize {
    match scenario {
        Scenario::HotSingle | Scenario::ManySingle => 1,
        Scenario::HotBatch | Scenario::ManyBatch | Scenario::ManyEvents => args.batch,
    }
}

/// Pool size for `concurrency` workers, with one spare.
pub(crate) fn connections(concurrency: usize) -> u32 {
    u32::try_from(concurrency.saturating_add(1)).unwrap_or(u32::MAX)
}
