//! Spike 5, the NOTIFY commit lock: append throughput of an outbox store against a store without the outbox
//! as concurrency grows.
//!
//! Every append to an outbox store inserts into `pending` and sends `NOTIFY pges__outbox` from a trigger.
//! PostgreSQL serialises notifying commits through one server-wide lock held across the commit's WAL flush,
//! so the outbox store's throughput is expected to stop growing with concurrency earlier. No sequencer
//! runs; each (kind, concurrency) pair has a scratch database of its own.

use std::{sync::Arc, time::Duration};

use anyhow::Result;
use clap::ValueEnum;
use serde_json::json;

use pges::core::{stream, version};
use pges::postgres::store;

use crate::{append, database, event, measure, report};

/// Options of `pges-bench outbox-notify`.
#[derive(Debug, Clone, clap::Args)]
pub struct Args {
    /// Concurrency levels, comma-separated.
    #[arg(long, value_delimiter = ',', default_value = "1,4,16,64")]
    pub concurrency: Vec<usize>,
    /// How long each (kind, concurrency) pair runs.
    #[arg(long, default_value = "3s", value_parser = crate::parse_duration)]
    pub duration: Duration,
    /// How long each pair runs unmeasured first.
    #[arg(long, default_value = "1s", value_parser = crate::parse_duration)]
    pub warmup: Duration,
    /// Streams the appends spread over.
    #[arg(long, default_value_t = 10_000)]
    pub streams: usize,
    /// Bytes of payload per event.
    #[arg(long, default_value_t = 100)]
    pub payload: usize,
    /// The appenders' synchronous_commit. The NOTIFY lock is held across the commit's WAL flush, so `on`
    /// shows it most. The pges-testing servers run with it off.
    #[arg(long, value_enum, default_value = "server")]
    pub synchronous_commit: database::SynchronousCommit,
}

/// Whether the store queues its commits for the outbox source.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum Kind {
    /// A store without the outbox, as the logical source reads.
    Logical,
    /// A store with the outbox: `pending` and NOTIFY on every append.
    Outbox,
}

/// Runs both kinds at every concurrency level.
pub async fn run(server: &database::Server, args: &Args) -> Result<report::Report> {
    let mut report = report::Report::new(
        "outbox-notify",
        &[
            "sync commit",
            "conc",
            "logical ap/s",
            "outbox ap/s",
            "outbox/logical",
            "logical p99 ms",
            "outbox p99 ms",
        ],
    );
    for concurrency in &args.concurrency {
        let mut measured = Vec::new();
        for kind in [Kind::Logical, Kind::Outbox] {
            let scratch = server.scratch().await?;
            let result = measure_kind(&scratch, kind, *concurrency, args).await;
            scratch.cleanup().await?;
            let load = result?;

            let record = json!({
                "bench": "outbox-notify",
                "kind": label(kind),
                "concurrency": concurrency,
                "streams": args.streams,
                "payload_bytes": args.payload,
                "synchronous_commit": format!("{:?}", args.synchronous_commit).to_lowercase(),
                "seconds": load.elapsed.as_secs_f64(),
                "appends": load.calls,
                "appends_per_s": load.calls_per_second(),
                "latency": load.latency.summary().json(),
                "errors": load.errors,
                "first_error": load.first_error,
            });
            report::emit(&record);
            report.records.push(record);
            measured.push(load);
        }

        let [logical, outbox] = [&measured[0], &measured[1]];
        let ratio = if logical.calls_per_second() > 0.0 {
            format!(
                "{:.2}",
                outbox.calls_per_second() / logical.calls_per_second()
            )
        } else {
            "-".into()
        };
        report.table.rows.push(vec![
            format!("{:?}", args.synchronous_commit).to_lowercase(),
            concurrency.to_string(),
            report::rate(logical.calls_per_second()),
            report::rate(outbox.calls_per_second()),
            ratio,
            report::ms(logical.latency.summary().p99),
            report::ms(outbox.latency.summary().p99),
        ]);
    }

    Ok(report)
}

/// A single-event append to a random stream.
#[derive(Clone)]
struct SingleAppend {
    store: store::Store,
    streams: Arc<[stream::StreamId]>,
    events: Arc<[event::BenchEvent]>,
}

impl measure::Operation for SingleAppend {
    async fn call(&self, rng: &mut event::Rng) -> Result<u64> {
        let stream = self.streams[rng.below(self.streams.len())];
        let event = &self.events[rng.below(self.events.len())];
        self.store
            .append(
                stream,
                version::ExpectedVersion::Any,
                std::slice::from_ref(event),
            )
            .await?;

        Ok(1)
    }
}

async fn measure_kind(
    scratch: &database::Scratch,
    kind: Kind,
    concurrency: usize,
    args: &Args,
) -> Result<measure::Load> {
    let outbox = match kind {
        Kind::Logical => store::Outbox::Disabled,
        Kind::Outbox => store::Outbox::Enabled,
    };
    let pool = scratch
        .pool_with(append::connections(concurrency), args.synchronous_commit)
        .await?;
    let store =
        database::open_store(pool.clone(), pges::core::store_name!("bench"), outbox).await?;
    let mut rng = event::Rng::new(4);
    let operation = SingleAppend {
        store,
        streams: (0..args.streams.max(1))
            .map(|_| stream::StreamId::new_v7())
            .collect(),
        events: event::BenchEvent::batch(64, args.payload, &mut rng).into(),
    };
    let warmed = measure::warm_up(operation.clone(), concurrency, args.warmup).await;
    if let Err(error) = warmed {
        pool.close().await;
        return Err(error);
    }

    let load = measure::drive(
        operation,
        concurrency,
        args.duration,
        measure::Pacing::Closed,
        4,
    )
    .await;
    pool.close().await;

    load
}

fn label(kind: Kind) -> String {
    kind.to_possible_value()
        .map_or_else(|| format!("{kind:?}"), |value| value.get_name().to_owned())
}
