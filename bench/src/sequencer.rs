//! Spike 2 (throughput and lag) and spike 5 (the outbox against logical): the sequencer under sustained
//! load with each source, at payload sizes below and above the TOAST threshold.
//!
//! Each (source, payload) pair runs in a scratch database of its own, in two phases:
//!
//! 1. Drain: `--backlog` events are appended while no sequencer runs (for the logical source, after the
//!    slot is created). Then the sequencer starts, and the events it sequences from `Leading` until the
//!    store's high-water mark covers the backlog, over that time, give the sequencer's throughput.
//! 2. Sustained: writers append one event per call, due at `--rate` together, for `--duration`. The lag of
//!    an event is the time from its append returning to the in-process watch first announcing a
//!    high-water mark at or past the event's position. Each append's position is read back from the log by
//!    its commit id afterwards, so the lags are exact whatever order commits finish in.
//!
//! An append returns after its commit, so the lag excludes the append itself; an event the watch announces
//! before its append returns counts as zero lag, and the report says how many did.

use std::{
    collections::HashMap,
    sync::{Arc, Mutex, PoisonError},
    time::Duration,
};

use anyhow::{Context, Result, bail};
use clap::ValueEnum;
use serde_json::json;
use tokio::{
    sync::{oneshot, watch as channel},
    time::Instant,
};
use uuid::Uuid;

use pges::core::{high_water, stream, version};
use pges::postgres::{
    append,
    sql::{template, typed::Query as _},
    store,
};
use pges::replication::config;
use pges::sequencer::{logical, outbox, sequencer, watch};

use crate::{append as append_bench, database, event, measure, report, sql};

/// Options of `pges-bench sequencer`.
#[derive(Debug, Clone, clap::Args)]
pub struct Args {
    /// Sources to compare, comma-separated.
    #[arg(
        long,
        value_enum,
        value_delimiter = ',',
        default_value = "logical,outbox"
    )]
    pub sources: Vec<Source>,
    /// Payload sizes in bytes, comma-separated. PostgreSQL moves values above about 2 kB out of line
    /// (TOAST).
    #[arg(long, value_delimiter = ',', default_value = "100,4000")]
    pub payloads: Vec<usize>,
    /// Concurrent writers in the sustained phase.
    #[arg(long, default_value_t = 8)]
    pub concurrency: usize,
    /// Events per second all writers aim for together in the sustained phase; 0 appends as fast as they can.
    #[arg(long, default_value_t = 2000)]
    pub rate: u64,
    /// How long the sustained phase runs.
    #[arg(long, default_value = "5s", value_parser = crate::parse_duration)]
    pub duration: Duration,
    /// Events appended before the sequencer starts, for the drain phase; 0 skips it.
    #[arg(long, default_value_t = 20_000)]
    pub backlog: u64,
    /// Streams the appends spread over.
    #[arg(long, default_value_t = 10_000)]
    pub streams: usize,
    /// The writers' synchronous_commit. The pges-testing servers run with it off.
    #[arg(long, value_enum, default_value = "server")]
    pub synchronous_commit: database::SynchronousCommit,
    /// How long to wait for the sequencer to catch up after each phase before giving up.
    #[arg(long, default_value = "120s", value_parser = crate::parse_duration)]
    pub catch_up_limit: Duration,
}

/// Where the sequencer reads commits from.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum Source {
    /// Logical decoding of the `commits` inserts.
    Logical,
    /// The store's `pending` table, woken by LISTEN/NOTIFY.
    Outbox,
}

/// Runs every (source, payload) pair.
pub async fn run(server: &database::Server, args: &Args) -> Result<report::Report> {
    let mut report = report::Report::new(
        "sequencer",
        &[
            "source",
            "payload B",
            "sync commit",
            "drain ev/s",
            "target ev/s",
            "appended ev/s",
            "sequenced ev/s",
            "lag p50 ms",
            "lag p99 ms",
            "lag p99.9 ms",
            "lag max ms",
            "recovering",
        ],
    );
    for payload in &args.payloads {
        for source in &args.sources {
            let scratch = server.scratch().await?;
            let result = measure_pair(&scratch, *source, *payload, args).await;
            scratch.cleanup().await?;
            let measured = result?;

            let name = label(*source);
            let lag = measured.lag.summary();
            let drain_rate = measured
                .drain
                .map(|drain| measure::per_second(drain.events, drain.elapsed));
            let sequenced_rate = measure::per_second(measured.load.events, measured.sequenced);
            let record = json!({
                "bench": "sequencer",
                "source": name,
                "payload_bytes": payload,
                "concurrency": args.concurrency,
                "target_rate": args.rate,
                "synchronous_commit": format!("{:?}", args.synchronous_commit).to_lowercase(),
                "backlog": args.backlog,
                "drain_events": measured.drain.map(|drain| drain.events),
                "drain_seconds": measured.drain.map(|drain| drain.elapsed.as_secs_f64()),
                "drain_events_per_s": drain_rate,
                "seconds": measured.load.elapsed.as_secs_f64(),
                "events": measured.load.events,
                "appended_events_per_s": measured.load.events_per_second(),
                "sequenced_seconds": measured.sequenced.as_secs_f64(),
                "sequenced_events_per_s": sequenced_rate,
                "append_latency": measured.load.latency.summary().json(),
                "lag": lag.json(),
                "announced_before_return": measured.early,
                "recovering": measured.recovering,
                "errors": measured.load.errors,
                "first_error": measured.load.first_error,
            });
            report.push(
                record,
                vec![
                    name,
                    payload.to_string(),
                    format!("{:?}", args.synchronous_commit).to_lowercase(),
                    drain_rate.map_or_else(|| "-".into(), report::rate),
                    match args.rate {
                        0 => "max".into(),
                        rate => rate.to_string(),
                    },
                    report::rate(measured.load.events_per_second()),
                    report::rate(sequenced_rate),
                    report::ms(lag.p50),
                    report::ms(lag.p99),
                    report::ms(lag.p999),
                    report::ms(lag.max),
                    measured.recovering.to_string(),
                ],
            );
        }
    }

    Ok(report)
}

/// What one (source, payload) pair measured.
struct Measured {
    /// The backlog's sequencing; `None` without a backlog.
    drain: Option<Drain>,
    /// The sustained phase's appends.
    load: measure::Load,
    /// From the sustained phase's start until its last event was sequenced.
    sequenced: Duration,
    /// Commit-to-watch lag of each sustained event.
    lag: measure::Latency,
    /// Sustained events the watch announced before their append returned.
    early: u64,
    /// How often the sequencer reported `Recovering`.
    recovering: u64,
}

/// Events sequenced from `Leading` until the backlog was, and how long that took.
#[derive(Debug, Clone, Copy)]
struct Drain {
    events: u64,
    elapsed: Duration,
}

/// A single-event append that records its commit id and when it returned.
#[derive(Clone)]
struct TimedAppend {
    store: store::Store,
    streams: Arc<[stream::StreamId]>,
    events: Arc<[event::BenchEvent]>,
    returned: Arc<Mutex<Vec<(Uuid, Instant)>>>,
}

impl measure::Operation for TimedAppend {
    async fn call(&self, rng: &mut event::Rng) -> Result<u64> {
        let stream = self.streams[rng.below(self.streams.len())];
        let event = &self.events[rng.below(self.events.len())];
        let appended = self
            .store
            .append(
                stream,
                version::ExpectedVersion::Any,
                std::slice::from_ref(event),
            )
            .await?;
        let returned_at = Instant::now();
        self.returned
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push((appended.commit_id.as_uuid(), returned_at));

        Ok(1)
    }
}

async fn measure_pair(
    scratch: &database::Scratch,
    source: Source,
    payload: usize,
    args: &Args,
) -> Result<Measured> {
    let outbox_option = match source {
        Source::Logical => store::Outbox::Disabled,
        Source::Outbox => store::Outbox::Enabled,
    };
    let pool = scratch
        .pool_with(
            append_bench::connections(args.concurrency).max(9),
            args.synchronous_commit,
        )
        .await?;
    let store = database::open_store(
        pool.clone(),
        pges::core::store_name!("bench"),
        outbox_option,
    )
    .await?;
    let sequencer_pool = scratch.pool(8).await?;
    // At least as many streams as one backlog call names, so no call names a stream twice.
    let streams: Arc<[stream::StreamId]> = (0..args.streams.max(100))
        .map(|_| stream::StreamId::new_v7())
        .collect();
    let mut rng = event::Rng::new(3);
    let events: Arc<[event::BenchEvent]> = event::BenchEvent::batch(64, payload, &mut rng).into();

    let config = sequencer::SequencerConfig::default();
    let (stop, stopped) = oneshot::channel::<()>();
    let (watch, sequencing) = match source {
        Source::Logical => {
            let replication = config::Config::from_url(scratch.url())?;
            let mut source = logical::LogicalSource::new(
                sequencer_pool.clone(),
                replication,
                logical::LogicalConfig::default(),
            );
            source.bootstrap().await.context("creating the slot")?;
            append_backlog(&store, &streams, &events, args.backlog).await?;
            let sequencer = sequencer::Sequencer::new(sequencer_pool.clone(), source, config);
            let watch = sequencer.watch();
            (
                watch,
                tokio::spawn(sequencer.run(async { stopped.await.unwrap_or_default() })),
            )
        }
        Source::Outbox => {
            append_backlog(&store, &streams, &events, args.backlog).await?;
            let source =
                outbox::OutboxSource::new(sequencer_pool.clone(), outbox::OutboxConfig::default());
            let sequencer = sequencer::Sequencer::new(sequencer_pool.clone(), source, config);
            let watch = sequencer.watch();
            (
                watch,
                tokio::spawn(sequencer.run(async { stopped.await.unwrap_or_default() })),
            )
        }
    };
    let recovering = count_recovering(&watch);

    let measured = measure_sequenced(&store, &watch, streams, events, args).await;

    let _ = stop.send(());
    let ended = sequencing.await;
    sequencer_pool.close().await;
    pool.close().await;
    let mut measured = measured?;
    ended.context("the sequencer panicked")??;
    measured.recovering = *recovering.borrow();

    Ok(measured)
}

async fn measure_sequenced(
    store: &store::Store,
    watch: &watch::Watch,
    streams: Arc<[stream::StreamId]>,
    events: Arc<[event::BenchEvent]>,
    args: &Args,
) -> Result<Measured> {
    let mut high_water = watch.high_water(store.store_id());

    leading(watch, args.catch_up_limit).await?;
    let drain = if args.backlog > 0 {
        let started = Instant::now();
        let already = high_water.borrow().store().get();
        reach(&mut high_water, args.backlog, args.catch_up_limit).await?;
        Some(Drain {
            events: args.backlog.saturating_sub(already),
            elapsed: started.elapsed(),
        })
    } else {
        None
    };

    let returned = Arc::new(Mutex::new(Vec::new()));
    let (stop_timeline, timeline_stopped) = oneshot::channel::<()>();
    let timeline = tokio::spawn(record_timeline(
        watch.high_water(store.store_id()),
        timeline_stopped,
    ));
    let operation = TimedAppend {
        store: store.clone(),
        streams,
        events,
        returned: returned.clone(),
    };
    let pacing = match args.rate {
        0 => measure::Pacing::Closed,
        rate => measure::Pacing::Every(Duration::from_secs_f64(
            args.concurrency.max(1) as f64 / rate as f64,
        )),
    };
    let started = Instant::now();
    let load = measure::drive(operation, args.concurrency, args.duration, pacing, 3).await?;
    let returned = std::mem::take(&mut *returned.lock().unwrap_or_else(PoisonError::into_inner));
    let positions = sustained_positions(store, &returned, args.catch_up_limit).await?;
    let reached = reach(
        &mut high_water,
        positions.iter().copied().max().unwrap_or(0),
        args.catch_up_limit,
    )
    .await;
    let sequenced = started.elapsed();
    let _ = stop_timeline.send(());
    let timeline = timeline.await.context("the watch recorder panicked")?;
    reached?;

    let mut lag = measure::Latency::new()?;
    let mut early = 0;
    for ((_, returned_at), position) in returned.iter().zip(&positions) {
        let index = timeline.partition_point(|(_, high_water)| high_water < position);
        let Some((announced_at, _)) = timeline.get(index) else {
            bail!("the watch never announced position {position}");
        };
        if announced_at < returned_at {
            early += 1;
        }
        lag.record(announced_at.saturating_duration_since(*returned_at));
    }

    Ok(Measured {
        drain,
        load,
        sequenced,
        lag,
        early,
        recovering: 0,
    })
}

/// The log position of each sustained append, in the order of `returned`.
async fn sustained_positions(
    store: &store::Store,
    returned: &[(Uuid, Instant)],
    limit: Duration,
) -> Result<Vec<u64>> {
    let ids: Vec<Uuid> = returned.iter().map(|(id, _)| *id).collect();
    let mut positions = HashMap::with_capacity(ids.len());
    let deadline = Instant::now() + limit;
    let positions_sql = sql::statement::positions::Query::render(
        &template::Values::new().store(pges::postgres::sql::store_name(store)),
    );
    // The sequencer may still be catching up: wait until every commit is in the log.
    loop {
        let rows = positions_sql
            .bind(sql::statement::positions::Params { commit_ids: &ids })
            .fetch_all(store.pool())
            .await
            .context("reading the sustained commits' positions")?;
        positions.extend(
            rows.into_iter()
                .map(|row| (row.commit_id, row.last_position)),
        );
        if positions.len() >= ids.len() {
            break;
        }
        if Instant::now() >= deadline {
            bail!(
                "{} of {} sustained commits reached the log within {limit:?}",
                positions.len(),
                ids.len()
            );
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    ids.iter()
        .map(|id| {
            let position = positions
                .get(id)
                .context("a sustained commit is missing from the log")?;
            u64::try_from(*position).context("a negative log position")
        })
        .collect()
}

/// Appends `count` single-event commits in batches of `append_many`, before the sequencer runs.
async fn append_backlog(
    store: &store::Store,
    streams: &Arc<[stream::StreamId]>,
    events: &Arc<[event::BenchEvent]>,
    count: u64,
) -> Result<()> {
    const PER_CALL: u64 = 100;
    const WRITERS: u64 = 8;

    let calls = count.div_ceil(PER_CALL);
    let mut tasks = tokio::task::JoinSet::new();
    for writer in 0..WRITERS {
        let store = store.clone();
        let streams = streams.clone();
        let events = events.clone();
        tasks.spawn(async move {
            for call in (writer..calls).step_by(WRITERS as usize) {
                let first = call * PER_CALL;
                let last = (first + PER_CALL).min(count);
                // Consecutive streams, so one call never names a stream twice.
                let entries = (first..last).map(|n| {
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
    while let Some(written) = tasks.join_next().await {
        written??;
    }

    Ok(())
}

/// Records when the watch announces each high-water mark of the store, until `stop` fires. The marks only
/// grow, so the timeline is sorted by both time and mark.
async fn record_timeline(
    mut high_water: channel::Receiver<high_water::HighWater>,
    mut stop: oneshot::Receiver<()>,
) -> Vec<(Instant, u64)> {
    let mut timeline = Vec::new();

    loop {
        let stopping = tokio::select! {
            changed = high_water.changed() => changed.is_err(),
            _ = &mut stop => true,
        };
        let now = Instant::now();
        let reached = high_water.borrow_and_update().store().get();
        let advanced = timeline.last().is_none_or(|(_, last)| reached > *last);
        if advanced {
            timeline.push((now, reached));
        }
        if stopping {
            return timeline;
        }
    }
}

/// Counts `Recovering` statuses in the background. The count is in the returned receiver.
fn count_recovering(watch: &watch::Watch) -> channel::Receiver<u64> {
    let mut status = watch.status();
    let (count, counted) = channel::channel(0_u64);
    tokio::spawn(async move {
        while status.changed().await.is_ok() {
            let recovering = matches!(
                *status.borrow_and_update(),
                sequencer::Status::Recovering(_)
            );
            if recovering {
                count.send_modify(|count| *count += 1);
            }
        }
    });

    counted
}

/// Waits until the sequencer leads; fails if its run ends or after `limit`.
async fn leading(watch: &watch::Watch, limit: Duration) -> Result<()> {
    let mut status = watch.status();
    let waited = tokio::time::timeout(
        limit,
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
        Err(_) => bail!("the sequencer did not lead within {limit:?}"),
    };

    match status {
        sequencer::Status::Leading(_) => Ok(()),
        other => bail!("the sequencer stopped: {other:?}"),
    }
}

/// Waits until the store's high-water mark reaches `position`.
async fn reach(
    high_water: &mut channel::Receiver<high_water::HighWater>,
    position: u64,
    limit: Duration,
) -> Result<()> {
    let waited = tokio::time::timeout(
        limit,
        high_water.wait_for(|high_water| high_water.store().get() >= position),
    )
    .await
    // `wait_for` hands out a read lock on the channel; release it before reading the channel again.
    .map(|waited| waited.map(|_| ()));

    match waited {
        Ok(Ok(())) => Ok(()),
        Ok(Err(_)) => bail!("the sequencer is gone"),
        Err(_) => {
            let at = high_water.borrow().store().get();
            bail!("the log reached {at}, not {position}, within {limit:?}")
        }
    }
}

fn label(source: Source) -> String {
    source.to_possible_value().map_or_else(
        || format!("{source:?}"),
        |value| value.get_name().to_owned(),
    )
}
