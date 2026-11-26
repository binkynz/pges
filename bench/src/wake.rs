//! Commit to subscriber: how long after an append returns its event reaches the handlers of live
//! subscriptions, woken by the sequencer's in-process watch or its gRPC watch, and what many subscriptions on
//! one store cost.
//!
//! For each waker and each `--subscriptions` count, in a scratch database of its own: a sequencer with the
//! logical source leads (for the gRPC waker, a gRPC watch server serves it on a local port), and that many
//! subscriptions of the bench category run from the beginning on a pool of `--subscriber-connections`, each
//! with a waker of its own and the default batch size. After `--warmup`, writers append single events at
//! `--rate` events per second together, open loop, for `--duration`. Then the run waits until every
//! subscription has handled every event.
//!
//! The delivery lag of an event is the time from its append returning to a handler receiving it, recorded for
//! every subscription: the sequencer's lag, the wake-up and the subscription's read. An event a handler
//! receives before its append returns counts as zero lag, and the report says how many did. The subscriber
//! pool is sampled for connections in use, and the subscriptions' states for `Recovering`.

use std::{
    collections::HashMap,
    sync::{
        Arc, Mutex, PoisonError, RwLock,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Duration,
};

use anyhow::{Context, Result, bail};
use clap::ValueEnum;
use serde_json::json;
use tokio::{sync::oneshot, time::Instant};
use uuid::Uuid;

use pges::core::{non_empty, position, stream, version};
use pges::postgres::{sql::typed::Query as _, store};
use pges::replication::config;
use pges::sequencer::{grpc, logical, sequencer};
use pges::subscribe::{
    batch, handler,
    subscription::{self, StoreExt},
    waker,
};

use crate::{database, event, measure, report};

/// Options of `pges-bench wake`.
#[derive(Debug, Clone, clap::Args)]
pub struct Args {
    /// Wakers to compare, comma-separated.
    #[arg(long, value_enum, value_delimiter = ',', default_value = "inproc,grpc")]
    pub wakers: Vec<Waker>,
    /// Live subscriptions of the bench category, comma-separated: one measurement per count.
    #[arg(long, value_delimiter = ',', default_value = "1")]
    pub subscriptions: Vec<usize>,
    /// Connections the subscriptions share.
    #[arg(long, default_value_t = 20)]
    pub subscriber_connections: u32,
    /// Events per second all writers aim for together.
    #[arg(long, default_value_t = 1000)]
    pub rate: u64,
    /// Concurrent writers.
    #[arg(long, default_value_t = 16)]
    pub concurrency: usize,
    /// How long the writers append.
    #[arg(long, default_value = "10s", value_parser = crate::parse_duration)]
    pub duration: Duration,
    /// How long the writers append unmeasured first, once every subscription runs.
    #[arg(long, default_value = "2s", value_parser = crate::parse_duration)]
    pub warmup: Duration,
    /// Streams the appends spread over.
    #[arg(long, default_value_t = 10_000)]
    pub streams: usize,
    /// Bytes of payload per event.
    #[arg(long, default_value_t = 100)]
    pub payload: usize,
    /// How long to wait for every subscription to handle every event before giving up.
    #[arg(long, default_value = "120s", value_parser = crate::parse_duration)]
    pub catch_up_limit: Duration,
}

/// How the subscriptions learn that the log grew.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum Waker {
    /// The sequencer's in-process watch channel, in the same process.
    Inproc,
    /// The sequencer's gRPC watch, one stream per subscription.
    Grpc,
}

/// Runs every (waker, subscriptions) pair.
pub async fn run(server: &database::Server, args: &Args) -> Result<report::Report> {
    let mut report = report::Report::new(
        "wake",
        &[
            "waker",
            "subs",
            "target ev/s",
            "appended ev/s",
            "delivered ev/s",
            "lag p50 ms",
            "lag p99 ms",
            "lag p99.9 ms",
            "lag max ms",
            "conns in use",
            "recovering",
        ],
    );
    for subscriptions in &args.subscriptions {
        for waker in &args.wakers {
            let scratch = server.scratch().await?;
            let result = measure_pair(&scratch, *waker, *subscriptions, args).await;
            scratch.cleanup().await?;
            let measured = result?;

            let name = label(*waker);
            let lag = measured.lag.summary();
            let delivered_rate = measure::per_second(measured.delivered, measured.delivering);
            report.push(
                json!({
                    "bench": "wake",
                    "waker": name,
                    "subscriptions": subscriptions,
                    "subscriber_connections": args.subscriber_connections,
                    "target_rate": args.rate,
                    "concurrency": args.concurrency,
                    "seconds": measured.load.elapsed.as_secs_f64(),
                    "events": measured.load.events,
                    "appended_events_per_s": measured.load.events_per_second(),
                    "append_latency": measured.load.latency.summary().json(),
                    "delivered": measured.delivered,
                    "delivering_seconds": measured.delivering.as_secs_f64(),
                    "delivered_events_per_s": delivered_rate,
                    "lag": lag.json(),
                    "delivered_before_return": measured.early,
                    "connections_in_use_max": measured.in_use_max,
                    "connections_in_use_mean": measured.in_use_mean,
                    "recovering": measured.recovering,
                    "errors": measured.load.errors,
                    "first_error": measured.load.first_error,
                }),
                vec![
                    name,
                    subscriptions.to_string(),
                    args.rate.to_string(),
                    report::rate(measured.load.events_per_second()),
                    report::rate(delivered_rate),
                    report::ms(lag.p50),
                    report::ms(lag.p99),
                    report::ms(lag.p999),
                    report::ms(lag.max),
                    format!("{:.1} / {}", measured.in_use_mean, measured.in_use_max),
                    measured.recovering.to_string(),
                ],
            );
        }
    }

    Ok(report)
}

/// What one (waker, subscriptions) pair measured.
struct Measured {
    /// The measured appends.
    load: measure::Load,
    /// Events the handlers received, over every subscription.
    delivered: u64,
    /// From the measured appends' start until the last subscription handled the last event.
    delivering: Duration,
    /// Delivery lag of every measured event in every subscription.
    lag: measure::Latency,
    /// Deliveries before the append returned.
    early: u64,
    /// The most subscriber connections in use at once, and the mean, from samples.
    in_use_max: u32,
    in_use_mean: f64,
    /// How often a subscription reported `Recovering`.
    recovering: u64,
}

async fn measure_pair(
    scratch: &database::Scratch,
    waker: Waker,
    subscriptions: usize,
    args: &Args,
) -> Result<Measured> {
    let writer_pool = scratch
        .pool(u32::try_from(args.concurrency)?.max(4))
        .await?;
    let store = database::open_store(
        writer_pool.clone(),
        pges::core::store_name!("bench"),
        store::Outbox::Disabled,
    )
    .await?;
    let subscriber_pool = scratch.pool(args.subscriber_connections).await?;
    let subscriber_store = store::Store::open(
        subscriber_pool.clone(),
        store::StoreConfig::new(pges::core::store_name!("bench")),
    )
    .await
    .context("opening the subscribers' store")?;

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
    let watch = sequencer.watch();
    let (stop_sequencer, sequencer_stopped) = oneshot::channel::<()>();
    let sequencing =
        tokio::spawn(sequencer.run(async { sequencer_stopped.await.unwrap_or_default() }));

    let (stop_server, server_stopped) = oneshot::channel::<()>();
    let (serving, endpoint) = match waker {
        Waker::Inproc => (None, None),
        Waker::Grpc => {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
            let endpoint: waker::grpc::Endpoint = format!("http://{}", listener.local_addr()?)
                .parse()
                .context("the watch server's endpoint")?;
            let serving = tokio::spawn(grpc::WatchService::new(watch.clone()).serve(
                listener,
                async {
                    let _ = server_stopped.await;
                },
            ));
            (Some(serving), Some(endpoint))
        }
    };

    let measured = measure_subscriptions(
        &store,
        &subscriber_store,
        &watch,
        endpoint.as_ref(),
        subscriptions,
        args,
    )
    .await;

    let _ = stop_sequencer.send(());
    let ended = sequencing.await;
    let _ = stop_server.send(());
    if let Some(serving) = serving {
        serving.await.context("the watch server panicked")??;
    }
    subscriber_pool.close().await;
    sequencer_pool.close().await;
    writer_pool.close().await;
    let measured = measured?;
    ended.context("the sequencer panicked")??;

    Ok(measured)
}

async fn measure_subscriptions(
    store: &store::Store,
    subscriber_store: &store::Store,
    watch: &pges::sequencer::watch::Watch,
    endpoint: Option<&waker::grpc::Endpoint>,
    subscriptions: usize,
    args: &Args,
) -> Result<Measured> {
    let returned: Returned = Arc::new(RwLock::new(HashMap::new()));
    let measuring = Arc::new(AtomicBool::new(false));
    let recovering = Arc::new(AtomicU64::new(0));
    let mut handles = Vec::with_capacity(subscriptions);
    let mut handlers = Vec::with_capacity(subscriptions);
    for n in 0..subscriptions {
        let name = subscription::SubscriptionName::parse(format!("bench.wake-{n}"))
            .context("a subscription name")?;
        let delivered = Arc::new(Mutex::new(Delivered::new()?));
        let handler = LagHandler {
            returned: returned.clone(),
            measuring: measuring.clone(),
            delivered: delivered.clone(),
        };
        let builder = subscriber_store
            .subscription(name)
            .start(subscription::Start::Beginning)
            .category::<event::BenchEvent>();
        let builder = match endpoint {
            None => builder.waker(waker::inproc::InprocWaker::new(
                watch.high_water(store.store_id()),
            )),
            Some(endpoint) => builder.waker(waker::grpc::GrpcWaker::new(
                store.store_id(),
                non_empty![endpoint.clone()],
            )),
        };
        let handle = builder.run(handler).spawn();
        count_recovering(handle.status_changes(), recovering.clone());
        handles.push(handle);
        handlers.push(delivered);
    }

    let streams: Arc<[stream::StreamId]> = (0..args.streams.max(1))
        .map(|_| stream::StreamId::new_v7())
        .collect();
    let mut rng = event::Rng::new(5);
    let operation = TimedAppend {
        store: store.clone(),
        streams,
        events: event::BenchEvent::batch(64, args.payload, &mut rng).into(),
        returned: returned.clone(),
        measuring: measuring.clone(),
    };
    let pacing = measure::Pacing::Every(Duration::from_secs_f64(
        args.concurrency.max(1) as f64 / args.rate.max(1) as f64,
    ));
    if !args.warmup.is_zero() {
        measure::drive(operation.clone(), args.concurrency, args.warmup, pacing, 7).await?;
    }

    let (stop_sampler, sampler_stopped) = oneshot::channel::<()>();
    let sampler = tokio::spawn(sample_in_use(
        subscriber_store.pool().clone(),
        sampler_stopped,
    ));
    measuring.store(true, Ordering::Relaxed);
    let started = Instant::now();
    let load = measure::drive(operation, args.concurrency, args.duration, pacing, 9).await?;
    measuring.store(false, Ordering::Relaxed);

    let head = head_after(store.pool(), &returned, args.catch_up_limit).await?;
    let reached = every_subscription_reaches(&handles, head, args.catch_up_limit).await;
    let delivering = started.elapsed();
    let _ = stop_sampler.send(());
    let (in_use_max, in_use_mean) = sampler.await.context("the pool sampler panicked")?;
    for handle in handles {
        handle.shutdown().await?;
    }
    reached?;

    let mut lag = measure::Latency::new()?;
    let mut delivered = 0;
    let mut early = 0;
    let returned = returned.read().unwrap_or_else(PoisonError::into_inner);
    for handler in handlers {
        let handler = handler.lock().unwrap_or_else(PoisonError::into_inner);
        lag.add(&handler.lag)?;
        delivered += handler.count;
        for (commit_id, _) in &handler.before_return {
            if returned.contains_key(commit_id) {
                early += 1;
                delivered += 1;
                lag.record(Duration::ZERO);
            }
        }
    }

    Ok(Measured {
        load,
        delivered,
        delivering,
        lag,
        early,
        in_use_max,
        in_use_mean,
        recovering: recovering.load(Ordering::Relaxed),
    })
}

/// When each measured append returned, by commit id.
type Returned = Arc<RwLock<HashMap<Uuid, Instant>>>;

/// What one subscription's handler saw of the measured events.
struct Delivered {
    lag: measure::Latency,
    count: u64,
    /// Events received before any append had returned with their commit id: measured ones count as zero
    /// lag at the end, warm-up ones not at all.
    before_return: Vec<(Uuid, Instant)>,
}

impl Delivered {
    fn new() -> Result<Self> {
        Ok(Self {
            lag: measure::Latency::new()?,
            count: 0,
            before_return: Vec::new(),
        })
    }
}

/// Records each event's delivery lag.
struct LagHandler {
    returned: Returned,
    measuring: Arc<AtomicBool>,
    delivered: Arc<Mutex<Delivered>>,
}

impl handler::Handler for LagHandler {
    async fn handle(&mut self, batch: &batch::Batch) -> Result<(), handler::HandlerError> {
        let now = Instant::now();
        let returned = self.returned.read().unwrap_or_else(PoisonError::into_inner);
        let mut delivered = self
            .delivered
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        for recorded in batch.iter() {
            let batch::RawRecorded::Event(event) = recorded else {
                continue;
            };
            let commit_id = event.commit_id.as_uuid();
            match returned.get(&commit_id) {
                Some(at) => {
                    delivered.count += 1;
                    delivered.lag.record(now.saturating_duration_since(*at));
                }
                None if self.measuring.load(Ordering::Relaxed) => {
                    delivered.before_return.push((commit_id, now));
                }
                None => {}
            }
        }

        Ok(())
    }
}

/// A single-event append that, while measuring, records when it returned.
#[derive(Clone)]
struct TimedAppend {
    store: store::Store,
    streams: Arc<[stream::StreamId]>,
    events: Arc<[event::BenchEvent]>,
    returned: Returned,
    measuring: Arc<AtomicBool>,
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
        if self.measuring.load(Ordering::Relaxed) {
            self.returned
                .write()
                .unwrap_or_else(PoisonError::into_inner)
                .insert(appended.commit_id.as_uuid(), Instant::now());
        }

        Ok(1)
    }
}

/// The store's newest position once every measured commit is in the log.
async fn head_after(pool: &sqlx::PgPool, returned: &Returned, limit: Duration) -> Result<u64> {
    let ids: Vec<Uuid> = returned
        .read()
        .unwrap_or_else(PoisonError::into_inner)
        .keys()
        .copied()
        .collect();
    let positions = crate::sql::statement::positions::Query::render(
        &pges::postgres::sql::template::Values::new().store(&pges::core::store_name!("bench")),
    );
    let deadline = Instant::now() + limit;
    loop {
        let rows = positions
            .bind(crate::sql::statement::positions::Params { commit_ids: &ids })
            .fetch_all(pool)
            .await
            .context("reading the measured commits' positions")?;
        if rows.len() >= ids.len() {
            let newest = rows.iter().map(|row| row.last_position).max().unwrap_or(0);
            return u64::try_from(newest).context("a negative log position");
        }
        if Instant::now() >= deadline {
            bail!(
                "{} of {} measured commits reached the log within {limit:?}",
                rows.len(),
                ids.len()
            );
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Waits until every subscription's position reaches `head`.
async fn every_subscription_reaches(
    handles: &[subscription::SubscriptionHandle],
    head: u64,
    limit: Duration,
) -> Result<()> {
    let target = position::Position::new(head).context("a position")?;
    let deadline = Instant::now() + limit;
    for handle in handles {
        let mut status = handle.status_changes();
        let left = deadline.saturating_duration_since(Instant::now());
        let waited = tokio::time::timeout(
            left,
            status.wait_for(|status| {
                status.position >= target || matches!(status.state, subscription::State::Stopped)
            }),
        )
        .await;
        // `wait_for` hands out a read lock on the channel; it is released here, before the next await.
        let reached = match waited {
            Ok(Ok(status)) => status.position,
            Ok(Err(_)) => bail!("a subscription is gone"),
            Err(_) => bail!("a subscription did not reach {head} within {limit:?}"),
        };
        if reached < target {
            bail!("a subscription stopped at {reached}");
        }
    }

    Ok(())
}

/// Counts `Recovering` states of one subscription in the background.
fn count_recovering(
    mut status: tokio::sync::watch::Receiver<subscription::Status>,
    count: Arc<AtomicU64>,
) {
    tokio::spawn(async move {
        while status.changed().await.is_ok() {
            let recovering = matches!(
                status.borrow_and_update().state,
                subscription::State::Recovering { .. }
            );
            if recovering {
                count.fetch_add(1, Ordering::Relaxed);
            }
        }
    });
}

/// Samples the pool's connections in use every 5 ms until `stop` fires; returns the most and the mean.
async fn sample_in_use(pool: sqlx::PgPool, mut stop: oneshot::Receiver<()>) -> (u32, f64) {
    let mut most = 0;
    let mut total = 0_u64;
    let mut samples = 0_u64;
    loop {
        let in_use = pool
            .size()
            .saturating_sub(u32::try_from(pool.num_idle()).unwrap_or(u32::MAX));
        most = most.max(in_use);
        total += u64::from(in_use);
        samples += 1;
        tokio::select! {
            _ = &mut stop => break,
            () = tokio::time::sleep(Duration::from_millis(5)) => {}
        }
    }

    (most, total as f64 / samples.max(1) as f64)
}

fn label(waker: Waker) -> String {
    waker
        .to_possible_value()
        .map_or_else(|| format!("{waker:?}"), |value| value.get_name().to_owned())
}
