//! Shared fixtures: a store with three categories, a hand-driven "sequencer" that writes log rows directly,
//! a recording projection, and checks on what it recorded.

#![allow(dead_code, clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
    time::Duration,
};

use anyhow::{Context, Result, bail, ensure};
use serde::{Deserialize, Serialize};
use sqlx::{Executor, PgConnection, PgPool, Row};
use tokio::{
    sync::{oneshot, watch},
    task::JoinHandle,
};
use uuid::Uuid;

use pges_core::{category, high_water, stream, version};
use pges_postgres::{admin, append, store};
use pges_sequencer::{outbox, sequencer};
use pges_subscribe::{batch, handler, subscription};

pub const STORE: &str = "es";
pub const STORE_NAME: pges_core::store::StoreName = pges_core::store_name!("es");

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, pges_derive::Event)]
#[event(category = "jobs")]
pub enum JobEvent {
    #[event(name = "job.noted")]
    Noted { n: u32 },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, pges_derive::Event)]
#[event(category = "orders")]
pub enum OrderEvent {
    #[event(name = "order.noted")]
    Noted { n: u32 },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, pges_derive::Event)]
#[event(category = "parts")]
pub enum PartEvent {
    #[event(name = "part.noted")]
    Noted { n: u32 },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Cat {
    Jobs,
    Orders,
    Parts,
}

pub struct Fixture {
    pub db: pges_testing::TestDb,
    pub pool: PgPool,
    pub store: store::Store,
    pub ids: BTreeMap<String, category::CategoryId>,
}

impl Fixture {
    /// A store with `jobs`, `orders` and `parts`, their partitions and a log partition, and a `delivered`
    /// table for [`Recorder`].
    pub async fn new(outbox: store::Outbox) -> Result<Self> {
        let db = pges_testing::TestDb::new().await?;
        let mut conn = db.connect().await?;
        pges_testing::create_store(&mut conn, &STORE_NAME, &pges_testing::definition(outbox))
            .await?;
        conn.execute(
            "CREATE TABLE public.delivered (
               seq         int8 GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
               position    int8 NOT NULL,
               commit_id   uuid NOT NULL,
               category_id int4 NOT NULL,
               subscriber  text NOT NULL DEFAULT ''
             )",
        )
        .await?;
        let pool = db.pool().await?;
        let admin = admin::Admin::new(pool.clone(), STORE_NAME);
        for name in [
            <JobEvent as pges_core::event::Event>::CATEGORY,
            <OrderEvent as pges_core::event::Event>::CATEGORY,
            <PartEvent as pges_core::event::Event>::CATEGORY,
        ] {
            admin
                .register_category(name, admin::CategoryConfig::default())
                .await?;
        }
        admin.maintain().await?;
        let store = store::Store::open(pool.clone(), store::StoreConfig::new(STORE_NAME)).await?;
        store.register::<JobEvent>().await?;
        store.register::<OrderEvent>().await?;
        store.register::<PartEvent>().await?;

        let ids = sqlx::query_as::<_, (String, i32)>("SELECT name, category_id FROM es.categories")
            .fetch_all(&pool)
            .await?
            .into_iter()
            .map(|(name, id)| (name, category::CategoryId::new(id)))
            .collect();

        Ok(Self {
            db,
            pool,
            store,
            ids,
        })
    }

    pub fn id(&self, cat: Cat) -> category::CategoryId {
        self.ids[match cat {
            Cat::Jobs => "jobs",
            Cat::Orders => "orders",
            Cat::Parts => "parts",
        }]
    }

    /// Appends `events` events (n = 0..events) to a stream of `cat`.
    pub async fn append(
        &self,
        cat: Cat,
        stream_id: stream::StreamId,
        events: u32,
    ) -> Result<append::AppendResult> {
        let any = version::ExpectedVersion::Any;
        let result = match cat {
            Cat::Jobs => {
                let events: Vec<_> = (0..events).map(|n| JobEvent::Noted { n }).collect();
                self.store.append(stream_id, any, &events).await?
            }
            Cat::Orders => {
                let events: Vec<_> = (0..events).map(|n| OrderEvent::Noted { n }).collect();
                self.store.append(stream_id, any, &events).await?
            }
            Cat::Parts => {
                let events: Vec<_> = (0..events).map(|n| PartEvent::Noted { n }).collect();
                self.store.append(stream_id, any, &events).await?
            }
        };

        Ok(result)
    }

    /// Plays the sequencer by hand: gives the next `limit` unsequenced commits (in commit id order, which is
    /// commit order for the sequential appends of these tests) positions in one transaction. Returns how
    /// many it sequenced.
    pub async fn sequence(&self, limit: i64) -> Result<usize> {
        let mut tx = self.pool.begin().await?;
        tx.execute("LOCK TABLE es.log IN EXCLUSIVE MODE").await?;
        let rows: Vec<(Uuid, i32, i16)> = sqlx::query_as(
            "SELECT c.commit_id, c.category_id, c.event_count FROM es.commits c
             WHERE NOT EXISTS (SELECT 1 FROM es.log l WHERE l.commit_id = c.commit_id)
             ORDER BY c.commit_id LIMIT $1",
        )
        .bind(limit)
        .fetch_all(&mut *tx)
        .await?;
        let mut last: i64 =
            sqlx::query_scalar("SELECT coalesce(max(last_position), 0) FROM es.log")
                .fetch_one(&mut *tx)
                .await?;
        let (mut positions, mut commits, mut categories, mut counts) =
            (Vec::new(), Vec::new(), Vec::new(), Vec::new());
        for (commit_id, category_id, event_count) in &rows {
            last += i64::from(*event_count);
            positions.push(last);
            commits.push(*commit_id);
            categories.push(*category_id);
            counts.push(*event_count);
        }
        sqlx::query(
            "INSERT INTO es.log (last_position, commit_id, category_id, event_count)
             SELECT * FROM unnest($1::int8[], $2::uuid[], $3::int4[], $4::int2[])",
        )
        .bind(&positions)
        .bind(&commits)
        .bind(&categories)
        .bind(&counts)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;

        Ok(rows.len())
    }

    /// Sequences everything not yet sequenced.
    pub async fn sequence_all(&self) -> Result<()> {
        while self.sequence(1000).await? > 0 {}
        Ok(())
    }

    /// The log's high-water marks, as the sequencer would announce them.
    pub async fn high_water(&self) -> Result<high_water::HighWater> {
        let store: i64 = sqlx::query_scalar("SELECT coalesce(max(last_position), 0) FROM es.log")
            .fetch_one(&self.pool)
            .await?;
        let categories: Vec<(i32, i64)> = sqlx::query_as(
            "SELECT category_id, max(last_position) FROM es.log GROUP BY category_id",
        )
        .fetch_all(&self.pool)
        .await?;

        Ok(high_water::HighWater::new(
            pges_testing::position(store as u64),
            categories
                .into_iter()
                .map(|(id, p)| {
                    (
                        category::CategoryId::new(id),
                        pges_testing::position(p as u64),
                    )
                })
                .collect(),
            None,
        ))
    }

    /// Every event position of these categories in the log, in order.
    pub async fn expected(&self, categories: &[category::CategoryId]) -> Result<Vec<(i64, i32)>> {
        let ids: Vec<i32> = categories.iter().map(|id| id.get()).collect();
        let rows = sqlx::query_as(
            "SELECT p, l.category_id FROM es.log l,
                    generate_series(l.last_position - l.event_count + 1, l.last_position) AS p
             WHERE l.category_id = ANY($1) ORDER BY p",
        )
        .bind(&ids)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows)
    }

    /// What [`Recorder`]s recorded for `subscriber`, in the order they recorded it.
    pub async fn delivered(&self, subscriber: &str) -> Result<Vec<(i64, i32)>> {
        let rows = sqlx::query_as(
            "SELECT position, category_id FROM public.delivered WHERE subscriber = $1 ORDER BY seq",
        )
        .bind(subscriber)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows)
    }

    /// The stored cursor of a subscription.
    pub async fn cursor(&self, name: &str) -> Result<Option<i64>> {
        Ok(
            sqlx::query_scalar("SELECT position FROM es.subscriptions WHERE name = $1")
                .bind(name)
                .fetch_optional(&self.pool)
                .await?,
        )
    }

    pub async fn finish(self) -> Result<()> {
        self.store.pool().close().await;
        self.pool.close().await;
        self.db.cleanup().await
    }

    /// [`Fixture::finish`] for a database with a logical source's replication slot: drops the slot too.
    pub async fn finish_with_slots(self) -> Result<()> {
        self.store.pool().close().await;
        self.pool.close().await;
        self.db.cleanup_with_slots().await
    }
}

/// Checks that `delivered` is exactly `expected`: same events, in increasing position order, none twice.
pub fn check_delivery(delivered: &[(i64, i32)], expected: &[(i64, i32)]) -> Result<()> {
    for pair in delivered.windows(2) {
        ensure!(
            pair[0].0 < pair[1].0,
            "position {} delivered after {}",
            pair[1].0,
            pair[0].0
        );
    }
    if delivered != expected {
        let missing: Vec<_> = expected.iter().filter(|e| !delivered.contains(e)).collect();
        let extra: Vec<_> = delivered.iter().filter(|d| !expected.contains(d)).collect();
        bail!("delivery differs: missing {missing:?}, unexpected {extra:?}");
    }
    Ok(())
}

/// A projection recording every event it sees into `public.delivered`, in the batch's transaction.
#[derive(Debug, Clone)]
pub struct Recorder {
    pub subscriber: String,
    pub calls: Arc<Mutex<usize>>,
}

impl Recorder {
    pub fn new(subscriber: &str) -> Self {
        Self {
            subscriber: subscriber.to_owned(),
            calls: Arc::new(Mutex::new(0)),
        }
    }

    pub fn calls(&self) -> usize {
        *self.calls.lock().unwrap()
    }

    pub async fn record(
        &self,
        conn: &mut PgConnection,
        batch: &batch::Batch,
    ) -> Result<(), sqlx::Error> {
        let (mut positions, mut commits, mut categories) = (Vec::new(), Vec::new(), Vec::new());
        for record in batch.iter() {
            let batch::RawRecorded::Event(raw) = record else {
                continue;
            };
            positions.push(raw.position.get() as i64);
            commits.push(raw.commit_id.as_uuid());
            categories.push(raw.category_id.get());
        }
        sqlx::query(
            "INSERT INTO public.delivered (position, commit_id, category_id, subscriber)
             SELECT p, c, k, $4 FROM unnest($1::int8[], $2::uuid[], $3::int4[]) AS u(p, c, k)",
        )
        .bind(&positions)
        .bind(&commits)
        .bind(&categories)
        .bind(&self.subscriber)
        .execute(&mut *conn)
        .await?;
        Ok(())
    }
}

impl handler::PgHandler for Recorder {
    async fn handle(
        &mut self,
        tx: &mut PgConnection,
        batch: &batch::Batch,
    ) -> Result<(), handler::HandlerError> {
        *self.calls.lock().unwrap() += 1;
        self.record(tx, batch).await?;
        Ok(())
    }
}

/// A subscription running on a task, with the errors it recovered from.
pub struct Running {
    pub handle: subscription::SubscriptionHandle,
    errors: Arc<Mutex<Vec<String>>>,
}

impl Running {
    pub fn spawn(subscription: subscription::Subscription) -> Self {
        let errors = track(subscription.status());
        Self {
            handle: subscription.spawn(),
            errors,
        }
    }

    pub fn errors(&self) -> Vec<String> {
        self.errors.lock().unwrap().clone()
    }

    /// Waits until the stored-and-reported cursor reaches `at_least`.
    pub async fn wait_position(&self, at_least: u64) -> Result<()> {
        wait_position(self.handle.status_changes(), at_least).await
    }

    /// Waits for the subscription to stop by itself with a fatal error, and returns the error. Fails if it
    /// stopped without one, or recovered from an error first: no test expects a recovery before a fatal stop.
    pub async fn stopped(self) -> Result<subscription::SubscribeError> {
        let errors = self.errors.clone();
        let result = tokio::time::timeout(Duration::from_secs(20), self.handle.join())
            .await
            .context("the subscription did not stop")?;
        let Err(error) = result else {
            bail!("the subscription stopped without an error");
        };
        let errors = errors.lock().unwrap().clone();
        ensure!(
            errors.is_empty(),
            "the subscription recovered from {errors:?} before stopping with {error}"
        );
        Ok(error)
    }

    /// Graceful shutdown. Fails if the subscription recovered from an error: no test expects one.
    pub async fn stop(self) -> Result<()> {
        let errors = self.errors();
        self.handle.shutdown().await?;
        ensure!(
            errors.is_empty(),
            "the subscription recovered from errors: {errors:?}"
        );
        Ok(())
    }
}

/// A subscription running on a plain task that a test can abort, as a crash would. It runs on a store with a
/// pool of its own, as in another process, and a crash closes that pool.
///
/// Aborting the task drops its futures wherever they are, and their connections go back to the pool. A real
/// crash closes the process's connections instead, which is what closing the pool does.
pub struct Crashable {
    pub task: JoinHandle<Result<(), subscription::SubscribeError>>,
    pub status: watch::Receiver<subscription::Status>,
    errors: Arc<Mutex<Vec<String>>>,
    pool: PgPool,
}

impl Crashable {
    /// Spawns the subscription `build` makes on a store of its own.
    pub async fn spawn(
        f: &Fixture,
        build: impl FnOnce(&store::Store) -> subscription::Subscription,
    ) -> Result<Self> {
        let pool = f.db.pool().await?;
        let store = store::Store::open(pool.clone(), store::StoreConfig::new(STORE_NAME)).await?;
        let subscription = build(&store);
        let status = subscription.status();
        let errors = track(subscription.status());
        Ok(Self {
            task: tokio::spawn(subscription.run_until(std::future::pending())),
            status,
            errors,
            pool,
        })
    }

    pub fn errors(&self) -> Vec<String> {
        self.errors.lock().unwrap().clone()
    }

    pub async fn crash(self) -> Vec<String> {
        self.task.abort();
        let errors = self.errors.clone();
        let _ = self.task.await;
        self.pool.close().await;
        errors.lock().unwrap().clone()
    }
}

/// Records every error a subscription recovers from.
pub fn track(mut status: watch::Receiver<subscription::Status>) -> Arc<Mutex<Vec<String>>> {
    let errors = Arc::new(Mutex::new(Vec::new()));
    let seen = errors.clone();
    tokio::spawn(async move {
        loop {
            if let subscription::State::Recovering(error) = &status.borrow_and_update().state {
                seen.lock().unwrap().push(error.to_string());
            }
            if status.changed().await.is_err() {
                return;
            }
        }
    });
    errors
}

pub async fn wait_position(
    mut status: watch::Receiver<subscription::Status>,
    at_least: u64,
) -> Result<()> {
    let reached = tokio::time::timeout(
        Duration::from_secs(20),
        status.wait_for(|s| s.position >= pges_testing::position(at_least)),
    )
    .await
    .map(|result| result.map(|_| ()));
    match reached {
        Ok(result) => Ok(result?),
        Err(_) => bail!("waiting for position {at_least}: {:?}", status.borrow()),
    }
}

pub async fn wait_state(
    mut status: watch::Receiver<subscription::Status>,
    want: fn(&subscription::State) -> bool,
) -> Result<()> {
    let reached =
        tokio::time::timeout(Duration::from_secs(20), status.wait_for(|s| want(&s.state)))
            .await
            .map(|result| result.map(|_| ()));
    match reached {
        Ok(result) => Ok(result?),
        Err(_) => bail!("waiting for a state: {:?}", status.borrow()),
    }
}

/// Waits until the log reaches `at_least`.
pub async fn wait_sequenced(pool: &PgPool, at_least: u64) -> Result<()> {
    tokio::time::timeout(Duration::from_secs(20), async {
        while head(pool).await? < at_least {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        anyhow::Ok(())
    })
    .await
    .with_context(|| format!("waiting for the log to reach {at_least}"))?
}

/// The newest position of the log.
pub async fn head(pool: &PgPool) -> Result<u64> {
    let head: i64 = sqlx::query("SELECT coalesce(max(last_position), 0) FROM es.log")
        .fetch_one(pool)
        .await?
        .try_get(0)?;
    Ok(head as u64)
}

/// The real outbox sequencer on a task of its own, for stores made with `Outbox::Enabled`.
pub struct Sequencer {
    pub watch: pges_sequencer::watch::Watch,
    stop: oneshot::Sender<()>,
    task: JoinHandle<Result<(), sequencer::RunError>>,
    pool: PgPool,
    errors: Arc<Mutex<Vec<String>>>,
}

impl Sequencer {
    /// A fast sequencer: a token every 50 ms.
    pub fn config() -> sequencer::SequencerConfig {
        let mut config = sequencer::SequencerConfig::default();
        config.token_interval = Duration::from_millis(50);
        config
    }

    pub async fn start(f: &Fixture) -> Result<Self> {
        Self::start_with(f, Self::config()).await
    }

    pub async fn start_with(f: &Fixture, config: sequencer::SequencerConfig) -> Result<Self> {
        Self::start_on(f.db.pool().await?, config)
    }

    /// An outbox sequencer on `pool`, for example a pool that reaches the server through a proxy.
    pub fn start_on(pool: PgPool, config: sequencer::SequencerConfig) -> Result<Self> {
        let source = outbox::OutboxSource::new(pool.clone(), outbox::OutboxConfig::default());
        Self::spawn(pool, source, config)
    }

    /// The logical source, for stores made with `Outbox::Disabled`. Wait until it leads before the first
    /// append ([`Sequencer::wait_leading`]), so its slot exists; end with [`Fixture::finish_with_slots`].
    pub async fn start_logical(f: &Fixture, config: sequencer::SequencerConfig) -> Result<Self> {
        let pool = f.db.pool().await?;
        let replication = pges_replication::config::Config::from_url(f.db.url())?;
        let mut logical = pges_sequencer::logical::LogicalConfig::default();
        logical.start_timeout = pges_core::interval!(20 s);
        let source =
            pges_sequencer::logical::LogicalSource::new(pool.clone(), replication, logical);
        Self::spawn(pool, source, config)
    }

    /// Waits until this sequencer leads.
    pub async fn wait_leading(&self) -> Result<sequencer::Fence> {
        let mut status = self.watch.status();
        let leading = tokio::time::timeout(
            Duration::from_secs(20),
            status.wait_for(|s| matches!(s, sequencer::Status::Leading(_))),
        )
        .await
        .context("waiting to lead")??;
        let sequencer::Status::Leading(fence) = &*leading else {
            bail!("not leading: {:?}", *leading);
        };
        Ok(*fence)
    }

    fn spawn<S: pges_sequencer::source::Source + 'static>(
        pool: PgPool,
        source: S,
        config: sequencer::SequencerConfig,
    ) -> Result<Self> {
        let sequencer = sequencer::Sequencer::new(pool.clone(), source, config);
        let watch = sequencer.watch();
        let errors = track_sequencer(watch.status());
        let (stop, stopped) = oneshot::channel::<()>();
        let task = tokio::spawn(sequencer.run(async {
            let _ = stopped.await;
        }));
        Ok(Self {
            watch,
            stop,
            task,
            pool,
            errors,
        })
    }

    /// Stops the sequencer. Fails if it recovered from an error: no test expects one.
    pub async fn stop(self) -> Result<()> {
        let _ = self.stop.send(());
        tokio::time::timeout(Duration::from_secs(10), self.task)
            .await
            .context("sequencer did not stop")???;
        self.pool.close().await;
        let errors = self.errors.lock().unwrap().clone();
        ensure!(
            errors.is_empty(),
            "the sequencer recovered from errors: {errors:?}"
        );
        Ok(())
    }

    /// A crash: the run is dropped wherever it is, without releasing the lease, which stays until it
    /// expires. Fails if it recovered from an error before.
    pub async fn kill(self) -> Result<()> {
        self.task.abort();
        let _ = self.task.await;
        self.pool.close().await;
        let errors = self.errors.lock().unwrap().clone();
        ensure!(
            errors.is_empty(),
            "the sequencer recovered from errors: {errors:?}"
        );
        Ok(())
    }
}

fn track_sequencer(mut status: watch::Receiver<sequencer::Status>) -> Arc<Mutex<Vec<String>>> {
    let errors = Arc::new(Mutex::new(Vec::new()));
    let seen = errors.clone();
    tokio::spawn(async move {
        loop {
            if let sequencer::Status::Recovering(error) = &*status.borrow_and_update() {
                seen.lock().unwrap().push(error.to_string());
            }
            if status.changed().await.is_err() {
                return;
            }
        }
    });
    errors
}

/// The sequencer's gRPC watch server on a task of its own.
pub struct GrpcServer {
    pub addr: std::net::SocketAddr,
    scheme: &'static str,
    stop: oneshot::Sender<()>,
    task: JoinHandle<Result<(), pges_sequencer::grpc::ServeError>>,
}

impl GrpcServer {
    /// Serves `watch` on a free local port.
    pub async fn start(watch: pges_sequencer::watch::Watch) -> Result<Self> {
        Self::start_on(watch, "127.0.0.1:0".parse()?).await
    }

    /// Serves `watch` on `addr`, for example the address of a server that stopped.
    pub async fn start_on(
        watch: pges_sequencer::watch::Watch,
        addr: std::net::SocketAddr,
    ) -> Result<Self> {
        Self::start_with(watch, addr, pges_sequencer::grpc::WatchConfig::default()).await
    }

    /// Serves `watch` on `addr` with `config`; over TLS when it sets TLS up.
    pub async fn start_with(
        watch: pges_sequencer::watch::Watch,
        addr: std::net::SocketAddr,
        config: pges_sequencer::grpc::WatchConfig,
    ) -> Result<Self> {
        let scheme = if config.tls.is_some() {
            "https"
        } else {
            "http"
        };
        let listener = tokio::net::TcpListener::bind(addr).await?;
        let addr = listener.local_addr()?;
        let (stop, stopped) = oneshot::channel::<()>();
        let service = pges_sequencer::grpc::WatchService::with_config(watch, config);
        let task = tokio::spawn(service.serve(listener, async {
            let _ = stopped.await;
        }));
        Ok(Self {
            addr,
            scheme,
            stop,
            task,
        })
    }

    /// The endpoint a `GrpcWaker` connects to.
    pub fn endpoint(&self) -> String {
        format!("{}://{}", self.scheme, self.addr)
    }

    /// Stops the server; its streams end with `UNAVAILABLE`.
    pub async fn stop(self) -> Result<()> {
        let _ = self.stop.send(());
        tokio::time::timeout(Duration::from_secs(10), self.task)
            .await
            .context("the gRPC server did not stop")???;
        Ok(())
    }
}
