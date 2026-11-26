//! Shared fixtures: a store with two categories, sequenced by the outbox or the logical source, a fast
//! sequencer configuration, and checks of the log's invariants.

#![allow(dead_code, clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::{
    collections::{BTreeMap, HashSet},
    str::FromStr,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Duration,
};

use anyhow::{Context, Result, bail, ensure};
use serde::{Deserialize, Serialize};
use sqlx::{PgPool, Row, postgres::PgConnectOptions};
use tokio::{sync::oneshot, task::JoinHandle};
use uuid::Uuid;

use pges_core::{category, commit, high_water, interval, stream, version};
use pges_postgres::{admin, append, limit, store};
#[cfg(feature = "source-logical")]
use pges_sequencer::logical;
use pges_sequencer::{outbox, sequencer, source, watch};

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

/// Which source sequences the fixture's store.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    /// An outbox store, drained by `outbox::OutboxSource`.
    Outbox,
    /// A store without the outbox option, read through `logical::LogicalSource`. Its slot is dropped by
    /// `Fixture::finish`.
    Logical,
}

pub struct Fixture {
    pub kind: Kind,
    pub db: pges_testing::TestDb,
    pub pool: PgPool,
    pub store: store::Store,
    pub store_id: pges_core::store::StoreId,
    pub jobs: category::CategoryId,
    pub orders: category::CategoryId,
}

impl Fixture {
    /// An outbox store.
    pub async fn new() -> Result<Self> {
        Self::with(Kind::Outbox).await
    }

    /// A store for the logical source.
    pub async fn logical() -> Result<Self> {
        Self::with(Kind::Logical).await
    }

    pub async fn with(kind: Kind) -> Result<Self> {
        Self::on(pges_testing::TestDb::new().await?, kind).await
    }

    /// The fixture in an existing database that already has the shared `pges` migrations.
    pub async fn on(db: pges_testing::TestDb, kind: Kind) -> Result<Self> {
        let mut conn = db.connect().await?;
        if kind == Kind::Logical {
            let wal_level: String = sqlx::query_scalar("SHOW wal_level")
                .fetch_one(&mut conn)
                .await?;
            ensure!(
                wal_level == "logical",
                "the test server needs wal_level=logical, not {wal_level}"
            );
        }
        let outbox = match kind {
            Kind::Outbox => store::Outbox::Enabled,
            Kind::Logical => store::Outbox::Disabled,
        };
        pges_testing::create_store(&mut conn, &STORE_NAME, &pges_testing::definition(outbox))
            .await?;
        let pool = writers(&db).await?;
        let admin = admin::Admin::new(pool.clone(), STORE_NAME);
        admin
            .register_category(
                <JobEvent as pges_core::event::Event>::CATEGORY,
                admin::CategoryConfig::default(),
            )
            .await?;
        admin
            .register_category(
                <OrderEvent as pges_core::event::Event>::CATEGORY,
                admin::CategoryConfig::default(),
            )
            .await?;
        admin.maintain().await?;

        Self::opened(kind, db, pool, store::StoreConfig::new(STORE_NAME)).await
    }

    /// A store for the logical source whose categories have monthly leaves from `months_back` months ago,
    /// and whose writers may append commit ids up to `skew` from the clock, so a test can write into old
    /// leaves. The store records `skew` and the default `max_txn_age` as its writer limits.
    pub async fn logical_with_history(months_back: u16, skew: Duration) -> Result<Self> {
        let db = pges_testing::TestDb::new().await?;
        let mut conn = db.connect().await?;
        pges_testing::create_store(
            &mut conn,
            &STORE_NAME,
            &pges_testing::definition(store::Outbox::Disabled),
        )
        .await?;
        for category in [category!("jobs"), category!("orders")] {
            pges_testing::register_category(&mut conn, &STORE_NAME, category, months_back, 2)
                .await?;
        }
        pges_testing::create_log_partition(&mut conn, &STORE_NAME, 0).await?;
        let pool = writers(&db).await?;
        admin::Admin::new(pool.clone(), STORE_NAME)
            .set_writer_limits(limit::WriterLimits::new(
                skew,
                interval!(pges_core::store::DEFAULT_MAX_TXN_AGE),
            ))
            .await?;
        let mut config = store::StoreConfig::new(STORE_NAME);
        config.max_clock_skew = Some(skew);

        Self::opened(Kind::Logical, db, pool, config).await
    }

    async fn opened(
        kind: Kind,
        db: pges_testing::TestDb,
        pool: PgPool,
        config: store::StoreConfig,
    ) -> Result<Self> {
        let store = store::Store::open(pool.clone(), config).await?;
        store.register::<JobEvent>().await?;
        store.register::<OrderEvent>().await?;

        let ids: BTreeMap<String, i32> =
            sqlx::query_as("SELECT name, category_id FROM es.categories")
                .fetch_all(&pool)
                .await?
                .into_iter()
                .collect();

        Ok(Self {
            kind,
            store_id: store.store_id(),
            jobs: category::CategoryId::new(ids["jobs"]),
            orders: category::CategoryId::new(ids["orders"]),
            db,
            pool,
            store,
        })
    }

    /// A sequencer on its own pool, so killing it cannot disturb the appenders' connections.
    pub async fn start(&self, config: sequencer::SequencerConfig) -> Result<Running> {
        let pool = self.db.pool().await?;
        match self.kind {
            Kind::Outbox => {
                let source = outbox::OutboxSource::new(pool.clone(), outbox_config());
                Running::start(pool, source, config)
            }
            #[cfg(feature = "source-logical")]
            Kind::Logical => {
                let source = self.logical_source(pool.clone())?;
                Running::start(pool, source, config)
            }
            #[cfg(not(feature = "source-logical"))]
            Kind::Logical => bail!("the logical source needs the source-logical feature"),
        }
    }

    /// A pool for one sequencer whose connections carry `application_name`, so a test can find and
    /// terminate them.
    pub async fn named_pool(&self, application_name: &str) -> Result<PgPool> {
        let options = PgConnectOptions::from_str(self.db.url())?.application_name(application_name);
        Ok(sqlx::postgres::PgPoolOptions::new()
            .max_connections(8)
            .connect_with(options)
            .await?)
    }

    /// The fixture's kind of source, on `pool`.
    pub fn source(&self, pool: PgPool) -> Result<Box<dyn source::DynSource>> {
        match self.kind {
            Kind::Outbox => Ok(Box::new(outbox::OutboxSource::new(pool, outbox_config()))),
            #[cfg(feature = "source-logical")]
            Kind::Logical => Ok(Box::new(self.logical_source(pool)?)),
            #[cfg(not(feature = "source-logical"))]
            Kind::Logical => bail!("the logical source needs the source-logical feature"),
        }
    }

    /// Caps the test role's transactions in this database at the stores' default `max_txn_age`, as
    /// resequence and retention require of writers, and returns the role.
    pub async fn writer_role(&self) -> Result<String> {
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "ALTER ROLE CURRENT_USER IN DATABASE {} SET transaction_timeout = '1h'",
            self.db.name()
        )))
        .execute(&self.pool)
        .await?;
        Ok(sqlx::query_scalar("SELECT current_user::text")
            .fetch_one(&self.pool)
            .await?)
    }

    /// A resequence configuration whose writer role is the test's, capped by [`Fixture::writer_role`].
    #[cfg(feature = "source-logical")]
    pub async fn resequence_config(&self) -> Result<logical::ResequenceConfig> {
        Ok(logical::ResequenceConfig::new(limit::WriterRoles::one(
            self.writer_role().await?,
        )))
    }

    /// A logical source for this database.
    #[cfg(feature = "source-logical")]
    pub fn logical_source(&self, pool: PgPool) -> Result<logical::LogicalSource> {
        let replication = pges_replication::config::Config::from_url(self.db.url())?;
        Ok(logical::LogicalSource::new(
            pool,
            replication,
            logical_config(),
        ))
    }

    /// The logical source's slot, `pges_<oid>`.
    pub async fn slot(&self) -> Result<String> {
        let oid: i64 = sqlx::query_scalar(
            "SELECT oid::int8 FROM pg_database WHERE datname = current_database()",
        )
        .fetch_one(&self.pool)
        .await?;
        Ok(format!("pges_{oid}"))
    }

    /// Drops the logical source's slot once no process uses it. Every sequencer must have stopped.
    pub async fn drop_slot(&self) -> Result<()> {
        let slot = self.slot().await?;
        let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
        loop {
            let exists: bool = sqlx::query_scalar(
                "SELECT EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = $1)",
            )
            .bind(&slot)
            .fetch_one(&self.pool)
            .await?;
            if !exists {
                return Ok(());
            }
            match sqlx::query("SELECT pg_drop_replication_slot($1)")
                .bind(&slot)
                .execute(&self.pool)
                .await
            {
                Ok(_) => return Ok(()),
                Err(error) if tokio::time::Instant::now() < deadline => {
                    let _ = error;
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
                Err(error) => return Err(error).context("dropping the slot"),
            }
        }
    }

    /// Appends `events` events to a jobs (`Stream::Job`) or orders stream.
    pub async fn append(&self, target: Target, events: u32) -> Result<append::AppendResult> {
        let result = match target {
            Target::Job(id) => {
                let events: Vec<JobEvent> = (0..events).map(|n| JobEvent::Noted { n }).collect();
                self.store
                    .append(id, version::ExpectedVersion::Any, &events)
                    .await?
            }
            Target::Order(id) => {
                let events: Vec<OrderEvent> =
                    (0..events).map(|n| OrderEvent::Noted { n }).collect();
                self.store
                    .append(id, version::ExpectedVersion::Any, &events)
                    .await?
            }
        };

        Ok(result)
    }

    pub async fn finish(self) -> Result<()> {
        if self.kind == Kind::Logical {
            self.drop_slot().await?;
        }
        self.pool.close().await;
        self.db.cleanup().await
    }
}

#[derive(Debug, Clone, Copy)]
pub enum Target {
    Job(stream::StreamId),
    Order(stream::StreamId),
}

/// The outbox source waking on notifications only: the fallback recheck is an hour away, so a commit whose
/// notification went missing stalls the test instead of being picked up by the recheck.
pub fn outbox_config() -> outbox::OutboxConfig {
    let mut config = outbox::OutboxConfig::default();
    config.recheck_interval = interval!(1 h);
    config.store_refresh = interval!(200 ms);
    config
}

#[cfg(feature = "source-logical")]
pub fn logical_config() -> logical::LogicalConfig {
    let mut config = logical::LogicalConfig::default();
    config.start_timeout = interval!(20 s);
    config
}

/// The appenders' pool. Decoding only sees flushed WAL, and the test server runs with
/// `synchronous_commit = off`, so appends flush their commits to keep the logical source's latency low.
async fn writers(db: &pges_testing::TestDb) -> Result<PgPool> {
    let options = PgConnectOptions::from_str(db.url())?.options([("synchronous_commit", "on")]);
    Ok(sqlx::postgres::PgPoolOptions::new()
        .max_connections(8)
        .connect_with(options)
        .await?)
}

/// Short lease and token intervals, so takeovers and coarse writes happen within a test.
pub fn fast() -> sequencer::SequencerConfig {
    let mut config = sequencer::SequencerConfig::default();
    config.lease = sequencer::Lease::with_renew(interval!(800 ms), interval!(200 ms)).unwrap();
    config.acquire_interval = interval!(50 ms);
    config.token_interval = Duration::from_millis(100);
    config.retry_interval = interval!(50 ms);
    config
}

/// A sequencer running on a task.
pub struct Running {
    pub watch: watch::Watch,
    pub pool: PgPool,
    stop: Option<oneshot::Sender<()>>,
    task: JoinHandle<Result<(), sequencer::RunError>>,
    errors: Arc<Mutex<Vec<String>>>,
}

impl Running {
    pub fn start<S: source::Source + 'static>(
        pool: PgPool,
        source: S,
        config: sequencer::SequencerConfig,
    ) -> Result<Self> {
        let sequencer = sequencer::Sequencer::new(pool.clone(), source, config);
        let watch = sequencer.watch();
        let (stop, stopped) = oneshot::channel::<()>();
        let task = tokio::spawn(sequencer.run(async {
            let _ = stopped.await;
        }));

        let errors = Arc::new(Mutex::new(Vec::new()));
        let mut status = watch.status();
        let seen = errors.clone();
        tokio::spawn(async move {
            while status.changed().await.is_ok() {
                if let sequencer::Status::Recovering(error) = &*status.borrow_and_update() {
                    seen.lock().unwrap().push(error.to_string());
                }
            }
        });

        Ok(Self {
            watch,
            pool,
            stop: Some(stop),
            task,
            errors,
        })
    }

    pub fn is_leading(&self) -> bool {
        matches!(*self.watch.status().borrow(), sequencer::Status::Leading(_))
    }

    pub async fn wait_leading(&self) -> Result<()> {
        let mut status = self.watch.status();
        tokio::time::timeout(
            Duration::from_secs(10),
            status.wait_for(|s| matches!(s, sequencer::Status::Leading(_))),
        )
        .await
        .context("waiting to lead")??;
        Ok(())
    }

    /// Errors the sequencer recovered from so far.
    pub fn errors(&self) -> Vec<String> {
        self.errors.lock().unwrap().clone()
    }

    /// Graceful shutdown: finishes the batch, stores the token, releases the lease. Fails if the sequencer
    /// had to recover from an error: no test expects one.
    pub async fn stop(mut self) -> Result<()> {
        let errors = self.errors();
        ensure!(
            errors.is_empty(),
            "the sequencer recovered from errors: {errors:?}"
        );
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        tokio::time::timeout(Duration::from_secs(10), &mut self.task)
            .await
            .context("sequencer did not stop")???;
        self.pool.close().await;
        Ok(())
    }

    /// Graceful shutdown of a sequencer that was expected to recover from errors. Returns them.
    pub async fn stop_after_errors(mut self) -> Result<Vec<String>> {
        let errors = self.errors();
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        tokio::time::timeout(Duration::from_secs(10), &mut self.task)
            .await
            .context("sequencer did not stop")???;
        self.pool.close().await;
        Ok(errors)
    }

    /// A crash: the task is dropped wherever it is, and its connections with it. The lease stays until it
    /// expires.
    pub async fn kill(self) {
        self.task.abort();
        let _ = self.task.await;
        self.pool.close().await;
    }

    /// A crash as the server sees it: the task is dropped wherever it is, then the server ends every
    /// session of the sequencer's pool (opened with [`Fixture::named_pool`] as `application_name`), as if
    /// the process had died. A transaction the sequencer left open rolls back. The lease stays until it
    /// expires.
    pub async fn crash(self, server: &PgPool, application_name: &str) -> Result<()> {
        self.task.abort();
        let _ = self.task.await;
        sqlx::query(
            "SELECT pg_terminate_backend(pid) FROM pg_stat_activity
             WHERE application_name = $1 AND pid <> pg_backend_pid()",
        )
        .bind(application_name)
        .execute(server)
        .await?;
        tokio::time::timeout(Duration::from_secs(10), self.pool.close())
            .await
            .context("closing the crashed sequencer's pool")?;
        Ok(())
    }

    /// The run's result, once it ended by itself. Checks that the status says how it ended: `Halted` with
    /// the error's text, or `Stopped`.
    pub async fn result(self) -> Result<(), sequencer::RunError> {
        let result = tokio::time::timeout(Duration::from_secs(30), self.task)
            .await
            .expect("sequencer did not end")
            .expect("sequencer task panicked");
        match (&result, &*self.watch.status().borrow()) {
            (Ok(()), sequencer::Status::Stopped) => {}
            (Err(error), sequencer::Status::Halted(reason)) => {
                assert_eq!(**reason, *error.to_string());
            }
            (result, status) => panic!("run returned {result:?}, but the status is {status:?}"),
        }
        self.pool.close().await;
        result
    }
}

/// One log row with its commit.
#[derive(Debug, Clone)]
pub struct Logged {
    pub last_position: i64,
    pub event_count: i16,
    pub commit_id: Uuid,
    pub category_id: i32,
    pub stream_id: Uuid,
    pub last_version: i64,
}

/// Waits until `count` commits are in the log and `pending` (of an outbox store) is empty.
pub async fn wait_sequenced(pool: &PgPool, count: usize) -> Result<()> {
    let outbox: bool = sqlx::query_scalar("SELECT to_regclass('es.pending') IS NOT NULL")
        .fetch_one(pool)
        .await?;
    let counts = if outbox {
        "SELECT (SELECT count(*) FROM es.log), (SELECT count(*) FROM es.pending)"
    } else {
        "SELECT (SELECT count(*) FROM es.log), 0::int8"
    };
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    loop {
        let (logged, pending): (i64, i64) = sqlx::query_as(counts).fetch_one(pool).await?;
        if logged as usize == count && pending == 0 {
            return Ok(());
        }
        ensure!(
            (logged as usize) <= count,
            "log has {logged} rows, expected {count}"
        );
        if tokio::time::Instant::now() > deadline {
            bail!("timed out: {logged} of {count} commits sequenced, {pending} pending");
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

/// Reads the log in position order and checks the invariants every test relies on: positions start at 1
/// and are gap-free, every commit appears once and exists in `commits`, and within each stream positions
/// follow versions with no version skipped.
pub async fn check_log(pool: &PgPool) -> Result<Vec<Logged>> {
    let rows = sqlx::query(
        "SELECT l.last_position, l.event_count, l.commit_id, l.category_id, c.stream_id, c.last_version,
                c.event_count AS commit_events, c.category_id AS commit_category
         FROM es.log l LEFT JOIN es.commits c ON c.commit_id = l.commit_id
         ORDER BY l.last_position",
    )
    .fetch_all(pool)
    .await?;

    let mut logged = Vec::with_capacity(rows.len());
    let mut next = 1_i64;
    let mut seen = HashSet::new();
    let mut versions: BTreeMap<Uuid, i64> = BTreeMap::new();
    for row in rows {
        let commit_id: Uuid = row.try_get("commit_id")?;
        let stream_id: Option<Uuid> = row.try_get("stream_id")?;
        let stream_id =
            stream_id.with_context(|| format!("log row for missing commit {commit_id}"))?;
        let entry = Logged {
            last_position: row.try_get("last_position")?,
            event_count: row.try_get("event_count")?,
            commit_id,
            category_id: row.try_get("category_id")?,
            stream_id,
            last_version: row.try_get("last_version")?,
        };
        ensure!(
            entry.event_count == row.try_get::<i16, _>("commit_events")?,
            "event count differs for {commit_id}"
        );
        ensure!(
            entry.category_id == row.try_get::<i32, _>("commit_category")?,
            "category differs for {commit_id}"
        );
        ensure!(
            entry.last_position - i64::from(entry.event_count) + 1 == next,
            "gap or overlap before position {}: expected the commit to start at {next}",
            entry.last_position
        );
        ensure!(seen.insert(commit_id), "commit {commit_id} sequenced twice");

        let before = versions.insert(stream_id, entry.last_version).unwrap_or(0);
        ensure!(
            entry.last_version - i64::from(entry.event_count) == before,
            "stream {stream_id}: version {} sequenced after version {before}",
            entry.last_version
        );

        next = entry.last_position + 1;
        logged.push(entry);
    }

    Ok(logged)
}

/// Fails unless every commit of the store has exactly one log row.
pub async fn check_every_commit_once(pool: &PgPool) -> Result<()> {
    let (commits, wrong): (i64, i64) = sqlx::query_as(
        "SELECT count(*),
                count(*) FILTER (WHERE (SELECT count(*) FROM es.log l WHERE l.commit_id = c.commit_id) <> 1)
         FROM es.commits c",
    )
    .fetch_one(pool)
    .await?;
    ensure!(
        wrong == 0,
        "{wrong} of {commits} commits do not have exactly one log row"
    );
    Ok(())
}

/// The number of commits in the store.
pub async fn commit_count(pool: &PgPool) -> Result<usize> {
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM es.commits")
        .fetch_one(pool)
        .await?;
    Ok(count as usize)
}

/// Commit ids in log order.
pub fn commit_order(log: &[Logged]) -> Vec<commit::CommitId> {
    log.iter()
        .map(|row| commit::CommitId::from_uuid(row.commit_id).unwrap())
        .collect()
}

/// Waits until the watch reports at least `store` for the store.
pub async fn wait_high_water(
    watch: &watch::Watch,
    store: pges_core::store::StoreId,
    at_least: u64,
) -> Result<high_water::HighWater> {
    let mut receiver = watch.high_water(store);
    let high = tokio::time::timeout(
        Duration::from_secs(10),
        receiver.wait_for(|high| high.store() >= pges_testing::position(at_least)),
    )
    .await
    .context("waiting for the high-water mark")??
    .clone();
    Ok(high)
}

/// Moves the store's only log row to position `i64::MAX - 1`, in a log partition that reaches the end of
/// the range, so the next commit runs out of positions.
pub async fn move_log_to_the_last_positions(pool: &PgPool) -> Result<()> {
    let mut tx = pool.begin().await?;
    sqlx::query(
        "CREATE TABLE es.log_top PARTITION OF es.log FOR VALUES FROM (9200000000000000000) TO (MAXVALUE)",
    )
    .execute(&mut *tx)
    .await?;
    let moved = sqlx::query("UPDATE es.log SET last_position = $1")
        .bind(i64::MAX - 1)
        .execute(&mut *tx)
        .await?;
    ensure!(moved.rows_affected() == 1, "the log has more than one row");
    tx.commit().await?;
    Ok(())
}

/// One append made by a [`Load`] writer, with the ticks of a clock shared by all writers taken before it
/// started and after it returned.
#[derive(Debug, Clone, Copy)]
pub struct Appended {
    pub commit_id: Uuid,
    pub started: u64,
    pub ended: u64,
}

/// Writers appending concurrently, each to three streams of its own across both categories, until stopped.
pub struct Load {
    stop: Arc<AtomicBool>,
    tasks: Vec<JoinHandle<Result<Vec<Appended>>>>,
}

impl Load {
    pub fn start(f: &Arc<Fixture>, writers: usize) -> Self {
        let stop = Arc::new(AtomicBool::new(false));
        let clock = Arc::new(AtomicU64::new(0));
        let tasks = (0..writers)
            .map(|writer| {
                let (f, stop, clock) = (f.clone(), stop.clone(), clock.clone());
                tokio::spawn(async move {
                    let streams: Vec<Target> = (0..3)
                        .map(|i| {
                            let id = stream::StreamId::new_v7();
                            if (writer + i) % 2 == 0 {
                                Target::Job(id)
                            } else {
                                Target::Order(id)
                            }
                        })
                        .collect();
                    let mut appended = Vec::new();
                    let mut n = writer;
                    while !stop.load(Ordering::SeqCst) {
                        let started = clock.fetch_add(1, Ordering::SeqCst);
                        let result = f.append(streams[n % 3], (n % 3) as u32 + 1).await?;
                        let ended = clock.fetch_add(1, Ordering::SeqCst);
                        appended.push(Appended {
                            commit_id: result.commit_id.as_uuid(),
                            started,
                            ended,
                        });
                        n += 1;
                        tokio::time::sleep(Duration::from_millis(2)).await;
                    }
                    Ok(appended)
                })
            })
            .collect();

        Self { stop, tasks }
    }

    /// Stops the writers and returns every append they made.
    pub async fn stop(self) -> Result<Vec<Appended>> {
        self.stop.store(true, Ordering::SeqCst);
        let mut appended = Vec::new();
        for task in self.tasks {
            appended.extend(task.await??);
        }
        Ok(appended)
    }
}

/// Fails unless the log keeps commit order: an append that started after another one returned comes after
/// it in its category, and in the store.
pub fn check_commit_order(log: &[Logged], appended: &[Appended]) -> Result<()> {
    let positions: BTreeMap<Uuid, (i64, i32)> = log
        .iter()
        .map(|row| (row.commit_id, (row.last_position, row.category_id)))
        .collect();
    let position = |commit_id: &Uuid| {
        positions
            .get(commit_id)
            .copied()
            .with_context(|| format!("commit {commit_id} is not in the log"))
    };
    let mut by_end = appended.to_vec();
    by_end.sort_by_key(|append| append.ended);
    let mut by_start = appended.to_vec();
    by_start.sort_by_key(|append| append.started);

    // The newest position among the appends that returned before the current one started.
    let mut returned = by_end.iter().peekable();
    let mut newest_in_store: Option<(i64, Uuid)> = None;
    let mut newest_in_category: BTreeMap<i32, (i64, Uuid)> = BTreeMap::new();
    for append in &by_start {
        while let Some(before) = returned.next_if(|before| before.ended < append.started) {
            let (at, category) = position(&before.commit_id)?;
            if newest_in_store.is_none_or(|(newest, _)| at > newest) {
                newest_in_store = Some((at, before.commit_id));
            }
            let newest = newest_in_category
                .entry(category)
                .or_insert((0, Uuid::nil()));
            if at > newest.0 {
                *newest = (at, before.commit_id);
            }
        }

        let (at, category) = position(&append.commit_id)?;
        if let Some((newest, before)) = newest_in_category.get(&category) {
            ensure!(
                at > *newest,
                "commit {} (position {at}) started after commit {before} (position {newest}) of its category returned, but comes first",
                append.commit_id
            );
        }
        if let Some((newest, before)) = newest_in_store {
            ensure!(
                at > newest,
                "commit {} (position {at}) started after commit {before} (position {newest}) returned, but comes first in the store",
                append.commit_id
            );
        }
    }

    Ok(())
}
