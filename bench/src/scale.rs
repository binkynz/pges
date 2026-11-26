//! The scale targets: 100M+ streams and billions of retained events per cluster.
//!
//! Unlike the other benchmarks, `scale` works in a database that outlives a command: `scale fill` generates
//! a store once (an hour or more at full size), and the other actions measure it, as often as needed. The
//! database is never dropped by the benchmark; drop it, or stop the server, when done.
//!
//! `fill` writes the rows appends and the sequencer would have written, in SQL, into the tables the crate's
//! DDL created: categories registered with `Admin::register_category`, past time leaves created with the
//! crate's leaf statements, then commits, log rows and streams generated from one formula. It differs from
//! appends in that rows arrive in large multi-row inserts, `streams` rows are inserted once and never updated
//! (no dead tuples or HOT chains), and no sequencer runs: log rows are written in commit id order.
//!
//! The store has three categories: `orders` (monthly leaves, single-event commits, 60% of the streams with
//! 2.5 commits each), `audit` (monthly, 10-event commits, 20% of the streams with 2 commits each) and `jobs`
//! (daily leaves over `--days`, single-event commits, 20% of the streams with 2 commits each). Commits are
//! spread evenly in time from the first leaf to a minute before the fill started, and stream `s` of a
//! category holds its commits `s`, `s + streams`, `s + 2 × streams`, and so on.

use std::{
    marker::PhantomData,
    num::NonZeroU16,
    str::FromStr,
    sync::{
        Arc,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
    time::Duration,
};

use anyhow::{Context, Result, bail, ensure};
use serde_json::json;
use sqlx::{Connection, Executor as _, PgConnection, PgPool, postgres::PgPoolOptions};
use tokio::time::Instant;
use url::Url;
use uuid::Uuid;

use pges::core::{aggregate, category, event::Event as _, position, stream, version};
use pges::postgres::{
    admin, limit, retention,
    sql::{self as postgres_sql, template, typed::Query as _},
    store,
};
use pges::replication::config;
use pges::sequencer::{logical, sequencer, source};
use pges::subscribe::{
    batch, handler,
    subscription::{self, StoreExt},
};

use crate::{database, event, measure, partitions, report, sql};

/// Options of `pges-bench scale`.
#[derive(Debug, Clone, clap::Args)]
pub struct Args {
    /// What to do.
    #[command(subcommand)]
    pub action: Action,
}

/// What `pges-bench scale` does.
#[derive(Debug, Clone, clap::Subcommand)]
pub enum Action {
    /// Creates the database if needed and generates a store in it.
    Fill(FillArgs),
    /// Single-event appends to random existing streams, or to new ones.
    Append(AppendArgs),
    /// `load` of random existing streams.
    Load(LoadArgs),
    /// A subscription catching up from a position for a while.
    CatchUp(CatchUpArgs),
    /// Bootstraps the logical source (and resequences, as after writers started), appends a backlog and
    /// times the sequencer draining it.
    Drain(DrainArgs),
    /// Sets a window retention on a category and times `Admin::apply_retention`.
    Retention(RetentionArgs),
}

/// The database and store a scale action works on.
#[derive(Debug, Clone, clap::Args)]
pub struct Target {
    /// The database, on the server of `--database-url`. `fill` creates it if it is missing.
    #[arg(long, default_value = "pges_scale")]
    pub database: String,
    /// The store.
    #[arg(long, default_value = "scale")]
    pub store: String,
}

/// Options of `pges-bench scale fill`.
#[derive(Debug, Clone, clap::Args)]
pub struct FillArgs {
    /// The database and store.
    #[command(flatten)]
    pub target: Target,
    /// Streams in all, across the three categories.
    #[arg(long, default_value_t = 1_000_000)]
    pub streams: u64,
    /// Hash partitions of `streams` and `snapshots`.
    #[arg(long, default_value_t = 64)]
    pub stream_partitions: u16,
    /// Monthly leaves of `orders` and `audit` with commits, the current month included.
    #[arg(long, default_value_t = 24)]
    pub months: u32,
    /// Daily leaves of `jobs` with commits, today included.
    #[arg(long, default_value_t = 60)]
    pub days: u32,
    /// What to generate: everything, or only the `streams` rows (to compare partition counts).
    #[arg(long, value_enum, default_value = "all")]
    pub content: Content,
    /// Bytes of payload per event.
    #[arg(long, default_value_t = 100)]
    pub payload: usize,
    /// Connections inserting commits at once.
    #[arg(long, default_value_t = 8)]
    pub workers: usize,
    /// Connections inserting streams at once.
    #[arg(long, default_value_t = 4)]
    pub stream_workers: usize,
    /// Rows per insert statement.
    #[arg(long, default_value_t = 500_000)]
    pub chunk: u64,
    /// Commits per log window: the log is written one window at a time, each sorted by commit id.
    #[arg(long, default_value_t = 8_000_000)]
    pub log_window: u64,
}

/// What `fill` generates.
#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
pub enum Content {
    /// Streams, commits and log rows.
    All,
    /// Only `streams` rows; their commit ids name commits that do not exist.
    Streams,
}

/// Options of `pges-bench scale append`.
#[derive(Debug, Clone, clap::Args)]
pub struct AppendArgs {
    /// The database and store.
    #[command(flatten)]
    pub target: Target,
    /// Which streams.
    #[arg(long, value_enum, default_value = "existing")]
    pub streams: Streams,
    /// The category appended to.
    #[arg(long, value_enum, default_value = "orders")]
    pub category: Kind,
    /// Concurrent appenders, comma-separated: one measurement per level.
    #[arg(long, value_delimiter = ',', default_value = "1,16,64")]
    pub concurrency: Vec<usize>,
    /// How long each level runs.
    #[arg(long, default_value = "60s", value_parser = crate::parse_duration)]
    pub duration: Duration,
    /// How long each level runs unmeasured first.
    #[arg(long, default_value = "5s", value_parser = crate::parse_duration)]
    pub warmup: Duration,
    /// Existing streams drawn at random before the run; appends pick among them.
    #[arg(long, default_value_t = 1_000_000)]
    pub sample: u32,
    /// Bytes of payload per event.
    #[arg(long, default_value_t = 100)]
    pub payload: usize,
}

/// Which streams `append` writes to.
#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
pub enum Streams {
    /// A random existing stream, any version.
    Existing,
    /// A new stream per append.
    New,
}

/// Options of `pges-bench scale load`.
#[derive(Debug, Clone, clap::Args)]
pub struct LoadArgs {
    /// The database and store.
    #[command(flatten)]
    pub target: Target,
    /// The category loaded.
    #[arg(long, value_enum, default_value = "orders")]
    pub category: Kind,
    /// Concurrent loaders, comma-separated: one measurement per level.
    #[arg(long, value_delimiter = ',', default_value = "1,16")]
    pub concurrency: Vec<usize>,
    /// How long each level runs.
    #[arg(long, default_value = "30s", value_parser = crate::parse_duration)]
    pub duration: Duration,
    /// How long each level runs unmeasured first.
    #[arg(long, default_value = "3s", value_parser = crate::parse_duration)]
    pub warmup: Duration,
    /// Existing streams drawn at random before the run; loads pick among them.
    #[arg(long, default_value_t = 1_000_000)]
    pub sample: u32,
}

/// Options of `pges-bench scale catch-up`.
#[derive(Debug, Clone, clap::Args)]
pub struct CatchUpArgs {
    /// The database and store.
    #[command(flatten)]
    pub target: Target,
    /// One category, or the whole store.
    #[arg(long, value_enum, default_value = "orders")]
    pub scope: Scope,
    /// Where the subscription starts, as a share of the store's newest position.
    #[arg(long, default_value_t = 0.5)]
    pub from: f64,
    /// How long it runs.
    #[arg(long, default_value = "60s", value_parser = crate::parse_duration)]
    pub duration: Duration,
    /// Events per page.
    #[arg(long, default_value_t = 1024)]
    pub batch_size: u32,
}

/// What `catch-up` reads.
#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
pub enum Scope {
    /// The `orders` category.
    Orders,
    /// The `audit` category.
    Audit,
    /// The `jobs` category.
    Jobs,
    /// Every category, through the store-wide read.
    Store,
}

/// Options of `pges-bench scale drain`.
#[derive(Debug, Clone, clap::Args)]
pub struct DrainArgs {
    /// The database and store.
    #[command(flatten)]
    pub target: Target,
    /// Single-event commits appended to random existing `orders` streams while no sequencer runs.
    #[arg(long, default_value_t = 500_000)]
    pub commits: u64,
    /// The role appends run as: resequence checks that it caps its transactions (`transaction_timeout`).
    #[arg(long, default_value = "postgres")]
    pub writer_role: String,
    /// How long to wait for the drain before giving up.
    #[arg(long, default_value = "600s", value_parser = crate::parse_duration)]
    pub limit: Duration,
}

/// Options of `pges-bench scale retention`.
#[derive(Debug, Clone, clap::Args)]
pub struct RetentionArgs {
    /// The database and store.
    #[command(flatten)]
    pub target: Target,
    /// The category whose old leaves are dropped.
    #[arg(long, value_enum, default_value = "jobs")]
    pub category: Kind,
    /// Days of commits the window keeps.
    #[arg(long, default_value_t = 7)]
    pub keep_days: u64,
    /// The role appends run as: retention checks that it caps its transactions (`transaction_timeout`).
    #[arg(long, default_value = "postgres")]
    pub writer_role: String,
}

/// The generated categories.
#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
pub enum Kind {
    /// Monthly leaves, single-event commits, long-lived streams.
    Orders,
    /// Monthly leaves, 10-event commits.
    Audit,
    /// Daily leaves, single-event commits, short-lived streams.
    Jobs,
}

/// Runs `pges-bench scale`.
pub async fn run(server: &database::Server, args: &Args) -> Result<report::Report> {
    let database::Server::Url(server_url) = server else {
        bail!("scale needs --database-url: it works in a database that outlives the command");
    };

    match &args.action {
        Action::Fill(fill_args) => fill(server_url, fill_args).await,
        Action::Append(append_args) => append(server_url, append_args).await,
        Action::Load(load_args) => load(server_url, load_args).await,
        Action::CatchUp(catch_up_args) => catch_up(server_url, catch_up_args).await,
        Action::Drain(drain_args) => drain(server_url, drain_args).await,
        Action::Retention(retention_args) => apply_retention(server_url, retention_args).await,
    }
}

/// An `orders` event.
#[derive(Debug, Clone, PartialEq, pges::Event)]
#[event(category = "orders")]
pub enum OrderEvent {
    /// A recorded payload.
    #[event(name = "orders.recorded")]
    Recorded {
        /// Pseudo-random text.
        data: String,
    },
}

/// An `audit` event.
#[derive(Debug, Clone, PartialEq, pges::Event)]
#[event(category = "audit")]
pub enum AuditEvent {
    /// A recorded payload.
    #[event(name = "audit.recorded")]
    Recorded {
        /// Pseudo-random text.
        data: String,
    },
}

/// A `jobs` event.
#[derive(Debug, Clone, PartialEq, pges::Event)]
#[event(category = "jobs")]
pub enum JobEvent {
    /// A recorded payload.
    #[event(name = "jobs.recorded")]
    Recorded {
        /// Pseudo-random text.
        data: String,
    },
}

/// Counts a stream's events, for `load`.
#[derive(Debug)]
pub struct Tally<E> {
    /// Events applied.
    pub events: u64,
    kind: PhantomData<fn(E)>,
}

impl<E> Default for Tally<E> {
    fn default() -> Self {
        Self {
            events: 0,
            kind: PhantomData,
        }
    }
}

impl<E: pges::core::event::Event> aggregate::Aggregate for Tally<E> {
    type Event = E;

    fn apply(&mut self, _event: &E) {
        self.events += 1;
    }
}

/// An event of a generated category with a payload of a given size.
trait Generated: pges::core::event::Event + Clone + Send + Sync + 'static {
    fn sized(size: usize, rng: &mut event::Rng) -> Self;
}

impl Generated for OrderEvent {
    fn sized(size: usize, rng: &mut event::Rng) -> Self {
        Self::Recorded {
            data: text(size, rng),
        }
    }
}

impl Generated for AuditEvent {
    fn sized(size: usize, rng: &mut event::Rng) -> Self {
        Self::Recorded {
            data: text(size, rng),
        }
    }
}

impl Generated for JobEvent {
    fn sized(size: usize, rng: &mut event::Rng) -> Self {
        Self::Recorded {
            data: text(size, rng),
        }
    }
}

/// How a category is generated.
struct Shape {
    kind: Kind,
    bucket: admin::Bucket,
    /// Thousandths of `--streams`.
    share: u64,
    /// Commits per 10 streams.
    commits_per_ten: u64,
    events: i16,
}

const SHAPES: [Shape; 3] = [
    Shape {
        kind: Kind::Orders,
        bucket: admin::Bucket::Month,
        share: 600,
        commits_per_ten: 25,
        events: 1,
    },
    Shape {
        kind: Kind::Audit,
        bucket: admin::Bucket::Month,
        share: 200,
        commits_per_ten: 20,
        events: 10,
    },
    Shape {
        kind: Kind::Jobs,
        bucket: admin::Bucket::Day,
        share: 200,
        commits_per_ten: 20,
        events: 1,
    },
];

impl Kind {
    fn category(self) -> category::CategoryName {
        match self {
            Self::Orders => OrderEvent::CATEGORY,
            Self::Audit => AuditEvent::CATEGORY,
            Self::Jobs => JobEvent::CATEGORY,
        }
    }
}

/// One generated category, as `fill` recorded it.
#[derive(Debug, Clone)]
struct Generation {
    category: String,
    category_id: i32,
    streams: i64,
    commits: i64,
    events: i16,
    t0: i64,
    span: i64,
}

impl Generation {
    /// The time of commit `g`, in milliseconds since the epoch, as `scale_commit_id` computes it.
    fn time(&self, g: i64) -> i64 {
        let offset = i128::from(self.span) * i128::from(g) / i128::from(self.commits);
        self.t0 + i64::try_from(offset).unwrap_or(i64::MAX)
    }

    /// The first commit whose time is at or after `at`.
    fn first_at(&self, at: i64) -> i64 {
        let numerator = i128::from(at - self.t0) * i128::from(self.commits);
        let span = i128::from(self.span);
        let first = if numerator <= 0 {
            0
        } else {
            (numerator + span - 1) / span
        };
        i64::try_from(first.min(i128::from(self.commits))).unwrap_or(self.commits)
    }
}

async fn fill(server_url: &str, args: &FillArgs) -> Result<report::Report> {
    ensure!(
        args.workers > 0 && args.stream_workers > 0 && args.chunk > 0 && args.log_window > 0,
        "--workers, --stream-workers, --chunk and --log-window must be at least 1"
    );
    let started = Instant::now();
    create_database(server_url, &args.target.database).await?;
    let pool = pool(
        server_url,
        &args.target.database,
        u32::try_from(args.workers + args.stream_workers + 4)?,
    )
    .await?;
    for statement in [
        &sql::statement::COMMIT_ID_FUNCTION,
        &sql::statement::STREAM_ID_FUNCTION,
        &sql::statement::DATASET_CREATE,
    ] {
        pool.execute(statement.sql()).await?;
    }
    ensure!(
        dataset(&pool, &args.target.store).await?.is_empty(),
        "store {} was filled before; drop the database or pick another --store",
        args.target.store
    );

    let mut definition = store::StoreDefinition::default();
    definition.stream_partitions =
        NonZeroU16::new(args.stream_partitions).context("--stream-partitions must not be 0")?;
    let store = open(&pool, &args.target.store, Some(definition)).await?;
    let values = template::Values::new().store(postgres_sql::store_name(&store));

    let mut generations = Vec::new();
    let mut payloads = Vec::new();
    for shape in &SHAPES {
        let (generation, payload) = prepare_category(&pool, &store, &values, shape, args).await?;
        sql::statement::dataset_insert::Query::fixed()
            .bind(sql::statement::dataset_insert::Params {
                store: &args.target.store,
                category: &generation.category,
                category_id: generation.category_id,
                streams: generation.streams,
                commits: generation.commits,
                events: generation.events,
                t0: generation.t0,
                span: generation.span,
            })
            .execute(&pool)
            .await?;
        generations.push(generation);
        payloads.push(payload);
    }
    store
        .admin()
        .maintain()
        .await
        .context("creating partitions")?;
    eprintln!("prepared the store in {:.0?}", started.elapsed());

    let generations: Arc<[Generation]> = generations.into();
    let mut tasks = tokio::task::JoinSet::new();
    tasks.spawn(fill_streams(
        pool.clone(),
        values.clone(),
        generations.clone(),
        args.chunk,
        args.stream_workers,
    ));
    if args.content == Content::All {
        tasks.spawn(fill_commits(
            pool.clone(),
            values.clone(),
            generations.clone(),
            payloads.into(),
            args.chunk,
            args.workers,
        ));
        tasks.spawn(fill_log(
            pool.clone(),
            store.clone(),
            values.clone(),
            generations.clone(),
            args.log_window,
        ));
    }
    while let Some(filled) = tasks.join_next().await {
        filled.context("a fill task panicked")??;
    }

    sql::statement::set_positions::Query::render(&values)
        .bind(sql::statement::set_positions::Params {
            store: &args.target.store,
        })
        .execute(&pool)
        .await?;
    let analyzed = Instant::now();
    pool.execute(sql::statement::ANALYZE.sql()).await?;
    eprintln!("analysed in {:.0?}", analyzed.elapsed());
    pool.close().await;

    let mut report = report::Report::new(
        "scale fill",
        &["category", "streams", "commits", "events/commit", "seconds"],
    );
    let seconds = started.elapsed().as_secs_f64();
    for generation in generations.iter() {
        let commits = match args.content {
            Content::All => generation.commits,
            Content::Streams => 0,
        };
        report.push(
            json!({
                "bench": "scale",
                "what": "fill",
                "store": args.target.store,
                "stream_partitions": args.stream_partitions,
                "category": generation.category,
                "streams": generation.streams,
                "commits": commits,
                "events_per_commit": generation.events,
                "seconds": seconds,
            }),
            vec![
                generation.category.clone(),
                generation.streams.to_string(),
                commits.to_string(),
                generation.events.to_string(),
                format!("{seconds:.0}"),
            ],
        );
    }

    Ok(report)
}

/// Registers a category, creates its past leaves and takes a payload frame from one real append. Returns
/// what will be generated, and the frame's format and bytes.
async fn prepare_category(
    pool: &PgPool,
    store: &store::Store,
    values: &template::Values,
    shape: &Shape,
    args: &FillArgs,
) -> Result<(Generation, (i16, Vec<u8>))> {
    let category = shape.kind.category();
    let mut config = admin::CategoryConfig::default();
    config.bucket = shape.bucket;
    store
        .admin()
        .register_category(category, config)
        .await
        .with_context(|| format!("registering {category}"))?;
    let category_id = sql::statement::category_named::Query::render(values)
        .bind(sql::statement::category_named::Params {
            name: category.as_str(),
        })
        .fetch_one(pool)
        .await?
        .category_id;

    let (unit, step, format, back) = match shape.bucket {
        admin::Bucket::Day => ("day", "1 day", "YYYYMMDD", args.days.saturating_sub(1)),
        _ => ("month", "1 month", "YYYYMM", args.months.saturating_sub(1)),
    };
    let back = i32::try_from(back)?;
    let span = sql::statement::span::Query::fixed()
        .bind(sql::statement::span::Params { unit, step, back })
        .fetch_one(pool)
        .await?;
    if args.content == Content::All {
        create_past_leaves(
            pool,
            values,
            category,
            category_id,
            (unit, step, format, back),
        )
        .await?;
    }

    let payload = match shape.kind {
        Kind::Orders => template_payload::<OrderEvent>(pool, store, values, shape, args).await?,
        Kind::Audit => template_payload::<AuditEvent>(pool, store, values, shape, args).await?,
        Kind::Jobs => template_payload::<JobEvent>(pool, store, values, shape, args).await?,
    };

    let streams = i64::try_from(args.streams * shape.share / 1000)?.max(1);
    let generation = Generation {
        category: category.as_str().to_owned(),
        category_id,
        streams,
        commits: streams * i64::try_from(shape.commits_per_ten)? / 10,
        events: shape.events,
        t0: span.t0,
        span: span.t1 - span.t0,
    };
    Ok((generation, payload))
}

/// Creates the category's leaves before those `Admin::maintain` creates, in one transaction, with the
/// statements `Admin::maintain` runs.
async fn create_past_leaves(
    pool: &PgPool,
    values: &template::Values,
    category: category::CategoryName,
    category_id: i32,
    (unit, step, format, back): (&str, &str, &str, i32),
) -> Result<()> {
    let values = values
        .clone()
        .category(category)
        .category_id(category::CategoryId::new(category_id));
    let mut tx = pool.begin().await?;
    tx.execute(sql::statement::UTC.sql()).await?;
    let buckets = sql::statement::past_buckets::Query::fixed()
        .bind(sql::statement::past_buckets::Params {
            unit,
            step,
            format,
            back,
        })
        .fetch_all(&mut *tx)
        .await?;
    for bucket in buckets {
        let leaf = partitions::ident(&format!("commits_{}_{}", category.as_str(), bucket.suffix))?;
        let leaf_values = values
            .clone()
            .ident(template::IdentPlaceholder::Leaf, &leaf)
            .ident(template::IdentPlaceholder::Table, &leaf)
            .ident(
                template::IdentPlaceholder::Index,
                &partitions::ident(&format!("{leaf}_s"))?,
            )
            .ident(
                template::IdentPlaceholder::Bound,
                &partitions::ident("bound")?,
            )
            .range(
                &partitions::timestamptz(&bucket.lower)?,
                &partitions::timestamptz(&bucket.upper)?,
            );
        for statement in [
            &postgres_sql::statement::CREATE_LEAF,
            &postgres_sql::statement::COMPRESS_LEAF,
            &postgres_sql::statement::CREATE_LEAF_INDEX,
            &postgres_sql::statement::ADD_LEAF_BOUND,
            &postgres_sql::statement::ATTACH_LEAF,
            &postgres_sql::statement::DROP_BOUND,
        ] {
            tx.execute(statement.render_text(&leaf_values)).await?;
        }
    }
    tx.commit().await?;

    Ok(())
}

/// Appends one commit of the shape's events to a new stream, reads back its payload frame, and deletes the
/// commit and the stream again.
async fn template_payload<E: Generated>(
    pool: &PgPool,
    store: &store::Store,
    values: &template::Values,
    shape: &Shape,
    args: &FillArgs,
) -> Result<(i16, Vec<u8>)> {
    store.register::<E>().await?;
    let mut rng = event::Rng::new(11);
    let events: Vec<E> = (0..shape.events)
        .map(|_| E::sized(args.payload, &mut rng))
        .collect();
    let stream = stream::StreamId::new_v7();
    let appended = store
        .append(stream, version::ExpectedVersion::NoStream, &events)
        .await
        .context("appending the template commit")?;
    let commit_id = appended.commit_id.as_uuid();

    let frame = sql::statement::payload_frame::Query::render(values)
        .bind(sql::statement::payload_frame::Params { commit_id })
        .fetch_one(pool)
        .await?;
    sql::statement::forget_template::Query::render(values)
        .bind(sql::statement::forget_template::Params {
            commit_id,
            stream_id: stream.as_uuid(),
        })
        .execute(pool)
        .await?;

    Ok((frame.format, frame.payload))
}

/// Inserts every category's commits, in chunks ordered by time, from `workers` connections.
async fn fill_commits(
    pool: PgPool,
    values: template::Values,
    generations: Arc<[Generation]>,
    payloads: Arc<[(i16, Vec<u8>)]>,
    chunk: u64,
    workers: usize,
) -> Result<()> {
    let chunk = i64::try_from(chunk)?;
    let mut chunks: Vec<(usize, i64, i64)> = Vec::new();
    for (index, generation) in generations.iter().enumerate() {
        let mut from = 0;
        while from < generation.commits {
            let to = (from + chunk).min(generation.commits);
            chunks.push((index, from, to));
            from = to;
        }
    }
    chunks.sort_by_key(|(index, from, _)| generations[*index].time(*from));
    let total: i64 = generations
        .iter()
        .map(|generation| generation.commits)
        .sum();

    let statement = sql::statement::fill_commits::Query::render(&values);
    let chunks: Arc<[(usize, i64, i64)]> = chunks.into();
    let next = Arc::new(AtomicUsize::new(0));
    let done = Arc::new(AtomicU64::new(0));
    let started = Instant::now();
    let mut tasks = tokio::task::JoinSet::new();
    for _ in 0..workers {
        let (pool, statement, generations, payloads, chunks, next, done) = (
            pool.clone(),
            statement.clone(),
            generations.clone(),
            payloads.clone(),
            chunks.clone(),
            next.clone(),
            done.clone(),
        );
        tasks.spawn(async move {
            while let Some(&(index, from, to)) = chunks.get(next.fetch_add(1, Ordering::Relaxed)) {
                let generation = &generations[index];
                let (format, payload) = &payloads[index];
                statement
                    .bind(sql::statement::fill_commits::Params {
                        category_id: generation.category_id,
                        from,
                        to,
                        streams: generation.streams,
                        commits: generation.commits,
                        t0: generation.t0,
                        span: generation.span,
                        events: generation.events,
                        format: *format,
                        payload,
                    })
                    .execute(&pool)
                    .await
                    .with_context(|| {
                        format!("inserting commits {from}..{to} of {}", generation.category)
                    })?;
                let filled = done.fetch_add(u64::try_from(to - from)?, Ordering::Relaxed)
                    + u64::try_from(to - from)?;
                progress("commits", filled, total, started);
            }
            anyhow::Ok(())
        });
    }
    while let Some(filled) = tasks.join_next().await {
        filled??;
    }

    Ok(())
}

/// Inserts every category's streams in chunks, from `workers` connections.
async fn fill_streams(
    pool: PgPool,
    values: template::Values,
    generations: Arc<[Generation]>,
    chunk: u64,
    workers: usize,
) -> Result<()> {
    let chunk = i64::try_from(chunk)?;
    let mut chunks: Vec<(usize, i64, i64)> = Vec::new();
    for (index, generation) in generations.iter().enumerate() {
        let mut from = 0;
        while from < generation.streams {
            let to = (from + chunk).min(generation.streams);
            chunks.push((index, from, to));
            from = to;
        }
    }
    let total: i64 = generations
        .iter()
        .map(|generation| generation.streams)
        .sum();

    let statement = sql::statement::fill_streams::Query::render(&values);
    let chunks: Arc<[(usize, i64, i64)]> = chunks.into();
    let next = Arc::new(AtomicUsize::new(0));
    let done = Arc::new(AtomicU64::new(0));
    let started = Instant::now();
    let mut tasks = tokio::task::JoinSet::new();
    for _ in 0..workers {
        let (pool, statement, generations, chunks, next, done) = (
            pool.clone(),
            statement.clone(),
            generations.clone(),
            chunks.clone(),
            next.clone(),
            done.clone(),
        );
        tasks.spawn(async move {
            while let Some(&(index, from, to)) = chunks.get(next.fetch_add(1, Ordering::Relaxed)) {
                let generation = &generations[index];
                statement
                    .bind(sql::statement::fill_streams::Params {
                        category_id: generation.category_id,
                        from,
                        to,
                        streams: generation.streams,
                        commits: generation.commits,
                        t0: generation.t0,
                        span: generation.span,
                        events: generation.events,
                    })
                    .execute(&pool)
                    .await
                    .with_context(|| {
                        format!("inserting streams {from}..{to} of {}", generation.category)
                    })?;
                let filled = done.fetch_add(u64::try_from(to - from)?, Ordering::Relaxed)
                    + u64::try_from(to - from)?;
                progress("streams", filled, total, started);
            }
            anyhow::Ok(())
        });
    }
    while let Some(filled) = tasks.join_next().await {
        filled??;
    }

    Ok(())
}

/// Writes the log one time window at a time, oldest first, so positions follow commit id order across the
/// categories; runs partition maintenance between windows, which adds log partitions as positions grow.
async fn fill_log(
    pool: PgPool,
    store: store::Store,
    values: template::Values,
    generations: Arc<[Generation]>,
    window: u64,
) -> Result<()> {
    let total: i64 = generations
        .iter()
        .map(|generation| generation.commits)
        .sum();
    let windows = i64::try_from(u64::try_from(total)?.div_ceil(window).max(1))?;
    let first = generations
        .iter()
        .map(|generation| generation.t0)
        .min()
        .context("no categories")?;
    let end = generations
        .iter()
        .map(|generation| generation.t0 + generation.span)
        .max()
        .context("no categories")?
        + 1;

    let statement = sql::statement::fill_log::Query::render(&values);
    let started = Instant::now();
    let mut position = 0;
    let mut written = 0;
    for window in 0..windows {
        let lower = first + (end - first) * window / windows;
        let upper = first + (end - first) * (window + 1) / windows;
        let ranges: Vec<(i64, i64)> = generations
            .iter()
            .map(|generation| (generation.first_at(lower), generation.first_at(upper)))
            .collect();
        let column =
            |pick: fn(&Generation) -> i64| -> Vec<i64> { generations.iter().map(pick).collect() };
        position = statement
            .bind(sql::statement::fill_log::Params {
                category_ids: &generations
                    .iter()
                    .map(|generation| generation.category_id)
                    .collect::<Vec<_>>(),
                froms: &ranges.iter().map(|range| range.0).collect::<Vec<_>>(),
                tos: &ranges.iter().map(|range| range.1).collect::<Vec<_>>(),
                t0s: &column(|generation| generation.t0),
                spans: &column(|generation| generation.span),
                commits: &column(|generation| generation.commits),
                events: &generations
                    .iter()
                    .map(|generation| generation.events)
                    .collect::<Vec<_>>(),
                after: position,
            })
            .fetch_one(&pool)
            .await
            .with_context(|| format!("writing log window {window}"))?
            .last_position;
        written += ranges.iter().map(|range| range.1 - range.0).sum::<i64>();
        progress("log", u64::try_from(written)?, total, started);
        store
            .admin()
            .maintain()
            .await
            .context("creating log partitions")?;
    }

    Ok(())
}

/// Prints how far a fill task is.
fn progress(what: &str, done: u64, total: i64, started: Instant) {
    let total = u64::try_from(total).unwrap_or(1).max(1);
    let elapsed = started.elapsed();
    eprintln!(
        "{what}: {done} of {total} ({:.0}%) in {:.0?}, {:.0}/s",
        done as f64 * 100.0 / total as f64,
        elapsed,
        measure::per_second(done, elapsed)
    );
}

async fn append(server_url: &str, args: &AppendArgs) -> Result<report::Report> {
    let connections = args.concurrency.iter().copied().max().unwrap_or(1);
    let pool = pool(
        server_url,
        &args.target.database,
        crate::append::connections(connections),
    )
    .await?;
    let store = open(&pool, &args.target.store, None).await?;
    let generation = generation(&pool, &args.target.store, args.category).await?;
    let streams = match args.streams {
        Streams::Existing => Some(sample(&pool, &generation, args.sample).await?),
        Streams::New => None,
    };

    let mut report = report::Report::new(
        "scale append",
        &[
            "streams", "category", "conc", "calls/s", "p50 ms", "p99 ms", "p99.9 ms", "errors",
        ],
    );
    for concurrency in &args.concurrency {
        let load = match args.category {
            Kind::Orders => {
                append_load::<OrderEvent>(&store, streams.clone(), *concurrency, args).await?
            }
            Kind::Audit => {
                append_load::<AuditEvent>(&store, streams.clone(), *concurrency, args).await?
            }
            Kind::Jobs => {
                append_load::<JobEvent>(&store, streams.clone(), *concurrency, args).await?
            }
        };
        let summary = load.latency.summary();
        let which = format!("{:?}", args.streams).to_lowercase();
        let [p50, p99, p999] = report::latency_cells(&summary);
        report.push(
            json!({
                "bench": "scale",
                "what": "append",
                "streams": which,
                "category": generation.category,
                "category_streams": generation.streams,
                "concurrency": concurrency,
                "seconds": load.elapsed.as_secs_f64(),
                "calls": load.calls,
                "calls_per_s": load.calls_per_second(),
                "latency": summary.json(),
                "errors": load.errors,
                "first_error": load.first_error,
            }),
            vec![
                which,
                generation.category.clone(),
                concurrency.to_string(),
                report::rate(load.calls_per_second()),
                p50,
                p99,
                p999,
                load.errors.to_string(),
            ],
        );
    }
    pool.close().await;

    Ok(report)
}

/// A single-event append to a random existing stream, or to a new stream.
#[derive(Clone)]
struct AppendOperation<E> {
    store: store::Store,
    streams: Option<Arc<[stream::StreamId]>>,
    events: Arc<[E]>,
}

impl<E: Generated> measure::Operation for AppendOperation<E> {
    async fn call(&self, rng: &mut event::Rng) -> Result<u64> {
        let event = &self.events[rng.below(self.events.len())..][..1];
        match &self.streams {
            Some(streams) => {
                let stream = streams[rng.below(streams.len())];
                self.store
                    .append(stream, version::ExpectedVersion::Any, event)
                    .await?;
            }
            None => {
                self.store
                    .append(
                        stream::StreamId::new_v7(),
                        version::ExpectedVersion::NoStream,
                        event,
                    )
                    .await?;
            }
        }

        Ok(1)
    }
}

async fn append_load<E: Generated>(
    store: &store::Store,
    streams: Option<Arc<[stream::StreamId]>>,
    concurrency: usize,
    args: &AppendArgs,
) -> Result<measure::Load> {
    let mut rng = event::Rng::new(3);
    let operation = AppendOperation {
        store: store.clone(),
        streams,
        events: (0..64)
            .map(|_| E::sized(args.payload, &mut rng))
            .collect::<Vec<E>>()
            .into(),
    };
    measure::warm_up(operation.clone(), concurrency, args.warmup).await?;
    measure::drive(
        operation,
        concurrency,
        args.duration,
        measure::Pacing::Closed,
        1,
    )
    .await
}

async fn load(server_url: &str, args: &LoadArgs) -> Result<report::Report> {
    let connections = args.concurrency.iter().copied().max().unwrap_or(1);
    let pool = pool(
        server_url,
        &args.target.database,
        crate::append::connections(connections),
    )
    .await?;
    let store = open(&pool, &args.target.store, None).await?;
    let generation = generation(&pool, &args.target.store, args.category).await?;
    let streams = sample(&pool, &generation, args.sample).await?;

    let mut report = report::Report::new(
        "scale load",
        &[
            "category",
            "conc",
            "loads/s",
            "events/load",
            "p50 ms",
            "p99 ms",
            "p99.9 ms",
            "errors",
        ],
    );
    for concurrency in &args.concurrency {
        let load = match args.category {
            Kind::Orders => {
                load_streams::<OrderEvent>(&store, &streams, *concurrency, args).await?
            }
            Kind::Audit => load_streams::<AuditEvent>(&store, &streams, *concurrency, args).await?,
            Kind::Jobs => load_streams::<JobEvent>(&store, &streams, *concurrency, args).await?,
        };
        let summary = load.latency.summary();
        let [p50, p99, p999] = report::latency_cells(&summary);
        let per_load = load.events as f64 / load.calls.max(1) as f64;
        report.push(
            json!({
                "bench": "scale",
                "what": "load",
                "category": generation.category,
                "category_streams": generation.streams,
                "concurrency": concurrency,
                "seconds": load.elapsed.as_secs_f64(),
                "loads": load.calls,
                "loads_per_s": load.calls_per_second(),
                "events_per_load": per_load,
                "latency": summary.json(),
                "errors": load.errors,
                "first_error": load.first_error,
            }),
            vec![
                generation.category.clone(),
                concurrency.to_string(),
                report::rate(load.calls_per_second()),
                format!("{per_load:.1}"),
                p50,
                p99,
                p999,
                load.errors.to_string(),
            ],
        );
    }
    pool.close().await;

    Ok(report)
}

/// A `load` of a random existing stream.
#[derive(Clone)]
struct LoadOperation<E> {
    store: store::Store,
    streams: Arc<[stream::StreamId]>,
    kind: PhantomData<fn(E)>,
}

impl<E: Generated> measure::Operation for LoadOperation<E> {
    async fn call(&self, rng: &mut event::Rng) -> Result<u64> {
        let stream = self.streams[rng.below(self.streams.len())];
        let loaded = self.store.load::<Tally<E>>(stream).await?;
        ensure!(loaded.state().events > 0, "stream {stream:?} loaded empty");

        Ok(loaded.state().events)
    }
}

async fn load_streams<E: Generated>(
    store: &store::Store,
    streams: &Arc<[stream::StreamId]>,
    concurrency: usize,
    args: &LoadArgs,
) -> Result<measure::Load> {
    let operation = LoadOperation::<E> {
        store: store.clone(),
        streams: streams.clone(),
        kind: PhantomData,
    };
    measure::warm_up(operation.clone(), concurrency, args.warmup).await?;
    measure::drive(
        operation,
        concurrency,
        args.duration,
        measure::Pacing::Closed,
        1,
    )
    .await
}

async fn catch_up(server_url: &str, args: &CatchUpArgs) -> Result<report::Report> {
    ensure!(
        (0.0..1.0).contains(&args.from),
        "--from is a share of the newest position, from 0 up to 1"
    );
    let pool = pool(server_url, &args.target.database, 4).await?;
    let store = open(&pool, &args.target.store, None).await?;
    let values = template::Values::new().store(postgres_sql::store_name(&store));
    let newest = newest_position(&pool, &values).await?;
    let from = (newest as f64 * args.from) as u64;

    let decoded = Arc::new(AtomicU64::new(0));
    let name = subscription::SubscriptionName::parse(format!(
        "scale.catch-up.{}",
        Uuid::now_v7().simple()
    ))?;
    let builder = store
        .subscription(name)
        .start(match position::Position::new(from) {
            Some(position) if from > 0 => subscription::Start::Position(position),
            _ => subscription::Start::Beginning,
        })
        .batch_size(
            std::num::NonZeroU32::new(args.batch_size).context("--batch-size must not be 0")?,
        );
    let handler = Counting {
        scope: args.scope,
        events: decoded.clone(),
    };
    let started = Instant::now();
    let handle = match args.scope {
        Scope::Orders => builder.category::<OrderEvent>().run(handler),
        Scope::Audit => builder.category::<AuditEvent>().run(handler),
        Scope::Jobs => builder.category::<JobEvent>().run(handler),
        Scope::Store => builder.all_categories().run(handler),
    }
    .spawn();
    let mut status = handle.status_changes();
    let stopped = tokio::time::timeout(
        args.duration,
        status.wait_for(|status| {
            status.position.get() >= newest || matches!(status.state, subscription::State::Stopped)
        }),
    )
    .await
    .map(|waited| waited.map(|_| ()));
    let elapsed = started.elapsed();
    let events = decoded.load(Ordering::Relaxed);
    let reached = status.borrow().position.get();
    handle.shutdown().await?;
    pool.close().await;
    if let Ok(waited) = stopped {
        waited.context("the subscription is gone")?;
        ensure!(reached >= newest, "the subscription stopped at {reached}");
    }

    let rate = measure::per_second(events, elapsed);
    let mut report = report::Report::new(
        "scale catch-up",
        &["scope", "from", "events", "positions", "events/s", "batch"],
    );
    let scope = format!("{:?}", args.scope).to_lowercase();
    report.push(
        json!({
            "bench": "scale",
            "what": "catch-up",
            "scope": scope,
            "from": from,
            "newest": newest,
            "reached": reached,
            "events": events,
            "seconds": elapsed.as_secs_f64(),
            "events_per_s": rate,
            "batch_size": args.batch_size,
        }),
        vec![
            scope,
            from.to_string(),
            events.to_string(),
            reached.saturating_sub(from).to_string(),
            report::rate(rate),
            args.batch_size.to_string(),
        ],
    );

    Ok(report)
}

/// Decodes and counts the events of its scope.
struct Counting {
    scope: Scope,
    events: Arc<AtomicU64>,
}

impl handler::Handler for Counting {
    async fn handle(&mut self, batch: &batch::Batch) -> Result<(), handler::HandlerError> {
        let decoded = match self.scope {
            Scope::Orders => count::<OrderEvent>(batch)?,
            Scope::Audit => count::<AuditEvent>(batch)?,
            Scope::Jobs => count::<JobEvent>(batch)?,
            Scope::Store => {
                count::<OrderEvent>(batch)?
                    + count::<AuditEvent>(batch)?
                    + count::<JobEvent>(batch)?
            }
        };
        self.events.fetch_add(decoded, Ordering::Relaxed);

        Ok(())
    }
}

fn count<E: pges::core::event::Event>(batch: &batch::Batch) -> Result<u64, handler::HandlerError> {
    let mut decoded = 0;
    for recorded in batch.decode::<E>() {
        recorded?;
        decoded += 1;
    }

    Ok(decoded)
}

async fn drain(server_url: &str, args: &DrainArgs) -> Result<report::Report> {
    let pool = pool(server_url, &args.target.database, 16).await?;
    let store = open(&pool, &args.target.store, None).await?;
    let values = template::Values::new().store(postgres_sql::store_name(&store));
    let url = database_url(server_url, &args.target.database)?;

    let mut source = logical::LogicalSource::new(
        pool.clone(),
        config::Config::from_url(&url)?,
        logical::LogicalConfig::default(),
    );
    let bootstrapped = Instant::now();
    let resequenced = match source.bootstrap().await {
        Ok(outcome) => {
            eprintln!("bootstrap: {outcome:?} in {:.1?}", bootstrapped.elapsed());
            None
        }
        Err(source::SourceError::Halt(halt)) => {
            eprintln!(
                "bootstrap halted in {:.1?} ({halt}); resequencing",
                bootstrapped.elapsed()
            );
            let started = Instant::now();
            let resequenced = source
                .resequence(&logical::ResequenceConfig::new(limit::WriterRoles::one(
                    args.writer_role.clone(),
                )))
                .await
                .context("resequencing")?;
            let elapsed = started.elapsed();
            let recovered: Vec<String> = resequenced
                .stores
                .iter()
                .map(|recovered| format!("{recovered:?}"))
                .collect();
            eprintln!("resequenced in {elapsed:.1?}: {}", recovered.join(", "));
            Some(elapsed)
        }
        Err(error) => return Err(error).context("bootstrapping the logical source"),
    };

    let before = newest_position(&pool, &values).await?;
    let generation = generation(&pool, &args.target.store, Kind::Orders).await?;
    let streams = sample(&pool, &generation, 1_000_000).await?;
    let appended = Instant::now();
    let commits = append_backlog(&store, &streams, args.commits).await?;
    let append_elapsed = appended.elapsed();

    let sequencer =
        sequencer::Sequencer::new(pool.clone(), source, sequencer::SequencerConfig::default());
    let mut high_water = sequencer.watch().high_water(store.store_id());
    let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
    let started = Instant::now();
    let sequencing = tokio::spawn(sequencer.run(async { stopped.await.unwrap_or_default() }));
    let target = before + commits;
    let reached = tokio::time::timeout(
        args.limit,
        high_water.wait_for(|high_water| high_water.store().get() >= target),
    )
    .await;
    let drain_elapsed = started.elapsed();
    let _ = stop.send(());
    let ended = sequencing.await;
    pool.close().await;
    match reached {
        Ok(Ok(_)) => {}
        Ok(Err(_)) => bail!("the sequencer is gone"),
        Err(_) => bail!("the log did not reach {target} within {:?}", args.limit),
    }
    ended.context("the sequencer panicked")??;

    let rate = measure::per_second(commits, drain_elapsed);
    let mut report = report::Report::new(
        "scale drain",
        &[
            "resequence s",
            "backlog",
            "append s",
            "drain s",
            "drain commits/s",
        ],
    );
    report.push(
        json!({
            "bench": "scale",
            "what": "drain",
            "resequence_seconds": resequenced.map(|elapsed| elapsed.as_secs_f64()),
            "log_before": before,
            "commits": commits,
            "append_seconds": append_elapsed.as_secs_f64(),
            "drain_seconds": drain_elapsed.as_secs_f64(),
            "drain_commits_per_s": rate,
        }),
        vec![
            resequenced.map_or_else(
                || "-".into(),
                |elapsed| format!("{:.1}", elapsed.as_secs_f64()),
            ),
            commits.to_string(),
            format!("{:.1}", append_elapsed.as_secs_f64()),
            format!("{:.1}", drain_elapsed.as_secs_f64()),
            report::rate(rate),
        ],
    );

    Ok(report)
}

/// Appends about `commits` single-event commits to random streams of `streams`, 100 per `append_many`, from
/// 8 writers. A stream drawn twice for one call is appended to once. Returns the commits appended.
async fn append_backlog(
    store: &store::Store,
    streams: &Arc<[stream::StreamId]>,
    commits: u64,
) -> Result<u64> {
    const PER_CALL: u64 = 100;
    const WRITERS: u64 = 8;

    let events: Arc<[OrderEvent]> = {
        let mut rng = event::Rng::new(5);
        (0..64)
            .map(|_| OrderEvent::sized(100, &mut rng))
            .collect::<Vec<_>>()
            .into()
    };
    let calls = commits.div_ceil(PER_CALL);
    let mut tasks = tokio::task::JoinSet::new();
    for writer in 0..WRITERS {
        let (store, streams, events) = (store.clone(), streams.clone(), events.clone());
        tasks.spawn(async move {
            let mut rng = event::Rng::new(100 + writer);
            let mut appended = 0;
            for call in (writer..calls).step_by(WRITERS as usize) {
                let first = call * PER_CALL;
                let mut picked: Vec<stream::StreamId> = (first..(first + PER_CALL).min(commits))
                    .map(|_| streams[rng.below(streams.len())])
                    .collect();
                picked.sort_by_key(|stream| stream.as_uuid());
                picked.dedup();
                appended += picked.len() as u64;
                let entries = picked.iter().enumerate().map(|(n, stream)| {
                    pges::postgres::append::AppendEntry::new(
                        *stream,
                        version::ExpectedVersion::Any,
                        std::slice::from_ref(&events[n % events.len()]),
                    )
                });
                store.append_many(entries).await?;
            }
            anyhow::Ok(appended)
        });
    }
    let mut appended = 0;
    while let Some(written) = tasks.join_next().await {
        appended += written??;
    }

    Ok(appended)
}

async fn apply_retention(server_url: &str, args: &RetentionArgs) -> Result<report::Report> {
    let pool = pool(server_url, &args.target.database, 4).await?;
    let store = open(&pool, &args.target.store, None).await?;
    let admin = store.admin();
    let category = args.category.category();
    admin
        .set_retention(
            category,
            retention::Retention::Window {
                keep: Duration::from_secs(args.keep_days * 86_400),
                then: retention::AfterWindow::Drop,
            },
        )
        .await?;

    let started = Instant::now();
    let applied = admin
        .apply_retention(&retention::RetentionConfig::new(limit::WriterRoles::one(
            args.writer_role.clone(),
        )))
        .await?;
    let elapsed = started.elapsed();
    admin
        .set_retention(category, retention::Retention::Forever)
        .await?;
    pool.close().await;

    let refused: Vec<String> = applied
        .refused
        .iter()
        .map(|refusal| format!("{refusal:?}"))
        .collect();
    let mut report = report::Report::new(
        "scale retention",
        &[
            "category",
            "keep days",
            "leaves dropped",
            "refused",
            "seconds",
        ],
    );
    report.push(
        json!({
            "bench": "scale",
            "what": "retention",
            "category": category.as_str(),
            "keep_days": args.keep_days,
            "dropped": applied.dropped.iter().map(|leaf| leaf.leaf.to_string()).collect::<Vec<_>>(),
            "log_partitions_dropped": applied.log_partitions.len(),
            "refused": refused,
            "seconds": elapsed.as_secs_f64(),
        }),
        vec![
            category.as_str().to_owned(),
            args.keep_days.to_string(),
            applied.dropped.len().to_string(),
            applied.refused.len().to_string(),
            format!("{:.1}", elapsed.as_secs_f64()),
        ],
    );

    Ok(report)
}

/// The URL of `database` on the server of `server_url`.
fn database_url(server_url: &str, database: &str) -> Result<String> {
    let mut url = Url::parse(server_url).context("parsing --database-url")?;
    url.set_path(&format!("/{database}"));
    Ok(url.into())
}

async fn create_database(server_url: &str, database: &str) -> Result<()> {
    let mut admin = PgConnection::connect(server_url)
        .await
        .context("connecting to --database-url")?;
    let exists = sql::statement::database_exists::Query::fixed()
        .bind(sql::statement::database_exists::Params { name: database })
        .fetch_one(&mut admin)
        .await?
        .exists;
    if !exists {
        let name =
            template::Ident::new(database).with_context(|| format!("database name {database}"))?;
        let create = pges_testing::sql::statement::CREATE_DATABASE
            .render(&template::Values::new().ident(template::IdentPlaceholder::Database, &name));
        admin
            .execute(template::text(create.as_str()))
            .await
            .with_context(|| format!("creating {database}; the role needs CREATEDB"))?;
    }
    admin.close().await?;

    Ok(())
}

async fn pool(server_url: &str, database: &str, connections: u32) -> Result<PgPool> {
    let options = sqlx::postgres::PgConnectOptions::from_str(&database_url(server_url, database)?)
        .context("parsing the database URL")?;
    PgPoolOptions::new()
        .max_connections(connections)
        .connect_with(options)
        .await
        .with_context(|| format!("connecting to {database}"))
}

/// Opens the store; with a definition, creates it if it is missing.
async fn open(
    pool: &PgPool,
    name: &str,
    definition: Option<store::StoreDefinition>,
) -> Result<store::Store> {
    let name =
        pges::core::store::StoreName::parse(name).with_context(|| format!("store name {name}"))?;
    let mut config = store::StoreConfig::new(name);
    if let Some(definition) = definition {
        config.migrate = store::AutoMigrate::CreateOrMigrate(definition);
    }
    store::Store::open(pool.clone(), config)
        .await
        .context("opening the store")
}

async fn dataset(pool: &PgPool, store: &str) -> Result<Vec<Generation>> {
    let rows = sql::statement::dataset::Query::fixed()
        .bind(sql::statement::dataset::Params { store })
        .fetch_all(pool)
        .await?;

    Ok(rows
        .into_iter()
        .map(|row| Generation {
            category: row.category,
            category_id: row.category_id,
            streams: row.streams,
            commits: row.commits,
            events: row.events,
            t0: row.t0,
            span: row.span,
        })
        .collect())
}

async fn generation(pool: &PgPool, store: &str, kind: Kind) -> Result<Generation> {
    dataset(pool, store)
        .await?
        .into_iter()
        .find(|generation| generation.category == kind.category().as_str())
        .with_context(|| {
            format!(
                "store {store} has no generated {}; run scale fill",
                kind.category()
            )
        })
}

/// `count` ids of the category's generated streams, drawn at random.
async fn sample(
    pool: &PgPool,
    generation: &Generation,
    count: u32,
) -> Result<Arc<[stream::StreamId]>> {
    let rows = sql::statement::sample_streams::Query::fixed()
        .bind(sql::statement::sample_streams::Params {
            category_id: generation.category_id,
            streams: generation.streams,
            count: i32::try_from(count.max(1))?,
        })
        .fetch_all(pool)
        .await?;

    Ok(rows
        .into_iter()
        .map(|row| stream::StreamId::from_uuid(row.stream_id))
        .collect())
}

async fn newest_position(pool: &PgPool, values: &template::Values) -> Result<u64> {
    let newest = sql::statement::newest_position::Query::render(values)
        .bind(sql::statement::newest_position::Params {})
        .fetch_one(pool)
        .await?
        .last_position;

    Ok(u64::try_from(newest)?)
}

/// `size` bytes of URL-safe text drawn from `rng`.
fn text(size: usize, rng: &mut event::Rng) -> String {
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";

    (0..size)
        .map(|_| char::from(ALPHABET[(rng.next_u64() >> 58) as usize]))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::Generation;

    #[test]
    fn windows_split_commits_exactly() {
        let generation = Generation {
            category: "orders".into(),
            category_id: 1,
            streams: 7,
            commits: 1_000,
            events: 1,
            t0: 1_000_000,
            span: 37_777,
        };

        for g in 0..generation.commits {
            let at = generation.time(g);
            assert!(generation.first_at(at) <= g, "{g}");
            assert!(generation.first_at(at + 1) > g, "{g}");
        }
        assert_eq!(generation.first_at(0), 0);
        assert_eq!(
            generation.first_at(generation.t0 + generation.span + 1),
            1_000
        );
    }
}
