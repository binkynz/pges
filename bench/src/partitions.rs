//! Spike 3, the rest: reads of `log ⋈ commits` as a category's live time leaves grow.
//!
//! For each `--leaves` count, a scratch database gets one category with that many daily leaves and
//! `--events` single-event commits spread evenly over them, in commit id order, with log rows for each
//! (written in SQL, not by a sequencer). Then it measures:
//!
//! - catch-up from the beginning with a category subscription and with a store-wide one;
//! - for the two statements of pges-subscribe's category read (a page of the category's log, then the page's
//!   commits by id, the fetch the store-wide read uses too) and for the same read in one statement with a
//!   `LATERAL` lookup (its earlier form): planning and execution time with a custom and a generic plan, the
//!   backend memory a prepared statement with a cached generic plan holds, and the relation locks one
//!   execution takes, with how many of them were fast-path locks;
//! - for each form of the category read, `--readers` connections running it at random cursors for
//!   `--duration`, while `pg_stat_activity` is sampled for backends waiting on the lock manager's LWLocks.

use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Duration,
};

use anyhow::{Context, Result, ensure};
use serde_json::{Value, json};
use sqlx::{Connection, Executor, PgConnection, PgPool};

use pges::core::{category, event::Event as _, store as core_store, stream, version};
use pges::postgres::{
    sql::{
        self as postgres_sql, template,
        typed::{Query as _, Rendered},
    },
    store,
};
use pges::subscribe::sql::statement as subscribe_statement;

use crate::{database, event, measure, read, report, sql};

mod composed;

/// Options of `pges-bench partitions`.
#[derive(Debug, Clone, clap::Args)]
pub struct Args {
    /// Daily time leaves of the category, comma-separated: one measurement per count.
    #[arg(long, value_delimiter = ',', default_value = "100,1000")]
    pub leaves: Vec<u32>,
    /// Single-event commits spread over the leaves.
    #[arg(long, default_value_t = 1_000_000)]
    pub events: u64,
    /// Streams the commits spread over.
    #[arg(long, default_value_t = 10_000)]
    pub streams: u64,
    /// Events per page: the subscriptions' batch size and the reads' `LIMIT`.
    #[arg(long, default_value_t = 256)]
    pub batch_size: u32,
    /// Bytes of payload per event.
    #[arg(long, default_value_t = 100)]
    pub payload: usize,
    /// Connections running the category read at once in the concurrent phase.
    #[arg(long, default_value_t = 16)]
    pub readers: usize,
    /// How long the concurrent phase runs.
    #[arg(long, default_value = "10s", value_parser = crate::parse_duration)]
    pub duration: Duration,
    /// How long to wait for a catch-up before giving up.
    #[arg(long, default_value = "600s", value_parser = crate::parse_duration)]
    pub limit: Duration,
}

/// Runs the measurement for each leaf count.
pub async fn run(server: &database::Server, args: &Args) -> Result<report::Report> {
    ensure!(
        args.events >= u64::from(args.batch_size) && args.streams > 0,
        "--events must be at least --batch-size, and --streams at least 1"
    );

    let mut report = report::Report::new(
        "partitions",
        &[
            "leaves",
            "what",
            "rate/s",
            "plan ms",
            "exec ms",
            "plan cache KiB generic / auto",
            "locks (fast-path) custom / generic / auto",
            "p50 ms",
            "p99 ms",
            "LockManager waits",
        ],
    );
    for leaves in &args.leaves {
        let scratch = server.scratch().await?;
        let result = measure_leaves(&scratch, *leaves, args, &mut report).await;
        scratch.cleanup().await?;
        result?;
    }

    Ok(report)
}

const STORE: core_store::StoreName = pges::core::store_name!("bench");

async fn measure_leaves(
    scratch: &database::Scratch,
    leaves: u32,
    args: &Args,
    report: &mut report::Report,
) -> Result<()> {
    let pool = scratch
        .pool(u32::try_from(args.readers)?.saturating_add(4))
        .await?;
    let (store, live_leaves) = prepare(&pool, leaves, args).await?;
    let values = template::Values::new()
        .store(postgres_sql::store_name(&store))
        .category(event::BenchEvent::CATEGORY);
    let category_id = sql::statement::category_id::Query::render(&values)
        .bind(sql::statement::category_id::Params {})
        .fetch_one(&pool)
        .await?
        .category_id;
    let events = args.events + 1;

    for scope in [read::Scope::Category, read::Scope::Store] {
        let (elapsed, decoded) =
            read::catch_up(&store, scope, events, args.batch_size, args.limit).await?;
        ensure!(
            decoded == events,
            "catch-up decoded {decoded} events, not {events}"
        );
        let rate = measure::per_second(decoded, elapsed);
        let what = match scope {
            read::Scope::Category => "catch-up category",
            read::Scope::Store => "catch-up store",
        };
        report.push(
            json!({
                "bench": "partitions",
                "leaves": live_leaves,
                "what": what,
                "events": decoded,
                "seconds": elapsed.as_secs_f64(),
                "events_per_s": rate,
            }),
            row(live_leaves, what, &report::rate(rate), &[]),
        );
    }

    let page: Vec<uuid::Uuid> = sql::statement::log_page::Query::render(&values)
        .bind(sql::statement::log_page::Params {
            after: i64::try_from(events / 2)?,
            limit: i64::from(args.batch_size),
        })
        .fetch_all(&pool)
        .await?
        .into_iter()
        .map(|row| row.commit_id)
        .collect();
    let read_values = format!(
        "({category_id}, {}, {events}, {})",
        events / 2,
        args.batch_size
    );
    let statements = [
        (
            "category log page",
            subscribe_statement::READ_CATEGORY_LOG.render(&values),
            &sql::statement::PREPARE_READ,
            read_values.clone(),
        ),
        (
            "commits by id",
            subscribe_statement::FETCH_COMMITS.render(&values),
            &sql::statement::PREPARE_FETCH,
            format!(
                "('{{{}}}')",
                page.iter()
                    .map(uuid::Uuid::to_string)
                    .collect::<Vec<_>>()
                    .join(",")
            ),
        ),
        (
            "category read, LATERAL",
            sql::statement::CATEGORY_READ_LATERAL.render(&values),
            &sql::statement::PREPARE_READ,
            read_values,
        ),
    ];
    for (what, read, prepare, parameters) in &statements {
        // A connection of its own: the statement cache it fills is measured, and it is closed afterwards.
        let mut conn = pool.acquire().await?.detach();
        let cost = statement_cost(&mut conn, prepare, read, parameters).await?;
        conn.close().await?;
        report.push(
            json!({
                "bench": "partitions",
                "leaves": live_leaves,
                "what": what,
                "statement": cost.json(),
            }),
            row(
                live_leaves,
                what,
                "-",
                &[
                    format!("{:.2} / {:.2}", cost.custom.planning, cost.generic.planning),
                    format!(
                        "{:.2} / {:.2}",
                        cost.custom.execution, cost.generic.execution
                    ),
                    format!(
                        "{} / {}",
                        cost.plan_cache_bytes / 1024,
                        cost.auto_plan_cache_bytes / 1024
                    ),
                    format!(
                        "{} ({}) / {} ({}) / {} ({})",
                        cost.custom.locks.0,
                        cost.custom.locks.1,
                        cost.generic.locks.0,
                        cost.generic.locks.1,
                        cost.auto.locks.0,
                        cost.auto.locks.1
                    ),
                ],
            ),
        );
    }

    for (form, what) in [
        (Form::TwoStep, "concurrent category reads"),
        (Form::Lateral, "concurrent category reads, LATERAL"),
    ] {
        let concurrent = concurrent_reads(&pool, form, &values, category_id, events, args).await?;
        let summary = concurrent.load.latency.summary();
        let [p50, p99, _] = report::latency_cells(&summary);
        let waits = concurrent.lock_manager_share();
        report.push(
            json!({
                "bench": "partitions",
                "leaves": live_leaves,
                "what": what,
                "readers": args.readers,
                "seconds": concurrent.load.elapsed.as_secs_f64(),
                "reads": concurrent.load.calls,
                "reads_per_s": concurrent.load.calls_per_second(),
                "events_per_s": concurrent.load.events_per_second(),
                "latency": summary.json(),
                "errors": concurrent.load.errors,
                "first_error": concurrent.load.first_error,
                "samples_active": concurrent.active,
                "samples_lock_manager": concurrent.lock_manager,
                "lock_manager_share": waits,
            }),
            row(
                live_leaves,
                what,
                &report::rate(concurrent.load.events_per_second()),
                &[
                    "-".into(),
                    "-".into(),
                    "-".into(),
                    "-".into(),
                    p50,
                    p99,
                    waits.map_or_else(|| "-".into(), |share| format!("{:.1}%", share * 100.0)),
                ],
            ),
        );
    }

    pool.close().await;
    Ok(())
}

/// Opens the store, creates the category with `leaves` daily leaves and fills them. Returns the store and
/// the number of leaves the category has after maintenance.
async fn prepare(pool: &PgPool, leaves: u32, args: &Args) -> Result<(store::Store, i64)> {
    let mut config = store::StoreConfig::new(STORE);
    config.migrate = store::AutoMigrate::CreateOrMigrate(store::StoreDefinition::default());
    let store = store::Store::open(pool.clone(), config)
        .await
        .context("opening the store")?;

    // Daily leaves ending tomorrow, created detached, constrained and attached with the statements
    // Admin::maintain runs, in one transaction.
    let back = i64::from(leaves.max(2)) - 2;
    let category = event::BenchEvent::CATEGORY;
    let values = template::Values::new()
        .store(postgres_sql::store_name(&store))
        .category(category);
    create_leaves(pool, &values, category, back)
        .await
        .context("creating the category and its leaves")?;
    store
        .admin()
        .maintain()
        .await
        .context("creating the log partitions")?;
    store
        .register::<event::BenchEvent>()
        .await
        .context("registering the event type")?;

    // One real append gives a payload frame the subscriptions can decode; every generated commit copies it.
    let template = event::BenchEvent::sized(args.payload, &mut event::Rng::new(9));
    store
        .append(
            stream::StreamId::new_v7(),
            version::ExpectedVersion::NoStream,
            std::slice::from_ref(&template),
        )
        .await
        .context("appending the template event")?;

    // Commit ids spread evenly from the first leaf's start to a minute ago, so every leaf holds about the same
    // number of commits; positions follow commit id order, as the logical source would give them.
    let filled = sql::statement::fill::Query::render(&values)
        .bind(sql::statement::fill::Params {
            events: i32::try_from(args.events)?,
            streams: i32::try_from(args.streams)?,
            back: i32::try_from(back)?,
        })
        .execute(pool)
        .await
        .context("filling the leaves")?;
    ensure!(
        filled.rows_affected() == args.events,
        "filled {} commits",
        filled.rows_affected()
    );

    pool.execute(sql::statement::WRITE_LOG.render_text(&values))
        .await
        .context("writing the log")?;
    pool.execute(sql::statement::VACUUM_ANALYZE.sql()).await?;

    let live_leaves = sql::statement::live_leaves::Query::render(&values)
        .bind(sql::statement::live_leaves::Params {})
        .fetch_one(pool)
        .await?
        .leaves;

    Ok((store, live_leaves))
}

/// Registers `category` with daily buckets and creates its partition and leaves from `back` days before today
/// to tomorrow, in one transaction.
async fn create_leaves(
    pool: &PgPool,
    values: &template::Values,
    category: category::CategoryName,
    back: i64,
) -> Result<()> {
    let mut tx = pool.begin().await?;
    tx.execute(sql::statement::UTC.sql()).await?;
    let id = sql::statement::insert_category::Query::render(values)
        .bind(sql::statement::insert_category::Params {
            name: category.as_str(),
        })
        .fetch_one(&mut *tx)
        .await?
        .category_id;
    let values = values.clone().category_id(category::CategoryId::new(id));
    for statement in [
        &postgres_sql::statement::CREATE_CATEGORY_PARTITION,
        &postgres_sql::statement::ATTACH_CATEGORY_PARTITION,
    ] {
        tx.execute(statement.render_text(&values)).await?;
    }

    let days = sql::statement::days::Query::fixed()
        .bind(sql::statement::days::Params {
            back: i32::try_from(back)?,
        })
        .fetch_all(&mut *tx)
        .await?;
    for sql::statement::days::Row { suffix, from, to } in days {
        let leaf = ident(&format!("commits_{}_{suffix}", category.as_str()))?;
        let leaf_values = values
            .clone()
            .ident(template::IdentPlaceholder::Leaf, &leaf)
            .ident(template::IdentPlaceholder::Table, &leaf)
            .ident(
                template::IdentPlaceholder::Index,
                &ident(&format!("{leaf}_s"))?,
            )
            .ident(template::IdentPlaceholder::Bound, &ident("bound")?)
            .range(&timestamptz(&from)?, &timestamptz(&to)?);
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

pub(crate) fn ident(name: &str) -> Result<template::Ident> {
    template::Ident::new(name).with_context(|| format!("identifier {name}"))
}

pub(crate) fn timestamptz(text: &str) -> Result<template::Literal> {
    template::Literal::timestamptz(text).with_context(|| format!("timestamptz text {text}"))
}

/// What one statement costs with each kind of plan.
struct StatementCost {
    custom: PlanCost,
    generic: PlanCost,
    /// PostgreSQL's default, `plan_cache_mode = auto`, as the crates run statements.
    auto: PlanCost,
    plan_cache_bytes: i64,
    auto_plan_cache_bytes: i64,
}

impl StatementCost {
    fn json(&self) -> Value {
        json!({
            "custom": self.custom.json(),
            "generic": self.generic.json(),
            "auto": self.auto.json(),
            "plan_cache_bytes": self.plan_cache_bytes,
            "auto_plan_cache_bytes": self.auto_plan_cache_bytes,
        })
    }
}

/// Median planning and execution time in milliseconds, and the relation locks one execution holds (all,
/// fast-path).
struct PlanCost {
    planning: f64,
    execution: f64,
    locks: (i64, i64),
}

impl PlanCost {
    fn json(&self) -> Value {
        json!({
            "planning_ms": self.planning,
            "execution_ms": self.execution,
            "relation_locks": self.locks.0,
            "fast_path_locks": self.locks.1,
        })
    }
}

/// Measures `read` prepared on `conn` by `prepare` (a statement of the `partitions/prepare_*` fragments),
/// executed with `parameters` (a parenthesised list of SQL literals).
async fn statement_cost(
    conn: &mut PgConnection,
    prepare: &template::Statement,
    read: &str,
    parameters: &str,
) -> Result<StatementCost> {
    const RUNS: usize = 21;

    let prepare = [prepare.sql(), read].join("\n");
    let explain = [sql::statement::EXPLAIN.sql(), parameters].join(" ");
    let execute = [sql::statement::EXECUTE.sql(), parameters].join(" ");
    let mut costs = Vec::new();
    let mut plan_cache_bytes = 0;
    let mut auto_plan_cache_bytes = 0;
    for mode in [PlanMode::Custom, PlanMode::Generic, PlanMode::Auto] {
        let set_mode = match mode {
            PlanMode::Custom => &sql::statement::CUSTOM_PLAN,
            PlanMode::Generic => &sql::statement::GENERIC_PLAN,
            PlanMode::Auto => &sql::statement::RESET_PLAN_MODE,
        };
        conn.execute(set_mode.sql()).await?;
        let before = plan_memory(conn).await?;
        conn.execute(template::text(prepare.as_str())).await?;

        let mut planning = Vec::new();
        let mut execution = Vec::new();
        for _ in 0..RUNS {
            let plan = composed::plan(conn, &explain).await?;
            planning.push(plan[0]["Planning Time"].as_f64().unwrap_or_default());
            execution.push(plan[0]["Execution Time"].as_f64().unwrap_or_default());
        }
        match mode {
            PlanMode::Custom => {}
            PlanMode::Generic => plan_cache_bytes = plan_memory(conn).await? - before,
            PlanMode::Auto => auto_plan_cache_bytes = plan_memory(conn).await? - before,
        }

        conn.execute(sql::statement::BEGIN.sql()).await?;
        conn.execute(template::text(execute.as_str())).await?;
        let locks = sql::statement::locks::Query::fixed()
            .bind(sql::statement::locks::Params {})
            .fetch_one(&mut *conn)
            .await?;
        conn.execute(sql::statement::ROLLBACK.sql()).await?;
        conn.execute(sql::statement::DEALLOCATE.sql()).await?;

        // The first generic execution builds the plan; later ones reuse it. In auto mode the first five
        // executions are planned for their values before PostgreSQL compares a generic plan.
        let settled = match mode {
            PlanMode::Auto => 6,
            PlanMode::Custom | PlanMode::Generic => 1,
        };
        costs.push(PlanCost {
            planning: median(&mut planning[settled..].to_vec()),
            execution: median(&mut execution),
            locks: (locks.relation, locks.fast_path),
        });
    }
    conn.execute(sql::statement::RESET_PLAN_MODE.sql()).await?;

    let auto = costs.pop().context("no auto cost")?;
    let generic = costs.pop().context("no generic cost")?;
    let custom = costs.pop().context("no custom cost")?;
    Ok(StatementCost {
        custom,
        generic,
        auto,
        plan_cache_bytes,
        auto_plan_cache_bytes,
    })
}

/// The plan cache mode a cost is measured with.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PlanMode {
    /// Planned for each execution's parameter values.
    Custom,
    /// One generic plan for every execution.
    Generic,
    /// PostgreSQL's choice: custom plans for five executions, then a generic plan if it does not cost
    /// more.
    Auto,
}

fn median(values: &mut [f64]) -> f64 {
    values.sort_by(f64::total_cmp);
    values.get(values.len() / 2).copied().unwrap_or_default()
}

/// A category read at a random cursor, through sqlx's statement cache as a subscription runs it.
#[derive(Clone)]
struct CategoryRead {
    pool: PgPool,
    sql: Arc<ReadSql>,
    category_id: i32,
    events: u64,
    batch_size: u32,
}

/// The statements of one form of the category read.
enum ReadSql {
    TwoStep {
        log: Rendered<subscribe_statement::read_category_log::Query>,
        fetch: Rendered<subscribe_statement::fetch_commits::Query>,
    },
    Lateral(Rendered<sql::statement::category_read_lateral::Query>),
}

impl measure::Operation for CategoryRead {
    async fn call(&self, rng: &mut event::Rng) -> Result<u64> {
        let span = self.events - u64::from(self.batch_size);
        let cursor = rng.below(usize::try_from(span)?) as i64;
        let head = i64::try_from(self.events)?;
        let limit = i64::from(self.batch_size);
        let rows = match &*self.sql {
            ReadSql::TwoStep { log, fetch } => {
                let commit_ids: Vec<uuid::Uuid> = log
                    .bind(subscribe_statement::read_category_log::Params {
                        category_id: self.category_id,
                        after: cursor,
                        head,
                        limit,
                    })
                    .fetch_all(&self.pool)
                    .await?
                    .into_iter()
                    .map(|row| row.commit_id)
                    .collect();
                fetch
                    .bind(subscribe_statement::fetch_commits::Params {
                        commit_ids: &commit_ids,
                    })
                    .fetch_all(&self.pool)
                    .await?
                    .len()
            }
            ReadSql::Lateral(read) => read
                .bind(sql::statement::category_read_lateral::Params {
                    category_id: self.category_id,
                    after: cursor,
                    head,
                    limit,
                })
                .fetch_all(&self.pool)
                .await?
                .len(),
        };

        Ok(rows as u64)
    }
}

/// The concurrent phase: reads, and samples of the readers' wait events.
struct Concurrent {
    load: measure::Load,
    active: u64,
    lock_manager: u64,
}

impl Concurrent {
    /// The share of active reader samples that waited on the lock manager.
    fn lock_manager_share(&self) -> Option<f64> {
        (self.active > 0).then(|| self.lock_manager as f64 / self.active as f64)
    }
}

async fn concurrent_reads(
    pool: &PgPool,
    form: Form,
    values: &template::Values,
    category_id: i32,
    events: u64,
    args: &Args,
) -> Result<Concurrent> {
    let sql = match form {
        Form::TwoStep => ReadSql::TwoStep {
            log: subscribe_statement::read_category_log::Query::render(values),
            fetch: subscribe_statement::fetch_commits::Query::render(values),
        },
        Form::Lateral => {
            ReadSql::Lateral(sql::statement::category_read_lateral::Query::render(values))
        }
    };
    let operation = CategoryRead {
        pool: pool.clone(),
        sql: Arc::new(sql),
        category_id,
        events,
        batch_size: args.batch_size,
    };
    measure::warm_up(operation.clone(), args.readers, Duration::from_secs(1)).await?;

    let done = Arc::new(AtomicBool::new(false));
    let active = Arc::new(AtomicU64::new(0));
    let lock_manager = Arc::new(AtomicU64::new(0));
    let mut sampler_conn = pool.acquire().await?.detach();
    let sampler = tokio::spawn({
        let done = done.clone();
        let active = active.clone();
        let lock_manager = lock_manager.clone();
        async move {
            while !done.load(Ordering::Relaxed) {
                let row = sql::statement::activity::Query::fixed()
                    .bind(sql::statement::activity::Params {})
                    .fetch_one(&mut sampler_conn)
                    .await?;
                active.fetch_add(u64::try_from(row.active)?, Ordering::Relaxed);
                lock_manager.fetch_add(u64::try_from(row.lock_manager)?, Ordering::Relaxed);
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
            anyhow::Ok(())
        }
    });

    let load = measure::drive(
        operation,
        args.readers,
        args.duration,
        measure::Pacing::Closed,
        11,
    )
    .await;
    done.store(true, Ordering::Relaxed);
    sampler.await.context("the sampler panicked")??;
    let load = load?;

    Ok(Concurrent {
        load,
        active: active.load(Ordering::Relaxed),
        lock_manager: lock_manager.load(Ordering::Relaxed),
    })
}

fn row(leaves: i64, what: &str, rate: &str, rest: &[String]) -> Vec<String> {
    let mut cells = vec![leaves.to_string(), what.to_owned(), rate.to_owned()];
    cells.extend(rest.iter().cloned());
    cells.resize(10, "-".into());
    cells
}

/// A form of the category read.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Form {
    /// A page of the category's log, then its commits by id, as `pges-subscribe` reads.
    TwoStep,
    /// One statement with a `LATERAL` lookup per log row, as `pges-subscribe` read before.
    Lateral,
}

/// The backend memory held by cached plans.
async fn plan_memory(conn: &mut PgConnection) -> Result<i64> {
    let row = sql::statement::plan_memory::Query::fixed()
        .bind(sql::statement::plan_memory::Params {})
        .fetch_one(conn)
        .await?;

    Ok(row.bytes)
}
