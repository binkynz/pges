//! The statement registry against PostgreSQL 18: every query is described with the registry's parameter and
//! column types, every DDL statement runs, and no SQL bypasses the registry.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::{collections::BTreeSet, path::Path};

use anyhow::Result;
use sqlx::{Connection, Executor, PgConnection};

use pges_core::category;
use pges_postgres::{
    admin,
    sql::{statement, template},
};

const STORE: &str = "reg";
const STORE_NAME: pges_core::store::StoreName = pges_core::store_name!("reg");

const JOBS: category::CategoryName = category!("jobs");

const EXTRA: category::CategoryName = category!("extra");

#[test]
fn files_match_the_registry() -> Result<()> {
    let values = every_value(&ident("commits_jobs__202611"), category::CategoryId::new(1));

    pges_testing::statement::check_files(
        Path::new(env!("CARGO_MANIFEST_DIR")),
        statement::ALL,
        &values,
    )
}

#[test]
fn all_sql_comes_from_the_registry() -> Result<()> {
    pges_testing::statement::check_no_inline_sql(&Path::new(env!("CARGO_MANIFEST_DIR")).join("src"))
}

#[test]
fn binds_and_reads_are_typed() -> Result<()> {
    pges_testing::statement::check_typed_calls(
        &Path::new(env!("CARGO_MANIFEST_DIR")).join("src"),
        &["sql/typed.rs"],
    )
}

#[tokio::test]
async fn queries_match_their_registry_types() -> Result<()> {
    let db = pges_testing::TestDb::new().await?;
    let mut conn = db.connect().await?;
    pges_testing::create_store(
        &mut conn,
        &STORE_NAME,
        &pges_testing::definition(pges_postgres::store::Outbox::Enabled),
    )
    .await?;
    let pool = db.pool().await?;
    let admin = admin::Admin::new(pool.clone(), STORE_NAME);
    admin
        .register_category(JOBS, admin::CategoryConfig::default())
        .await?;
    admin.maintain().await?;
    let (category_id, leaf): (i32, String) = sqlx::query_as(
        "SELECT k.category_id, c.relname::text FROM reg.categories k, pg_inherits i
         JOIN pg_class c ON c.oid = i.inhrelid
         WHERE k.name = 'jobs' AND i.inhparent = 'reg.commits_jobs'::regclass
         ORDER BY 2 LIMIT 1",
    )
    .fetch_one(&mut conn)
    .await?;
    let values = every_value(&ident(&leaf), category::CategoryId::new(category_id));

    let mut failures = Vec::new();
    for shape in statement::TYPED {
        if let Err(error) = pges_testing::statement::check_typed(&mut conn, shape, &values).await {
            failures.push(format!("{error:#}"));
        }
    }
    // A store that routes its appends runs them, and snapshot saves, on one partition of streams.
    let partition = values.clone().stream_table(Some(&ident("streams_p000")));
    for shape in statement::TYPED.iter().filter(|shape| {
        shape.statement.name == statement::APPEND.name
            || shape.statement.name == statement::APPEND_OUTBOX.name
            || shape.statement.name == statement::SNAPSHOT_SAVE.name
    }) {
        if let Err(error) = pges_testing::statement::check_typed(&mut conn, shape, &partition).await
        {
            failures.push(format!("{error:#}"));
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n\n"));

    pool.close().await;
    drop(conn);
    db.cleanup().await
}

#[test]
fn every_query_is_typed() {
    let queries: BTreeSet<&str> = statement::ALL
        .iter()
        .filter(|s| s.kind == template::Kind::Query)
        .map(|s| s.name)
        .collect();
    let typed: Vec<&str> = statement::TYPED.iter().map(|s| s.statement.name).collect();

    assert_eq!(typed.iter().copied().collect::<BTreeSet<_>>(), queries);
    assert_eq!(typed.len(), queries.len(), "a query is listed twice");
}

#[tokio::test]
async fn ddl_statements_run() -> Result<()> {
    let db = pges_testing::TestDb::new().await?;
    let mut conn = db.connect().await?;
    pges_testing::create_store(
        &mut conn,
        &STORE_NAME,
        &pges_testing::definition(pges_postgres::store::Outbox::Disabled),
    )
    .await?;
    let pool = db.pool().await?;
    let admin = admin::Admin::new(pool.clone(), STORE_NAME);
    admin.maintain().await?;
    let store = pges_core::store::StoreName::new(STORE).unwrap();
    let mut ran = BTreeSet::new();

    run(
        &mut conn,
        &mut ran,
        &statement::MAINTAIN_SESSION,
        &template::Values::new(),
    )
    .await?;

    let other = pges_core::store::StoreName::new("reg_other").unwrap();
    let other_values = template::Values::new().store(&other);
    run(
        &mut conn,
        &mut ran,
        &statement::CREATE_SCHEMA,
        &other_values,
    )
    .await?;
    let mut tx = conn.begin().await?;
    run(
        &mut tx,
        &mut ran,
        &statement::SET_SEARCH_PATH,
        &other_values,
    )
    .await?;
    tx.rollback().await?;
    let mut tx = conn.begin().await?;
    run(
        &mut tx,
        &mut ran,
        &statement::SCAN_SNAPSHOT,
        &template::Values::new(),
    )
    .await?;
    tx.rollback().await?;
    // Begins a transaction itself, as `Connection::begin_with` sends it.
    run(
        &mut conn,
        &mut ran,
        &statement::BEGIN_MANY,
        &template::Values::new(),
    )
    .await?;
    let mode: String = sqlx::query_scalar("SELECT current_setting('plan_cache_mode')")
        .fetch_one(&mut conn)
        .await?;
    assert_eq!(mode, "force_custom_plan");
    sqlx::query("ROLLBACK").execute(&mut conn).await?;
    let mut tx = conn.begin().await?;
    for statement in [&statement::PROBE_JOINS, &statement::PLAN_JOINS] {
        run(&mut tx, &mut ran, statement, &template::Values::new()).await?;
    }
    tx.rollback().await?;

    // A category's partition, attached, with two leaves.
    let category_id: i32 = sqlx::query_scalar(
        "INSERT INTO reg.categories (name, bucket, retention, retain_for)
         VALUES ('extra', '1 month', 0, NULL) RETURNING category_id",
    )
    .fetch_one(&mut conn)
    .await?;
    let category_id = category::CategoryId::new(category_id);
    let category_values = template::Values::new()
        .store(&store)
        .category(EXTRA)
        .category_id(category_id);
    run(
        &mut conn,
        &mut ran,
        &statement::CREATE_CATEGORY_PARTITION,
        &category_values.clone().ident(
            template::IdentPlaceholder::Index,
            &ident("commits_extra__pkey"),
        ),
    )
    .await?;
    run(
        &mut conn,
        &mut ran,
        &statement::ATTACH_CATEGORY_PARTITION,
        &category_values,
    )
    .await?;

    let parent = ident("commits_extra");
    let mut leaves = Vec::new();
    for (suffix, from, to) in [
        ("202001", "2020-01-01 00:00:00+00", "2020-02-01 00:00:00+00"),
        ("202002", "2020-02-01 00:00:00+00", "2020-03-01 00:00:00+00"),
    ] {
        let leaf = ident(&format!("commits_extra__{suffix}"));
        let values = category_values
            .clone()
            .ident(template::IdentPlaceholder::Leaf, &leaf)
            .ident(template::IdentPlaceholder::Table, &leaf)
            .ident(template::IdentPlaceholder::Parent, &parent)
            .ident(
                template::IdentPlaceholder::Index,
                &ident(&format!("commits_extra__{suffix}_s")),
            )
            .ident(template::IdentPlaceholder::Bound, &ident("bound"))
            .range(
                &template::Literal::timestamptz(from).unwrap(),
                &template::Literal::timestamptz(to).unwrap(),
            );
        for statement in [
            &statement::CREATE_LEAF,
            &statement::COMPRESS_LEAF,
            &statement::CREATE_LEAF_INDEX,
            &statement::ADD_LEAF_BOUND,
            &statement::ATTACH_LEAF,
            &statement::DROP_BOUND,
        ] {
            run(&mut conn, &mut ran, statement, &values).await?;
        }
        leaves.push(values);
    }

    // The first leaf detaches in one go. The second one's detach is interrupted after its first transaction
    // by a reader that holds the parent, so it is finished with FINALIZE.
    run(
        &mut conn,
        &mut ran,
        &statement::DETACH_CONCURRENTLY,
        &leaves[0],
    )
    .await?;
    run(&mut conn, &mut ran, &statement::DROP_PARTITION, &leaves[0]).await?;

    let mut reader = db.connect().await?;
    let mut reading = reader.begin().await?;
    reading
        .execute("SELECT 1 FROM reg.commits_extra LIMIT 1")
        .await?;
    conn.execute("SET statement_timeout = '300ms'").await?;
    let interrupted = conn
        .execute(statement::DETACH_CONCURRENTLY.render_text(&leaves[1]))
        .await;
    assert!(interrupted.is_err(), "the detach waits for the reader");
    conn.execute("RESET statement_timeout").await?;
    reading.rollback().await?;
    run(&mut conn, &mut ran, &statement::DETACH_FINALIZE, &leaves[1]).await?;
    run(&mut conn, &mut ran, &statement::DROP_PARTITION, &leaves[1]).await?;

    // The next log partition, which a few rows are vacuumed in, then dropped.
    let partition = ident("log_0001");
    let log_values = template::Values::new()
        .store(&store)
        .ident(template::IdentPlaceholder::LogPartition, &partition)
        .ident(template::IdentPlaceholder::Table, &partition)
        .ident(template::IdentPlaceholder::Parent, &ident("log"))
        .ident(template::IdentPlaceholder::Bound, &ident("bound"))
        .range(
            &template::Literal::int(100_000_000),
            &template::Literal::int(200_000_000),
        );
    for statement in [
        &statement::CREATE_LOG_PARTITION,
        &statement::ADD_LOG_BOUND,
        &statement::ATTACH_LOG_PARTITION,
        &statement::DROP_BOUND,
        &statement::VACUUM_LOG_PARTITION,
        &statement::FREEZE_PARTITION,
        &statement::DETACH_CONCURRENTLY,
        &statement::DROP_PARTITION,
    ] {
        run(&mut conn, &mut ran, statement, &log_values).await?;
    }

    let ddl: BTreeSet<&str> = statement::ALL
        .iter()
        .filter(|s| s.kind == template::Kind::Ddl)
        .map(|s| s.name)
        .collect();
    assert_eq!(ran, ddl, "every DDL statement runs in this test");

    drop(reader);
    pool.close().await;
    drop(conn);
    db.cleanup().await
}

/// A value for every placeholder: the store, the category `jobs` and one of its leaves, the store's first
/// log partition.
fn every_value(leaf: &template::Ident, category_id: category::CategoryId) -> template::Values {
    template::Values::new()
        .store(&STORE_NAME)
        .category(JOBS)
        .category_id(category_id)
        .ident(template::IdentPlaceholder::Leaf, leaf)
        .ident(template::IdentPlaceholder::LogPartition, &ident("log_0000"))
        .ident(template::IdentPlaceholder::Table, leaf)
        .ident(template::IdentPlaceholder::Parent, &ident("commits_jobs"))
        .ident(template::IdentPlaceholder::Index, &ident("commits_jobs_s"))
        .ident(template::IdentPlaceholder::Bound, &ident("bound"))
        .range(&template::Literal::int(0), &template::Literal::int(1))
        .stream_table(None)
}

fn ident(name: &str) -> template::Ident {
    template::Ident::new(name).unwrap()
}

async fn run(
    conn: &mut PgConnection,
    ran: &mut BTreeSet<&'static str>,
    statement: &'static template::Statement,
    values: &template::Values,
) -> Result<()> {
    pges_testing::statement::run_ddl(conn, statement, values).await?;
    ran.insert(statement.name);

    Ok(())
}
