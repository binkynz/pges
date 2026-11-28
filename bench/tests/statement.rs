//! The statement registry against PostgreSQL 18: every query is checked against the Rust types of its parameters
//! and columns, every DDL statement runs, and no SQL or untyped bind or read bypasses the registry.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::{collections::BTreeSet, path::Path};

use anyhow::Result;
use sqlx::{Connection, Executor, PgConnection};

use pges::core::category;
use pges::postgres::sql::template;
use pges::subscribe::sql::statement as subscribe_statement;
use pges_bench::sql::statement;

const STORE_NAME: pges::core::store::StoreName = pges::core::store_name!("bench");

const BENCH: category::CategoryName = category!("bench");

/// Changes the server's configuration, so it does not run against a shared test server.
const NOT_RUN: &[&str] = &["server/synchronous_standby"];

#[test]
fn files_match_the_registry() -> Result<()> {
    pges_testing::statement::check_files(
        Path::new(env!("CARGO_MANIFEST_DIR")),
        statement::ALL,
        &values(),
    )
}

#[test]
fn all_sql_comes_from_the_registry() -> Result<()> {
    pges_testing::statement::check_no_inline_sql(&Path::new(env!("CARGO_MANIFEST_DIR")).join("src"))
}

#[tokio::test]
async fn queries_match_their_registry_types() -> Result<()> {
    let db = pges_testing::TestDb::new().await?;
    let mut conn = setup(&db).await?;
    let values = values();

    let mut failures = Vec::new();
    for shape in statement::TYPED {
        if let Err(error) = pges_testing::statement::check_typed(&mut conn, shape, &values).await {
            failures.push(format!("{error:#}"));
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n\n"));

    conn.close().await?;
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

/// `partitions/composed.rs` reads the result of statements composed at run time, which no typed statement
/// describes.
#[test]
fn binds_and_reads_are_typed() -> Result<()> {
    pges_testing::statement::check_typed_calls(
        &Path::new(env!("CARGO_MANIFEST_DIR")).join("src"),
        &["partitions/composed.rs"],
    )
}

#[tokio::test]
async fn ddl_statements_run() -> Result<()> {
    let db = pges_testing::TestDb::new().await?;
    let mut conn = setup(&db).await?;
    let values = values();
    let mut ran = BTreeSet::new();

    for statement in [
        &statement::WALINSPECT,
        &statement::STAT_STATEMENTS,
        &statement::ANALYZE,
        &statement::VACUUM_ANALYZE,
        &statement::CHECKPOINT,
        &statement::CUSTOM_PLAN,
        &statement::GENERIC_PLAN,
        &statement::RESET_PLAN_MODE,
        &statement::COMMIT_ID_FUNCTION,
        &statement::STREAM_ID_FUNCTION,
        &statement::DATASET_CREATE,
    ] {
        run(&mut conn, &mut ran, statement, &values).await?;
    }
    let mut tx = conn.begin().await?;
    run(&mut tx, &mut ran, &statement::UTC, &values).await?;
    tx.rollback().await?;

    // The prepared statement's fragments, composed as the partitions benchmark composes them.
    let read = subscribe_statement::READ_CATEGORY_LOG.render(&values);
    composed(&mut conn, &mut ran, &statement::PREPARE_READ, &read, "\n").await?;
    composed(
        &mut conn,
        &mut ran,
        &statement::EXPLAIN,
        "(1, 0, 10, 10, 0)",
        " ",
    )
    .await?;
    run(&mut conn, &mut ran, &statement::BEGIN, &values).await?;
    composed(
        &mut conn,
        &mut ran,
        &statement::EXECUTE,
        "(1, 0, 10, 10, 0)",
        " ",
    )
    .await?;
    run(&mut conn, &mut ran, &statement::ROLLBACK, &values).await?;
    run(&mut conn, &mut ran, &statement::DEALLOCATE, &values).await?;
    let fetch = subscribe_statement::FETCH_COMMITS.render(&values);
    composed(&mut conn, &mut ran, &statement::PREPARE_FETCH, &fetch, "\n").await?;
    run(&mut conn, &mut ran, &statement::DEALLOCATE, &values).await?;

    let ddl: BTreeSet<&str> = statement::ALL
        .iter()
        .filter(|s| s.kind == template::Kind::Ddl)
        .map(|s| s.name)
        .filter(|name| !NOT_RUN.contains(name))
        .collect();
    assert_eq!(
        ran, ddl,
        "every DDL statement but NOT_RUN runs in this test"
    );

    conn.close().await?;
    db.cleanup().await
}

/// A store `bench` with the category `bench`, whose partition the statements name.
async fn setup(db: &pges_testing::TestDb) -> Result<PgConnection> {
    let mut conn = db.connect().await?;
    pges_testing::create_store(
        &mut conn,
        &STORE_NAME,
        &pges_testing::definition(pges::postgres::store::Outbox::Disabled),
    )
    .await?;
    pges_testing::register_category(&mut conn, &STORE_NAME, BENCH, 0, 1).await?;
    conn.execute(statement::WALINSPECT.sql()).await?;
    // The scale statements call these functions and read this table.
    for scale in [
        &statement::COMMIT_ID_FUNCTION,
        &statement::STREAM_ID_FUNCTION,
        &statement::DATASET_CREATE,
    ] {
        conn.execute(scale.sql()).await?;
    }

    Ok(conn)
}

/// A value for every placeholder the crate's statements use.
fn values() -> template::Values {
    template::Values::new().store(&STORE_NAME).category(BENCH)
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

/// Runs a fragment `statement` followed by `rest`.
async fn composed(
    conn: &mut PgConnection,
    ran: &mut BTreeSet<&'static str>,
    statement: &'static template::Statement,
    rest: &str,
    separator: &str,
) -> Result<()> {
    assert_eq!(statement.kind, template::Kind::Ddl);
    conn.execute(sqlx::AssertSqlSafe([statement.sql(), rest].join(separator)))
        .await?;
    ran.insert(statement.name);

    Ok(())
}
