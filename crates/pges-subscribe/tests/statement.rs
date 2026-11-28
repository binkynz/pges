//! The statement registry against PostgreSQL 18: every query is checked against the Rust types of its parameters
//! and columns, and no SQL or untyped bind or read bypasses the registry.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::{collections::BTreeSet, path::Path};

use anyhow::{Context, Result};

use pges_core::category;
use pges_postgres::{admin, sql::template};
use pges_subscribe::sql::statement;

const STORE_NAME: pges_core::store::StoreName = pges_core::store_name!("reg");

const JOBS: category::CategoryName = category!("jobs");

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

#[test]
fn binds_and_reads_are_typed() -> Result<()> {
    pges_testing::statement::check_typed_calls(
        &Path::new(env!("CARGO_MANIFEST_DIR")).join("src"),
        &[],
    )
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

/// The only statements that are not queries begin a handler's transaction at an isolation level, with the
/// batch's savepoint, and roll back to that savepoint. The session's default level is SERIALIZABLE here, so
/// READ COMMITTED has to be asked for; rolling back to the savepoint fails once the transaction is gone.
#[tokio::test]
async fn ddl_statements_run() -> Result<()> {
    let db = pges_testing::TestDb::new().await?;
    let mut conn = db.connect().await?;
    let mut ran = BTreeSet::new();
    sqlx::query("SET default_transaction_isolation = 'serializable'")
        .execute(&mut conn)
        .await?;

    for (begin, level) in [
        (&statement::BEGIN, "serializable"),
        (&statement::BEGIN_READ_COMMITTED, "read committed"),
        (&statement::BEGIN_REPEATABLE_READ, "repeatable read"),
        (&statement::BEGIN_SERIALIZABLE, "serializable"),
    ] {
        pges_testing::statement::run_ddl(&mut conn, begin, &values()).await?;
        ran.insert(begin.name);
        let current: String = sqlx::query_scalar("SELECT current_setting('transaction_isolation')")
            .fetch_one(&mut conn)
            .await?;
        assert_eq!(current, level, "{}", begin.name);
        pges_testing::statement::run_ddl(&mut conn, &statement::ROLLBACK_TO_BATCH, &values())
            .await
            .with_context(|| format!("no savepoint after {}", begin.name))?;
        sqlx::query("ROLLBACK").execute(&mut conn).await?;
    }
    ran.insert(statement::ROLLBACK_TO_BATCH.name);
    assert!(
        pges_testing::statement::run_ddl(&mut conn, &statement::ROLLBACK_TO_BATCH, &values())
            .await
            .is_err(),
        "rolled back to the batch's savepoint outside its transaction"
    );

    let ddl: BTreeSet<&str> = statement::ALL
        .iter()
        .filter(|s| s.kind == template::Kind::Ddl)
        .map(|s| s.name)
        .collect();
    assert_eq!(ran, ddl, "every DDL statement runs in this test");

    drop(conn);
    db.cleanup().await
}

#[tokio::test]
async fn queries_match_their_registry_types() -> Result<()> {
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
    admin
        .register_category(JOBS, admin::CategoryConfig::default())
        .await?;
    admin.maintain().await?;
    let values = values();

    let mut failures = Vec::new();
    for shape in statement::TYPED {
        if let Err(error) = pges_testing::statement::check_typed(&mut conn, shape, &values).await {
            failures.push(format!("{error:#}"));
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n\n"));

    pool.close().await;
    drop(conn);
    db.cleanup().await
}

/// A value for every placeholder the crate's statements use: the store and its category `jobs`.
fn values() -> template::Values {
    template::Values::new().store(&STORE_NAME).category(JOBS)
}
