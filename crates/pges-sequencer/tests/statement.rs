//! The statement registry against PostgreSQL 18: every query is described and checked against its Rust
//! parameter and column types, every DDL statement runs, and no SQL or untyped bind or read bypasses the
//! registry.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::{collections::BTreeSet, path::Path};

use anyhow::Result;
use sqlx::{Connection, Executor, PgConnection};

use pges_postgres::sql::template;
use pges_sequencer::sql::statement;

const STORE: &str = "reg";
const STORE_NAME: pges_core::store::StoreName = pges_core::store_name!("reg");

#[test]
fn files_match_the_registry() -> Result<()> {
    let snapshot = template::SnapshotName::new("00000003-0000001B-1").unwrap();
    let values = store_values().snapshot(&snapshot);

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

#[tokio::test]
async fn queries_match_their_registry_types() -> Result<()> {
    let db = pges_testing::TestDb::new().await?;
    let mut conn = db.connect().await?;
    // An outbox store has every table the statements name, `pending` included.
    pges_testing::create_store(
        &mut conn,
        &STORE_NAME,
        &pges_testing::definition(pges_postgres::store::Outbox::Enabled),
    )
    .await?;
    let values = store_values();

    let mut failures = Vec::new();
    for shape in statement::TYPED {
        if let Err(error) = pges_testing::statement::check_typed(&mut conn, shape, &values).await {
            failures.push(format!("{error:#}"));
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n\n"));

    drop(conn);
    db.cleanup().await
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
    let values = store_values();
    let mut ran = BTreeSet::new();

    let mut tx = conn.begin().await?;
    run(&mut tx, &mut ran, &statement::SYNCHRONOUS_COMMIT, &values).await?;
    tx.rollback().await?;

    // A snapshot exported by another transaction, imported as the logical source imports a new slot's.
    let mut exporter = db.connect().await?;
    let mut exporting = exporter.begin().await?;
    exporting
        .execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
        .await?;
    let exported: String = sqlx::query_scalar("SELECT pg_export_snapshot()")
        .fetch_one(&mut *exporting)
        .await?;
    let snapshot = template::SnapshotName::new(&exported).unwrap();
    let mut tx = conn.begin().await?;
    run(&mut tx, &mut ran, &statement::REPEATABLE_READ, &values).await?;
    run(
        &mut tx,
        &mut ran,
        &statement::SET_SNAPSHOT,
        &values.clone().snapshot(&snapshot),
    )
    .await?;
    tx.rollback().await?;
    exporting.rollback().await?;

    // The publication the migrations created is repaired from scratch: created, then given the store.
    conn.execute("DROP PUBLICATION pges").await?;
    run(&mut conn, &mut ran, &statement::CREATE_PUBLICATION, &values).await?;
    run(&mut conn, &mut ran, &statement::PUBLISH_STORE, &values).await?;
    let published: Vec<String> = sqlx::query_scalar(
        "SELECT schemaname::text FROM pg_publication_tables WHERE pubname = 'pges'",
    )
    .fetch_all(&mut conn)
    .await?;
    assert_eq!(published, [STORE]);

    let ddl: BTreeSet<&str> = statement::ALL
        .iter()
        .filter(|s| s.kind == template::Kind::Ddl)
        .map(|s| s.name)
        .collect();
    assert_eq!(ran, ddl, "every DDL statement runs in this test");

    drop(exporter);
    drop(conn);
    db.cleanup().await
}

fn store_values() -> template::Values {
    template::Values::new().store(&STORE_NAME)
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
