//! A `TestDb` dropped without `cleanup` (a failed test) drops its database and replication slots.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use anyhow::Result;
use sqlx::{Connection, Executor};

/// Creates a logical slot in `db`, named after it.
async fn slot_in(db: &pges_testing::TestDb) -> Result<String> {
    let slot = format!("cleanup_{}", db.name());
    let mut conn = db.connect().await?;
    conn.execute(
        sqlx::query("SELECT pg_create_logical_replication_slot($1, 'pgoutput')").bind(&slot),
    )
    .await?;
    conn.close().await?;

    Ok(slot)
}

/// Whether the database and the slot are still there.
async fn left(name: &str, slot: &str) -> Result<(bool, bool)> {
    let mut admin = pges_testing::server().await?.admin().await?;
    let row = sqlx::query_as(
        "SELECT EXISTS (SELECT 1 FROM pg_database WHERE datname = $1), \
                EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = $2)",
    )
    .bind(name)
    .bind(slot)
    .fetch_one(&mut admin)
    .await?;
    admin.close().await?;

    Ok(row)
}

#[tokio::test]
async fn dropping_a_test_db_drops_its_database_and_slots() -> Result<()> {
    let db = pges_testing::TestDb::new().await?;
    let name = db.name().to_owned();
    let slot = slot_in(&db).await?;
    // An open connection does not stop it.
    let conn = db.connect().await?;
    assert_eq!(left(&name, &slot).await?, (true, true));

    drop(db);

    assert_eq!(left(&name, &slot).await?, (false, false));
    drop(conn);
    Ok(())
}

#[tokio::test]
async fn a_test_that_panics_leaves_nothing_behind() -> Result<()> {
    let (created, made) = tokio::sync::oneshot::channel();
    let failed = tokio::spawn(async move {
        let db = pges_testing::TestDb::new().await.unwrap();
        let slot = slot_in(&db).await.unwrap();
        created.send((db.name().to_owned(), slot)).unwrap();
        panic!("the test failed");
    })
    .await;

    assert!(failed.unwrap_err().is_panic());
    let (name, slot) = made.await?;
    assert_eq!(left(&name, &slot).await?, (false, false));
    Ok(())
}

#[tokio::test]
async fn an_existing_database_is_left_alone() -> Result<()> {
    let db = pges_testing::TestDb::new().await?;
    let existing = pges_testing::TestDb::existing(db.name().to_owned(), db.url().to_owned());

    drop(existing);
    let mut conn = db.connect().await?;
    conn.execute("SELECT 1").await?;
    conn.close().await?;

    db.cleanup().await
}

#[tokio::test]
async fn cleanup_with_slots_drops_the_slots_first() -> Result<()> {
    let db = pges_testing::TestDb::new().await?;
    let name = db.name().to_owned();
    let slot = slot_in(&db).await?;

    db.cleanup_with_slots().await?;

    assert_eq!(left(&name, &slot).await?, (false, false));
    Ok(())
}
