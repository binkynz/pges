//! The replica pair: the replica follows the primary, and pausing replay holds it back.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use anyhow::Result;
use sqlx::{Connection, Executor, PgConnection};

use pges_testing::replica;

#[tokio::test]
async fn the_replica_follows_the_primary_and_can_be_held_back() -> Result<()> {
    let pair = replica::Pair::start().await?;
    let db = pair.create_database("t_replica").await?;
    let mut primary = db.connect().await?;
    let mut standby = PgConnection::connect(&pair.replica_url("t_replica")?).await?;

    let in_recovery: bool = sqlx::query_scalar("SELECT pg_is_in_recovery()")
        .fetch_one(&mut standby)
        .await?;
    assert!(in_recovery);
    let schemas: i64 =
        sqlx::query_scalar("SELECT count(*) FROM pg_namespace WHERE nspname = 'pges'")
            .fetch_one(&mut standby)
            .await?;
    assert_eq!(schemas, 1, "the shared migrations reached the replica");

    primary
        .execute("CREATE TABLE t (n int); INSERT INTO t VALUES (1)")
        .await?;
    pair.caught_up().await?;
    assert_eq!(count(&mut standby).await?, 1);

    pair.pause_replay().await?;
    primary.execute("INSERT INTO t VALUES (2)").await?;
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    assert_eq!(count(&mut standby).await?, 1, "replay is paused");
    pair.resume_replay().await?;
    pair.caught_up().await?;
    assert_eq!(count(&mut standby).await?, 2);

    standby.close().await?;
    primary.close().await?;
    pair.stop().await
}

async fn count(conn: &mut PgConnection) -> Result<i64> {
    Ok(sqlx::query_scalar("SELECT count(*) FROM t")
        .fetch_one(conn)
        .await?)
}
