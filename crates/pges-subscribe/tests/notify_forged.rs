//! A notice on `pges_<store>` that the sequencer did not send. `NOTIFY` needs no privilege: any role that may
//! connect to the database can send one.

#![cfg(feature = "wake-notify")]
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

mod common;

use std::{
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use anyhow::Result;
use sqlx::{Connection, Executor};

use pges_core::{category, high_water, non_empty, stream};
use pges_subscribe::{
    subscription::StoreExt,
    subscription_name,
    waker::{self, notify},
};

use common::{Cat, Fixture, Recorder};

/// Counts the calls of the waker it wraps.
struct Counting {
    inner: notify::NotifyWaker,
    calls: Arc<AtomicU64>,
}

impl waker::Waker for Counting {
    async fn changed(
        &mut self,
        known: &high_water::HighWater,
    ) -> Result<high_water::HighWater, waker::WakeError> {
        self.calls.fetch_add(1, Ordering::Relaxed);
        waker::Waker::changed(&mut self.inner, known).await
    }

    fn store_connections(&self) -> u32 {
        waker::Waker::store_connections(&self.inner)
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_forged_notice_from_an_unprivileged_role_does_not_spin_a_subscription() -> Result<()> {
    let f = Fixture::new(pges_postgres::store::Outbox::Enabled).await?;
    let nobody = format!("{}_nobody", f.db.name());
    f.pool
        .execute(sqlx::AssertSqlSafe(format!(
            "CREATE ROLE {nobody} LOGIN PASSWORD 'pw'"
        )))
        .await?;
    let sequencer = common::Sequencer::start(&f).await?;
    let calls = Arc::new(AtomicU64::new(0));
    let running = common::Running::spawn(
        f.store
            .subscription(subscription_name!("forged"))
            .categories(non_empty![category!("jobs")])
            .waker(Counting {
                inner: notify::NotifyWaker::new(&f.store),
                calls: calls.clone(),
            })
            .run_pg(Recorder::new("forged")),
    );
    f.append(Cat::Jobs, stream::StreamId::new_v7(), 1).await?;
    running.wait_position(1).await?;
    tokio::time::sleep(Duration::from_millis(300)).await;
    let before = calls.load(Ordering::Relaxed);
    tokio::time::sleep(Duration::from_secs(1)).await;
    let idle = calls.load(Ordering::Relaxed) - before;

    // A role with no grant on any pges object.
    let url =
        f.db.url()
            .replacen("postgres:postgres@", &format!("{nobody}:pw@"), 1);
    let mut forger = sqlx::PgConnection::connect(&url).await?;
    forger
        .execute("NOTIFY pges_es, '1;9223372036854775807;;*'")
        .await?;
    forger.close().await?;
    tokio::time::sleep(Duration::from_millis(300)).await;
    let before = calls.load(Ordering::Relaxed);
    tokio::time::sleep(Duration::from_secs(1)).await;
    let forged = calls.load(Ordering::Relaxed) - before;

    running.stop().await?;
    sequencer.stop().await?;
    f.pool
        .execute(sqlx::AssertSqlSafe(format!("DROP ROLE {nobody}")))
        .await?;
    f.finish().await?;

    assert!(
        forged <= idle + 5,
        "after one forged notice the subscription called its waker {forged} times in a second \
         (idle: {idle}): each call reads the log"
    );
    Ok(())
}

/// Transactions the test database has committed so far, as the statistics last flushed. Idle backends flush
/// theirs late (up to 10 s), so only a large difference means anything: a spin commits about 10,000 a second.
async fn transactions(pool: &sqlx::PgPool) -> Result<i64> {
    Ok(sqlx::query_scalar(
        "SELECT xact_commit FROM pg_stat_database WHERE datname = current_database()",
    )
    .fetch_one(pool)
    .await?)
}

/// In a sharded subscription one task waits on the waker for every shard. A forged huge position must
/// neither make the shards read in a loop nor hide the notices that follow it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_forged_notice_neither_spins_nor_silences_a_sharded_subscription() -> Result<()> {
    let f = Fixture::new(pges_postgres::store::Outbox::Enabled).await?;
    let sequencer = common::Sequencer::start(&f).await?;
    let calls = Arc::new(AtomicU64::new(0));
    let running = common::Running::spawn(
        f.store
            .subscription(subscription_name!("forged_sharded"))
            .categories(non_empty![category!("jobs")])
            .sharded(pges_subscribe::shards!(2))
            // Longer than the test waits: only a notice can wake the shards in time.
            .max_idle(pges_core::interval!(60 s))
            .waker(Counting {
                inner: notify::NotifyWaker::new(&f.store),
                calls: calls.clone(),
            })
            .run_pg(Recorder::new("forged_sharded")),
    );
    f.append(Cat::Jobs, stream::StreamId::new_v7(), 1).await?;
    running.wait_position(1).await?;
    tokio::time::sleep(Duration::from_millis(1500)).await;
    let before = (calls.load(Ordering::Relaxed), transactions(&f.pool).await?);
    tokio::time::sleep(Duration::from_millis(1500)).await;
    let idle = (
        calls.load(Ordering::Relaxed) - before.0,
        transactions(&f.pool).await? - before.1,
    );

    f.pool
        .execute("NOTIFY pges_es, '1;9223372036854775807;;*'")
        .await?;
    tokio::time::sleep(Duration::from_millis(1500)).await;
    let before = (calls.load(Ordering::Relaxed), transactions(&f.pool).await?);
    tokio::time::sleep(Duration::from_millis(1500)).await;
    let forged = (
        calls.load(Ordering::Relaxed) - before.0,
        transactions(&f.pool).await? - before.1,
    );

    f.append(Cat::Jobs, stream::StreamId::new_v7(), 1).await?;
    let reached = tokio::time::timeout(Duration::from_secs(10), running.wait_position(2)).await;
    let position = running.handle.status().position;

    running.stop().await?;
    sequencer.stop().await?;
    f.finish().await?;

    assert!(
        forged.0 <= idle.0 + 5,
        "after one forged notice the waker was called {} times in 1.5 s (idle: {})",
        forged.0,
        idle.0
    );
    assert!(
        forged.1 <= idle.1 + 1000,
        "after one forged notice the database committed {} transactions in 1.5 s (idle: {})",
        forged.1,
        idle.1
    );
    assert!(
        matches!(reached, Ok(Ok(()))),
        "a real append after a forged notice was not delivered within 10 s: still at {position:?}"
    );
    Ok(())
}

/// A forged notice's LSN becomes the WAL position a replica subscription waits for. No replica ever replays
/// it, so without a fallback the subscription stops delivering.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_forged_lsn_does_not_stall_a_replica_subscription() -> Result<()> {
    let pair = pges_testing::replica::Pair::start().await?;
    let db = pair.create_database("t_forged_lsn").await?;
    let mut conn = db.connect().await?;
    pges_testing::create_store(
        &mut conn,
        &common::STORE_NAME,
        &pges_testing::definition(pges_postgres::store::Outbox::Enabled),
    )
    .await?;
    conn.execute(
        "CREATE TABLE public.delivered (
           seq int8 GENERATED ALWAYS AS IDENTITY PRIMARY KEY, position int8 NOT NULL,
           commit_id uuid NOT NULL, category_id int4 NOT NULL, subscriber text NOT NULL DEFAULT '')",
    )
    .await?;
    let pool = db.pool().await?;
    let admin = pges_postgres::admin::Admin::new(pool.clone(), common::STORE_NAME);
    admin
        .register_category(
            <common::JobEvent as pges_core::event::Event>::CATEGORY,
            pges_postgres::admin::CategoryConfig::default(),
        )
        .await?;
    admin.maintain().await?;
    let store = pges_postgres::store::Store::open(
        pool.clone(),
        pges_postgres::store::StoreConfig::new(common::STORE_NAME),
    )
    .await?;
    store.register::<common::JobEvent>().await?;
    pair.caught_up().await?;
    let replica_pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(4)
        .connect(&pair.replica_url(db.name())?)
        .await?;
    let sequencer = common::Sequencer::start_on(db.pool().await?, common::Sequencer::config())?;
    sequencer.wait_leading().await?;

    let running = common::Running::spawn(
        store
            .subscription(subscription_name!("replica"))
            .categories(non_empty![category!("jobs")])
            .waker(notify::NotifyWaker::new(&store))
            .replica(replica_pool.clone())
            .run_pg(Recorder::new("replica")),
    );
    let job = stream::StreamId::new_v7();
    store
        .append(
            job,
            pges_core::version::ExpectedVersion::Any,
            &[common::JobEvent::Noted { n: 0 }],
        )
        .await?;
    running.wait_position(1).await?;
    // Listening by now.
    tokio::time::sleep(Duration::from_millis(500)).await;

    pool.execute("NOTIFY pges_es, '1;2;FFFFFFFF/FFFFFFFF;*'")
        .await?;
    tokio::time::sleep(Duration::from_millis(300)).await;
    store
        .append(
            job,
            pges_core::version::ExpectedVersion::Any,
            &[common::JobEvent::Noted { n: 1 }],
        )
        .await?;
    let reached = running.wait_position(2).await;
    let position = running.handle.status().position;

    running.handle.shutdown().await?;
    sequencer.stop().await?;
    replica_pool.close().await;
    pool.close().await;
    drop(conn);
    db.cleanup().await?;
    pair.stop().await?;

    assert!(
        reached.is_ok(),
        "the subscription stayed at {position:?} for 20 s after a forged LSN"
    );
    Ok(())
}
