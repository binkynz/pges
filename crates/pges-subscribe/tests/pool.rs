//! The store's pool must allow the connections a subscription holds at once: 1 for `run_pg` or `run`, plus
//! a `NotifyWaker` on the store's pool.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

mod common;

use std::time::Duration;

use anyhow::{Context, Result, ensure};
use sqlx::{PgConnection, PgPool, postgres::PgPoolOptions};

use pges_core::{category, interval, non_empty, stream};
use pges_postgres::store;
use pges_subscribe::{
    batch, handler,
    subscription::{self, StoreExt},
    subscription_name,
};

use common::{Cat, Fixture, Recorder, Running, STORE_NAME};

struct Ignore;

impl handler::Handler for Ignore {
    async fn handle(&mut self, _batch: &batch::Batch) -> Result<(), handler::HandlerError> {
        Ok(())
    }
}

async fn store_on(f: &Fixture, max_connections: u32) -> Result<(PgPool, store::Store)> {
    let pool = PgPoolOptions::new()
        .max_connections(max_connections)
        .connect(f.db.url())
        .await?;
    let store = store::Store::open(pool.clone(), store::StoreConfig::new(STORE_NAME)).await?;

    Ok((pool, store))
}

async fn too_small(
    subscription: subscription::Subscription,
    max_connections: u32,
    needed: u32,
) -> Result<()> {
    let error = Running::spawn(subscription).stopped().await?;
    ensure!(
        matches!(
            error,
            subscription::SubscribeError::PoolTooSmall { max_connections: m, needed: n }
                if m == max_connections && n == needed
        ),
        "{error:?}"
    );

    Ok(())
}

async fn starts(subscription: subscription::Subscription) -> Result<()> {
    let running = Running::spawn(subscription);
    let mut status = running.handle.status_changes();
    tokio::time::timeout(
        Duration::from_secs(10),
        status.wait_for(|status| matches!(status.state, subscription::State::Running)),
    )
    .await
    .context("the subscription did not start")??;

    running.stop().await
}

/// Records after a pause longer than the lease the test gives it.
struct Slow(Recorder);

impl handler::PgHandler for Slow {
    async fn handle(
        &mut self,
        tx: &mut PgConnection,
        batch: &batch::Batch,
    ) -> Result<(), handler::HandlerError> {
        tokio::time::sleep(Duration::from_millis(500)).await;
        self.0.record(tx, batch).await?;
        Ok(())
    }
}

/// `run_pg` needs no connection beside its transaction: its row lock keeps the lease, so a batch slower than
/// the lease completes on a pool of one, and no standby takes over meanwhile.
#[tokio::test]
async fn run_pg_works_on_one_connection() -> Result<()> {
    let f = Fixture::new(pges_postgres::store::Outbox::Disabled).await?;
    f.append(Cat::Jobs, stream::StreamId::new_v7(), 2).await?;
    f.sequence_all().await?;

    let subscription = |store: &store::Store| {
        store
            .subscription(subscription_name!("pool.pg"))
            .categories(non_empty![category!("jobs")])
            .lease(interval!(200 ms))
            .acquire_interval(interval!(10 ms))
            .run_pg(Slow(Recorder::new("pool")))
    };
    let (one, store) = store_on(&f, 1).await?;
    let running = Running::spawn(subscription(&store));
    let (other, standby_store) = store_on(&f, 1).await?;
    let standby = Running::spawn(subscription(&standby_store));
    let mut standby_status = standby.handle.status_changes();
    let took_over = tokio::spawn(async move {
        let _ = standby_status
            .wait_for(|s| matches!(s.state, subscription::State::Running))
            .await;
    });

    running.wait_position(1).await?;
    ensure!(!took_over.is_finished(), "the standby took over");
    took_over.abort();
    ensure!(f.delivered("pool").await?.len() == 2);

    standby.stop().await?;
    running.stop().await?;
    other.close().await;
    one.close().await;
    f.finish().await
}

#[tokio::test]
async fn run_works_on_one_connection() -> Result<()> {
    let f = Fixture::new(pges_postgres::store::Outbox::Disabled).await?;

    let (one, store) = store_on(&f, 1).await?;
    starts(
        store
            .subscription(subscription_name!("pool.run"))
            .categories(non_empty![category!("jobs")])
            .run(Ignore),
    )
    .await?;
    one.close().await;

    f.finish().await
}

#[cfg(feature = "wake-notify")]
#[tokio::test]
async fn a_notify_waker_on_the_store_pool_needs_its_own_connection() -> Result<()> {
    use pges_subscribe::waker::notify;

    let f = Fixture::new(pges_postgres::store::Outbox::Disabled).await?;

    let (one, store) = store_on(&f, 1).await?;
    let subscription = store
        .subscription(subscription_name!("pool.notify"))
        .categories(non_empty![category!("jobs")])
        .waker(notify::NotifyWaker::new(&store))
        .run(Ignore);
    too_small(subscription, 1, 2).await?;
    let subscription = store
        .subscription(subscription_name!("pool.notify"))
        .categories(non_empty![category!("jobs")])
        .waker(notify::NotifyWaker::new(&store))
        .run_pg(Recorder::new("pool"));
    too_small(subscription, 1, 2).await?;

    // On a separate pool, the waker's connection does not count.
    starts(
        store
            .subscription(subscription_name!("pool.notify"))
            .categories(non_empty![category!("jobs")])
            .waker(notify::NotifyWaker::with_separate_pool(
                &store,
                f.pool.clone(),
            ))
            .run_pg(Recorder::new("pool")),
    )
    .await?;
    one.close().await;

    let (two, store) = store_on(&f, 2).await?;
    starts(
        store
            .subscription(subscription_name!("pool.notify"))
            .categories(non_empty![category!("jobs")])
            .waker(notify::NotifyWaker::new(&store))
            .run_pg(Recorder::new("pool")),
    )
    .await?;
    two.close().await;

    f.finish().await
}
