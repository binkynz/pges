//! The store's pool must allow the connections a subscription holds at once: `run_pg` 2, `run` 1, plus a
//! `NotifyWaker` on the store's pool.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

mod common;

use std::time::Duration;

use anyhow::{Context, Result, ensure};
use sqlx::{PgPool, postgres::PgPoolOptions};

use pges_core::{category, non_empty};
use pges_postgres::store;
use pges_subscribe::{
    batch, handler,
    subscription::{self, StoreExt},
    subscription_name,
};

use common::{Fixture, Recorder, Running, STORE_NAME};

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

#[tokio::test]
async fn run_pg_needs_a_connection_for_side_renewal() -> Result<()> {
    let f = Fixture::new(pges_postgres::store::Outbox::Disabled).await?;

    let (small, store) = store_on(&f, 1).await?;
    let subscription = store
        .subscription(subscription_name!("pool.pg"))
        .categories(non_empty![category!("jobs")])
        .run_pg(Recorder::new("pool"));
    too_small(subscription, 1, 2).await?;
    small.close().await;

    let (enough, store) = store_on(&f, 2).await?;
    starts(
        store
            .subscription(subscription_name!("pool.pg"))
            .categories(non_empty![category!("jobs")])
            .run_pg(Recorder::new("pool")),
    )
    .await?;
    enough.close().await;

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
    one.close().await;

    let (two, store) = store_on(&f, 2).await?;
    let subscription = store
        .subscription(subscription_name!("pool.notify"))
        .categories(non_empty![category!("jobs")])
        .waker(notify::NotifyWaker::new(&store))
        .run_pg(Recorder::new("pool"));
    too_small(subscription, 2, 3).await?;

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
    two.close().await;

    let (three, store) = store_on(&f, 3).await?;
    starts(
        store
            .subscription(subscription_name!("pool.notify"))
            .categories(non_empty![category!("jobs")])
            .waker(notify::NotifyWaker::new(&store))
            .run_pg(Recorder::new("pool")),
    )
    .await?;
    three.close().await;

    f.finish().await
}
