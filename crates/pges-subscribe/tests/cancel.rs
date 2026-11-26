//! Cancellation: a subscription or a backfill whose future is dropped at any await point, up to and past its
//! first delivered batch, leaves no pooled connection inside a transaction and no lock held.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

mod common;

use std::num::NonZeroU32;

use anyhow::Result;

use pges_core::{category, non_empty, stream};
use pges_postgres::store;
use pges_subscribe::{
    backfill,
    subscription::{self, StoreExt},
};
use pges_testing::cancel;

use common::{Cat, Fixture, Recorder};

/// Each run uses a subscription of its own, so every run takes the same path from the start.
#[tokio::test]
async fn a_dropped_subscription_leaves_no_transaction_open() -> Result<()> {
    let f = Fixture::new(store::Outbox::Disabled).await?;
    write(&f).await?;
    let head = common::head(&f.pool).await? as i64;
    let pool = cancel::pool(f.db.url(), 2).await?;
    let store =
        store::Store::open(pool.clone(), store::StoreConfig::new(common::STORE_NAME)).await?;

    for polls in 1.. {
        let name = format!("live{polls}");
        cancel::check_released(&pool).await?;
        let run = store
            .subscription(subscription::SubscriptionName::parse(name.as_str())?)
            .categories(non_empty![category!("jobs")])
            .batch_size(NonZeroU32::new(2).unwrap())
            .run_pg(Recorder::new(&name))
            .run_until(std::future::pending());
        let ended = cancel::poll_then_drop(run, polls).await?;
        assert!(ended.is_none(), "the subscription stopped: {ended:?}");
        cancel::check_released(&pool).await?;

        // Every run up to here was dropped before its last checkpoint; this one got past it.
        if f.cursor(&name).await? == Some(head) {
            break;
        }
    }

    pool.close().await;
    f.finish().await
}

/// Each run is a backfill of its own, so every run takes the same path from the start.
#[tokio::test]
async fn a_dropped_backfill_leaves_no_transaction_open() -> Result<()> {
    let f = Fixture::new(store::Outbox::Disabled).await?;
    write(&f).await?;
    let pool = cancel::pool(f.db.url(), 2).await?;
    let store =
        store::Store::open(pool.clone(), store::StoreConfig::new(common::STORE_NAME)).await?;

    for polls in 1.. {
        let name = format!("backfill{polls}");
        cancel::check_released(&pool).await?;
        let run = store
            .backfill(
                subscription::SubscriptionName::parse(name.as_str())?,
                backfill::Until::Head,
            )
            .categories(non_empty![category!("jobs")])
            .batch_size(NonZeroU32::new(2).unwrap())
            .run_pg(Recorder::new(&name));
        let finished = cancel::poll_then_drop(run, polls).await?;
        cancel::check_released(&pool).await?;

        if let Some(finished) = finished {
            assert_eq!(finished?.events, 4);
            break;
        }
    }

    pool.close().await;
    f.finish().await
}

/// Four sequenced events in two jobs streams.
async fn write(f: &Fixture) -> Result<()> {
    for _ in 0..2 {
        f.append(Cat::Jobs, stream::StreamId::new_v7(), 2).await?;
    }

    f.sequence_all().await
}
