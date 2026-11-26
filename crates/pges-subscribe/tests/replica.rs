//! Replica reads against a real streaming replica (`pges_testing::replica::Pair`): a subscription waits until
//! the replica has replayed the waker's LSN, or the primary's WAL position when the waker has none, and
//! falls back to the primary only when asked to, within the fallback limit. Replay is paused on the replica
//! to make it lag.

#![cfg(feature = "wake-inproc")]
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

mod common;

use std::{
    collections::BTreeMap,
    num::NonZeroU32,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use anyhow::Result;
use sha2::{Digest, Sha256};
use sqlx::{Executor, PgPool, postgres::PgPoolOptions};

use pges_core::{category, high_water, interval, non_empty, stream};
use pges_postgres::{admin, delete, store};
use pges_subscribe::{
    replica,
    subscription::{self, StoreExt},
    subscription_name,
    waker::{self, inproc, recheck},
};

use common::{Cat, Fixture, JobEvent, OrderEvent, PartEvent, Recorder, Running};

#[tokio::test]
async fn replica_reads_wait_until_the_replica_has_replayed() -> Result<()> {
    let pair = pges_testing::replica::Pair::start().await?;
    let f = fixture(&pair).await?;
    let replica_pool = replica_pool(&pair, &f).await?;
    let sequencer = common::Sequencer::start(&f).await?;
    sequencer.wait_leading().await?;

    let calls = Arc::new(AtomicUsize::new(0));
    let woken = Running::spawn(
        f.store
            .subscription(subscription_name!("woken"))
            .categories(non_empty![category!("jobs")])
            .waker(Counting {
                inner: inproc::InprocWaker::new(sequencer.watch.high_water(f.store.store_id())),
                calls: calls.clone(),
            })
            .replica(replica_pool.clone())
            .run_pg(Recorder::new("woken")),
    );
    let rechecking = Running::spawn(
        f.store
            .subscription(subscription_name!("rechecking"))
            .categories(non_empty![category!("jobs")])
            .waker(recheck::Recheck::every(interval!(50 ms)))
            .replica(replica_pool.clone())
            .run_pg(Recorder::new("rechecking")),
    );

    let job = stream::StreamId::new_v7();
    f.append(Cat::Jobs, job, 2).await?;
    common::wait_sequenced(&f.pool, 2).await?;
    woken.wait_position(2).await?;
    rechecking.wait_position(2).await?;
    assert!(
        sequencer
            .watch
            .high_water(f.store.store_id())
            .borrow()
            .lsn()
            .is_some(),
        "the sequencer announces an LSN"
    );

    // The primary moves on while the replica shows nothing new: neither subscription may read the primary.
    // The woken one waits for the replica after one wake-up; reading the stale replica instead would find
    // itself caught up and call its level-triggered waker again at once, over and over.
    pair.pause_replay().await?;
    let before = calls.load(Ordering::SeqCst);
    f.append(Cat::Jobs, job, 3).await?;
    f.append(Cat::Orders, stream::StreamId::new_v7(), 1).await?;
    common::wait_sequenced(&f.pool, 6).await?;
    tokio::time::sleep(Duration::from_millis(700)).await;
    let during = calls.load(Ordering::SeqCst) - before;
    assert!(
        during <= 3,
        "the waker was called {during} times while the replica lagged"
    );
    assert_eq!(woken.handle.status().position, pges_testing::position(2));
    assert_eq!(
        rechecking.handle.status().position,
        pges_testing::position(2)
    );
    assert_eq!(f.delivered("woken").await?.len(), 2);

    pair.resume_replay().await?;
    woken.wait_position(6).await?;
    rechecking.wait_position(6).await?;
    let expected = f.expected(&[f.id(Cat::Jobs)]).await?;
    common::check_delivery(&f.delivered("woken").await?, &expected)?;
    common::check_delivery(&f.delivered("rechecking").await?, &expected)?;

    woken.stop().await?;
    rechecking.stop().await?;
    sequencer.stop().await?;
    replica_pool.close().await;
    finish(f, pair).await
}

#[tokio::test]
async fn a_lagging_replica_falls_back_to_the_primary_within_the_limit() -> Result<()> {
    let pair = pges_testing::replica::Pair::start().await?;
    let f = fixture(&pair).await?;
    let replica_pool = replica_pool(&pair, &f).await?;
    let sequencer = common::Sequencer::start(&f).await?;
    sequencer.wait_leading().await?;

    let subscribe = |name: &str, limit: replica::FallbackLimit| {
        Running::spawn(
            f.store
                .subscription(subscription::SubscriptionName::parse(name).unwrap())
                .categories(non_empty![category!("jobs")])
                .waker(inproc::InprocWaker::new(
                    sequencer.watch.high_water(f.store.store_id()),
                ))
                .replica(
                    replica::Replica::new(replica_pool.clone()).fallback(interval!(200 ms), limit),
                )
                .run_pg(Recorder::new(name)),
        )
    };
    let falling_back = subscribe("falling-back", replica::FallbackLimit::new(1));
    let capped = subscribe("capped", replica::FallbackLimit::new(0));

    let job = stream::StreamId::new_v7();
    f.append(Cat::Jobs, job, 1).await?;
    common::wait_sequenced(&f.pool, 1).await?;
    falling_back.wait_position(1).await?;
    capped.wait_position(1).await?;

    pair.pause_replay().await?;
    f.append(Cat::Jobs, job, 2).await?;
    common::wait_sequenced(&f.pool, 3).await?;
    falling_back.wait_position(3).await?;
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(
        capped.handle.status().position,
        pges_testing::position(1),
        "a limit without permits never falls back"
    );

    pair.resume_replay().await?;
    capped.wait_position(3).await?;
    let expected = f.expected(&[f.id(Cat::Jobs)]).await?;
    common::check_delivery(&f.delivered("falling-back").await?, &expected)?;
    common::check_delivery(&f.delivered("capped").await?, &expected)?;

    falling_back.stop().await?;
    capped.stop().await?;
    sequencer.stop().await?;
    replica_pool.close().await;
    finish(f, pair).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn store_wide_and_sharded_subscriptions_read_every_page_from_the_replica() -> Result<()> {
    let pair = pges_testing::replica::Pair::start().await?;
    let f = fixture(&pair).await?;
    let replica_pool = replica_pool(&pair, &f).await?;
    let sequencer = common::Sequencer::start(&f).await?;
    sequencer.wait_leading().await?;

    let subscription = |name: &str| {
        f.store
            .subscription(subscription::SubscriptionName::parse(name).unwrap())
            .waker(inproc::InprocWaker::new(
                sequencer.watch.high_water(f.store.store_id()),
            ))
            .replica(replica_pool.clone())
            .acquire_interval(interval!(20 ms))
    };
    let store_wide = Running::spawn(
        subscription("store-wide")
            .all_categories()
            .run_pg(Recorder::new("store-wide")),
    );
    let sharded = Running::spawn(
        subscription("sharded")
            .categories(non_empty![category!("jobs"), category!("orders")])
            .sharded(pges_subscribe::shards!(2))
            .run_pg(Recorder::new("sharded")),
    );
    let store_wide_sharded = Running::spawn(
        subscription("store-wide-sharded")
            .all_categories()
            .sharded(pges_subscribe::shards!(2))
            .run_pg(Recorder::new("store-wide-sharded")),
    );
    let all = [&store_wide, &sharded, &store_wide_sharded];

    // One stream per shard, so a shard that read the primary while the replica lags would deliver. The
    // unlisted category goes first: a waker does not wake a listed subscription for it, so a listed cursor
    // reaches the last position only when that position is in one of its categories.
    let [first, second] = one_stream_per_shard();
    f.append(Cat::Parts, stream::StreamId::new_v7(), 1).await?;
    f.append(Cat::Jobs, first, 1).await?;
    f.append(Cat::Orders, second, 1).await?;
    common::wait_sequenced(&f.pool, 3).await?;
    for running in all {
        running.wait_position(3).await?;
    }
    let delivered = |name| f.delivered(name);
    let before = [
        delivered("store-wide").await?.len(),
        delivered("sharded").await?.len(),
        delivered("store-wide-sharded").await?.len(),
    ];

    pair.pause_replay().await?;
    f.append(Cat::Parts, stream::StreamId::new_v7(), 1).await?;
    f.append(Cat::Jobs, first, 2).await?;
    f.append(Cat::Orders, second, 2).await?;
    common::wait_sequenced(&f.pool, 8).await?;
    tokio::time::sleep(Duration::from_millis(700)).await;
    let during = [
        delivered("store-wide").await?.len(),
        delivered("sharded").await?.len(),
        delivered("store-wide-sharded").await?.len(),
    ];
    assert_eq!(during, before, "a page was read from the primary");
    let furthest: Option<i64> =
        sqlx::query_scalar("SELECT max(position) FROM es.subscriptions WHERE shard < shards")
            .fetch_one(&f.pool)
            .await?;
    assert_eq!(furthest, Some(3), "a cursor moved past the replica");

    pair.resume_replay().await?;
    for running in all {
        running.wait_position(8).await?;
    }
    let everything = f
        .expected(&[f.id(Cat::Jobs), f.id(Cat::Orders), f.id(Cat::Parts)])
        .await?;
    common::check_delivery(&f.delivered("store-wide").await?, &everything)?;
    let listed = f.expected(&[f.id(Cat::Jobs), f.id(Cat::Orders)]).await?;
    assert_eq!(sorted(f.delivered("sharded").await?), listed);
    assert_eq!(sorted(f.delivered("store-wide-sharded").await?), everything);

    store_wide.stop().await?;
    sharded.stop().await?;
    store_wide_sharded.stop().await?;
    sequencer.stop().await?;
    replica_pool.close().await;
    finish(f, pair).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_deleted_stream_is_skipped_by_store_wide_reads_from_the_replica() -> Result<()> {
    let pair = pges_testing::replica::Pair::start().await?;
    let f = fixture(&pair).await?;
    let replica_pool = replica_pool(&pair, &f).await?;
    let sequencer = common::Sequencer::start(&f).await?;
    sequencer.wait_leading().await?;

    // The deleted stream's log rows (1-2) stay on the replica, before its `$stream-deleted` (4).
    let deleted = stream::StreamId::new_v7();
    f.append(Cat::Jobs, deleted, 2).await?;
    f.append(Cat::Orders, stream::StreamId::new_v7(), 1).await?;
    common::wait_sequenced(&f.pool, 3).await?;
    f.store
        .delete_stream::<JobEvent>(deleted, delete::DeleteExpected::Any)
        .await?;
    f.append(Cat::Jobs, stream::StreamId::new_v7(), 1).await?;
    common::wait_sequenced(&f.pool, 5).await?;

    let subscribe = |name: &str, builder: subscription::Builder<subscription::AllCategories>| {
        Running::spawn(
            builder
                .waker(inproc::InprocWaker::new(
                    sequencer.watch.high_water(f.store.store_id()),
                ))
                .replica(replica_pool.clone())
                .batch_size(NonZeroU32::new(1).unwrap())
                .acquire_interval(interval!(20 ms))
                .run_pg(Recorder::new(name)),
        )
    };
    let store_wide = subscribe(
        "store-wide",
        f.store
            .subscription(subscription_name!("store-wide"))
            .all_categories(),
    );
    let store_wide_sharded = subscribe(
        "store-wide-sharded",
        f.store
            .subscription(subscription_name!("store-wide-sharded"))
            .all_categories()
            .sharded(pges_subscribe::shards!(2)),
    );
    store_wide.wait_position(5).await?;
    store_wide_sharded.wait_position(5).await?;

    let expected = vec![(3, f.id(Cat::Orders).get()), (5, f.id(Cat::Jobs).get())];
    assert_eq!(f.delivered("store-wide").await?, expected);
    assert_eq!(sorted(f.delivered("store-wide-sharded").await?), expected);

    store_wide.stop().await?;
    store_wide_sharded.stop().await?;
    sequencer.stop().await?;
    replica_pool.close().await;
    finish(f, pair).await
}

/// A stream in shard 0 and one in shard 1 of 2, by the hash `Builder::sharded` documents.
fn one_stream_per_shard() -> [stream::StreamId; 2] {
    let mut found: [Option<stream::StreamId>; 2] = [None, None];
    while found.iter().any(Option::is_none) {
        let stream_id = stream::StreamId::new_v7();
        let digest = Sha256::digest(stream_id.as_uuid().as_bytes());
        let first: [u8; 8] = digest[..8].try_into().unwrap();
        let shard = (u64::from_be_bytes(first) & i64::MAX as u64) % 2;
        found[shard as usize].get_or_insert(stream_id);
    }

    found.map(Option::unwrap)
}

fn sorted(mut delivered: Vec<(i64, i32)>) -> Vec<(i64, i32)> {
    delivered.sort_unstable();
    delivered
}

/// Counts the calls to a waker.
struct Counting<W> {
    inner: W,
    calls: Arc<AtomicUsize>,
}

impl<W: waker::Waker> waker::Waker for Counting<W> {
    async fn changed(
        &mut self,
        known: &high_water::HighWater,
    ) -> Result<high_water::HighWater, waker::WakeError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.inner.changed(known).await
    }
}

/// [`Fixture::new`] on the pair's primary: an outbox store, so the sequencer needs no replication slot.
async fn fixture(pair: &pges_testing::replica::Pair) -> Result<Fixture> {
    let db = pair.create_database("t_replica_reads").await?;
    let mut conn = db.connect().await?;
    pges_testing::create_store(
        &mut conn,
        &common::STORE_NAME,
        &pges_testing::definition(pges_postgres::store::Outbox::Enabled),
    )
    .await?;
    conn.execute(
        "CREATE TABLE public.delivered (
           seq         int8 GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
           position    int8 NOT NULL,
           commit_id   uuid NOT NULL,
           category_id int4 NOT NULL,
           subscriber  text NOT NULL DEFAULT ''
         )",
    )
    .await?;
    let pool = db.pool().await?;
    let admin = admin::Admin::new(pool.clone(), common::STORE_NAME);
    for name in [
        <JobEvent as pges_core::event::Event>::CATEGORY,
        <OrderEvent as pges_core::event::Event>::CATEGORY,
        <PartEvent as pges_core::event::Event>::CATEGORY,
    ] {
        admin
            .register_category(name, admin::CategoryConfig::default())
            .await?;
    }
    admin.maintain().await?;
    let store =
        store::Store::open(pool.clone(), store::StoreConfig::new(common::STORE_NAME)).await?;
    store.register::<JobEvent>().await?;
    store.register::<OrderEvent>().await?;
    store.register::<PartEvent>().await?;
    let ids: BTreeMap<_, _> =
        sqlx::query_as::<_, (String, i32)>("SELECT name, category_id FROM es.categories")
            .fetch_all(&pool)
            .await?
            .into_iter()
            .map(|(name, id)| (name, pges_core::category::CategoryId::new(id)))
            .collect();

    Ok(Fixture {
        db,
        pool,
        store,
        ids,
    })
}

/// A pool on the replica, once it has the store.
async fn replica_pool(pair: &pges_testing::replica::Pair, f: &Fixture) -> Result<PgPool> {
    pair.caught_up().await?;

    Ok(PgPoolOptions::new()
        .max_connections(4)
        .connect(&pair.replica_url(f.db.name())?)
        .await?)
}

async fn finish(f: Fixture, pair: pges_testing::replica::Pair) -> Result<()> {
    f.finish().await?;
    pair.stop().await
}
