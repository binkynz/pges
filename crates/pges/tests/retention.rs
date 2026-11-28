//! Retention and stream deletion as subscribers see them: a subscription behind the horizon retention
//! recorded stops with `CursorBeforeHorizon`, and a deleted stream reaches subscribers as
//! `SystemEvent::StreamDeleted` while its deleted events never do.
//!
//! Log rows are written by hand, in commit id order, instead of by a sequencer.

#![cfg(all(feature = "derive", feature = "subscribe"))]
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

use anyhow::{Result, bail};
use sqlx::{Executor, PgPool};

use pges::core::{commit, event::Event as _, position, stream, version};
use pges::postgres::{admin, limit, retention, store};
use pges::subscribe::{
    batch, handler,
    subscription::{self, StoreExt},
    waker::recheck,
};

#[derive(Debug, Clone, PartialEq, pges::Event)]
#[event(category = "jobs")]
enum JobEvent {
    #[event(name = "job.created")]
    Created { title: String },
}

#[derive(Debug, Clone, PartialEq, pges::Event)]
#[event(category = "orders")]
enum OrderEvent {
    #[event(name = "order.placed")]
    Placed { n: u32 },
}

fn created(title: &str) -> [JobEvent; 1] {
    [JobEvent::Created {
        title: title.into(),
    }]
}

/// What a subscription delivered: each record's position, and the stream of each `$stream-deleted`.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Seen {
    Event(u64),
    Deleted(u64, stream::StreamId),
}

#[derive(Clone, Default)]
struct Collector(Arc<Mutex<Vec<Seen>>>);

impl Collector {
    fn seen(&self) -> Vec<Seen> {
        self.0.lock().unwrap().clone()
    }
}

impl handler::Handler for Collector {
    async fn handle(&mut self, batch: &batch::Batch) -> Result<(), handler::HandlerError> {
        let mut seen = self.0.lock().unwrap();
        for record in batch.iter() {
            seen.push(match record {
                batch::RawRecorded::System(batch::SystemEvent::StreamDeleted {
                    position,
                    stream_id,
                    ..
                }) => Seen::Deleted(position.get(), *stream_id),
                other => Seen::Event(other.position().get()),
            });
        }
        Ok(())
    }
}

impl handler::PgHandler for Collector {
    async fn handle(
        &mut self,
        _tx: &mut sqlx::PgConnection,
        batch: &batch::Batch,
    ) -> Result<(), handler::HandlerError> {
        handler::Handler::handle(self, batch).await
    }
}

/// Gives every commit without a log row one, in commit id order.
async fn sequence(pool: &PgPool) -> Result<()> {
    pool.execute(
        "INSERT INTO es.log (last_position, commit_id, category_id, event_count)
         SELECT coalesce((SELECT max(last_position) FROM es.log), 0)
                + sum(c.event_count) OVER (ORDER BY c.commit_id),
                c.commit_id, c.category_id, c.event_count
         FROM es.commits c
         WHERE NOT EXISTS (SELECT 1 FROM es.log l WHERE l.commit_id = c.commit_id)",
    )
    .await?;
    Ok(())
}

async fn wait_for(handle: &subscription::SubscriptionHandle, position: u64) -> Result<()> {
    for _ in 0..500 {
        if handle.status().position >= pges_testing::position(position) {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    bail!("the subscription did not reach position {position}");
}

#[tokio::test]
async fn a_subscription_behind_the_retention_horizon_stops() -> Result<()> {
    let db = pges_testing::TestDb::new().await?;
    let mut conn = db.connect().await?;
    let es = pges::core::store_name!("es");
    pges_testing::create_store(
        &mut conn,
        &es,
        &pges_testing::definition(store::Outbox::Disabled),
    )
    .await?;
    pges_testing::register_category(&mut conn, &es, JobEvent::CATEGORY, 3, 1).await?;
    pges_testing::create_log_partition(&mut conn, &es, 0).await?;
    conn.execute(sqlx::AssertSqlSafe(format!(
        "ALTER ROLE CURRENT_USER IN DATABASE {} SET transaction_timeout = '1min'",
        db.name()
    )))
    .await?;
    let role: String = sqlx::query_scalar("SELECT current_user::text")
        .fetch_one(&mut conn)
        .await?;
    let pool = db.pool().await?;
    // Lets the test write a commit three months back.
    let wide = Duration::from_secs(200 * 86_400);
    let admin = admin::Admin::new(pool.clone(), es.clone());
    admin
        .set_writer_limits(limit::WriterLimits::new(
            wide,
            pges::core::interval!(pges::core::store::DEFAULT_MAX_TXN_AGE),
        ))
        .await?;
    let mut config = store::StoreConfig::new(es);
    config.max_clock_skew = Some(wide);
    let store = store::Store::open(pool.clone(), config).await?;

    let old: sqlx::types::Uuid = sqlx::query_scalar(
        "SELECT pges.uuidv7_floor(date_trunc('month', now() AT TIME ZONE 'UTC') AT TIME ZONE 'UTC'
                                  - interval '3 months' + interval '1 day')",
    )
    .fetch_one(&pool)
    .await?;
    store
        .append(
            stream::StreamId::new_v7(),
            version::ExpectedVersion::NoStream,
            &created("old"),
        )
        .commit_id(commit::CommitId::from_uuid(old).unwrap())
        .await?;
    store
        .append(
            stream::StreamId::new_v7(),
            version::ExpectedVersion::NoStream,
            &created("new"),
        )
        .await?;
    sequence(&pool).await?;

    // The writer with the wide skew is done. `set_writer_limits` never lowers a limit, so the test writes
    // the column itself: its history is one honest writers produce, compressed from months into seconds.
    pool.execute(
        "UPDATE pges.stores SET max_clock_skew = '5 seconds', max_txn_age = '1 minute'
         WHERE schema_name = 'es'",
    )
    .await?;
    admin
        .set_retention(
            JobEvent::CATEGORY,
            retention::Retention::Window {
                keep: Duration::from_secs(7 * 86_400),
                then: retention::AfterWindow::Drop,
            },
        )
        .await?;
    let report = admin
        .apply_retention(&retention::RetentionConfig::new(limit::WriterRoles::one(
            role,
        )))
        .await?;
    assert!(!report.dropped.is_empty(), "{report:?}");
    assert!(report.refused.is_empty(), "{report:?}");

    let behind = store
        .subscription(pges::subscribe::subscription_name!("behind"))
        .category::<JobEvent>()
        .waker(recheck::Recheck::every(pges::core::interval!(10 ms)))
        .run(Collector::default())
        .spawn();
    match behind.join().await {
        Err(subscription::SubscribeError::CursorBeforeHorizon {
            cursor, horizon, ..
        }) => {
            assert_eq!(cursor, position::Position::BEGINNING);
            assert_eq!(horizon, pges_testing::position(1));
        }
        other => panic!("unexpected end {other:?}"),
    }

    // From the horizon, what retention kept is delivered.
    let collector = Collector::default();
    let at_horizon = store
        .subscription(pges::subscribe::subscription_name!("at-horizon"))
        .category::<JobEvent>()
        .start(subscription::Start::Position(pges_testing::position(1)))
        .waker(recheck::Recheck::every(pges::core::interval!(10 ms)))
        .run(collector.clone())
        .spawn();
    wait_for(&at_horizon, 2).await?;
    at_horizon.shutdown().await?;
    assert_eq!(collector.seen(), [Seen::Event(2)]);

    pool.close().await;
    db.cleanup().await
}

#[tokio::test]
async fn subscribers_see_a_deleted_stream_but_not_its_events() -> Result<()> {
    let db = pges_testing::TestDb::new().await?;
    let pool = db.pool().await?;
    let mut config = store::StoreConfig::new(pges::core::store_name!("es"));
    config.migrate = store::AutoMigrate::CreateOrMigrate(store::StoreDefinition::default());
    let store = store::Store::open(pool.clone(), config).await?;
    let admin = store.admin();
    admin
        .register_category(JobEvent::CATEGORY, admin::CategoryConfig::default())
        .await?;
    admin.maintain().await?;

    let (deleted, kept) = (stream::StreamId::new_v7(), stream::StreamId::new_v7());
    let first = store
        .append(deleted, version::ExpectedVersion::NoStream, &created("a"))
        .await?;
    let second = store
        .append(deleted, first.expected(), &created("b"))
        .await?;
    store
        .append(kept, version::ExpectedVersion::NoStream, &created("c"))
        .await?;
    sequence(&pool).await?;
    store
        .delete_stream::<JobEvent>(deleted, second.last_version())
        .await?;
    sequence(&pool).await?;

    let collector = Collector::default();
    let handle = store
        .subscription(pges::subscribe::subscription_name!("purge"))
        .category::<JobEvent>()
        .waker(recheck::Recheck::every(pges::core::interval!(10 ms)))
        .run(collector.clone())
        .spawn();
    wait_for(&handle, 4).await?;
    handle.shutdown().await?;
    assert_eq!(
        collector.seen(),
        [Seen::Event(3), Seen::Deleted(4, deleted)]
    );

    pool.close().await;
    db.cleanup().await
}

/// A store with `jobs` and `orders` whose writer limits let a test write commits with ids from two months
/// back, and the writer role apply_retention checks.
struct RetentionStore {
    db: pges_testing::TestDb,
    pool: PgPool,
    store: store::Store,
    admin: admin::Admin,
    role: String,
}

async fn retention_store() -> Result<RetentionStore> {
    let db = pges_testing::TestDb::new().await?;
    let mut conn = db.connect().await?;
    let es = pges::core::store_name!("es");
    pges_testing::create_store(
        &mut conn,
        &es,
        &pges_testing::definition(store::Outbox::Disabled),
    )
    .await?;
    pges_testing::register_category(&mut conn, &es, JobEvent::CATEGORY, 3, 1).await?;
    pges_testing::register_category(&mut conn, &es, OrderEvent::CATEGORY, 3, 1).await?;
    pges_testing::create_log_partition(&mut conn, &es, 0).await?;
    conn.execute(sqlx::AssertSqlSafe(format!(
        "ALTER ROLE CURRENT_USER IN DATABASE {} SET transaction_timeout = '1min'",
        db.name()
    )))
    .await?;
    let role: String = sqlx::query_scalar("SELECT current_user::text")
        .fetch_one(&mut conn)
        .await?;
    let pool = db.pool().await?;
    let wide = Duration::from_secs(200 * 86_400);
    let admin = admin::Admin::new(pool.clone(), es.clone());
    admin
        .set_writer_limits(limit::WriterLimits::new(
            wide,
            pges::core::interval!(pges::core::store::DEFAULT_MAX_TXN_AGE),
        ))
        .await?;
    let mut config = store::StoreConfig::new(es);
    config.max_clock_skew = Some(wide);
    let store = store::Store::open(pool.clone(), config).await?;

    Ok(RetentionStore {
        db,
        pool,
        store,
        admin,
        role,
    })
}

/// `before` (an SQL interval) before the first millisecond of last month, and that millisecond, the upper
/// bound of the leaf retention drops, as commit ids.
async fn ids_around_the_bound(
    pool: &PgPool,
    before: &str,
) -> Result<(sqlx::types::Uuid, sqlx::types::Uuid)> {
    Ok(sqlx::query_as(
        "WITH b AS (SELECT date_trunc('month', now() AT TIME ZONE 'UTC') AT TIME ZONE 'UTC'
                           - interval '1 month' AS bound)
         SELECT pges.uuidv7_floor(bound - $1::interval), pges.uuidv7_floor(bound) FROM b",
    )
    .bind(before)
    .fetch_one(pool)
    .await?)
}

/// Applies a jobs retention that keeps last month's leaf and drops the one before, with the store's writer
/// limits narrowed first, and returns the leaf it dropped.
async fn drop_the_leaf_before_last_month(f: &RetentionStore) -> Result<retention::DroppedLeaf> {
    f.pool
        .execute(
            "UPDATE pges.stores SET max_clock_skew = '5 seconds', max_txn_age = '1 minute'
             WHERE schema_name = 'es'",
        )
        .await?;
    let since_month_start: f64 = sqlx::query_scalar(
        "SELECT extract(epoch FROM now() - (date_trunc('month', now() AT TIME ZONE 'UTC') AT TIME ZONE 'UTC'))::float8",
    )
    .fetch_one(&f.pool)
    .await?;
    f.admin
        .set_retention(
            JobEvent::CATEGORY,
            retention::Retention::Window {
                keep: Duration::from_secs_f64(since_month_start) + Duration::from_secs(86_400),
                then: retention::AfterWindow::Drop,
            },
        )
        .await?;
    let report = f
        .admin
        .apply_retention(&retention::RetentionConfig::new(limit::WriterRoles::one(
            f.role.clone(),
        )))
        .await?;
    assert!(report.refused.is_empty(), "{report:?}");

    Ok(report.dropped.last().expect("a leaf dropped").clone())
}

/// Waits until `handle` reaches `position`, then shuts it down. A subscription that stopped never reaches
/// it, so its own error is returned instead of the timeout.
async fn reach_and_stop(handle: subscription::SubscriptionHandle, position: u64) -> Result<()> {
    let reached = wait_for(&handle, position).await;
    if reached.is_err() {
        handle.join().await?;
    } else {
        handle.shutdown().await?;
    }

    reached
}

/// A commit of the next leaf can be sequenced before the last commit of the leaf retention drops: its id is
/// past the leaf's upper bound, but it committed first. Its position is then below the horizon, so a
/// subscription started at the horizon would never deliver it, although it still exists. Retention records
/// the start position before it, and a subscription started at `Start::Retained` reads from there.
///
/// The ids are 1 ms apart, so the later one committing first stays within the store's `max_txn_age`.
#[tokio::test]
async fn a_subscription_started_at_retained_reads_every_commit_retention_kept() -> Result<()> {
    let f = retention_store().await?;
    let (before, after) = ids_around_the_bound(&f.pool, "1 millisecond").await?;
    // The later id commits first: the log gives it position 1, and the earlier id position 2.
    for (id, title) in [(after, "after"), (before, "before")] {
        f.store
            .append(
                stream::StreamId::new_v7(),
                version::ExpectedVersion::NoStream,
                &created(title),
            )
            .commit_id(commit::CommitId::from_uuid(id).unwrap())
            .await?;
        sequence(&f.pool).await?;
    }
    let dropped = drop_the_leaf_before_last_month(&f).await?;
    let (horizon, start) = (dropped.horizon, dropped.start);
    assert_eq!(
        horizon,
        pges_testing::position(2),
        "the dropped commit's position"
    );
    assert_eq!(
        start,
        position::Position::BEGINNING,
        "before the kept commit"
    );
    let kept: i64 = sqlx::query_scalar("SELECT count(*) FROM es.commits")
        .fetch_one(&f.pool)
        .await?;
    assert_eq!(kept, 1, "the commit after the bound is kept");

    let collector = Collector::default();
    let retained = f
        .store
        .subscription(pges::subscribe::subscription_name!("retained"))
        .category::<JobEvent>()
        .start(subscription::Start::Retained)
        .waker(recheck::Recheck::every(pges::core::interval!(10 ms)))
        .run(collector.clone())
        .spawn();
    reach_and_stop(retained, 2).await?;
    assert_eq!(
        collector.seen(),
        [Seen::Event(1)],
        "horizon {horizon:?}, start {start:?}: the kept commit at position 1 is delivered"
    );

    // A backfill reads it too.
    let backfilled = Collector::default();
    f.store
        .backfill(
            pges::subscribe::subscription_name!("backfill"),
            pges::subscribe::backfill::Until::Head,
        )
        .category::<JobEvent>()
        .run_pg(backfilled.clone())
        .await?;
    assert_eq!(backfilled.seen(), [Seen::Event(1)]);

    f.pool.close().await;
    f.db.cleanup().await
}

/// `Admin::prune_log` can delete the store's newest log row, the dropped commit's, so the head falls below
/// the category's horizon. A subscription started at `Start::Retained` must still skip the dropped commit
/// rather than fail with `CursorBeforeHorizon`, whose advice is to start at `Start::Retained`.
///
/// An orders commit is at position 1. A jobs commit from the leaf before last month, sequenced after it,
/// is at 2, and retention drops it. Each category's commits are sequenced in commit id order.
#[tokio::test]
async fn a_subscription_started_at_retained_after_prune_log_removed_the_head() -> Result<()> {
    let f = retention_store().await?;
    f.store
        .append(
            stream::StreamId::new_v7(),
            version::ExpectedVersion::NoStream,
            &[OrderEvent::Placed { n: 1 }],
        )
        .await?;
    sequence(&f.pool).await?;
    let (dropped_id, _) = ids_around_the_bound(&f.pool, "1 day").await?;
    f.store
        .append(
            stream::StreamId::new_v7(),
            version::ExpectedVersion::NoStream,
            &created("dropped"),
        )
        .commit_id(commit::CommitId::from_uuid(dropped_id).unwrap())
        .await?;
    sequence(&f.pool).await?;
    let dropped = drop_the_leaf_before_last_month(&f).await?;
    assert_eq!(
        dropped.horizon,
        pges_testing::position(2),
        "the dropped commit's position"
    );
    assert_eq!(
        dropped.start,
        pges_testing::position(2),
        "no jobs commit is kept"
    );

    // prune_log deletes no row above the store's coarse mark, which the sequencer writes with its token.
    f.pool
        .execute(
            "UPDATE pges.stores SET last_position = (SELECT max(last_position) FROM es.log)
             WHERE schema_name = 'es'",
        )
        .await?;
    let pruned = f.admin.prune_log(JobEvent::CATEGORY).await?;
    assert_eq!(pruned.rows, 1, "the dropped commit's log row");
    let head: i64 = sqlx::query_scalar("SELECT max(last_position) FROM es.log")
        .fetch_one(&f.pool)
        .await?;
    assert_eq!(head, 1, "the head is below the jobs horizon");

    let jobs = Collector::default();
    let by_category = f
        .store
        .subscription(pges::subscribe::subscription_name!("retained-category"))
        .category::<JobEvent>()
        .start(subscription::Start::Retained)
        .waker(recheck::Recheck::every(pges::core::interval!(10 ms)))
        .run(jobs.clone())
        .spawn();
    let all = Collector::default();
    let store_wide = f
        .store
        .subscription(pges::subscribe::subscription_name!("retained-all"))
        .all_categories()
        .start(subscription::Start::Retained)
        .waker(recheck::Recheck::every(pges::core::interval!(10 ms)))
        .run(all.clone())
        .spawn();
    let reached = (
        reach_and_stop(by_category, 1).await,
        reach_and_stop(store_wide, 1).await,
    );
    reached.0?;
    reached.1?;
    assert_eq!(jobs.seen(), []);
    assert_eq!(all.seen(), [Seen::Event(1)]);

    f.pool.close().await;
    f.db.cleanup().await
}
