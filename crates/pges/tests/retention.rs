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

/// A store whose category kept one commit below its horizon: a commit of the next leaf sequenced before
/// the last commit of the leaf retention dropped.
struct KeptBelowHorizon {
    db: pges_testing::TestDb,
    pool: PgPool,
    store: store::Store,
    admin: admin::Admin,
    horizon: position::Position,
    start: position::Position,
}

/// Writes a commit at the first millisecond of last month and one `dropped_before` (an SQL interval)
/// before it, sequences the later id first (position 1, then 2), and applies a retention that drops the
/// leaf of the month before last.
async fn kept_below_horizon(dropped_before: &str) -> Result<KeptBelowHorizon> {
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

    // The first millisecond of last month, and `dropped_before` before it: two commits on either side of
    // a leaf bound.
    let (before, after): (sqlx::types::Uuid, sqlx::types::Uuid) = sqlx::query_as(
        "WITH b AS (SELECT date_trunc('month', now() AT TIME ZONE 'UTC') AT TIME ZONE 'UTC'
                           - interval '1 month' AS bound)
         SELECT pges.uuidv7_floor(bound - $1::interval), pges.uuidv7_floor(bound) FROM b",
    )
    .bind(dropped_before)
    .fetch_one(&pool)
    .await?;
    // The later id commits first: the log gives it position 1, and the earlier id position 2.
    store
        .append(
            stream::StreamId::new_v7(),
            version::ExpectedVersion::NoStream,
            &created("after"),
        )
        .commit_id(commit::CommitId::from_uuid(after).unwrap())
        .await?;
    sequence(&pool).await?;
    store
        .append(
            stream::StreamId::new_v7(),
            version::ExpectedVersion::NoStream,
            &created("before"),
        )
        .commit_id(commit::CommitId::from_uuid(before).unwrap())
        .await?;
    sequence(&pool).await?;

    pool.execute(
        "UPDATE pges.stores SET max_clock_skew = '5 seconds', max_txn_age = '1 minute'
         WHERE schema_name = 'es'",
    )
    .await?;
    // Keeps last month's leaf, drops the one before.
    let since_month_start: f64 = sqlx::query_scalar(
        "SELECT extract(epoch FROM now() - (date_trunc('month', now() AT TIME ZONE 'UTC') AT TIME ZONE 'UTC'))::float8",
    )
    .fetch_one(&pool)
    .await?;
    admin
        .set_retention(
            JobEvent::CATEGORY,
            retention::Retention::Window {
                keep: Duration::from_secs_f64(since_month_start) + Duration::from_secs(86_400),
                then: retention::AfterWindow::Drop,
            },
        )
        .await?;
    let report = admin
        .apply_retention(&retention::RetentionConfig::new(limit::WriterRoles::one(
            role,
        )))
        .await?;
    let dropped = report.dropped.last().expect("a leaf dropped");
    let (horizon, start) = (dropped.horizon, dropped.start);
    assert!(report.refused.is_empty(), "{report:?}");
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
        .fetch_one(&pool)
        .await?;
    assert_eq!(kept, 1, "the commit after the bound is kept");

    Ok(KeptBelowHorizon {
        db,
        pool,
        store,
        admin,
        horizon,
        start,
    })
}

/// A commit of the next leaf can be sequenced before the last commit of the leaf retention drops: its id is
/// past the leaf's upper bound, but it committed first. Its position is then below the horizon, so a
/// subscription started at the horizon would never deliver it, although it still exists. Retention records
/// the start position before it, and a subscription started at `Start::Retained` reads from there.
#[tokio::test]
async fn a_subscription_started_at_retained_reads_every_commit_retention_kept() -> Result<()> {
    let KeptBelowHorizon {
        db,
        pool,
        store,
        horizon,
        start,
        ..
    } = kept_below_horizon("1 millisecond").await?;

    let collector = Collector::default();
    let retained = store
        .subscription(pges::subscribe::subscription_name!("retained"))
        .category::<JobEvent>()
        .start(subscription::Start::Retained)
        .waker(recheck::Recheck::every(pges::core::interval!(10 ms)))
        .run(collector.clone())
        .spawn();
    tokio::time::sleep(Duration::from_millis(500)).await;
    retained.shutdown().await?;
    assert_eq!(
        collector.seen(),
        [Seen::Event(1)],
        "horizon {horizon:?}, start {start:?}: the kept commit at position 1 is delivered"
    );

    // A backfill reads it too.
    let backfilled = Collector::default();
    store
        .backfill(
            pges::subscribe::subscription_name!("backfill"),
            pges::subscribe::backfill::Until::Head,
        )
        .category::<JobEvent>()
        .run_pg(backfilled.clone())
        .await?;
    assert_eq!(backfilled.seen(), [Seen::Event(1)]);

    pool.close().await;
    db.cleanup().await
}

/// `Admin::prune_log` can delete the store's newest log row, the dropped commit's, so the head falls below
/// the category's horizon. A subscription started at `Start::Retained` must still skip the dropped commit
/// rather than fail with `CursorBeforeHorizon`, whose advice is to start at `Start::Retained`.
#[tokio::test]
async fn a_subscription_started_at_retained_after_prune_log_removed_the_head() -> Result<()> {
    let KeptBelowHorizon {
        db,
        pool,
        store,
        admin,
        horizon,
        ..
    } = kept_below_horizon("1 day").await?;
    // prune_log deletes no row above the store's coarse mark, which the sequencer writes with its token.
    pool.execute(
        "UPDATE pges.stores SET last_position = (SELECT max(last_position) FROM es.log)
         WHERE schema_name = 'es'",
    )
    .await?;
    let pruned = admin.prune_log(JobEvent::CATEGORY).await?;
    assert_eq!(pruned.rows, 1, "the dropped commit's log row");
    let head: i64 = sqlx::query_scalar("SELECT max(last_position) FROM es.log")
        .fetch_one(&pool)
        .await?;
    assert_eq!(head, 1, "the head is below the horizon {horizon:?}");

    let category = Collector::default();
    let by_category = store
        .subscription(pges::subscribe::subscription_name!("retained-category"))
        .category::<JobEvent>()
        .start(subscription::Start::Retained)
        .waker(recheck::Recheck::every(pges::core::interval!(10 ms)))
        .run(category.clone())
        .spawn();
    let all = Collector::default();
    let store_wide = store
        .subscription(pges::subscribe::subscription_name!("retained-all"))
        .all_categories()
        .start(subscription::Start::Retained)
        .waker(recheck::Recheck::every(pges::core::interval!(10 ms)))
        .run(all.clone())
        .spawn();
    // A subscription that failed never reaches the position: report its error first.
    let reached = (
        wait_for(&by_category, 1).await,
        wait_for(&store_wide, 1).await,
    );
    by_category.shutdown().await?;
    store_wide.shutdown().await?;
    reached.0?;
    reached.1?;
    assert_eq!(category.seen(), [Seen::Event(1)]);
    assert_eq!(all.seen(), [Seen::Event(1)]);

    pool.close().await;
    db.cleanup().await
}
