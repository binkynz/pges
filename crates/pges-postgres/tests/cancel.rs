//! Cancellation: dropping a future of the store at any await point leaves no pooled connection inside a
//! transaction and no lock held. Each test drops the future after 1, 2, 3, ... polls until it completes, on
//! a pool of one connection, and checks the connections after each drop.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

mod common;

use std::{
    future::IntoFuture,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use anyhow::Result;
use sqlx::{Connection, Executor, PgConnection, PgPool};
use uuid::Uuid;

use pges_core::{commit, store_name, stream, version};
use pges_postgres::{admin, append, delete, store, unit_of_work};
use pges_testing::{cancel, proxy};

use common::{Job, JobEvent, OrderEvent, Placed};

struct Fixture {
    db: pges_testing::TestDb,
    pool: PgPool,
    store: store::Store,
}

impl Fixture {
    /// The common store, opened on a cancellation pool of one connection, with its event types registered.
    async fn new() -> Result<Self> {
        let db = pges_testing::TestDb::new().await?;
        let mut conn = db.connect().await?;
        pges_testing::create_store(
            &mut conn,
            &common::STORE_NAME,
            &pges_testing::definition(store::Outbox::Disabled),
        )
        .await?;
        let pool = cancel::pool(db.url(), 1).await?;
        let admin = admin::Admin::new(pool.clone(), common::STORE_NAME);
        for name in [common::JOBS, common::ORDERS] {
            admin
                .register_category(name, admin::CategoryConfig::default())
                .await?;
        }
        let store =
            store::Store::open(pool.clone(), store::StoreConfig::new(common::STORE_NAME)).await?;
        store.register::<JobEvent>().await?;
        store.register::<OrderEvent>().await?;

        Ok(Self { db, pool, store })
    }

    async fn finish(self) -> Result<()> {
        self.pool.close().await;
        self.db.cleanup().await
    }
}

/// The harness catches what it is for: a future that ran `BEGIN` as text, which sqlx does not track, and is
/// dropped before its `COMMIT` returns its connection to the pool inside that transaction.
#[tokio::test]
async fn the_harness_catches_a_transaction_left_open() -> Result<()> {
    let db = pges_testing::TestDb::new().await?;
    let pool = cancel::pool(db.url(), 1).await?;

    let untracked = || async {
        let mut conn = pool.acquire().await?;
        conn.execute("BEGIN").await?;
        conn.execute("SELECT 1").await?;
        conn.execute("COMMIT").await?;
        Ok::<_, sqlx::Error>(())
    };
    let caught = cancel::drop_at_every_await(&pool, untracked).await;

    let error = caught.expect_err("a transaction left open was not caught");
    assert!(
        format!("{error:#}").contains("inside a transaction"),
        "{error:#}"
    );
    pool.close().await;
    db.cleanup().await
}

/// sqlx's own `begin`, dropped at any await point, leaves no transaction open: since 0.9.1 it counts the
/// transaction before the round trip, so the dropped begin queues its `ROLLBACK`.
#[tokio::test]
async fn a_dropped_sqlx_begin_leaves_no_transaction_open() -> Result<()> {
    let db = pges_testing::TestDb::new().await?;
    let pool = cancel::pool(db.url(), 1).await?;

    // The transaction of a completed run is dropped at once: the check after it needs the connection.
    let begin = || async { pool.begin().await.map(drop) };
    let (_, result) = cancel::drop_at_every_await(&pool, begin).await?;

    result?;
    pool.close().await;
    db.cleanup().await
}

/// The same, with the `BEGIN` certainly in flight when the future is dropped: the connection goes through a
/// proxy that holds the server's replies.
#[tokio::test]
async fn a_sqlx_begin_dropped_with_its_reply_held_leaves_no_transaction_open() -> Result<()> {
    let db = pges_testing::TestDb::new().await?;
    let proxy = proxy::Proxy::start(pges_testing::server().await?).await?;
    let pool = cancel::pool(&proxy.url(&db)?, 1).await?;
    let mut side = db.connect().await?;
    cancel::check_released(&pool).await?;

    proxy.hold(proxy::Direction::ToClient);
    // One poll sends `BEGIN`; its reply is held, so the future cannot complete. The server ran it before the
    // drop.
    let sent = cancel::wait_until_waiting(&mut side, "BEGIN");
    let completed = cancel::poll_check_drop(pool.begin(), 1, sent)
        .await?
        .is_some();
    assert!(!completed, "begin completed with its reply held");
    proxy.release();
    cancel::check_released(&pool).await?;

    pool.close().await;
    db.cleanup().await
}

/// Sentinel for the hazard sqlx 0.9.1 keeps: a savepoint begin on a transaction, dropped while it is in flight,
/// rolls back the whole transaction (the begin's guard rolls back to the savepoint, then the dropped
/// `Transaction` rolls back one level more) and leaves it at depth 0, so the outer `commit` does nothing. The
/// crates never begin a savepoint; the migration functions document it for a caller's connection. Fails once
/// sqlx fixes it, and those docs can then lose the caveat.
#[tokio::test]
async fn a_dropped_sqlx_savepoint_rolls_back_the_outer_transaction() -> Result<()> {
    let db = pges_testing::TestDb::new().await?;
    let mut setup = db.connect().await?;
    setup.execute("CREATE TABLE marks (n int)").await?;
    let proxy = proxy::Proxy::start(pges_testing::server().await?).await?;
    let mut conn = PgConnection::connect(&proxy.url(&db)?).await?;

    let mut outer = conn.begin().await?;
    outer.execute("INSERT INTO marks VALUES (1)").await?;
    proxy.hold(proxy::Direction::ToClient);
    let sent = cancel::wait_until_waiting(&mut setup, "SAVEPOINT _sqlx_savepoint_1");
    let completed = cancel::poll_check_drop(outer.begin(), 1, sent)
        .await?
        .is_some();
    assert!(!completed, "the savepoint completed with its reply held");
    proxy.release();
    outer.execute("INSERT INTO marks VALUES (2)").await?;
    outer.commit().await?;

    let marks: Vec<i32> = sqlx::query_scalar("SELECT n FROM marks ORDER BY n")
        .fetch_all(&mut setup)
        .await?;
    assert_eq!(
        marks,
        [2],
        "sqlx no longer rolls back the outer transaction: drop the savepoint caveat from pges_postgres::migrate"
    );

    conn.close().await?;
    setup.close().await?;
    db.cleanup().await
}

/// Sentinel for the same hazard without a drop: a savepoint begin on an aborted transaction fails, its guard
/// queues a rollback to the savepoint (which fails: the savepoint was never made) and the failed
/// `Transaction` a `ROLLBACK`, so the outer transaction ends at depth 0. The next statement reports the queued
/// error and does not run; the one after runs in autocommit. Fails once sqlx fixes it.
#[tokio::test]
async fn a_failed_sqlx_savepoint_rolls_back_the_outer_transaction() -> Result<()> {
    let db = pges_testing::TestDb::new().await?;
    let mut setup = db.connect().await?;
    setup.execute("CREATE TABLE marks (n int)").await?;
    let mut conn = db.connect().await?;

    let mut outer = conn.begin().await?;
    outer.execute("INSERT INTO marks VALUES (1)").await?;
    outer.execute("SELECT 1 / 0").await.unwrap_err();
    outer.begin().await.unwrap_err();
    assert!(
        !outer.is_in_transaction(),
        "sqlx no longer ends the outer transaction on a failed savepoint begin"
    );
    let queued = outer
        .execute("INSERT INTO marks VALUES (2)")
        .await
        .unwrap_err();
    assert_eq!(
        queued.as_database_error().and_then(|e| e.code()).as_deref(),
        Some("3B001"),
        "{queued}"
    );
    outer.execute("INSERT INTO marks VALUES (3)").await?;
    outer.commit().await?;

    let marks: Vec<i32> = sqlx::query_scalar("SELECT n FROM marks ORDER BY n")
        .fetch_all(&mut setup)
        .await?;
    assert_eq!(marks, [3]);

    conn.close().await?;
    setup.close().await?;
    db.cleanup().await
}

/// A savepoint begin on a unit of work's connection, dropped while the server runs it, ends the whole
/// transaction (see the sentinel above). The unit of work refuses to append after it and fails to commit.
#[tokio::test]
async fn a_unit_of_work_fails_after_a_dropped_savepoint_begin() -> Result<()> {
    let f = Fixture::new().await?;
    let proxy = proxy::Proxy::start(pges_testing::server().await?).await?;
    let pool = cancel::pool(&proxy.url(&f.db)?, 2).await?;
    let store =
        store::Store::open(pool.clone(), store::StoreConfig::new(common::STORE_NAME)).await?;
    let mut side = f.db.connect().await?;
    let (first, second) = (stream::StreamId::new_v7(), stream::StreamId::new_v7());
    let created = [JobEvent::created("a")];
    let none = version::ExpectedVersion::NoStream;

    let mut uow = store.begin().await?;
    uow.append(first, none, &created).await?;
    proxy.hold(proxy::Direction::ToClient);
    let sent = cancel::wait_until_waiting(&mut side, "SAVEPOINT _sqlx_savepoint_1");
    let completed = cancel::poll_check_drop(uow.conn().begin(), 1, sent)
        .await?
        .is_some();
    assert!(!completed, "the savepoint completed with its reply held");
    proxy.release();
    let error = uow.append(second, none, &created).await.unwrap_err();
    assert!(
        matches!(error, append::AppendError::TransactionEnded),
        "{error}"
    );
    let error = uow.commit().await.unwrap_err();
    assert!(
        matches!(error, unit_of_work::TransactionError::Aborted),
        "{error}"
    );

    let ids = [first.as_uuid(), second.as_uuid()];
    let written: i64 =
        sqlx::query_scalar("SELECT count(*) FROM es.streams WHERE stream_id = ANY($1)")
            .bind(&ids[..])
            .fetch_one(&mut side)
            .await?;
    assert_eq!(written, 0, "the ended unit of work wrote nothing");

    side.close().await?;
    pool.close().await;
    f.finish().await
}

#[tokio::test]
async fn a_dropped_append_leaves_no_transaction_open() -> Result<()> {
    let f = Fixture::new().await?;
    let store = &f.store;
    let id = stream::StreamId::new_v7();
    let events = [JobEvent::created("a")];
    let events = &events;

    let (polls, result) = cancel::drop_at_every_await(&f.pool, move || {
        store
            .append(id, version::ExpectedVersion::Any, events)
            .into_future()
    })
    .await?;

    result?;
    assert!(polls > 1);
    f.finish().await
}

#[tokio::test]
async fn a_dropped_append_with_a_snapshot_leaves_no_transaction_open() -> Result<()> {
    let f = Fixture::new().await?;
    let store = &f.store;
    let id = stream::StreamId::new_v7();
    let events = [JobEvent::created("a")];
    let events = &events;
    let state = Job {
        titles: vec!["a".into()],
        assignments: 0,
    };
    let state = &state;

    let (_, result) = cancel::drop_at_every_await(&f.pool, move || {
        store
            .append(id, version::ExpectedVersion::Any, events)
            .snapshot_after(state)
            .into_future()
    })
    .await?;

    result?;
    f.finish().await
}

/// A generated commit id below the stream's last one runs the append again in a transaction that locks
/// the stream.
#[tokio::test]
async fn a_dropped_append_under_the_stream_lock_leaves_no_transaction_open() -> Result<()> {
    let f = Fixture::new().await?;
    let events = [JobEvent::assigned()];

    for polls in 1.. {
        let id = stream::StreamId::new_v7();
        f.store
            .append(
                id,
                version::ExpectedVersion::NoStream,
                &[JobEvent::created("a")],
            )
            .commit_id(commit_id_ahead(Duration::from_secs(2)))
            .await?;

        let append = f
            .store
            .append(id, version::ExpectedVersion::Any, &events)
            .into_future();
        cancel::check_released(&f.pool).await?;
        let done = cancel::poll_then_drop(append, polls).await?;
        cancel::check_released(&f.pool).await?;
        if let Some(result) = done {
            assert_eq!(result?.versions, common::v(2)..=common::v(2));
            break;
        }
    }
    f.finish().await
}

#[tokio::test]
async fn a_dropped_append_many_leaves_no_transaction_open() -> Result<()> {
    let f = Fixture::new().await?;
    let store = &f.store;
    let (jobs, orders) = (stream::StreamId::new_v7(), stream::StreamId::new_v7());
    let job_events = [JobEvent::created("a")];
    let order_events = [OrderEvent::Placed(Placed { total: 1 })];
    let (job_events, order_events) = (&job_events, &order_events);

    let (_, result) = cancel::drop_at_every_await(&f.pool, move || {
        store
            .append_many([
                append::AppendEntry::new(jobs, version::ExpectedVersion::Any, job_events),
                append::AppendEntry::new(orders, version::ExpectedVersion::Any, order_events),
            ])
            .into_future()
    })
    .await?;

    assert_eq!(result?.len(), 2);
    f.finish().await
}

#[tokio::test]
async fn a_dropped_delete_stream_leaves_no_transaction_open() -> Result<()> {
    let f = Fixture::new().await?;
    let store = &f.store;
    let [warm, id] = [stream::StreamId::new_v7(), stream::StreamId::new_v7()];
    for id in [warm, id] {
        store
            .append(
                id,
                version::ExpectedVersion::NoStream,
                &[JobEvent::created("a")],
            )
            .await?;
    }
    // The first deletion registers the tombstone's event type; the runs below then all take the same path.
    store
        .delete_stream::<JobEvent>(warm, delete::DeleteExpected::Any)
        .await?;

    let (_, result) = cancel::drop_at_every_await(&f.pool, move || {
        store.delete_stream::<JobEvent>(id, delete::DeleteExpected::Any)
    })
    .await?;

    // A run dropped after its COMMIT reached the server deleted the stream already.
    match result {
        Ok(_) | Err(delete::DeleteStreamError::StreamDeleted(_)) => {}
        Err(error) => panic!("{error:?}"),
    }
    f.finish().await
}

#[tokio::test]
async fn a_dropped_unit_of_work_leaves_no_transaction_open() -> Result<()> {
    let f = Fixture::new().await?;
    let store = &f.store;
    let id = stream::StreamId::new_v7();
    let events = [JobEvent::created("a")];
    let events = &events;

    let (_, result) = cancel::drop_at_every_await(&f.pool, move || async move {
        let mut work = store.begin().await?;
        work.append(id, version::ExpectedVersion::Any, events)
            .await?;
        work.commit().await?;
        anyhow::Ok(())
    })
    .await?;

    result?;
    f.finish().await
}

#[tokio::test]
async fn a_dropped_snapshot_save_leaves_no_transaction_open() -> Result<()> {
    let f = Fixture::new().await?;
    let store = &f.store;
    let id = stream::StreamId::new_v7();
    store
        .append(
            id,
            version::ExpectedVersion::NoStream,
            &[JobEvent::created("a")],
        )
        .await?;
    let loaded = store.load_snapshotted::<Job>(id).await?;
    let loaded = &loaded;

    let (_, result) =
        cancel::drop_at_every_await(&f.pool, move || store.save_snapshot(loaded)).await?;

    result?;
    f.finish().await
}

/// Opening with migrations creates the store in transactions on the open's own connection.
#[tokio::test]
async fn a_dropped_open_that_creates_the_store_leaves_no_transaction_open() -> Result<()> {
    let f = Fixture::new().await?;
    let pool = &f.pool;
    let mut config = store::StoreConfig::new(store_name!("created"));
    config.migrate =
        store::AutoMigrate::CreateOrMigrate(pges_testing::definition(store::Outbox::Disabled));
    let config = &config;

    let (_, result) = cancel::drop_at_every_await(pool, move || {
        store::Store::open(pool.clone(), config.clone())
    })
    .await?;

    result?;
    f.finish().await
}

/// Maintenance runs on connections it closes after use, whatever happens.
#[tokio::test]
async fn a_dropped_category_registration_leaves_no_transaction_open() -> Result<()> {
    let f = Fixture::new().await?;
    let admin = admin::Admin::new(f.pool.clone(), common::STORE_NAME);
    let admin = &admin;

    let (_, result) = cancel::drop_at_every_await(&f.pool, move || {
        admin.register_category(
            pges_core::category!("parts"),
            admin::CategoryConfig::default(),
        )
    })
    .await?;

    result?;
    f.finish().await
}

fn commit_id_ahead(offset: Duration) -> commit::CommitId {
    let at = SystemTime::now().duration_since(UNIX_EPOCH).unwrap() + offset;
    let ts = uuid::Timestamp::from_unix(uuid::NoContext, at.as_secs(), at.subsec_nanos());

    commit::CommitId::from_uuid(Uuid::new_v7(ts)).unwrap()
}
