//! The outbox source end to end: appends to an outbox store become log rows with gap-free positions.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

mod common;

use std::time::Duration;

use anyhow::Result;
use sqlx::Connection;

use pges_core::{interval, position, stream, version};
use pges_postgres::{admin, store};
use pges_sequencer::{outbox, sequencer};
use pges_testing::proxy;

use common::{Fixture, JobEvent, OrderEvent, Target};

/// A commit that would take positions past `i64::MAX` stops the sequencer with `PositionOverflow`: nothing
/// is written, the commit stays pending, and the lease is released without a halt.
#[tokio::test]
async fn positions_past_the_end_stop_the_sequencer() -> Result<()> {
    let f = Fixture::new().await?;
    let running = f.start(common::fast()).await?;
    running.wait_leading().await?;
    f.append(Target::Job(stream::StreamId::new_v7()), 1).await?;
    common::wait_sequenced(&f.pool, 1).await?;
    running.stop().await?;
    common::move_log_to_the_last_positions(&f.pool).await?;

    let running = f.start(common::fast()).await?;
    running.wait_leading().await?;
    f.append(Target::Order(stream::StreamId::new_v7()), 2)
        .await?;
    let result = running.result().await;
    assert!(
        matches!(result, Err(sequencer::RunError::PositionOverflow(store)) if store == f.store_id),
        "{result:?}"
    );

    let (logged, last, pending, halted, released): (i64, i64, i64, Option<String>, bool) =
        sqlx::query_as(
            "SELECT (SELECT count(*) FROM es.log), (SELECT max(last_position) FROM es.log),
                    (SELECT count(*) FROM es.pending), halted, lease_until IS NULL
             FROM pges.sequencer",
        )
        .fetch_one(&f.pool)
        .await?;
    assert_eq!((logged, last, pending), (1, i64::MAX - 1, 1));
    assert_eq!(halted, None);
    assert!(released, "the lease was kept");

    f.finish().await
}

#[tokio::test]
async fn appends_become_log_entries_in_commit_order() -> Result<()> {
    let f = Fixture::new().await?;
    let job = stream::StreamId::new_v7();
    let order = stream::StreamId::new_v7();

    let mut appended = Vec::new();
    for (target, events) in [
        (Target::Job(job), 2),
        (Target::Order(order), 1),
        (Target::Job(job), 3),
        (Target::Order(order), 1),
        (Target::Job(stream::StreamId::new_v7()), 1),
    ] {
        appended.push(f.append(target, events).await?.commit_id);
    }
    let running = f.start(common::fast()).await?;
    common::wait_sequenced(&f.pool, 5).await?;

    let log = common::check_log(&f.pool).await?;
    assert_eq!(common::commit_order(&log), appended);
    assert_eq!(log.last().unwrap().last_position, 8);
    let high = common::wait_high_water(&running.watch, f.store_id, 8).await?;
    assert_eq!(high.category(f.jobs), pges_testing::position(8));
    assert_eq!(high.category(f.orders), pges_testing::position(7));
    assert!(high.lsn().is_some());

    running.stop().await?;
    f.finish().await
}

/// Positions follow `pending.seq`, not commit order: two units of work commit in the opposite order of
/// their appends, and one drain sees both. `DELETE ... RETURNING` has no order guarantee, so the drain sorts
/// by `seq`.
#[tokio::test]
async fn drain_assigns_positions_in_seq_order() -> Result<()> {
    let f = Fixture::new().await?;
    let first = stream::StreamId::new_v7();
    let second = stream::StreamId::new_v7();

    let mut a = f.store.begin().await?;
    let mut b = f.store.begin().await?;
    let a_commit = a
        .append(
            first,
            version::ExpectedVersion::NoStream,
            &[JobEvent::Noted { n: 1 }],
        )
        .await?
        .commit_id;
    let b_commit = b
        .append(
            second,
            version::ExpectedVersion::NoStream,
            &[OrderEvent::Noted { n: 2 }],
        )
        .await?
        .commit_id;
    b.commit().await?;
    a.commit().await?;

    let running = f.start(common::fast()).await?;
    common::wait_sequenced(&f.pool, 2).await?;

    let log = common::check_log(&f.pool).await?;
    assert_eq!(common::commit_order(&log), vec![a_commit, b_commit]);

    running.stop().await?;
    f.finish().await
}

#[tokio::test]
async fn concurrent_appenders_get_gap_free_positions() -> Result<()> {
    let f = Fixture::new().await?;
    let shared = stream::StreamId::new_v7();
    let running = f.start(common::fast()).await?;

    let mut tasks = Vec::new();
    for writer in 0..6_u32 {
        let store = f.store.clone();
        tasks.push(tokio::spawn(async move {
            let own = stream::StreamId::new_v7();
            for n in 0..20_u32 {
                let events = vec![JobEvent::Noted { n }; (n % 3 + 1) as usize];
                store
                    .append(own, version::ExpectedVersion::Any, &events)
                    .await
                    .unwrap();
                if n % 4 == writer % 4 {
                    store
                        .append(
                            shared,
                            version::ExpectedVersion::Any,
                            &[OrderEvent::Noted { n }],
                        )
                        .await
                        .unwrap();
                }
            }
        }));
    }
    for task in tasks {
        task.await?;
    }

    let expected: i64 = sqlx::query_scalar("SELECT count(*) FROM es.commits")
        .fetch_one(&f.pool)
        .await?;
    common::wait_sequenced(&f.pool, expected as usize).await?;
    let log = common::check_log(&f.pool).await?;
    let events: i64 = log.iter().map(|row| i64::from(row.event_count)).sum();
    assert_eq!(log.last().unwrap().last_position, events);

    running.stop().await?;
    f.finish().await
}

#[tokio::test]
async fn restart_resumes_without_gaps_or_duplicates() -> Result<()> {
    let f = Fixture::new().await?;
    let job = stream::StreamId::new_v7();
    let mut count = 0;

    let running = f.start(common::fast()).await?;
    for _ in 0..10 {
        f.append(Target::Job(job), 2).await?;
        count += 1;
    }
    common::wait_sequenced(&f.pool, count).await?;
    running.stop().await?;

    // Appended while no sequencer runs, then a graceful restart.
    for _ in 0..5 {
        f.append(Target::Order(stream::StreamId::new_v7()), 1)
            .await?;
        count += 1;
    }
    let running = f.start(common::fast()).await?;
    for _ in 0..5 {
        f.append(Target::Job(job), 1).await?;
        count += 1;
    }
    common::wait_sequenced(&f.pool, count).await?;

    // A crash mid-stream: the next sequencer waits for the lease to expire, then carries on.
    for _ in 0..5 {
        f.append(Target::Job(job), 1).await?;
        count += 1;
    }
    running.kill().await;
    for _ in 0..5 {
        f.append(Target::Order(stream::StreamId::new_v7()), 3)
            .await?;
        count += 1;
    }
    let running = f.start(common::fast()).await?;
    common::wait_sequenced(&f.pool, count).await?;

    let log = common::check_log(&f.pool).await?;
    assert_eq!(log.len(), count);
    running.stop().await?;
    f.finish().await
}

#[tokio::test]
async fn high_water_follows_batches_and_is_persisted_coarsely() -> Result<()> {
    let f = Fixture::new().await?;
    let running = f.start(common::fast()).await?;
    running.wait_leading().await?;
    let mut receiver = running.watch.high_water(f.store_id);

    f.append(Target::Job(stream::StreamId::new_v7()), 3).await?;
    let high = common::wait_high_water(&running.watch, f.store_id, 3).await?;
    assert_eq!(high.store(), pges_testing::position(3));
    assert_eq!(high.category(f.jobs), pges_testing::position(3));
    assert_eq!(high.category(f.orders), position::Position::BEGINNING);
    let first_lsn = high.lsn().unwrap();

    f.append(Target::Order(stream::StreamId::new_v7()), 2)
        .await?;
    let high = common::wait_high_water(&running.watch, f.store_id, 5).await?;
    assert_eq!(high.category(f.jobs), pges_testing::position(3));
    assert_eq!(high.category(f.orders), pges_testing::position(5));
    assert!(high.lsn().unwrap() > first_lsn);
    assert!(receiver.has_changed()?);
    assert_eq!(
        receiver.borrow_and_update().store(),
        pges_testing::position(5)
    );

    // The coarse marks catch up within the token interval.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let (store, jobs, orders, source): (i64, i64, i64, Option<String>) = sqlx::query_as(
            "SELECT (SELECT last_position FROM pges.stores WHERE schema_name = 'es'),
                    (SELECT last_position FROM es.categories WHERE name = 'jobs'),
                    (SELECT last_position FROM es.categories WHERE name = 'orders'),
                    (SELECT source FROM pges.sequencer)",
        )
        .fetch_one(&f.pool)
        .await?;
        if (store, jobs, orders) == (5, 3, 5) {
            assert_eq!(source.as_deref(), Some("outbox"));
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "coarse marks at {store}, {jobs}, {orders}: {:?}",
            *running.watch.status().borrow()
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    running.stop().await?;
    f.finish().await
}

/// A second sequencer starts from the log: its first announcement carries the existing positions.
#[tokio::test]
async fn a_new_leader_announces_the_existing_log() -> Result<()> {
    let f = Fixture::new().await?;
    let running = f.start(common::fast()).await?;
    f.append(Target::Job(stream::StreamId::new_v7()), 4).await?;
    common::wait_sequenced(&f.pool, 1).await?;
    running.stop().await?;

    let running = f.start(common::fast()).await?;
    let high = common::wait_high_water(&running.watch, f.store_id, 4).await?;
    assert_eq!(high.category(f.jobs), pges_testing::position(4));

    running.stop().await?;
    f.finish().await
}

/// A batch that fails in the database rolls back with its drain: the commit stays pending, the sequencer
/// keeps its lease and retries, and sequences it once the cause is fixed.
#[tokio::test]
async fn a_failing_batch_is_retried_without_losing_commits() -> Result<()> {
    let f = Fixture::new().await?;
    sqlx::query("DROP TABLE es.log_0000")
        .execute(&f.pool)
        .await?;
    f.append(Target::Job(stream::StreamId::new_v7()), 2).await?;
    f.append(Target::Order(stream::StreamId::new_v7()), 1)
        .await?;

    let running = f.start(common::fast()).await?;
    let mut status = running.watch.status();
    tokio::time::timeout(
        Duration::from_secs(10),
        status.wait_for(|s| matches!(s, sequencer::Status::Recovering(_))),
    )
    .await??;
    let (logged, pending): (i64, i64) =
        sqlx::query_as("SELECT (SELECT count(*) FROM es.log), (SELECT count(*) FROM es.pending)")
            .fetch_one(&f.pool)
            .await?;
    assert_eq!((logged, pending), (0, 2));

    admin::Admin::new(f.pool.clone(), common::STORE_NAME)
        .maintain()
        .await?;
    common::wait_sequenced(&f.pool, 2).await?;
    let log = common::check_log(&f.pool).await?;
    assert_eq!(log.last().unwrap().last_position, 3);
    common::wait_high_water(&running.watch, f.store_id, 3).await?;
    assert!(running.is_leading());

    let errors = running.stop_after_errors().await?;
    assert!(!errors.is_empty());
    f.finish().await
}

/// An idle source does not poll: while nothing is appended it does not touch `pending` at all, and a commit
/// is sequenced on its notification, far sooner than the fallback recheck (an hour, in `outbox_config`).
/// Scans of `pending` are counted in `pg_stat_user_tables`, which a backend reports when it exits.
#[tokio::test]
async fn an_idle_source_does_not_poll_and_wakes_on_a_commit() -> Result<()> {
    let f = Fixture::new().await?;

    let before = pending_scans(&f, None).await?;
    let running = f.start(common::fast()).await?;
    running.wait_leading().await?;
    tokio::time::sleep(Duration::from_millis(1500)).await;
    running.stop().await?;
    let idle = pending_scans(&f, Some(before)).await? - before;
    // The drain after opening, and nothing else.
    assert!(idle <= 2, "{idle} scans of pending while idle");

    let running = f.start(common::fast()).await?;
    running.wait_leading().await?;
    for n in 1..=5 {
        tokio::time::sleep(Duration::from_millis(200)).await;
        let appended = tokio::time::Instant::now();
        f.append(Target::Job(stream::StreamId::new_v7()), 1).await?;
        common::wait_sequenced(&f.pool, n).await?;
        assert!(
            appended.elapsed() < Duration::from_millis(500),
            "commit {n} sequenced after {:?}",
            appended.elapsed()
        );
    }

    running.stop().await?;
    f.finish().await
}

/// The listening connection is killed: the source listens again and drains at once, so a commit made while
/// nobody listened is sequenced without the fallback recheck, and later commits wake it as before. Nothing
/// shows as `Recovering`.
#[tokio::test]
async fn a_lost_listening_connection_is_replaced() -> Result<()> {
    let f = Fixture::new().await?;
    let running = f.start(common::fast()).await?;
    running.wait_leading().await?;
    f.append(Target::Job(stream::StreamId::new_v7()), 1).await?;
    common::wait_sequenced(&f.pool, 1).await?;

    let first = wait_listener(&f, None).await?;
    let terminated: bool = sqlx::query_scalar("SELECT pg_terminate_backend($1)")
        .bind(first)
        .fetch_one(&f.pool)
        .await?;
    assert!(terminated);
    f.append(Target::Order(stream::StreamId::new_v7()), 2)
        .await?;
    common::wait_sequenced(&f.pool, 2).await?;

    let second = wait_listener(&f, Some(first)).await?;
    assert_ne!(first, second);
    f.append(Target::Job(stream::StreamId::new_v7()), 1).await?;
    common::wait_sequenced(&f.pool, 3).await?;
    let log = common::check_log(&f.pool).await?;
    assert_eq!(log.last().unwrap().last_position, 4);

    running.stop().await?;
    f.finish().await
}

/// The listening connection is lost and a new one cannot connect yet; a commit made meanwhile notifies no
/// one. Once the source listens again it drains at once, without waiting for the fallback recheck (an hour).
/// The source reaches the server through a proxy; the sequencer's batches do not.
#[tokio::test]
async fn a_commit_made_while_nobody_listens_is_drained_after_listening_again() -> Result<()> {
    let f = Fixture::new().await?;
    let proxy = proxy::Proxy::start(pges_testing::server().await?).await?;
    let proxied = sqlx::postgres::PgPoolOptions::new()
        .max_connections(2)
        .connect_lazy(&proxy.url(&f.db)?)?;
    let mut config = common::outbox_config();
    config.store_refresh = interval!(1 h);
    let pool = f.db.pool().await?;
    let running = common::Running::start(
        pool,
        outbox::OutboxSource::new(proxied, config),
        common::fast(),
    )?;
    running.wait_leading().await?;
    f.append(Target::Job(stream::StreamId::new_v7()), 1).await?;
    common::wait_sequenced(&f.pool, 1).await?;

    proxy.hold(proxy::Direction::Both);
    proxy.drop_connections();
    f.append(Target::Job(stream::StreamId::new_v7()), 1).await?;
    tokio::time::sleep(Duration::from_millis(300)).await;
    let logged: i64 = sqlx::query_scalar("SELECT count(*) FROM es.log")
        .fetch_one(&f.pool)
        .await?;
    assert_eq!(logged, 1, "sequenced while the source could not listen");

    proxy.release();
    common::wait_sequenced(&f.pool, 2).await?;
    f.append(Target::Order(stream::StreamId::new_v7()), 1)
        .await?;
    common::wait_sequenced(&f.pool, 3).await?;

    running.stop().await?;
    f.finish().await
}

/// The listening connection stops answering without being closed, as when a firewall drops it silently.
/// The recheck runs on that connection, so the source notices it gets no answer, listens on a new one and
/// drains. The source reaches the server through a proxy; the sequencer's batches do not.
#[tokio::test]
async fn a_listening_connection_that_stops_answering_is_replaced() -> Result<()> {
    let f = Fixture::new().await?;
    let proxy = proxy::Proxy::start(pges_testing::server().await?).await?;
    let proxied = sqlx::postgres::PgPoolOptions::new()
        .max_connections(2)
        .connect_lazy(&proxy.url(&f.db)?)?;
    let mut config = common::outbox_config();
    config.recheck_interval = interval!(200 ms);
    config.store_refresh = interval!(1 h);
    let pool = f.db.pool().await?;
    let running = common::Running::start(
        pool,
        outbox::OutboxSource::new(proxied, config),
        common::fast(),
    )?;
    running.wait_leading().await?;
    f.append(Target::Job(stream::StreamId::new_v7()), 1).await?;
    common::wait_sequenced(&f.pool, 1).await?;
    let first = wait_listener(&f, None).await?;

    // Past the recheck and the second the connection gets to answer it.
    proxy.hold(proxy::Direction::Both);
    f.append(Target::Job(stream::StreamId::new_v7()), 1).await?;
    tokio::time::sleep(Duration::from_millis(2000)).await;
    proxy.release();

    common::wait_sequenced(&f.pool, 2).await?;
    let second = wait_listener(&f, Some(first)).await?;
    assert_ne!(first, second);
    f.append(Target::Order(stream::StreamId::new_v7()), 1)
        .await?;
    common::wait_sequenced(&f.pool, 3).await?;

    running.stop().await?;
    f.finish().await
}

/// A store created while the sequencer runs is sequenced on its first notification, long before the
/// store list would be read again.
#[tokio::test]
async fn a_new_store_is_sequenced_on_its_first_notification() -> Result<()> {
    let f = Fixture::new().await?;
    let pool = f.db.pool().await?;
    let mut config = common::outbox_config();
    config.store_refresh = interval!(1 h);
    let running = common::Running::start(
        pool.clone(),
        outbox::OutboxSource::new(pool, config),
        common::fast(),
    )?;
    running.wait_leading().await?;

    let mut conn = f.db.connect().await?;
    pges_testing::create_store(
        &mut conn,
        &pges_core::store_name!("late"),
        &pges_testing::definition(pges_postgres::store::Outbox::Enabled),
    )
    .await?;
    conn.close().await?;
    let admin = admin::Admin::new(f.pool.clone(), pges_core::store_name!("late"));
    admin
        .register_category(
            <JobEvent as pges_core::event::Event>::CATEGORY,
            admin::CategoryConfig::default(),
        )
        .await?;
    admin.maintain().await?;
    let late = store::Store::open(
        f.pool.clone(),
        store::StoreConfig::new(pges_core::store_name!("late")),
    )
    .await?;
    late.register::<JobEvent>().await?;

    late.append(
        stream::StreamId::new_v7(),
        version::ExpectedVersion::NoStream,
        &[JobEvent::Noted { n: 1 }],
    )
    .await?;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let logged: i64 = sqlx::query_scalar("SELECT count(*) FROM late.log")
            .fetch_one(&f.pool)
            .await?;
        if logged == 1 {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "the new store's commit was not sequenced"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    running.stop().await?;
    f.finish().await
}

/// The trigger on `pending` notifies once per committed transaction, with the store's name, and not for an
/// append that enqueued nothing.
#[tokio::test]
async fn appends_notify_the_outbox_channel_once_per_transaction() -> Result<()> {
    let f = Fixture::new().await?;
    let mut listener = sqlx::postgres::PgListener::connect(f.db.url()).await?;
    listener.listen(outbox::CHANNEL).await?;
    let job = stream::StreamId::new_v7();

    let mut unit = f.store.begin().await?;
    for n in 0..3 {
        unit.append(
            stream::StreamId::new_v7(),
            version::ExpectedVersion::NoStream,
            &[JobEvent::Noted { n }],
        )
        .await?;
    }
    unit.append(
        job,
        version::ExpectedVersion::NoStream,
        &[JobEvent::Noted { n: 3 }],
    )
    .await?;
    unit.commit().await?;
    let notification = tokio::time::timeout(Duration::from_secs(5), listener.recv()).await??;
    assert_eq!(notification.payload(), "es");

    let conflict = f
        .store
        .append(
            job,
            version::ExpectedVersion::NoStream,
            &[JobEvent::Noted { n: 4 }],
        )
        .await;
    assert!(conflict.is_err(), "{conflict:?}");
    let received = tokio::time::timeout(Duration::from_millis(300), listener.recv()).await;
    assert!(received.is_err(), "{received:?}");

    drop(listener);
    f.finish().await
}

/// The backend listening on the outbox channel, once there is one other than `previous`.
async fn wait_listener(f: &Fixture, previous: Option<i32>) -> Result<i32> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let pid: Option<i32> = sqlx::query_scalar(
            "SELECT pid FROM pg_stat_activity
             WHERE datname = current_database()
               AND (query = 'LISTEN \"' || $2::text || '\"' OR query LIKE 'SELECT 1 FROM ((SELECT 1 FROM %')
               AND pid IS DISTINCT FROM $1",
        )
        .bind(previous)
        .bind(outbox::CHANNEL)
        .fetch_optional(&f.pool)
        .await?;
        if let Some(pid) = pid {
            return Ok(pid);
        }
        if tokio::time::Instant::now() >= deadline {
            let rows: Vec<(i32, Option<String>, Option<String>)> = sqlx::query_as(
                "SELECT pid, state, query FROM pg_stat_activity WHERE datname = current_database()",
            )
            .fetch_all(&f.pool)
            .await?;
            anyhow::bail!("no listener on the outbox channel other than {previous:?}: {rows:#?}");
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// Scans of `pending` so far. With `previous`, waits until the stopped sequencer's counts have arrived.
async fn pending_scans(f: &Fixture, previous: Option<i64>) -> Result<i64> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    let mut last = None;
    loop {
        let mut conn = f.db.connect().await?;
        let scans: i64 = sqlx::query_scalar(
            "SELECT coalesce(seq_scan, 0) + coalesce(idx_scan, 0) FROM pg_stat_user_tables
             WHERE schemaname = 'es' AND relname = 'pending'",
        )
        .fetch_one(&mut conn)
        .await?;
        conn.close().await?;

        let settled = match previous {
            None => true,
            Some(previous) => scans > previous && last == Some(scans),
        };
        if settled || tokio::time::Instant::now() > deadline {
            return Ok(scans);
        }
        last = Some(scans);
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}
