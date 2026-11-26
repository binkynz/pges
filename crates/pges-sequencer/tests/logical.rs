//! The logical source end to end against PostgreSQL 18 with `wal_level = logical`: appends become log rows in
//! WAL commit order, restarts resume from the token, the slot is confirmed (also while idle), leadership
//! moves between sequencers, and a lost slot halts sequencing.

#![cfg(feature = "source-logical")]
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

mod common;

use std::time::Duration;

use anyhow::{Context, Result, bail};
use sqlx::{Executor, PgPool};

use pges_core::{interval, stream, version};
use pges_postgres::{admin, store};
use pges_sequencer::{logical, sequencer, source};
use pges_testing::proxy;

use common::{Fixture, JobEvent, OrderEvent, Target};

#[tokio::test]
async fn appends_become_log_rows_in_commit_order() -> Result<()> {
    let f = Fixture::logical().await?;
    let running = f.start(common::fast()).await?;
    running.wait_leading().await?;

    let job = stream::StreamId::new_v7();
    let order = stream::StreamId::new_v7();
    let mut committed = Vec::new();
    for (target, events) in [
        (Target::Job(job), 2),
        (Target::Order(order), 1),
        (Target::Job(job), 3),
    ] {
        committed.push(f.append(target, events).await?.commit_id);
    }

    // Two units of work commit in the opposite order of their appends: the log follows commit order.
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
    committed.extend([b_commit, a_commit]);

    common::wait_sequenced(&f.pool, 5).await?;
    let log = common::check_log(&f.pool).await?;
    assert_eq!(common::commit_order(&log), committed);
    assert_eq!(log.last().unwrap().last_position, 8);
    let high = common::wait_high_water(&running.watch, f.store_id, 8).await?;
    assert_eq!(high.category(f.jobs), pges_testing::position(8));
    assert_eq!(high.category(f.orders), pges_testing::position(7));
    assert!(high.lsn().is_some());

    running.stop().await?;
    f.finish().await
}

#[tokio::test]
async fn restarts_resume_from_the_token_without_gaps_or_duplicates() -> Result<()> {
    let f = Fixture::logical().await?;
    let job = stream::StreamId::new_v7();
    let mut count = 0;

    // A graceful stop stores the token; the next sequencer resumes after it.
    let running = f.start(common::fast()).await?;
    running.wait_leading().await?;
    for _ in 0..3 {
        f.append(Target::Job(job), 1).await?;
        count += 1;
    }
    common::wait_sequenced(&f.pool, count).await?;
    running.stop().await?;
    let token = stored_token(&f.pool)
        .await?
        .context("no token after a graceful stop")?;
    assert_eq!(token.len(), 8);

    // This one never stores a token while it runs, and crashes after its log rows committed.
    let mut lazy = common::fast();
    lazy.token_interval = Duration::from_secs(3600);
    let running = f.start(lazy).await?;
    running.wait_leading().await?;
    let mut first_committed = None;
    for _ in 0..4 {
        f.append(Target::Order(stream::StreamId::new_v7()), 2)
            .await?;
        count += 1;
        // At or past the end of the first commit's record.
        if first_committed.is_none() {
            first_committed = Some(wal_insert_lsn(&f.pool).await?);
        }
    }
    let first_committed = first_committed.unwrap();
    common::wait_sequenced(&f.pool, count).await?;
    assert_eq!(stored_token(&f.pool).await?, Some(token.clone()));

    // Keepalives keep arriving, but the slot must not move past commits whose token is not stored: a server
    // crash could lose their log rows (asynchronous commit), and the slot would never send them again.
    // (Confirming into a transaction that commits later is safe, so the bound is the first commit's end.)
    unrelated_wal(&f).await?;
    tokio::time::sleep(Duration::from_secs(1)).await;
    let confirmed = confirmed_flush(&f.pool, &f.slot().await?).await?;
    assert!(
        confirmed < first_committed,
        "the slot was confirmed to {confirmed:X}, past an unstored commit ending by {first_committed:X}"
    );
    running.kill().await;

    // The slot sends the four again from the stored token; the dedupe window drops them.
    let running = f.start(common::fast()).await?;
    running.wait_leading().await?;
    for _ in 0..2 {
        f.append(Target::Job(job), 1).await?;
        count += 1;
    }
    common::wait_sequenced(&f.pool, count).await?;
    tokio::time::sleep(Duration::from_millis(300)).await;
    let log = common::check_log(&f.pool).await?;
    assert_eq!(log.len(), count);
    let newer = wait_for_token(&f.pool, |stored| stored > token.as_slice()).await?;
    assert!(newer > token);

    running.stop().await?;
    f.finish().await
}

#[tokio::test]
async fn the_slot_is_confirmed_up_to_the_token_and_past_idle_wal() -> Result<()> {
    let f = Fixture::logical().await?;
    let slot = f.slot().await?;
    let running = f.start(common::fast()).await?;
    running.wait_leading().await?;

    f.append(Target::Job(stream::StreamId::new_v7()), 1).await?;
    common::wait_sequenced(&f.pool, 1).await?;
    let token = wait_for_token(&f.pool, |stored| !stored.is_empty()).await?;
    let token = u64::from_be_bytes(token.try_into().unwrap());
    wait_confirmed(&f.pool, &slot, token).await?;

    // WAL the publication does not carry: the slot moves past it once the source is idle.
    let mut other = f.db.connect().await?;
    other
        .execute("SET synchronous_commit = on; CREATE TABLE unrelated (n int8)")
        .await?;
    for n in 0..20 {
        sqlx::query("INSERT INTO unrelated VALUES ($1)")
            .bind(n)
            .execute(&mut other)
            .await?;
    }
    let written: i64 = sqlx::query_scalar("SELECT (pg_current_wal_lsn() - '0/0'::pg_lsn)::int8")
        .fetch_one(&mut other)
        .await?;
    assert!(written as u64 > token);
    wait_confirmed(&f.pool, &slot, written as u64).await?;
    assert_eq!(common::check_log(&f.pool).await?.len(), 1);

    running.stop().await?;
    f.finish().await
}

#[tokio::test]
async fn a_standby_takes_over_the_slot() -> Result<()> {
    let f = Fixture::logical().await?;
    let config = common::fast();
    let a = f.start(config.clone()).await?;
    a.wait_leading().await?;
    let b = f.start(config.clone()).await?;
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(!b.is_leading());

    let job = stream::StreamId::new_v7();
    let mut count = 0;
    for _ in 0..5 {
        f.append(Target::Job(job), 1).await?;
        count += 1;
    }
    common::wait_sequenced(&f.pool, count).await?;

    // The leader crashes; its walsender may still hold the slot when the standby takes over.
    a.kill().await;
    for _ in 0..5 {
        f.append(Target::Order(stream::StreamId::new_v7()), 2)
            .await?;
        count += 1;
    }
    b.wait_leading().await?;
    common::wait_sequenced(&f.pool, count).await?;

    // A graceful handover.
    let c = f.start(config.clone()).await?;
    b.stop().await?;
    c.wait_leading().await?;
    f.append(Target::Job(job), 1).await?;
    count += 1;
    common::wait_sequenced(&f.pool, count).await?;

    assert_eq!(common::check_log(&f.pool).await?.len(), count);
    c.stop().await?;
    f.finish().await
}

/// The sequencer still believes it leads, but another one bumped the fence. It stands down and closes its
/// stream, so its walsender lets go of the slot without waiting for `wal_sender_timeout`; the next leader
/// streams.
#[tokio::test]
async fn a_fenced_leader_lets_go_of_the_slot() -> Result<()> {
    let f = Fixture::logical().await?;
    let mut config = common::fast();
    config.lease = sequencer::Lease::new(interval!(60 s));
    let stale = f.start(config).await?;
    stale.wait_leading().await?;
    let slot = f.slot().await?;
    let holder = active_pid(&f.pool, &slot)
        .await?
        .context("the slot is not active")?;

    sqlx::query(
        "UPDATE pges.sequencer SET fence = fence + 1, lease_until = now() + interval '1 hour'",
    )
    .execute(&f.pool)
    .await?;
    f.append(Target::Job(stream::StreamId::new_v7()), 1).await?;
    let mut status = stale.watch.status();
    tokio::time::timeout(
        Duration::from_secs(10),
        status.wait_for(|s| matches!(s, sequencer::Status::Standby)),
    )
    .await??;
    assert_eq!(common::check_log(&f.pool).await?.len(), 0);
    wait_inactive(&f.pool, &slot).await?;

    sqlx::query("UPDATE pges.sequencer SET lease_until = NULL")
        .execute(&f.pool)
        .await?;
    let next = f.start(common::fast()).await?;
    common::wait_sequenced(&f.pool, 1).await?;
    let now_holding = active_pid(&f.pool, &slot).await?;
    assert_ne!(now_holding, Some(holder));

    stale.stop().await?;
    next.stop().await?;
    f.finish().await
}

/// An append commits while the slot is being created. The slot's consistent point comes after it (creation
/// waits for open transactions), so the slot would never deliver it. The check in the snapshot exported at
/// the consistent point sees it: the source drops the slot and fails instead of skipping the commit.
#[tokio::test]
async fn a_commit_racing_the_slot_creation_is_not_skipped() -> Result<()> {
    let f = Fixture::logical().await?;
    let mut unit = f.store.begin().await?;
    unit.append(
        stream::StreamId::new_v7(),
        version::ExpectedVersion::NoStream,
        &[JobEvent::Noted { n: 1 }],
    )
    .await?;

    let running = f.start(common::fast()).await?;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    loop {
        let waiting: bool = sqlx::query_scalar(
            "SELECT EXISTS (SELECT 1 FROM pg_stat_activity
                            WHERE datname = current_database() AND backend_type = 'walsender'
                              AND wait_event_type = 'Lock')",
        )
        .fetch_one(&f.pool)
        .await?;
        if waiting {
            break;
        }
        if tokio::time::Instant::now() > deadline {
            bail!("the slot creation never waited for the open transaction");
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    unit.commit().await?;

    let result = running.result().await;
    let Err(sequencer::RunError::Source(source::SourceError::Halt(error))) = &result else {
        panic!("expected a halt, got {result:?}");
    };
    assert!(
        matches!(
            error.downcast_ref::<logical::SlotLost>(),
            Some(logical::SlotLost::CommitsBeforeSlot { store, .. }) if store == common::STORE
        ),
        "{error}"
    );
    assert_eq!(slot_count(&f).await?, 0);

    f.finish().await
}

/// Bootstrapped before any writer or sequencer starts, the slot keeps every commit.
#[tokio::test]
async fn bootstrap_creates_the_slot_before_writers_start() -> Result<()> {
    let f = Fixture::logical().await?;
    let slot = f.slot().await?;
    let mut source = f.logical_source(f.pool.clone())?;
    assert_eq!(source.bootstrap().await?, logical::Bootstrap::Created);
    assert_eq!(source.slot().map(|s| s.as_str()), Some(slot.as_str()));
    assert_eq!(slot_count(&f).await?, 1);
    assert_eq!(source.bootstrap().await?, logical::Bootstrap::Exists);

    let job = stream::StreamId::new_v7();
    for _ in 0..3 {
        f.append(Target::Job(job), 1).await?;
    }
    let running = f.start(common::fast()).await?;
    common::wait_sequenced(&f.pool, 3).await?;
    assert_eq!(common::check_log(&f.pool).await?.len(), 3);
    assert_eq!(source.bootstrap().await?, logical::Bootstrap::Exists);

    running.stop().await?;
    f.finish().await
}

/// Bootstrap keeps an existing slot's failover setting unless asked to change it, changes it in both
/// directions when asked, and refuses while a sequencer streams from the slot. Starting a sequencer never
/// changes it.
#[tokio::test]
async fn bootstrap_changes_failover_only_when_asked() -> Result<()> {
    let f = Fixture::logical().await?;
    let slot = f.slot().await?;
    let source_with = |failover| -> Result<logical::LogicalSource> {
        let replication = pges_replication::config::Config::from_url(f.db.url())?;
        let mut config = common::logical_config();
        config.failover = failover;
        Ok(logical::LogicalSource::new(
            f.pool.clone(),
            replication,
            config,
        ))
    };
    let failover = || async {
        sqlx::query_scalar::<_, bool>(
            "SELECT failover FROM pg_replication_slots WHERE slot_name = $1",
        )
        .bind(&slot)
        .fetch_one(&f.pool)
        .await
    };
    let keep = logical::SlotFailover::Keep;
    let enabled = logical::SlotFailover::Set(pges_replication::connection::Failover::Enabled);
    let disabled = logical::SlotFailover::Set(pges_replication::connection::Failover::Disabled);

    assert_eq!(
        source_with(keep)?.bootstrap().await?,
        logical::Bootstrap::Created
    );
    assert!(
        !failover().await?,
        "a new slot gets failover only when asked"
    );
    assert_eq!(
        source_with(enabled)?.bootstrap().await?,
        logical::Bootstrap::FailoverChanged
    );
    assert!(failover().await?);
    for config in [enabled, keep] {
        assert_eq!(
            source_with(config)?.bootstrap().await?,
            logical::Bootstrap::Exists,
            "{config:?}"
        );
        assert!(failover().await?, "{config:?} turned failover off");
    }

    // Opening never alters an existing slot, whatever the configuration asks for.
    let running =
        common::Running::start(f.db.pool().await?, source_with(disabled)?, common::fast())?;
    running.wait_leading().await?;
    // A bootstrap that did not see the slot in use would wait for it instead of failing.
    let busy =
        tokio::time::timeout(Duration::from_secs(10), source_with(disabled)?.bootstrap()).await?;
    assert!(
        matches!(busy, Err(source::SourceError::Transient(_))),
        "{busy:?}"
    );
    running.stop().await?;
    assert!(failover().await?, "opening the source turned failover off");

    assert_eq!(
        source_with(disabled)?.bootstrap().await?,
        logical::Bootstrap::FailoverChanged
    );
    assert!(!failover().await?);
    for config in [disabled, keep] {
        assert_eq!(
            source_with(config)?.bootstrap().await?,
            logical::Bootstrap::Exists,
            "{config:?}"
        );
        assert!(!failover().await?, "{config:?} turned failover on");
    }

    f.finish().await
}

#[tokio::test]
async fn bootstrap_refuses_to_skip_commits_or_replace_a_lost_slot() -> Result<()> {
    let f = Fixture::logical().await?;
    let slot = f.slot().await?;
    let mut source = f.logical_source(f.pool.clone())?;
    f.append(Target::Job(stream::StreamId::new_v7()), 1).await?;

    let result = source.bootstrap().await;
    let Err(source::SourceError::Halt(error)) = &result else {
        panic!("expected a halt, got {result:?}");
    };
    assert!(
        matches!(
            error.downcast_ref::<logical::SlotLost>(),
            Some(logical::SlotLost::CommitsBeforeSlot { store, .. }) if store == common::STORE
        ),
        "{error}"
    );
    assert_eq!(slot_count(&f).await?, 0);
    let halted: Option<String> = sqlx::query_scalar("SELECT halted FROM pges.sequencer")
        .fetch_one(&f.pool)
        .await?;
    assert_eq!(halted, Some(error.to_string()), "recorded for resequence");

    // A lost slot is recorded by the sequencer that meets it, not by bootstrap.
    sqlx::query(
        "UPDATE pges.sequencer SET halted = NULL, source = 'logical', \
         source_token = '\\x0000000001000000'",
    )
    .execute(&f.pool)
    .await?;
    let result = source.bootstrap().await;
    let Err(source::SourceError::Halt(error)) = &result else {
        panic!("expected a halt, got {result:?}");
    };
    assert_eq!(
        error.downcast_ref::<logical::SlotLost>(),
        Some(&logical::SlotLost::Missing { slot })
    );
    let halted: Option<String> = sqlx::query_scalar("SELECT halted FROM pges.sequencer")
        .fetch_one(&f.pool)
        .await?;
    assert_eq!(halted, None);
    assert_eq!(slot_count(&f).await?, 0);

    f.finish().await
}

#[tokio::test]
async fn bootstrap_refuses_an_outbox_database() -> Result<()> {
    let f = Fixture::new().await?;
    let mut source = f.logical_source(f.pool.clone())?;

    let result = source.bootstrap().await;
    assert!(
        matches!(&result, Err(source::SourceError::Invalid(_))),
        "{result:?}"
    );
    assert_eq!(slot_count(&f).await?, 0);

    f.finish().await
}

/// The slot is dropped while the sequencer streams: it recovers from the lost connection (expected), then
/// finds the slot missing with a token stored, records the halt and stops. Nothing restarts it.
///
/// The replication connection goes through a proxy that refuses new connections while the slot is dropped:
/// otherwise the sequencer could take the slot again between its walsender ending and the drop.
#[tokio::test]
async fn a_dropped_slot_halts_sequencing() -> Result<()> {
    let f = Fixture::logical().await?;
    let slot = f.slot().await?;
    let proxy = proxy::Proxy::start(pges_testing::server().await?).await?;
    let pool = f.db.pool().await?;
    let source = logical::LogicalSource::new(
        pool.clone(),
        pges_replication::config::Config::from_url(&proxy.url(&f.db)?)?,
        common::logical_config(),
    );
    let running = common::Running::start(pool, source, common::fast())?;
    running.wait_leading().await?;
    f.append(Target::Job(stream::StreamId::new_v7()), 1).await?;
    common::wait_sequenced(&f.pool, 1).await?;
    wait_for_token(&f.pool, |stored| !stored.is_empty()).await?;

    proxy.refuse();
    proxy.drop_connections();
    wait_inactive(&f.pool, &slot).await?;
    sqlx::query("SELECT pg_drop_replication_slot($1)")
        .bind(&slot)
        .execute(&f.pool)
        .await?;
    proxy.admit();

    let result = running.result().await;
    let Err(sequencer::RunError::Source(source::SourceError::Halt(error))) = &result else {
        panic!("expected a halt, got {result:?}");
    };
    assert_eq!(
        error.downcast_ref::<logical::SlotLost>(),
        Some(&logical::SlotLost::Missing { slot: slot.clone() })
    );
    let (halted, released): (Option<String>, bool) =
        sqlx::query_as("SELECT halted, lease_until IS NULL FROM pges.sequencer")
            .fetch_one(&f.pool)
            .await?;
    assert!(released);
    let halted = halted.context("halted is not set")?;
    assert!(halted.contains(&slot), "{halted}");

    let again = f.start(common::fast()).await?;
    let result = again.result().await;
    assert!(
        matches!(&result, Err(sequencer::RunError::Halted(reason)) if *reason == halted),
        "{result:?}"
    );

    f.finish().await
}

#[tokio::test]
async fn commits_from_before_the_slot_are_not_skipped() -> Result<()> {
    let f = Fixture::logical().await?;
    f.append(Target::Job(stream::StreamId::new_v7()), 1).await?;

    let running = f.start(common::fast()).await?;
    let result = running.result().await;
    let Err(sequencer::RunError::Source(source::SourceError::Halt(error))) = &result else {
        panic!("expected a halt, got {result:?}");
    };
    assert!(
        matches!(
            error.downcast_ref::<logical::SlotLost>(),
            Some(logical::SlotLost::CommitsBeforeSlot { store, .. }) if store == common::STORE
        ),
        "{error}"
    );
    let slots: i64 =
        sqlx::query_scalar("SELECT count(*) FROM pg_replication_slots WHERE slot_name = $1")
            .bind(f.slot().await?)
            .fetch_one(&f.pool)
            .await?;
    assert_eq!(slots, 0);

    f.finish().await
}

/// A store created while the sequencer streams is picked up from its first commit.
#[tokio::test]
async fn a_store_created_while_streaming_is_sequenced() -> Result<()> {
    let f = Fixture::logical().await?;
    let running = f.start(common::fast()).await?;
    running.wait_leading().await?;
    f.append(Target::Job(stream::StreamId::new_v7()), 1).await?;
    common::wait_sequenced(&f.pool, 1).await?;

    let mut conn = f.db.connect().await?;
    pges_testing::create_store(
        &mut conn,
        &pges_core::store_name!("later"),
        &pges_testing::definition(pges_postgres::store::Outbox::Disabled),
    )
    .await?;
    let admin = admin::Admin::new(f.pool.clone(), pges_core::store_name!("later"));
    admin
        .register_category(
            <JobEvent as pges_core::event::Event>::CATEGORY,
            admin::CategoryConfig::default(),
        )
        .await?;
    admin.maintain().await?;
    let later = store::Store::open(
        f.pool.clone(),
        store::StoreConfig::new(pges_core::store_name!("later")),
    )
    .await?;
    later.register::<JobEvent>().await?;
    let id = later.store_id();
    for n in 0..3 {
        later
            .append(
                stream::StreamId::new_v7(),
                version::ExpectedVersion::Any,
                &[JobEvent::Noted { n }],
            )
            .await?;
    }

    let high = common::wait_high_water(&running.watch, id, 3).await?;
    assert_eq!(high.store(), pges_testing::position(3));
    let logged: i64 = sqlx::query_scalar("SELECT count(*) FROM later.log")
        .fetch_one(&f.pool)
        .await?;
    assert_eq!(logged, 3);
    assert_eq!(common::check_log(&f.pool).await?.len(), 1);

    running.stop().await?;
    f.finish().await
}

/// A batch that waits longer than `wal_sender_timeout` (here on a lock of the log) does not lose the
/// replication connection: the source sends status updates while the batch runs. Without them the server
/// ends the connection, which the first run shows.
///
/// Each step waits for the state it needs, not for a fixed time: the batch waiting on the lock, the
/// walsender gone, the sequencer's error recorded. The sequencer notices the lost connection only when it
/// next uses it, after its batch commits, so its log rows can be visible before the error is.
#[tokio::test]
async fn a_slow_batch_keeps_the_replication_connection() -> Result<()> {
    let f = Fixture::logical().await?;
    sqlx::query(sqlx::AssertSqlSafe(format!(
        r#"ALTER DATABASE "{}" SET wal_sender_timeout = '1s'"#,
        f.db.name()
    )))
    .execute(&f.pool)
    .await?;
    let slot = f.slot().await?;
    let mut count = 0;

    let silent = start_with_status_interval(&f, interval!(30 s)).await?;
    let walsender = active_pid(&f.pool, &slot).await?;
    let locker = block_a_batch(&f, &mut count).await?;
    wait_walsender_ended(&f.pool, &slot, walsender).await?;
    unblock(&f, locker, count).await?;
    wait_recovered(&silent).await?;
    let errors = silent.stop_after_errors().await?;
    assert!(
        errors.iter().all(|e| e.starts_with("source failed")),
        "the sequencer recovered from {errors:?}, not only from the lost replication connection"
    );

    let running = start_with_status_interval(&f, interval!(200 ms)).await?;
    let walsender = active_pid(&f.pool, &slot).await?;
    let locker = block_a_batch(&f, &mut count).await?;
    // Three times `wal_sender_timeout`, counted from when the batch is known to wait.
    tokio::time::sleep(Duration::from_secs(3)).await;
    assert_eq!(
        active_pid(&f.pool, &slot).await?,
        walsender,
        "the walsender changed while the batch waited"
    );
    unblock(&f, locker, count).await?;
    running.stop().await?;

    assert_eq!(common::check_log(&f.pool).await?.len(), count);
    f.finish().await
}

/// Starts a sequencer whose source sends status updates every `status_interval`, and waits until it leads.
async fn start_with_status_interval(
    f: &Fixture,
    status_interval: interval::Interval,
) -> Result<common::Running> {
    let mut config = common::logical_config();
    config.status_interval = status_interval;
    let pool = f.db.pool().await?;
    let source = logical::LogicalSource::new(
        pool.clone(),
        pges_replication::config::Config::from_url(f.db.url())?,
        config,
    );
    let mut sequencer = common::fast();
    sequencer.lease = sequencer::Lease::with_renew(interval!(30 s), interval!(10 s)).unwrap();
    let running = common::Running::start(pool, source, sequencer)?;
    running.wait_leading().await?;

    Ok(running)
}

/// Locks the log, appends one commit, and waits until the sequencer's batch for it waits on the lock. The
/// returned connection holds the lock until [`unblock`].
async fn block_a_batch(f: &Fixture, count: &mut usize) -> Result<sqlx::PgConnection> {
    let mut locker = f.db.connect().await?;
    locker
        .execute("BEGIN; LOCK TABLE es.log IN EXCLUSIVE MODE")
        .await?;
    f.append(Target::Job(stream::StreamId::new_v7()), 1).await?;
    *count += 1;

    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    loop {
        let waiting: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM pg_locks WHERE relation = 'es.log'::regclass AND NOT granted",
        )
        .fetch_one(&f.pool)
        .await?;
        if waiting > 0 {
            return Ok(locker);
        }
        if tokio::time::Instant::now() > deadline {
            bail!("no batch waits on the lock of the log");
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

async fn unblock(f: &Fixture, mut locker: sqlx::PgConnection, count: usize) -> Result<()> {
    locker.execute("COMMIT").await?;
    common::wait_sequenced(&f.pool, count).await
}

/// Waits until `walsender` no longer streams from the slot: the server ended its connection.
async fn wait_walsender_ended(pool: &PgPool, slot: &str, walsender: Option<i32>) -> Result<()> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    loop {
        let pid: Option<i32> =
            sqlx::query_scalar("SELECT active_pid FROM pg_replication_slots WHERE slot_name = $1")
                .bind(slot)
                .fetch_one(pool)
                .await?;
        if pid != walsender {
            return Ok(());
        }
        if tokio::time::Instant::now() > deadline {
            bail!("the server kept a silent connection open; wal_sender_timeout did not apply");
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// Waits until the sequencer reports an error it recovers from.
async fn wait_recovered(running: &common::Running) -> Result<()> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    while running.errors().is_empty() {
        if tokio::time::Instant::now() > deadline {
            bail!("the sequencer did not notice that it lost the replication connection");
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    Ok(())
}

async fn stored_token(pool: &PgPool) -> Result<Option<Vec<u8>>> {
    let (source, token): (Option<String>, Option<Vec<u8>>) =
        sqlx::query_as("SELECT source, source_token FROM pges.sequencer")
            .fetch_one(pool)
            .await?;
    if token.is_some() {
        assert_eq!(source.as_deref(), Some("logical"));
    }
    Ok(token)
}

async fn wait_for_token(pool: &PgPool, done: impl Fn(&[u8]) -> bool) -> Result<Vec<u8>> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    loop {
        if let Some(token) = stored_token(pool).await?
            && done(&token)
        {
            return Ok(token);
        }
        if tokio::time::Instant::now() > deadline {
            bail!("timed out waiting for the token");
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

async fn confirmed_flush(pool: &PgPool, slot: &str) -> Result<u64> {
    let confirmed: Option<i64> = sqlx::query_scalar(
        "SELECT (confirmed_flush_lsn - '0/0'::pg_lsn)::int8 FROM pg_replication_slots
         WHERE slot_name = $1",
    )
    .bind(slot)
    .fetch_one(pool)
    .await?;
    Ok(confirmed.unwrap_or(0) as u64)
}

async fn wal_insert_lsn(pool: &PgPool) -> Result<u64> {
    let lsn: i64 = sqlx::query_scalar("SELECT (pg_current_wal_insert_lsn() - '0/0'::pg_lsn)::int8")
        .fetch_one(pool)
        .await?;
    Ok(lsn as u64)
}

/// Flushed WAL the publication does not carry, so the server sends keepalives past it.
async fn unrelated_wal(f: &Fixture) -> Result<()> {
    let mut other = f.db.connect().await?;
    other
        .execute(
            "SET synchronous_commit = on;
             CREATE TABLE IF NOT EXISTS unrelated (n int8);
             INSERT INTO unrelated SELECT generate_series(1, 20);",
        )
        .await?;
    Ok(())
}

/// Waits until the slot's `confirmed_flush_lsn` reaches `lsn`.
async fn wait_confirmed(pool: &PgPool, slot: &str, lsn: u64) -> Result<()> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        let confirmed = confirmed_flush(pool, slot).await?;
        if confirmed >= lsn {
            return Ok(());
        }
        if tokio::time::Instant::now() > deadline {
            bail!("the slot is confirmed up to {confirmed:X}, not {lsn:X}");
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

async fn slot_count(f: &Fixture) -> Result<i64> {
    Ok(
        sqlx::query_scalar("SELECT count(*) FROM pg_replication_slots WHERE slot_name = $1")
            .bind(f.slot().await?)
            .fetch_one(&f.pool)
            .await?,
    )
}

/// Waits until no process streams from the slot. Without a close, the walsender would hold it until
/// `wal_sender_timeout` (60 s).
async fn wait_inactive(pool: &PgPool, slot: &str) -> Result<()> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let active: bool =
            sqlx::query_scalar("SELECT active FROM pg_replication_slots WHERE slot_name = $1")
                .bind(slot)
                .fetch_one(pool)
                .await?;
        if !active {
            return Ok(());
        }
        if tokio::time::Instant::now() > deadline {
            bail!("the slot is still active");
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

async fn active_pid(pool: &PgPool, slot: &str) -> Result<Option<i32>> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let pid: Option<i32> =
            sqlx::query_scalar("SELECT active_pid FROM pg_replication_slots WHERE slot_name = $1")
                .bind(slot)
                .fetch_optional(pool)
                .await?
                .flatten();
        if pid.is_some() || tokio::time::Instant::now() > deadline {
            return Ok(pid);
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}
