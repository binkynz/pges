//! `LogicalSource::resequence` on the shared test server: it refuses unless sequencing is halted and the
//! lease is free; it recovers the commits a dropped slot never delivered and those from before the first
//! slot; the sequencers then resume from the new slot. The invalidated slot under load is in
//! `tests/invalidation.rs`.

#![cfg(feature = "source-logical")]
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

mod common;

use std::time::Duration;

use anyhow::{Result, bail};
use sqlx::Executor;

use pges_core::{commit, interval, stream};
use pges_postgres::limit;
use pges_sequencer::{logical, sequencer, source};

use common::{Fixture, Target};

#[tokio::test]
async fn resequence_refuses_unless_halted_with_the_lease_free() -> Result<()> {
    let f = Fixture::logical().await?;
    let mut source = f.logical_source(f.pool.clone())?;

    // The writer roles must cap their transactions at the store's max_txn_age, checked before anything
    // else.
    let role: String = sqlx::query_scalar("SELECT current_user::text")
        .fetch_one(&f.pool)
        .await?;
    let result = source
        .resequence(&logical::ResequenceConfig::new(limit::WriterRoles::one(
            role.clone(),
        )))
        .await;
    assert!(
        matches!(
            result,
            Err(logical::ResequenceError::WriterRole(
                limit::WriterRoleError::TransactionTimeoutNotSet { .. }
            ))
        ),
        "{result:?}"
    );
    // A role whose transactions may outlast the store's max_txn_age.
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "ALTER ROLE CURRENT_USER IN DATABASE {} SET transaction_timeout = '2h'",
        f.db.name()
    )))
    .execute(&f.pool)
    .await?;
    let result = source
        .resequence(&logical::ResequenceConfig::new(limit::WriterRoles::one(
            role.clone(),
        )))
        .await;
    assert!(
        matches!(
            result,
            Err(logical::ResequenceError::WriterRole(
                limit::WriterRoleError::TransactionTimeoutTooLong { max_txn_age, transaction_timeout, .. }
            )) if max_txn_age == pges_core::store::DEFAULT_MAX_TXN_AGE
                && transaction_timeout == Duration::from_secs(7200)
        ),
        "{result:?}"
    );
    let config = f.resequence_config().await?;

    let result = source.resequence(&config).await;
    assert!(
        matches!(result, Err(logical::ResequenceError::NotHalted)),
        "{result:?}"
    );
    assert_eq!(slot_count(&f).await?, 0, "a refused resequence made a slot");

    let running = f.start(common::fast()).await?;
    running.wait_leading().await?;
    let job = stream::StreamId::new_v7();
    f.append(Target::Job(job), 1).await?;
    common::wait_sequenced(&f.pool, 1).await?;
    let result = source.resequence(&config).await;
    assert!(
        matches!(result, Err(logical::ResequenceError::NotHalted)),
        "{result:?}"
    );

    sqlx::query("UPDATE pges.sequencer SET halted = 'halted by the test'")
        .execute(&f.pool)
        .await?;
    let fence = fence(&f).await?;
    let result = source.resequence(&config).await;
    assert!(
        matches!(result, Err(logical::ResequenceError::LeaseHeld)),
        "{result:?}"
    );
    assert_eq!(
        fence,
        self::fence(&f).await?,
        "a refused resequence took the lease"
    );
    assert!(running.is_leading());
    assert!(
        slot_active(&f).await?,
        "a refused resequence touched the slot"
    );

    // Once the sequencer released the lease, the valid slot is replaced; nothing was lost, so nothing is
    // recovered.
    running.stop().await?;
    let resequenced = source.resequence(&config).await?;
    assert_eq!(resequenced.stores.len(), 1);
    assert_eq!(resequenced.stores[0].store, f.store_id);
    assert_eq!(resequenced.stores[0].commits, 0);
    assert_eq!(resequenced.stores[0].last_position.get(), 1);
    check_cleared(&f, &resequenced).await?;

    let running = f.start(common::fast()).await?;
    running.wait_leading().await?;
    f.append(Target::Job(job), 2).await?;
    common::wait_sequenced(&f.pool, 2).await?;
    running.stop().await?;
    common::check_log(&f.pool).await?;
    common::check_every_commit_once(&f.pool).await?;

    f.finish().await
}

/// The slot is dropped while no sequencer runs; commits keep arriving. The next sequencer halts, and an
/// interrupted resequence leaves a slot behind. Resequence recovers every commit without a log row in commit
/// id order, refuses once done, and the sequencer resumes from the new slot.
#[tokio::test]
async fn resequence_recovers_the_commits_of_a_dropped_slot() -> Result<()> {
    let f = Fixture::logical().await?;
    let jobs: Vec<_> = (0..3).map(|_| stream::StreamId::new_v7()).collect();
    let order = stream::StreamId::new_v7();

    let running = f.start(common::fast()).await?;
    running.wait_leading().await?;
    for job in &jobs {
        f.append(Target::Job(*job), 1).await?;
    }
    common::wait_sequenced(&f.pool, 3).await?;
    running.stop().await?;
    f.drop_slot().await?;

    let mut orphans = Vec::new();
    for job in &jobs {
        orphans.push(f.append(Target::Job(*job), 2).await?.commit_id);
    }
    orphans.push(f.append(Target::Order(order), 1).await?.commit_id);

    let halting = f.start(common::fast()).await?;
    let result = halting.result().await;
    let Err(sequencer::RunError::Source(source::SourceError::Halt(error))) = &result else {
        panic!("expected a halt, got {result:?}");
    };
    assert!(
        matches!(
            error.downcast_ref::<logical::SlotLost>(),
            Some(logical::SlotLost::Missing { .. })
        ),
        "{error}"
    );
    orphans.push(f.append(Target::Job(jobs[0]), 1).await?.commit_id);

    // A resequence that died after creating its slot leaves the slot, with halted still set. The commits
    // that slot would deliver are recovered too.
    sqlx::query("SELECT pg_create_logical_replication_slot($1, 'pgoutput')")
        .bind(f.slot().await?)
        .execute(&f.pool)
        .await?;
    orphans.push(f.append(Target::Order(order), 3).await?.commit_id);

    let mut source = f.logical_source(f.pool.clone())?;
    let config = f.resequence_config().await?;
    let resequenced = source.resequence(&config).await?;
    assert_eq!(resequenced.stores.len(), 1);
    assert_eq!(resequenced.stores[0].commits, orphans.len());
    check_cleared(&f, &resequenced).await?;
    let again = source.resequence(&config).await;
    assert!(
        matches!(again, Err(logical::ResequenceError::NotHalted)),
        "{again:?}"
    );

    let log = common::check_log(&f.pool).await?;
    let recovered = common::commit_order(&log[3..]);
    orphans.sort();
    assert_eq!(
        recovered, orphans,
        "recovered commits are not in commit id order"
    );
    assert_eq!(
        resequenced.stores[0].last_position.get(),
        log.last().unwrap().last_position as u64
    );

    let running = f.start(common::fast()).await?;
    running.wait_leading().await?;
    let mut after = Vec::new();
    for (n, job) in jobs.iter().enumerate() {
        after.push(f.append(Target::Job(*job), 1).await?.commit_id);
        if n == 0 {
            after.push(f.append(Target::Order(order), 1).await?.commit_id);
        }
    }
    let total = common::commit_count(&f.pool).await?;
    common::wait_sequenced(&f.pool, total).await?;
    running.stop().await?;

    let log = common::check_log(&f.pool).await?;
    common::check_every_commit_once(&f.pool).await?;
    assert_eq!(common::commit_order(&log[3 + orphans.len()..]), after);

    f.finish().await
}

/// Commits made before the first slot halt the first sequencer (`CommitsBeforeSlot`); resequence
/// sequences them and creates the slot.
#[tokio::test]
async fn resequence_sequences_commits_from_before_the_first_slot() -> Result<()> {
    let f = Fixture::logical().await?;
    let job = stream::StreamId::new_v7();
    let before = vec![
        f.append(Target::Job(job), 2).await?.commit_id,
        f.append(Target::Order(stream::StreamId::new_v7()), 1)
            .await?
            .commit_id,
    ];

    let halting = f.start(common::fast()).await?;
    let result = halting.result().await;
    let Err(sequencer::RunError::Source(source::SourceError::Halt(error))) = &result else {
        panic!("expected a halt, got {result:?}");
    };
    assert!(
        matches!(
            error.downcast_ref::<logical::SlotLost>(),
            Some(logical::SlotLost::CommitsBeforeSlot { .. })
        ),
        "{error}"
    );
    assert_eq!(slot_count(&f).await?, 0);

    let mut source = f.logical_source(f.pool.clone())?;
    let resequenced = source.resequence(&f.resequence_config().await?).await?;
    assert_eq!(resequenced.stores[0].commits, 2);
    assert_eq!(resequenced.stores[0].last_position.get(), 3);
    check_cleared(&f, &resequenced).await?;

    let running = f.start(common::fast()).await?;
    running.wait_leading().await?;
    let after = f.append(Target::Job(job), 1).await?.commit_id;
    common::wait_sequenced(&f.pool, 3).await?;
    running.stop().await?;

    let log = common::check_log(&f.pool).await?;
    let mut expected: Vec<commit::CommitId> = before;
    expected.sort();
    expected.push(after);
    assert_eq!(common::commit_order(&log), expected);
    common::check_every_commit_once(&f.pool).await?;

    f.finish().await
}

/// Commits made before the first slot fail the bootstrap (`CommitsBeforeSlot`), which records the halt, so
/// resequence recovers before any sequencer has started.
#[tokio::test]
async fn resequence_recovers_after_a_bootstrap_found_commits_before_the_slot() -> Result<()> {
    let f = Fixture::logical().await?;
    let job = stream::StreamId::new_v7();
    let mut before = vec![
        f.append(Target::Job(job), 2).await?.commit_id,
        f.append(Target::Order(stream::StreamId::new_v7()), 1)
            .await?
            .commit_id,
    ];

    let mut source = f.logical_source(f.pool.clone())?;
    let result = source.bootstrap().await;
    let Err(source::SourceError::Halt(error)) = &result else {
        panic!("expected a halt, got {result:?}");
    };
    assert!(
        matches!(
            error.downcast_ref::<logical::SlotLost>(),
            Some(logical::SlotLost::CommitsBeforeSlot { .. })
        ),
        "{error}"
    );
    assert_eq!(slot_count(&f).await?, 0);

    let resequenced = source.resequence(&f.resequence_config().await?).await?;
    assert_eq!(resequenced.stores[0].commits, 2);
    assert_eq!(resequenced.stores[0].last_position.get(), 3);
    check_cleared(&f, &resequenced).await?;
    assert_eq!(source.bootstrap().await?, logical::Bootstrap::Exists);

    let running = f.start(common::fast()).await?;
    running.wait_leading().await?;
    let after = f.append(Target::Job(job), 1).await?.commit_id;
    common::wait_sequenced(&f.pool, 3).await?;
    running.stop().await?;

    let log = common::check_log(&f.pool).await?;
    before.sort();
    before.push(after);
    assert_eq!(common::commit_order(&log), before);
    common::check_every_commit_once(&f.pool).await?;

    f.finish().await
}

/// A process that is not a sequencer streams from the slot: resequence refuses with `SlotInUse` and leaves
/// the slot, `halted` and the log as they were, with the lease released. Once the stream ends it recovers.
#[tokio::test]
async fn resequence_refuses_while_another_process_streams_from_the_slot() -> Result<()> {
    let f = Fixture::logical().await?;
    let running = f.start(common::fast()).await?;
    running.wait_leading().await?;
    let job = stream::StreamId::new_v7();
    f.append(Target::Job(job), 1).await?;
    common::wait_sequenced(&f.pool, 1).await?;
    running.stop().await?;
    halt(&f).await?;

    let slot = f.slot().await?;
    let replication = pges_replication::config::Config::from_url(f.db.url())?;
    let stream = pges_replication::connection::ReplicationConnection::connect(&replication)
        .await?
        .start(&pges_replication::connection::StartOptions::new(
            pges_replication::name::SlotName::new(slot.clone())?,
            [pges_replication::name::PublicationName::new("pges")?],
        ))
        .await?;
    assert!(slot_active(&f).await?);
    let pid = active_pid(&f).await?;

    let mut source = f.logical_source(f.pool.clone())?;
    let config = f.resequence_config().await?;
    let result = source.resequence(&config).await;
    assert!(
        matches!(&result, Err(logical::ResequenceError::SlotInUse { slot: used }) if *used == slot),
        "{result:?}"
    );
    assert_eq!(active_pid(&f).await?, pid, "the slot was touched");
    check_untouched(&f, 1).await?;

    stream.stop().await?.close().await?;
    wait_inactive(&f).await?;
    let resequenced = source.resequence(&config).await?;
    assert_eq!(resequenced.stores[0].commits, 0);
    check_cleared(&f, &resequenced).await?;
    common::check_every_commit_once(&f.pool).await?;

    f.finish().await
}

/// Another process takes the lease over while resequence waits for the new slot (an open transaction
/// holds up its creation): resequence stops with `LeaseLost`, writes nothing and leaves the new owner's
/// lease alone. Run again, it drops the slot the first run left behind and recovers the orphan.
#[tokio::test]
async fn resequence_that_loses_its_lease_writes_nothing() -> Result<()> {
    let f = Fixture::logical().await?;
    let running = f.start(common::fast()).await?;
    running.wait_leading().await?;
    let job = stream::StreamId::new_v7();
    f.append(Target::Job(job), 1).await?;
    common::wait_sequenced(&f.pool, 1).await?;
    running.stop().await?;
    f.drop_slot().await?;
    let orphan = f.append(Target::Job(job), 2).await?.commit_id;
    halt(&f).await?;

    let mut blocker = f.db.connect().await?;
    blocker.execute("BEGIN").await?;
    blocker.execute("SELECT pg_current_xact_id()").await?;

    let mut source = f.logical_source(f.pool.clone())?;
    let mut config = f.resequence_config().await?;
    config.lease = sequencer::Lease::new(interval!(400 ms));
    let resequencing = tokio::spawn(async move {
        let result = source.resequence(&config).await;
        (source, config, result)
    });
    wait_for_slot_creation(&f).await?;
    let taken: i64 = sqlx::query_scalar(
        "UPDATE pges.sequencer SET fence = fence + 1, lease_until = now() + interval '1 hour'
         RETURNING fence",
    )
    .fetch_one(&f.pool)
    .await?;

    let (mut source, mut config, result) =
        tokio::time::timeout(Duration::from_secs(10), resequencing).await??;
    assert!(
        matches!(result, Err(logical::ResequenceError::LeaseLost)),
        "{result:?}"
    );
    blocker.execute("ROLLBACK").await?;
    let (fence, held): (i64, bool) =
        sqlx::query_as("SELECT fence, lease_until IS NOT NULL FROM pges.sequencer")
            .fetch_one(&f.pool)
            .await?;
    assert_eq!(
        (fence, held),
        (taken, true),
        "the new owner's lease changed"
    );
    let (logged, halted): (i64, Option<String>) =
        sqlx::query_as("SELECT (SELECT count(*) FROM es.log), halted FROM pges.sequencer")
            .fetch_one(&f.pool)
            .await?;
    assert_eq!(logged, 1, "a resequence that lost its lease wrote log rows");
    assert!(halted.is_some(), "halted was cleared");

    // The new owner gives the lease up; the next resequence replaces whatever slot the first one left.
    sqlx::query("UPDATE pges.sequencer SET lease_until = NULL")
        .execute(&f.pool)
        .await?;
    wait_inactive(&f).await?;
    config.lease = sequencer::Lease::DEFAULT;
    let resequenced = source.resequence(&config).await?;
    assert_eq!(resequenced.stores[0].commits, 1);
    check_cleared(&f, &resequenced).await?;
    let log = common::check_log(&f.pool).await?;
    assert_eq!(common::commit_order(&log[1..]), vec![orphan]);
    common::check_every_commit_once(&f.pool).await?;

    f.finish().await
}

/// An orphan that would take positions past `i64::MAX` stops resequence with `PositionOverflow`: nothing is
/// written, `halted` stays, the lease is released and the new slot is dropped again.
#[tokio::test]
async fn resequence_refuses_positions_past_the_end() -> Result<()> {
    let f = Fixture::logical().await?;
    let running = f.start(common::fast()).await?;
    running.wait_leading().await?;
    f.append(Target::Job(stream::StreamId::new_v7()), 1).await?;
    common::wait_sequenced(&f.pool, 1).await?;
    running.stop().await?;
    common::move_log_to_the_last_positions(&f.pool).await?;
    f.drop_slot().await?;
    f.append(Target::Order(stream::StreamId::new_v7()), 2)
        .await?;
    halt(&f).await?;

    let mut source = f.logical_source(f.pool.clone())?;
    let result = source.resequence(&f.resequence_config().await?).await;
    assert!(
        matches!(result, Err(logical::ResequenceError::PositionOverflow(store)) if store == f.store_id),
        "{result:?}"
    );
    check_untouched(&f, 1).await?;
    let last: i64 = sqlx::query_scalar("SELECT max(last_position) FROM es.log")
        .fetch_one(&f.pool)
        .await?;
    assert_eq!(last, i64::MAX - 1);
    assert_eq!(slot_count(&f).await?, 0, "the new slot was kept");

    f.finish().await
}

/// Sets `halted` by hand, as a halting sequencer would.
async fn halt(f: &Fixture) -> Result<()> {
    sqlx::query("UPDATE pges.sequencer SET halted = 'halted by the test'")
        .execute(&f.pool)
        .await?;
    Ok(())
}

/// After a refused resequence: `halted` is still set, the lease is free, and the log has `logged` rows.
async fn check_untouched(f: &Fixture, logged: i64) -> Result<()> {
    let (rows, halted, released): (i64, Option<String>, bool) = sqlx::query_as(
        "SELECT (SELECT count(*) FROM es.log), halted, lease_until IS NULL FROM pges.sequencer",
    )
    .fetch_one(&f.pool)
    .await?;
    assert_eq!(rows, logged, "the log changed");
    assert_eq!(halted.as_deref(), Some("halted by the test"));
    assert!(released, "the lease was kept");
    Ok(())
}

/// Waits until a walsender of this database waits for a transaction lock: `CREATE_REPLICATION_SLOT` waits
/// for every transaction that was open when it started.
async fn wait_for_slot_creation(f: &Fixture) -> Result<()> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let waiting: bool = sqlx::query_scalar(
            "SELECT EXISTS (SELECT 1 FROM pg_stat_activity
                            WHERE backend_type = 'walsender' AND datname = current_database()
                              AND wait_event_type = 'Lock')",
        )
        .fetch_one(&f.pool)
        .await?;
        if waiting {
            return Ok(());
        }
        if tokio::time::Instant::now() > deadline {
            bail!("resequence never waited to create its slot");
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

/// The pid streaming from the slot, if any.
async fn active_pid(f: &Fixture) -> Result<Option<i32>> {
    Ok(
        sqlx::query_scalar("SELECT active_pid FROM pg_replication_slots WHERE slot_name = $1")
            .bind(f.slot().await?)
            .fetch_optional(&f.pool)
            .await?
            .flatten(),
    )
}

/// Waits until no process streams from the slot, or it is gone.
async fn wait_inactive(f: &Fixture) -> Result<()> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while active_pid(f).await?.is_some() {
        if tokio::time::Instant::now() > deadline {
            bail!("the slot stays active");
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    Ok(())
}

/// After a resequence: `halted` is clear, the lease free, the token the new slot's consistent point, and the
/// slot exists and is valid.
async fn check_cleared(f: &Fixture, resequenced: &logical::Resequenced) -> Result<()> {
    let (halted, released, source, token): (Option<String>, bool, Option<String>, Option<Vec<u8>>) =
        sqlx::query_as(
            "SELECT halted, lease_until IS NULL, source, source_token FROM pges.sequencer",
        )
        .fetch_one(&f.pool)
        .await?;
    assert_eq!(halted, None);
    assert!(released, "resequence kept the lease");
    assert_eq!(source.as_deref(), Some("logical"));
    assert_eq!(
        token,
        Some(resequenced.consistent_point.get().to_be_bytes().to_vec())
    );
    assert_eq!(resequenced.slot.as_str(), f.slot().await?);
    let invalidated: Option<String> = sqlx::query_scalar(
        "SELECT invalidation_reason FROM pg_replication_slots WHERE slot_name = $1",
    )
    .bind(resequenced.slot.as_str())
    .fetch_one(&f.pool)
    .await?;
    assert_eq!(invalidated, None);
    Ok(())
}

async fn fence(f: &Fixture) -> Result<i64> {
    Ok(sqlx::query_scalar("SELECT fence FROM pges.sequencer")
        .fetch_one(&f.pool)
        .await?)
}

async fn slot_count(f: &Fixture) -> Result<i64> {
    Ok(
        sqlx::query_scalar("SELECT count(*) FROM pg_replication_slots WHERE slot_name = $1")
            .bind(f.slot().await?)
            .fetch_one(&f.pool)
            .await?,
    )
}

/// Whether a process streams from the slot, waiting a little for a sequencer that is starting.
async fn slot_active(f: &Fixture) -> Result<bool> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let active: bool =
            sqlx::query_scalar("SELECT active FROM pg_replication_slots WHERE slot_name = $1")
                .bind(f.slot().await?)
                .fetch_one(&f.pool)
                .await?;
        if active {
            return Ok(true);
        }
        if tokio::time::Instant::now() > deadline {
            bail!("the slot is not active");
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}
