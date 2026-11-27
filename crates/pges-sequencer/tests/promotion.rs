//! A replication slot confirmed past the stored token: the commits in between have no log rows, and the
//! server would silently stream from the slot's position, so they would never be sequenced. It happens when
//! an asynchronous standby with a synchronised failover slot is promoted before it received the sequencer's
//! last batch. The sequencer halts instead (`SlotLost::Ahead`), and `resequence` sequences every commit.
//!
//! The state is built twice: directly, by advancing the slot past the token on the shared server
//! (`pg_replication_slot_advance`, which is what slot synchronisation does to the standby's copy), and by a
//! real promotion of a private replica pair (`replica::Pair::start_failover`).

#![cfg(feature = "source-logical")]
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

mod common;

use std::time::Duration;

use anyhow::{Context, Result, bail, ensure};
use sqlx::{PgPool, postgres::PgPoolOptions};

use pges_core::{lsn, stream};
use pges_postgres::limit;
use pges_replication::{config, connection};
use pges_sequencer::{logical, sequencer, source};
use pges_testing::replica;

use common::{Fixture, Kind, Target};

#[tokio::test]
async fn a_slot_confirmed_past_the_token_halts_until_resequenced() -> Result<()> {
    let f = Fixture::logical().await?;
    let slot = f.slot().await?;
    let job = stream::StreamId::new_v7();

    let running = f.start(common::fast()).await?;
    running.wait_leading().await?;
    f.append(Target::Job(job), 1).await?;
    common::wait_sequenced(&f.pool, 1).await?;
    running.stop().await?;
    let token = stored_token(&f.pool).await?.context("no token")?;

    // Committed while no sequencer runs; then the slot moves past it, as a synchronised slot does on a
    // standby that never received the batch that sequenced it.
    let skipped = f.append(Target::Job(job), 1).await?.commit_id;
    let confirmed = advance_slot(&f.pool, &slot).await?;
    assert!(confirmed > token);

    let watch = start_expecting_halt(&f).await?;
    let result = watch.result().await;
    assert_ahead(&result, &slot, token, confirmed);
    let halted: Option<String> = sqlx::query_scalar("SELECT halted FROM pges.sequencer")
        .fetch_one(&f.pool)
        .await?;
    assert!(halted.is_some_and(|reason| reason.contains("past the stored token")));
    assert_eq!(common::check_log(&f.pool).await?.len(), 1);

    let resequenced = f
        .logical_source(f.pool.clone())?
        .resequence(&f.resequence_config().await?)
        .await?;
    assert_eq!(resequenced.stores[0].commits, 1);
    let log = common::check_log(&f.pool).await?;
    assert_eq!(
        common::commit_order(&log).last(),
        Some(&skipped),
        "the skipped commit was not resequenced"
    );

    let running = f.start(common::fast()).await?;
    running.wait_leading().await?;
    f.append(Target::Job(job), 1).await?;
    common::wait_sequenced(&f.pool, 3).await?;
    running.stop().await?;
    common::check_every_commit_once(&f.pool).await?;

    f.finish().await
}

/// A slot the source creates has its consistent point stored as the token, so a later start can compare.
/// A slot without any token halts only when the stores hold commits: without commits nothing can be lost.
#[tokio::test]
async fn a_new_slot_stores_its_consistent_point_and_a_missing_token_halts_only_with_commits()
-> Result<()> {
    let f = Fixture::logical().await?;
    let slot = f.slot().await?;
    let mut bootstrap = f.logical_source(f.pool.clone())?;
    assert_eq!(bootstrap.bootstrap().await?, logical::Bootstrap::Created);
    let token = stored_token(&f.pool).await?.context("no token")?;
    assert_eq!(token, confirmed_flush(&f.pool, &slot).await?);

    // No token and no commit: the sequencer starts.
    clear_token(&f.pool).await?;
    let running = f.start(common::fast()).await?;
    running.wait_leading().await?;
    running.stop().await?;

    // No token, but a commit: it halts.
    clear_token(&f.pool).await?;
    f.append(Target::Job(stream::StreamId::new_v7()), 1).await?;
    let confirmed = confirmed_flush(&f.pool, &slot).await?;
    let result = start_expecting_halt(&f).await?.result().await;
    assert_ahead(&result, &slot, lsn::Lsn::ZERO, confirmed);

    f.finish().await
}

/// A real promotion. The replica synchronises the failover slot and has every commit, but its WAL stream is
/// cut before the sequencer's batch that sequences the last commit and stores its token. The primary then
/// confirms that commit to the slot, slot synchronisation copies the position to the replica (it has the
/// WAL up to it), and the replica is promoted: its slot is ahead of its token.
#[tokio::test]
async fn a_promoted_asynchronous_standby_halts_until_resequenced() -> Result<()> {
    let pair = replica::Pair::start_failover().await?;
    let result = promote(&pair).await;
    pair.stop().await?;

    result
}

async fn promote(pair: &replica::Pair) -> Result<()> {
    let name = format!(
        "t_promote_{}",
        &uuid::Uuid::now_v7().simple().to_string()[20..]
    );
    let db = pair.create_database(&name).await?;
    let f = Fixture::on(db, Kind::Logical).await?;
    let slot = f.slot().await?;
    let writer_roles = limit::WriterRoles::one(f.writer_role().await?);
    let job = stream::StreamId::new_v7();

    let mut bootstrap = failover_source(f.pool.clone(), f.db.url())?;
    assert_eq!(bootstrap.bootstrap().await?, logical::Bootstrap::Created);

    // Sequenced on the primary until the replica holds a synchronised copy of the slot at the token.
    let running = start(f.db.url(), common::fast()).await?;
    running.wait_leading().await?;
    let replica_url = pair.replica_url(&name)?;
    let replica_pool = pool(&replica_url).await?;
    let mut count = 0;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(90);
    loop {
        f.append(Target::Job(job), 1).await?;
        count += 1;
        common::wait_sequenced(&f.pool, count).await?;
        sqlx::query("SELECT pg_log_standby_snapshot()")
            .execute(&f.pool)
            .await?;
        tokio::time::sleep(Duration::from_millis(500)).await;
        let primary = confirmed_flush(&f.pool, &slot).await?;
        if synced_slot(&replica_pool, &slot).await? == Some(primary) {
            break;
        }
        if tokio::time::Instant::now() > deadline {
            bail!("the replica did not synchronise the slot");
        }
    }
    running.stop().await?;
    let token = stored_token(&f.pool).await?.context("no token")?;
    wait_synced(&replica_pool, &slot, confirmed_flush(&f.pool, &slot).await?).await?;

    // Committed on the primary and received by the replica; then the replica's stream is cut.
    let skipped = f.append(Target::Job(job), 1).await?.commit_id;
    count += 1;
    pair.caught_up().await?;
    pair.cut_off().await?;

    // The primary sequences it, stores the token and confirms it to the slot. No idle position goes out:
    // the slot stays at that commit, which the replica has, so it synchronises it.
    let mut quiet = common::logical_config();
    quiet.status_interval = pges_core::interval!(1 h);
    let running = start_with(f.db.url(), common::fast(), quiet).await?;
    running.wait_leading().await?;
    common::wait_sequenced(&f.pool, count).await?;
    let confirmed = wait_confirmed_past(&f.pool, &slot, token).await?;
    running.stop().await?;
    assert_eq!(stored_token(&f.pool).await?, Some(confirmed));
    wait_synced(&replica_pool, &slot, confirmed).await?;

    pair.promote().await?;
    let promoted = pool(&replica_url).await?;
    assert_eq!(stored_token(&promoted).await?, Some(token));
    assert_eq!(common::check_log(&promoted).await?.len(), count - 1);

    let result = start(&replica_url, common::fast()).await?.result().await;
    assert_ahead(&result, &slot, token, confirmed);

    let resequenced = failover_source(promoted.clone(), &replica_url)?
        .resequence(&logical::ResequenceConfig::new(writer_roles))
        .await?;
    assert_eq!(resequenced.stores[0].commits, 1);
    let log = common::check_log(&promoted).await?;
    assert_eq!(common::commit_order(&log).last(), Some(&skipped));

    let running = start(&replica_url, common::fast()).await?;
    running.wait_leading().await?;
    let writers = pges_postgres::store::Store::open(
        promoted.clone(),
        pges_postgres::store::StoreConfig::new(common::STORE_NAME),
    )
    .await?;
    writers
        .append(
            job,
            pges_core::version::ExpectedVersion::Any,
            &[common::JobEvent::Noted { n: 0 }],
        )
        .await?;
    common::wait_sequenced(&promoted, count + 1).await?;
    running.stop().await?;
    common::check_every_commit_once(&promoted).await?;

    promoted.close().await;
    replica_pool.close().await;
    f.pool.close().await;
    Ok(())
}

/// Starts a sequencer and expects it to halt: it is not checked for recoveries.
async fn start_expecting_halt(f: &Fixture) -> Result<common::Running> {
    let pool = f.db.pool().await?;
    let source = f.logical_source(pool.clone())?;
    common::Running::start(pool, source, common::fast())
}

fn assert_ahead(
    result: &Result<(), sequencer::RunError>,
    slot: &str,
    token: lsn::Lsn,
    confirmed: lsn::Lsn,
) {
    let Err(sequencer::RunError::Source(source::SourceError::Halt(error))) = result else {
        panic!("expected a halt, got {result:?}");
    };
    assert_eq!(
        error.downcast_ref::<logical::SlotLost>(),
        Some(&logical::SlotLost::Ahead {
            slot: slot.to_owned(),
            token,
            confirmed,
        })
    );
}

/// A logical source whose slot is a failover slot.
fn failover_source(pool: PgPool, url: &str) -> Result<logical::LogicalSource> {
    let mut config = common::logical_config();
    config.failover = logical::SlotFailover::Set(connection::Failover::Enabled);
    Ok(logical::LogicalSource::new(
        pool,
        config::Config::from_url(url)?,
        config,
    ))
}

async fn start(url: &str, config: sequencer::SequencerConfig) -> Result<common::Running> {
    start_with(url, config, common::logical_config()).await
}

async fn start_with(
    url: &str,
    config: sequencer::SequencerConfig,
    logical: logical::LogicalConfig,
) -> Result<common::Running> {
    let pool = pool(url).await?;
    let source = logical::LogicalSource::new(pool.clone(), config::Config::from_url(url)?, logical);
    common::Running::start(pool, source, config)
}

async fn pool(url: &str) -> Result<PgPool> {
    Ok(PgPoolOptions::new().max_connections(8).connect(url).await?)
}

async fn stored_token(pool: &PgPool) -> Result<Option<lsn::Lsn>> {
    let token: Option<Vec<u8>> = sqlx::query_scalar("SELECT source_token FROM pges.sequencer")
        .fetch_one(pool)
        .await?;
    Ok(token
        .filter(|token| !token.is_empty())
        .map(|token| lsn::Lsn::new(u64::from_be_bytes(token.try_into().unwrap()))))
}

async fn clear_token(pool: &PgPool) -> Result<()> {
    sqlx::query("UPDATE pges.sequencer SET source_token = NULL")
        .execute(pool)
        .await?;
    Ok(())
}

async fn confirmed_flush(pool: &PgPool, slot: &str) -> Result<lsn::Lsn> {
    let confirmed: Option<i64> = sqlx::query_scalar(
        "SELECT (confirmed_flush_lsn - '0/0'::pg_lsn)::int8 FROM pg_replication_slots
         WHERE slot_name = $1",
    )
    .bind(slot)
    .fetch_one(pool)
    .await?;
    Ok(lsn::Lsn::new(
        confirmed.context("no confirmed_flush_lsn")? as u64
    ))
}

/// Moves the slot to the current WAL position.
async fn advance_slot(pool: &PgPool, slot: &str) -> Result<lsn::Lsn> {
    sqlx::query("SELECT pg_replication_slot_advance($1, pg_current_wal_lsn())")
        .bind(slot)
        .execute(pool)
        .await?;
    confirmed_flush(pool, slot).await
}

async fn wait_confirmed_past(pool: &PgPool, slot: &str, token: lsn::Lsn) -> Result<lsn::Lsn> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    loop {
        let confirmed = confirmed_flush(pool, slot).await?;
        if confirmed > token {
            return Ok(confirmed);
        }
        if tokio::time::Instant::now() > deadline {
            bail!("the slot was not confirmed past {token}");
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// The replica's copy of the slot, once slot synchronisation made it persistent.
async fn synced_slot(replica: &PgPool, slot: &str) -> Result<Option<lsn::Lsn>> {
    let row: Option<(bool, bool, Option<i64>)> = sqlx::query_as(
        "SELECT synced, temporary, (confirmed_flush_lsn - '0/0'::pg_lsn)::int8
         FROM pg_replication_slots WHERE slot_name = $1",
    )
    .bind(slot)
    .fetch_optional(replica)
    .await?;
    Ok(match row {
        Some((true, false, Some(confirmed))) => Some(lsn::Lsn::new(confirmed as u64)),
        _ => None,
    })
}

/// Waits until the replica's copy of the slot is confirmed up to `confirmed`. Slot synchronisation naps up
/// to 30 s between rounds while nothing changes.
async fn wait_synced(replica: &PgPool, slot: &str, confirmed: lsn::Lsn) -> Result<()> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(90);
    loop {
        let synced = synced_slot(replica, slot).await?;
        if synced == Some(confirmed) {
            return Ok(());
        }
        ensure!(
            tokio::time::Instant::now() < deadline,
            "the replica's slot is at {synced:?}, not {confirmed}"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}
