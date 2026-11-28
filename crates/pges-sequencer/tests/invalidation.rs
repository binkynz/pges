//! A live replication slot invalidated by the server (design.md, spike 7): in a private PostgreSQL 18
//! container with `max_slot_wal_keep_size = 1MB`, the slot falls behind while the sequencer streams and
//! writers keep appending, a checkpoint invalidates it (`wal_removed`) and ends the walsender, and the
//! sequencer halts instead of recreating it. `resequence`, run while the writers go on, recovers every commit
//! the slot never delivered; the restarted sequencer resumes from the new slot. Every commit then has
//! exactly one log row, positions are gap-free, and appends made after the resequence are sequenced after
//! the recovered ones, in commit order.
//!
//! The setting must not be used on the shared test server, where it would invalidate other tests' slots, so
//! this test starts its own container (`pges-test-slot-loss-<uuid>`) and removes it at the end. It skips
//! when Docker is not available.

#![cfg(feature = "source-logical")]
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

mod common;

use std::{collections::HashMap, process::Command, time::Duration};

use anyhow::{Context, Result, bail, ensure};
use sqlx::{Connection, Executor, PgConnection};
use testcontainers::{ContainerAsync, ImageExt, runners::AsyncRunner};
use testcontainers_modules::postgres::Postgres;
use tokio::{sync::watch, task::JoinHandle};

use pges_core::{commit, stream, version};
use pges_postgres::store;
use pges_sequencer::{logical, sequencer, source};

use common::{Fixture, JobEvent, Kind, OrderEvent};

const WRITERS: usize = 4;

const SERVER_ARGS: &[&str] = &[
    "-c",
    "wal_level=logical",
    "-c",
    "max_slot_wal_keep_size=1MB",
    "-c",
    "wal_keep_size=0",
    "-c",
    "fsync=off",
    "-c",
    "full_page_writes=off",
];

#[tokio::test]
async fn an_invalidated_slot_halts_sequencing_until_resequenced() -> Result<()> {
    if !docker_available() {
        eprintln!(
            "skipped: Docker is not available, so no server with max_slot_wal_keep_size can be started"
        );
        return Ok(());
    }

    let name = format!("pges-test-slot-loss-{}", uuid::Uuid::now_v7().simple());
    let container = Postgres::default()
        .with_tag("18")
        .with_cmd(SERVER_ARGS.iter().copied())
        .with_container_name(&name)
        .start()
        .await
        .with_context(|| format!("starting the container {name}"))?;
    let result = invalidate(&container).await;
    container.rm().await?;

    result
}

async fn invalidate(container: &ContainerAsync<Postgres>) -> Result<()> {
    let host = container.get_host().await?;
    let port = container.get_host_port_ipv4(5432).await?;
    let url = format!("postgres://postgres:postgres@{host}:{port}/postgres");
    let mut conn = connect(&url).await?;
    pges_postgres::migrate::run_shared(&mut conn).await?;
    let db = pges_testing::TestDb::existing("postgres", url);
    let f = Fixture::on(db, Kind::Logical).await?;
    let slot = f.slot().await?;

    // The token is never stored, so the slot is never confirmed past the first commit and falls behind.
    let mut config = common::fast();
    config.token_interval = Duration::from_secs(3600);
    let running = f.start(config).await?;
    running.wait_leading().await?;
    let (phase, phases) = watch::channel(Phase::BeforeResequence);
    let (stop, stopping) = watch::channel(());
    let writers: Vec<_> = (0..WRITERS)
        .map(|n| write(f.store.clone(), n, phases.clone(), stopping.clone()))
        .collect();
    wait_logged(&f.pool, 1).await?;

    conn.execute("CREATE TABLE filler (t text)").await?;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    loop {
        conn.execute(
            "INSERT INTO filler SELECT repeat('x', 1000) FROM generate_series(1, 2000);
             SELECT pg_switch_wal();
             CHECKPOINT;",
        )
        .await?;
        let reason: Option<String> = sqlx::query_scalar(
            "SELECT invalidation_reason FROM pg_replication_slots WHERE slot_name = $1",
        )
        .bind(&slot)
        .fetch_one(&mut conn)
        .await?;
        if reason.is_some() {
            break;
        }
        if tokio::time::Instant::now() > deadline {
            bail!("the slot was not invalidated");
        }
    }

    // The walsender is ended with the invalidation: the sequencer recovers from the lost stream (expected),
    // finds the slot invalidated when it reopens, records the halt and stops.
    let watch = running.watch.clone();
    let result = running.result().await;
    let Err(sequencer::RunError::Source(source::SourceError::Halt(error))) =
        result.as_ref().map_err(|error| &**error)
    else {
        panic!("expected a halt, got {result:?}");
    };
    assert_eq!(
        error.downcast_ref::<logical::SlotLost>(),
        Some(&logical::SlotLost::Invalidated {
            slot: slot.clone(),
            reason: "wal_removed".to_owned(),
        })
    );
    let status = watch.status().borrow().clone();
    let sequencer::Status::Halted(error) = &status else {
        panic!("the status is {status:?}, not Halted");
    };
    assert!(error.to_string().contains("wal_removed"), "{error}");
    let (halted, released): (Option<String>, bool) =
        sqlx::query_as("SELECT halted, lease_until IS NULL FROM pges.sequencer")
            .fetch_one(&mut conn)
            .await?;
    assert!(released);
    let halted = halted.context("halted is not set")?;
    assert!(
        halted.contains(&slot) && halted.contains("wal_removed"),
        "{halted}"
    );

    // Every later start refuses to run; nothing recreates the slot.
    let again = f.start(common::fast()).await?;
    let result = again.result().await;
    assert!(
        matches!(result.as_ref().map_err(|error| &**error), Err(sequencer::RunError::Halted(reason)) if *reason == halted),
        "{result:?}"
    );
    let invalidated: Option<String> = sqlx::query_scalar(
        "SELECT invalidation_reason FROM pg_replication_slots WHERE slot_name = $1",
    )
    .bind(&slot)
    .fetch_one(&mut conn)
    .await?;
    assert_eq!(invalidated.as_deref(), Some("wal_removed"));

    // Resequence while the writers go on: commits up to the new slot's consistent point get their log rows
    // now, later ones come from the slot.
    let sequenced_before: i64 = sqlx::query_scalar("SELECT count(*) FROM es.log")
        .fetch_one(&mut conn)
        .await?;
    let mut source = f.logical_source(f.pool.clone())?;
    let resequenced = source.resequence(&f.resequence_config().await?).await?;
    phase.send(Phase::AfterResequence)?;
    let recovered = resequenced.stores[0];
    assert!(recovered.commits > 0, "nothing was recovered");
    let (halted, token): (Option<String>, Option<Vec<u8>>) =
        sqlx::query_as("SELECT halted, source_token FROM pges.sequencer")
            .fetch_one(&mut conn)
            .await?;
    assert_eq!(halted, None);
    assert_eq!(
        token,
        Some(resequenced.consistent_point.get().to_be_bytes().to_vec())
    );

    let running = f.start(common::fast()).await?;
    running.wait_leading().await?;
    tokio::time::sleep(Duration::from_millis(500)).await;
    stop.send(())?;
    let mut appended = Vec::new();
    for writer in writers {
        appended.push(writer.await??);
    }
    let total = common::commit_count(&f.pool).await?;
    common::wait_sequenced(&f.pool, total).await?;
    running.stop().await?;

    let log = common::check_log(&f.pool).await?;
    common::check_every_commit_once(&f.pool).await?;
    assert_eq!(
        log.len(),
        appended.iter().map(Vec::len).sum::<usize>(),
        "the log does not hold every append"
    );
    let recovered_end = sequenced_before as usize + recovered.commits;
    let recovered_ids = common::commit_order(&log[sequenced_before as usize..recovered_end]);
    assert!(
        recovered_ids.is_sorted(),
        "recovered commits are not in commit id order"
    );
    assert_eq!(recovered_end as u64, recovered.last_position.get());

    // Appends made after the resequence are positioned after every recovered commit, and each writer's
    // appends (one after the other, so in commit order) in the order they were made.
    let positions: HashMap<commit::CommitId, i64> = log
        .iter()
        .map(|row| {
            (
                commit::CommitId::from_uuid(row.commit_id).unwrap(),
                row.last_position,
            )
        })
        .collect();
    let last_recovered = recovered.last_position.get() as i64;
    let mut after = 0;
    for commits in &appended {
        let mut previous = 0;
        for (commit_id, phase) in commits {
            let at = positions[commit_id];
            ensure!(at > previous, "a writer's commits are out of order");
            previous = at;
            if *phase == Phase::AfterResequence {
                ensure!(
                    at > last_recovered,
                    "commit {commit_id} appended after the resequence got position {at}, not after {last_recovered}"
                );
                after += 1;
            }
        }
    }
    ensure!(after > 0, "no append was made after the resequence");

    f.pool.close().await;
    conn.close().await?;
    Ok(())
}

/// Whether an append started before or after the resequence returned.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Phase {
    BeforeResequence,
    AfterResequence,
}

/// Appends one event at a time to a stream of its own until told to stop. Returns its commits in the order
/// they were made, with the phase each started in.
fn write(
    store: store::Store,
    n: usize,
    phase: watch::Receiver<Phase>,
    mut stop: watch::Receiver<()>,
) -> JoinHandle<Result<Vec<(commit::CommitId, Phase)>>> {
    tokio::spawn(async move {
        let stream = stream::StreamId::new_v7();
        let mut commits = Vec::new();
        loop {
            if stop.has_changed()? {
                return Ok(commits);
            }

            let started = *phase.borrow();
            let appended = if n.is_multiple_of(2) {
                store
                    .append(
                        stream,
                        version::ExpectedVersion::Any,
                        &[JobEvent::Noted { n: n as u32 }],
                    )
                    .await?
            } else {
                store
                    .append(
                        stream,
                        version::ExpectedVersion::Any,
                        &[OrderEvent::Noted { n: n as u32 }],
                    )
                    .await?
            };
            commits.push((appended.commit_id, started));
            tokio::select! {
                _ = stop.changed() => return Ok(commits),
                () = tokio::time::sleep(Duration::from_millis(5)) => {}
            }
        }
    })
}

/// Waits until the log holds at least `count` rows.
async fn wait_logged(pool: &sqlx::PgPool, count: usize) -> Result<()> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    loop {
        let logged: i64 = sqlx::query_scalar("SELECT count(*) FROM es.log")
            .fetch_one(pool)
            .await?;
        if logged as usize >= count {
            return Ok(());
        }
        if tokio::time::Instant::now() > deadline {
            bail!("timed out: {logged} of {count} commits logged");
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

/// Connects once the server accepts connections; the container may restart it once after initdb.
async fn connect(url: &str) -> Result<PgConnection> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        match PgConnection::connect(url).await {
            Ok(conn) => return Ok(conn),
            Err(error) if tokio::time::Instant::now() > deadline => {
                return Err(error).context("connecting to the container");
            }
            Err(_) => tokio::time::sleep(Duration::from_millis(200)).await,
        }
    }
}

fn docker_available() -> bool {
    Command::new("docker")
        .arg("info")
        .output()
        .is_ok_and(|output| output.status.success())
}
