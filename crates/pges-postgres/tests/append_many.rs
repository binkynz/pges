//! `append_many` under concurrency: the SKIP LOCKED restart and lock order (`append/many_head.sql`).

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

mod common;

use std::{collections::HashMap, sync::Arc, time::Duration};

use anyhow::{Result, bail};
use sqlx::{Connection, PgConnection, PgPool};
use uuid::Uuid;

use pges_core::{stream, version};
use pges_postgres::{append, store};

use common::{Fixture, JobEvent, v};

/// The backend running the batch statement, while it waits for a row lock: its pid and transaction start.
async fn batch_waiting(pool: &PgPool) -> Result<Option<(i32, f64)>> {
    Ok(sqlx::query_as(
        "SELECT pid, extract(epoch FROM xact_start)::float8 FROM pg_stat_activity
         WHERE datname = current_database() AND wait_event_type = 'Lock'
           AND query LIKE 'WITH locked AS MATERIALIZED%'",
    )
    .fetch_optional(pool)
    .await?)
}

async fn wait_for_batch(
    pool: &PgPool,
    until: impl Fn(i32, f64) -> bool,
    what: &str,
) -> Result<(i32, f64)> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        if let Some((pid, started)) = batch_waiting(pool).await?
            && until(pid, started)
        {
            return Ok((pid, started));
        }
        if tokio::time::Instant::now() > deadline {
            bail!("{what}");
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

async fn lock_row(conn: &mut PgConnection, id: stream::StreamId) -> Result<()> {
    sqlx::query("SELECT 1 FROM es.streams WHERE stream_id = $1 FOR UPDATE")
        .bind(id.as_uuid())
        .execute(&mut *conn)
        .await?;

    Ok(())
}

/// Two ids, the first sorting before the second.
fn ordered_ids() -> (stream::StreamId, stream::StreamId) {
    let a = stream::StreamId::new_v7();
    let b = stream::StreamId::new_v7();

    if a.as_uuid() < b.as_uuid() {
        (a, b)
    } else {
        (b, a)
    }
}

/// A batch over an existing stream `E` and a stream `R` it sees as new. While the batch waits for `E`'s
/// row lock, another append creates `R` and a third transaction locks it. The batch's insert of `R` finds
/// the committed row and writes nothing, so `R` must be retried; its row is locked by another transaction
/// and was not locked by the batch's `locked` step, so waiting for it could deadlock. The batch rolls back
/// and starts over in a new transaction instead, which locks `R` in stream id order (before `E`, so `E` is
/// free while it waits), and succeeds once `R` is released.
#[tokio::test]
async fn a_batch_whose_retry_is_locked_elsewhere_starts_over() -> Result<()> {
    let f = Fixture::new().await?;
    f.store.register::<JobEvent>().await?;
    let (r, e) = ordered_ids();
    f.store
        .append(
            e,
            version::ExpectedVersion::NoStream,
            &[JobEvent::created("e")],
        )
        .await?;

    let mut holder = f.db.connect().await?;
    let mut e_lock = holder.begin().await?;
    lock_row(&mut e_lock, e).await?;

    let batch = {
        let store = f.store.clone();
        tokio::spawn(async move {
            let jobs = [JobEvent::assigned()];
            store
                .append_many([
                    append::AppendEntry::new(e, version::ExpectedVersion::Any, &jobs),
                    append::AppendEntry::new(r, version::ExpectedVersion::Any, &jobs),
                ])
                .await
        })
    };
    let (pid, first_start) = wait_for_batch(
        &f.pool,
        |_, _| true,
        "the batch never waited for the lock of E",
    )
    .await?;

    // R is created after the batch's snapshot, then locked by another transaction.
    f.store
        .append(
            r,
            version::ExpectedVersion::NoStream,
            &[JobEvent::created("r")],
        )
        .await?;
    let mut other = f.db.connect().await?;
    let mut r_lock = other.begin().await?;
    lock_row(&mut r_lock, r).await?;
    e_lock.rollback().await?;

    wait_for_batch(
        &f.pool,
        |waiting, started| waiting == pid && started > first_start,
        "the batch never started over in a new transaction",
    )
    .await?;
    // The new transaction waits for R before locking E, so E is free: the first one was rolled back.
    let mut probe = holder.begin().await?;
    sqlx::query("SELECT 1 FROM es.streams WHERE stream_id = $1 FOR UPDATE NOWAIT")
        .bind(e.as_uuid())
        .execute(&mut *probe)
        .await?;
    probe.rollback().await?;
    assert!(!batch.is_finished());
    r_lock.rollback().await?;

    let results = batch.await??;
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].stream_id, e);
    assert_eq!(results[0].versions, v(2)..=v(2));
    assert_eq!(results[1].stream_id, r);
    assert_eq!(results[1].versions, v(2)..=v(2));
    assert!(
        results
            .iter()
            .all(|result| result.outcome == append::Outcome::Appended)
    );

    other.close().await?;
    holder.close().await?;
    f.finish().await
}

const WRITERS: usize = 16;
const ROUNDS: usize = 20;
const NEW_PER_ROUND: usize = 4;
const EXISTING: usize = 6;

/// Overlapping batches that create the same new streams, while other writers make some of those streams
/// exist for one batch's snapshot and not for another's. Each round starts
/// every writer at once on a fresh set of new streams: batches mix them with existing streams, in different
/// orders and subsets, on a normal pool and on one that forbids nested loops and merge joins; every fourth
/// writer creates the new streams with single appends instead. No append may fail, in particular with a
/// deadlock, and every stream ends with one version per append.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn batches_creating_the_same_streams_never_deadlock() -> Result<()> {
    let f = Fixture::new().await?;
    let hashed_pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(WRITERS as u32)
        .after_connect(|conn, _| {
            Box::pin(async move {
                sqlx::Executor::execute(
                    conn,
                    "SET enable_nestloop = off; SET enable_mergejoin = off",
                )
                .await?;
                Ok(())
            })
        })
        .connect(f.db.url())
        .await?;
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(WRITERS as u32)
        .connect(f.db.url())
        .await?;
    let normal =
        store::Store::open(pool.clone(), store::StoreConfig::new(common::STORE_NAME)).await?;
    let hashed = store::Store::open(
        hashed_pool.clone(),
        store::StoreConfig::new(common::STORE_NAME),
    )
    .await?;
    normal.register::<JobEvent>().await?;
    hashed.register::<JobEvent>().await?;

    let mut existing = Vec::new();
    for _ in 0..EXISTING {
        let id = stream::StreamId::new_v7();
        normal
            .append(
                id,
                version::ExpectedVersion::NoStream,
                &[JobEvent::created("e")],
            )
            .await?;
        existing.push(id);
    }
    let rounds: Vec<Vec<stream::StreamId>> = (0..ROUNDS)
        .map(|_| {
            (0..NEW_PER_ROUND)
                .map(|_| stream::StreamId::new_v7())
                .collect()
        })
        .collect();
    let existing = Arc::new(existing);
    let rounds = Arc::new(rounds);
    let barrier = Arc::new(tokio::sync::Barrier::new(WRITERS));

    let writers: Vec<_> = (0..WRITERS)
        .map(|writer| {
            let store = if writer % 2 == 0 {
                normal.clone()
            } else {
                hashed.clone()
            };
            let (existing, rounds, barrier) = (existing.clone(), rounds.clone(), barrier.clone());
            tokio::spawn(async move {
                let jobs = [JobEvent::assigned()];
                let mut appended: Vec<stream::StreamId> = Vec::new();
                // After a failure the writer keeps meeting the others at the barrier, so they finish.
                let mut failure: Option<append::AppendError> = None;
                for (round, new) in rounds.iter().enumerate() {
                    let mut ids: Vec<stream::StreamId> = new
                        .iter()
                        .copied()
                        .skip((writer + round) % 2)
                        .chain(
                            existing
                                .iter()
                                .copied()
                                .skip((writer + round) % 4)
                                .step_by(2),
                        )
                        .collect();
                    if (writer / 2 + round) % 2 == 1 {
                        ids.reverse();
                    } else {
                        let shift = writer % ids.len();
                        ids.rotate_left(shift);
                    }
                    barrier.wait().await;
                    if failure.is_some() {
                        continue;
                    }

                    if writer % 4 == 3 {
                        for id in new.iter().rev() {
                            match store
                                .append(*id, version::ExpectedVersion::Any, &jobs)
                                .await
                            {
                                Ok(_) => appended.push(*id),
                                Err(error) => {
                                    failure = Some(error);
                                    break;
                                }
                            }
                        }
                    } else {
                        let entries = ids.iter().map(|id| {
                            append::AppendEntry::new(*id, version::ExpectedVersion::Any, &jobs)
                        });
                        match store.append_many(entries).await {
                            Ok(_) => appended.extend(ids),
                            Err(error) => failure = Some(error),
                        }
                    }
                }

                match failure {
                    Some(error) => Err(error),
                    None => Ok(appended),
                }
            })
        })
        .collect();

    let mut expected: HashMap<Uuid, i64> = existing.iter().map(|id| (id.as_uuid(), 1)).collect();
    for writer in writers {
        match writer.await? {
            Ok(appended) => {
                for id in appended {
                    *expected.entry(id.as_uuid()).or_default() += 1;
                }
            }
            Err(error) => panic!("an append failed: {error}"),
        }
    }
    let versions: Vec<(Uuid, i64)> = sqlx::query_as("SELECT stream_id, version FROM es.streams")
        .fetch_all(&pool)
        .await?;
    assert_eq!(versions.len(), expected.len());
    for (id, version) in versions {
        assert_eq!(Some(&version), expected.get(&id), "stream {id}");
    }

    hashed_pool.close().await;
    pool.close().await;
    f.finish().await
}
