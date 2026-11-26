//! `append_many` under concurrency: the SKIP LOCKED restart and lock order (`append/many_head.sql`), and the
//! partitions a batch locks in every planner regime (`append/begin_many.sql`, `append/plan_mode.sql`).

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

mod common;

use std::{collections::HashMap, sync::Arc, time::Duration};

use anyhow::{Result, bail};
use sqlx::{Connection, PgConnection, PgPool};
use uuid::Uuid;

use pges_core::{stream, version};
use pges_postgres::{append, store, unit_of_work};

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

/// The planner's view of `streams` when a batch runs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Regime {
    /// Never analyzed.
    Never,
    /// Analyzed with 3 streams in all.
    NearlyEmpty,
    /// The session sets `plan_cache_mode = force_generic_plan`.
    ForcedGeneric,
}

/// A store with 64 partitions whose `streams` hold `streams` streams, created by one batch, in `regime`.
async fn regime_store(
    regime: Regime,
    outbox: store::Outbox,
    routing: store::AppendRouting,
    streams: usize,
) -> Result<(Fixture, store::Store, Vec<stream::StreamId>)> {
    let mut definition = pges_testing::definition(outbox);
    definition.stream_partitions = std::num::NonZeroU16::new(64).unwrap();
    let f = Fixture::with_definition(definition).await?;
    let mut config = store::StoreConfig::new(common::STORE_NAME);
    config.append_routing = routing;
    let store = store::Store::open(f.pool.clone(), config).await?;
    let ids: Vec<stream::StreamId> = (0..streams).map(|_| stream::StreamId::new_v7()).collect();
    let events = [JobEvent::assigned()];
    store
        .append_many(
            ids.iter().map(|id| {
                append::AppendEntry::new(*id, version::ExpectedVersion::NoStream, &events)
            }),
        )
        .await?;
    if regime == Regime::NearlyEmpty {
        sqlx::query("ANALYZE es.streams").execute(&f.pool).await?;
    }

    Ok((f, store, ids))
}

/// Relation locks a batch takes on the partitions of `streams`, pinned per regime: 4 per partition it
/// touches (the row lock step and the update, each on the partition and its key), whatever the statistics
/// and the session's `plan_cache_mode`, with routing on or off and with the outbox. The batch statement goes
/// through `streams` and prunes its partitions only in a custom plan; inside a unit of work it sets
/// `force_custom_plan` for itself and puts the transaction's mode back. With the plan cache's own choice, a
/// store analyzed nearly empty locked all 64 partitions twice (128) and `force_generic_plan` 256.
#[tokio::test]
async fn a_batch_locks_the_streams_partitions_it_touches() -> Result<()> {
    use store::AppendRouting::{Auto, Streams};

    for (regime, outbox, routing, streams, batch) in [
        (
            Regime::Never,
            store::Outbox::Disabled,
            Auto,
            64_usize,
            1_usize,
        ),
        (Regime::Never, store::Outbox::Disabled, Auto, 64, 4),
        (Regime::Never, store::Outbox::Disabled, Auto, 64, 16),
        (Regime::NearlyEmpty, store::Outbox::Disabled, Auto, 3, 2),
        (Regime::NearlyEmpty, store::Outbox::Enabled, Auto, 3, 2),
        (Regime::NearlyEmpty, store::Outbox::Disabled, Streams, 3, 2),
        (Regime::ForcedGeneric, store::Outbox::Disabled, Auto, 64, 4),
        (Regime::ForcedGeneric, store::Outbox::Disabled, Auto, 64, 16),
        (Regime::ForcedGeneric, store::Outbox::Enabled, Auto, 64, 16),
        (
            Regime::ForcedGeneric,
            store::Outbox::Disabled,
            Streams,
            64,
            4,
        ),
    ] {
        let case = format!("{regime:?}, {outbox:?}, {routing:?}, batch of {batch}");
        let (f, store, ids) = regime_store(regime, outbox, routing, streams).await?;
        let events = [JobEvent::assigned()];
        let mut conn = f.pool.acquire().await?;
        if regime == Regime::ForcedGeneric {
            sqlx::query("SET plan_cache_mode = force_generic_plan")
                .execute(&mut *conn)
                .await?;
        }

        // Ten batches, past the sixth execution, from which the plan cache weighs a generic plan; the last is
        // counted.
        let mut counted = (0, 0);
        for round in 0..10 {
            let batch_ids: Vec<stream::StreamId> = (0..batch)
                .map(|i| ids[(round * batch + i) % ids.len()])
                .collect();
            let mut tx = conn.begin().await?;
            let mut work = unit_of_work::UnitOfWork::wrap(&store, &mut tx);
            work.append_many(
                batch_ids.iter().map(|id| {
                    append::AppendEntry::new(*id, version::ExpectedVersion::Any, &events)
                }),
            )
            .await?;
            work.finish().await?;
            let partition_locks: i64 = sqlx::query_scalar(
                "SELECT count(*) FROM pg_locks l JOIN pg_class c ON c.oid = l.relation
                 WHERE l.pid = pg_backend_pid() AND c.relnamespace = 'es'::regnamespace
                   AND c.relname ~ '^streams_p[0-9]'",
            )
            .fetch_one(&mut *tx)
            .await?;
            let touched: i64 = sqlx::query_scalar(
                "SELECT count(DISTINCT tableoid) FROM es.streams WHERE stream_id = ANY($1)",
            )
            .bind(batch_ids.iter().map(|id| id.as_uuid()).collect::<Vec<_>>())
            .fetch_one(&mut *tx)
            .await?;
            let mode: String = sqlx::query_scalar("SELECT current_setting('plan_cache_mode')")
                .fetch_one(&mut *tx)
                .await?;
            let expected_mode = match regime {
                Regime::ForcedGeneric => "force_generic_plan",
                _ => "auto",
            };
            assert_eq!(
                mode, expected_mode,
                "{case}: the transaction's plan mode is put back"
            );
            tx.rollback().await?;
            counted = (partition_locks, touched);
        }

        let (partition_locks, touched) = counted;
        assert_eq!(
            partition_locks,
            4 * touched,
            "{case}, {touched} partitions touched"
        );

        sqlx::query("RESET plan_cache_mode")
            .execute(&mut *conn)
            .await?;
        drop(conn);
        f.finish().await?;
    }

    Ok(())
}

/// A batch in its own transaction runs with custom plans whatever the statistics and the role's or
/// session's `plan_cache_mode`, so the batch statement prunes `streams` to the batch's partitions, and the
/// setting ends with the transaction. On a one-connection pool whose sessions force generic plans, and on a
/// store analyzed with 3 streams in all, ten batches plan the batch statement ten times as custom plans and
/// never as a generic one (`pg_prepared_statements`); the session keeps its own mode. With the plan cache's
/// own choice the nearly empty store switched to a generic plan after five custom ones.
#[tokio::test]
async fn a_batch_in_its_own_transaction_plans_for_its_streams() -> Result<()> {
    for (regime, streams, batch) in [(Regime::NearlyEmpty, 3, 2), (Regime::ForcedGeneric, 64, 4)] {
        let (f, _, ids) = regime_store(
            regime,
            store::Outbox::Disabled,
            store::AppendRouting::Auto,
            streams,
        )
        .await?;
        let forced = regime == Regime::ForcedGeneric;
        let pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(1)
            .after_connect(move |conn, _| {
                Box::pin(async move {
                    if forced {
                        sqlx::Executor::execute(conn, "SET plan_cache_mode = force_generic_plan")
                            .await?;
                    }
                    Ok(())
                })
            })
            .connect(f.db.url())
            .await?;
        let store =
            store::Store::open(pool.clone(), store::StoreConfig::new(common::STORE_NAME)).await?;
        let events = [JobEvent::assigned()];

        for round in 0..10 {
            store
                .append_many((0..batch).map(|i| {
                    append::AppendEntry::new(
                        ids[(round * batch + i) % ids.len()],
                        version::ExpectedVersion::Any,
                        &events,
                    )
                }))
                .await?;
        }

        let mut conn = pool.acquire().await?;
        let (generic, custom): (i64, i64) = sqlx::query_as(
            "SELECT generic_plans, custom_plans FROM pg_prepared_statements
             WHERE statement LIKE 'WITH locked AS MATERIALIZED%'",
        )
        .fetch_one(&mut *conn)
        .await?;
        let mode: String = sqlx::query_scalar("SELECT current_setting('plan_cache_mode')")
            .fetch_one(&mut *conn)
            .await?;
        assert_eq!((generic, custom), (0, 10), "{regime:?}");
        assert_eq!(
            mode,
            if forced { "force_generic_plan" } else { "auto" },
            "{regime:?}"
        );

        drop(conn);
        pool.close().await;
        f.finish().await?;
    }

    Ok(())
}
