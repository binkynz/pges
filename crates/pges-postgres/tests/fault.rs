//! Appends when the network fails or is slow, through `pges_testing::proxy`.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

mod common;

use std::time::Duration;

use anyhow::{Result, bail};
use sqlx::{Connection, PgConnection, PgPool};

use pges_core::{commit, stream, version};
use pges_postgres::{append, store, unit_of_work};
use pges_testing::proxy;

use common::{Fixture, JobEvent, v};

/// A store on a one-connection pool through the proxy, with `JobEvent` registered and the append statement
/// of every `streams` partition prepared on that connection, so a held response does not stop the next
/// append before it is sent (preparing one waits for the server's answer).
async fn proxied_store(f: &Fixture, proxy: &proxy::Proxy) -> Result<(PgPool, store::Store)> {
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .test_before_acquire(false)
        .connect(&proxy.url(&f.db)?)
        .await?;
    let s = store::Store::open(pool.clone(), store::StoreConfig::new(common::STORE_NAME)).await?;
    s.register::<JobEvent>().await?;
    let partitions: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM pg_inherits WHERE inhparent = 'es.streams'::regclass",
    )
    .fetch_one(&f.pool)
    .await?;
    loop {
        let written: i64 = sqlx::query_scalar("SELECT count(DISTINCT tableoid) FROM es.streams")
            .fetch_one(&f.pool)
            .await?;
        if written == partitions {
            break;
        }
        s.append(
            stream::StreamId::new_v7(),
            version::ExpectedVersion::NoStream,
            &[JobEvent::created("warm-up")],
        )
        .await?;
    }
    // sqlx pings a connection as it returns to the pool; that answer must not be held.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while pool.num_idle() == 0 {
        if tokio::time::Instant::now() > deadline {
            bail!("the warm-up connection never returned to the pool");
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }

    Ok((pool, s))
}

async fn commits_of(pool: &PgPool, id: stream::StreamId) -> Result<i64> {
    Ok(
        sqlx::query_scalar("SELECT count(*) FROM es.commits WHERE stream_id = $1")
            .bind(id.as_uuid())
            .fetch_one(pool)
            .await?,
    )
}

/// The append committed, but its response was lost with the connection. The crate retries on a new
/// connection with the same commit id and returns the original result as deduplicated; the stream has one
/// commit.
#[tokio::test]
async fn appends_whose_response_is_lost_are_retried_idempotently() -> Result<()> {
    let f = Fixture::new().await?;
    let proxy = proxy::Proxy::start(pges_testing::server().await?).await?;
    let (pool, s) = proxied_store(&f, &proxy).await?;
    let id = stream::StreamId::new_v7();

    proxy.hold(proxy::Direction::ToClient);
    let appending = {
        let s = s.clone();
        tokio::spawn(async move {
            s.append(
                id,
                version::ExpectedVersion::NoStream,
                &[JobEvent::created("a"), JobEvent::assigned()],
            )
            .await
        })
    };
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while commits_of(&f.pool, id).await? == 0 {
        if tokio::time::Instant::now() > deadline {
            bail!("the append never reached the server");
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    proxy.drop_connections();
    proxy.release();

    let result = appending.await??;
    assert_eq!(result.outcome, append::Outcome::Deduplicated);
    assert_eq!(result.versions, v(1)..=v(2));
    assert_eq!(commits_of(&f.pool, id).await?, 1);
    let next = s
        .append(id, result.expected(), &[JobEvent::assigned()])
        .await?;
    assert_eq!(next.versions, v(3)..=v(3));

    pool.close().await;
    f.finish().await
}

/// The connection was lost before the append reached the server. The retry on a new connection appends it.
#[tokio::test]
async fn appends_lost_before_the_server_are_retried() -> Result<()> {
    let f = Fixture::new().await?;
    let proxy = proxy::Proxy::start(pges_testing::server().await?).await?;
    let (pool, s) = proxied_store(&f, &proxy).await?;
    let id = stream::StreamId::new_v7();

    proxy.hold(proxy::Direction::ToServer);
    let appending = {
        let s = s.clone();
        tokio::spawn(async move {
            s.append(
                id,
                version::ExpectedVersion::NoStream,
                &[JobEvent::created("a")],
            )
            .await
        })
    };
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(commits_of(&f.pool, id).await?, 0);
    proxy.drop_connections();
    proxy.release();

    let result = appending.await??;
    assert_eq!(result.outcome, append::Outcome::Appended);
    assert_eq!(result.versions, v(1)..=v(1));
    assert_eq!(commits_of(&f.pool, id).await?, 1);

    pool.close().await;
    f.finish().await
}

/// An `Exact(1)` append runs while another transaction is creating the stream: its statement cannot see the
/// new row and writes nothing. By the time it classifies the outcome the creation has committed and the
/// stream matches the expectation, so the crate runs the statement once more and it succeeds.
#[tokio::test]
async fn exact_racing_the_stream_creation_is_retried_once() -> Result<()> {
    let f = Fixture::new().await?;
    let proxy = proxy::Proxy::start(pges_testing::server().await?).await?;
    f.store.register::<JobEvent>().await?;
    let mut racer = PgConnection::connect(&proxy.url(&f.db)?).await?;
    f.store
        .append_in(
            &mut racer,
            stream::StreamId::new_v7(),
            version::ExpectedVersion::NoStream,
            &[JobEvent::created("warm-up")],
        )
        .await?;
    let pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&mut racer)
        .await?;
    let activity = |pool: PgPool| async move {
        sqlx::query_as::<_, (String, f64)>(
            "SELECT state, extract(epoch FROM state_change)::float8 FROM pg_stat_activity WHERE pid = $1",
        )
        .bind(pid)
        .fetch_one(&pool)
        .await
    };
    let (_, idle_since) = activity(f.pool.clone()).await?;

    let id = stream::StreamId::new_v7();
    let mut creator = f.store.begin().await?;
    creator
        .append(
            id,
            version::ExpectedVersion::NoStream,
            &[JobEvent::created("a")],
        )
        .await?;

    proxy.hold(proxy::Direction::ToClient);
    let racing = {
        let s = f.store.clone();
        tokio::spawn(async move {
            let result = s
                .append_in(
                    &mut racer,
                    id,
                    version::ExpectedVersion::Exact(v(1)),
                    &[JobEvent::assigned()],
                )
                .await;
            (result, racer)
        })
    };
    // The racer's statement has run (and found no row) once its backend is idle again.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let (state, since) = activity(f.pool.clone()).await?;
        if state == "idle" && since > idle_since {
            break;
        }
        if tokio::time::Instant::now() > deadline {
            bail!("the racing append never ran");
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    creator.commit().await?;
    proxy.release();

    let (result, racer) = racing.await?;
    let result = result?;
    assert_eq!(result.outcome, append::Outcome::Appended);
    assert_eq!(result.versions, v(2)..=v(2));
    assert_eq!(commits_of(&f.pool, id).await?, 2);

    racer.close().await?;
    f.finish().await
}

/// A unit of work whose append aborted the transaction loses its connection before `commit`. The rollback
/// fails too, and `commit` still reports `Aborted`, the error that matters to the caller, not the rollback's.
#[tokio::test]
async fn a_failed_rollback_keeps_the_aborted_error() -> Result<()> {
    let f = Fixture::new().await?;
    let proxy = proxy::Proxy::start(pges_testing::server().await?).await?;
    let (pool, s) = proxied_store(&f, &proxy).await?;
    let commit_id = commit::CommitId::generate();

    let mut uow = s.begin().await?;
    uow.append(
        stream::StreamId::new_v7(),
        version::ExpectedVersion::NoStream,
        &[JobEvent::created("a")],
    )
    .commit_id(commit_id)
    .await?;
    let duplicate = uow
        .append(
            stream::StreamId::new_v7(),
            version::ExpectedVersion::NoStream,
            &[JobEvent::created("b")],
        )
        .commit_id(commit_id)
        .await;
    assert!(matches!(
        duplicate,
        Err(append::AppendError::DuplicateCommit(id)) if id == commit_id
    ));
    proxy.drop_connections();

    let committed = uow.commit().await;
    assert!(
        matches!(committed, Err(unit_of_work::TransactionError::Aborted)),
        "{committed:?}"
    );

    pool.close().await;
    f.finish().await
}

/// A crate-generated commit id fell behind the stream's last one, so the autocommit append ran again in a
/// transaction holding the stream's row lock, with a new id. That transaction's COMMIT succeeded, but its
/// reply was lost with the connection. The retry on a new connection classifies by the new id and returns
/// the original result as deduplicated; the stream has the commit once.
///
/// A deferred trigger makes the COMMIT wait for an advisory lock the test holds, so the reply is held at
/// the proxy exactly when the COMMIT runs.
#[tokio::test]
async fn a_lost_commit_of_the_locked_append_is_deduplicated() -> Result<()> {
    let f = Fixture::new().await?;
    let proxy = proxy::Proxy::start(pges_testing::server().await?).await?;
    let (pool, s) = proxied_store(&f, &proxy).await?;
    let id = stream::StreamId::new_v7();
    let ahead = commit_id_ahead(Duration::from_secs(2));
    f.store
        .append(
            id,
            version::ExpectedVersion::NoStream,
            &[JobEvent::created("a")],
        )
        .commit_id(ahead)
        .await?;

    let mut setup = f.db.connect().await?;
    sqlx::raw_sql(
        "CREATE FUNCTION hold_commit() RETURNS trigger LANGUAGE plpgsql AS $$
         BEGIN
           PERFORM pg_advisory_xact_lock(4242);
           RETURN NULL;
         END $$;
         CREATE CONSTRAINT TRIGGER hold_commit AFTER INSERT ON es.commits
           DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION hold_commit();",
    )
    .execute(&mut setup)
    .await?;
    let mut holder = f.db.connect().await?;
    sqlx::query("SELECT pg_advisory_lock(4242)")
        .execute(&mut holder)
        .await?;

    let appending = {
        let s = s.clone();
        tokio::spawn(async move {
            s.append(id, version::ExpectedVersion::Any, &[JobEvent::assigned()])
                .await
        })
    };
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let waiting: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM pg_stat_activity
             WHERE datname = current_database() AND query = 'COMMIT' AND wait_event = 'advisory'",
        )
        .fetch_one(&f.pool)
        .await?;
        if waiting == 1 {
            break;
        }
        if tokio::time::Instant::now() > deadline {
            bail!("the locked append never reached its COMMIT");
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    proxy.hold(proxy::Direction::ToClient);
    sqlx::query("SELECT pg_advisory_unlock(4242)")
        .execute(&mut holder)
        .await?;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while commits_of(&f.pool, id).await? < 2 {
        if tokio::time::Instant::now() > deadline {
            bail!("the locked append never committed");
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    proxy.drop_connections();
    proxy.release();

    let result = appending.await??;
    assert_eq!(result.outcome, append::Outcome::Deduplicated);
    assert_eq!(result.versions, v(2)..=v(2));
    assert!(result.commit_id > ahead);
    assert_eq!(commits_of(&f.pool, id).await?, 2);
    let stored: uuid::Uuid = sqlx::query_scalar(
        "SELECT commit_id FROM es.commits WHERE stream_id = $1 AND last_version = 2",
    )
    .bind(id.as_uuid())
    .fetch_one(&f.pool)
    .await?;
    assert_eq!(stored, result.commit_id.as_uuid());

    holder.close().await?;
    setup.close().await?;
    pool.close().await;
    f.finish().await
}

/// An `append_many` committed, but its reply was lost with the connection, and the retry on a new connection
/// came after the commit ids had left the clock skew tolerance (a failover takes longer than the default
/// 5 s). The retry still finds the batch written and returns it as deduplicated, as a single append does:
/// a commit id at or below its stream's last one is not checked against the clock.
#[tokio::test]
async fn a_batch_whose_reply_is_lost_is_deduplicated_after_the_skew() -> Result<()> {
    let f = Fixture::new().await?;
    let proxy = proxy::Proxy::start(pges_testing::server().await?).await?;
    let (pool, _) = proxied_store(&f, &proxy).await?;
    let skew = Duration::from_millis(200);
    let mut config = store::StoreConfig::new(common::STORE_NAME);
    config.max_clock_skew = Some(skew);
    let s = store::Store::open(pool.clone(), config).await?;
    // Prepares the batch statement on the proxied connection, so the held batch is sent whole.
    s.append_many([append::AppendEntry::new(
        stream::StreamId::new_v7(),
        version::ExpectedVersion::NoStream,
        &[JobEvent::created("warm-up")],
    )])
    .await?;
    let (first, second) = (stream::StreamId::new_v7(), stream::StreamId::new_v7());

    // A deferred trigger makes the COMMIT wait for an advisory lock the test holds, so the reply is held at
    // the proxy exactly when the COMMIT runs.
    let mut setup = f.db.connect().await?;
    sqlx::raw_sql(
        "CREATE FUNCTION hold_commit() RETURNS trigger LANGUAGE plpgsql AS $$
         BEGIN
           PERFORM pg_advisory_xact_lock(4242);
           RETURN NULL;
         END $$;
         CREATE CONSTRAINT TRIGGER hold_commit AFTER INSERT ON es.commits
           DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION hold_commit();",
    )
    .execute(&mut setup)
    .await?;
    let mut holder = f.db.connect().await?;
    sqlx::query("SELECT pg_advisory_lock(4242)")
        .execute(&mut holder)
        .await?;

    let appending = {
        let s = s.clone();
        tokio::spawn(async move {
            s.append_many([
                append::AppendEntry::new(
                    first,
                    version::ExpectedVersion::NoStream,
                    &[JobEvent::created("a")],
                ),
                append::AppendEntry::new(
                    second,
                    version::ExpectedVersion::NoStream,
                    &[JobEvent::created("b"), JobEvent::assigned()],
                ),
            ])
            .await
        })
    };
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let waiting: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM pg_stat_activity
             WHERE datname = current_database() AND query = 'COMMIT' AND wait_event = 'advisory'",
        )
        .fetch_one(&f.pool)
        .await?;
        if waiting == 1 {
            break;
        }
        if tokio::time::Instant::now() > deadline {
            bail!("the batch never reached its COMMIT");
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    proxy.hold(proxy::Direction::ToClient);
    sqlx::query("SELECT pg_advisory_unlock(4242)")
        .execute(&mut holder)
        .await?;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while commits_of(&f.pool, first).await? == 0 {
        if tokio::time::Instant::now() > deadline {
            bail!("the batch never committed");
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    tokio::time::sleep(skew * 3).await;
    proxy.drop_connections();
    proxy.release();

    let results = appending.await??;
    assert_eq!(results.len(), 2);
    assert!(
        results
            .iter()
            .all(|r| r.outcome == append::Outcome::Deduplicated),
        "{results:?}"
    );
    assert_eq!(results[1].versions, v(1)..=v(2));
    assert_eq!(commits_of(&f.pool, first).await?, 1);
    assert_eq!(commits_of(&f.pool, second).await?, 1);

    holder.close().await?;
    setup.close().await?;
    pool.close().await;
    f.finish().await
}

fn commit_id_ahead(offset: Duration) -> commit::CommitId {
    let at = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        + offset;
    let ts = uuid::Timestamp::from_unix(uuid::NoContext, at.as_secs(), at.subsec_nanos());

    commit::CommitId::from_uuid(uuid::Uuid::new_v7(ts)).unwrap()
}
