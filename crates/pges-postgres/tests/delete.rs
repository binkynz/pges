//! `Store::delete_stream`: the tombstone commit, the deleted state, and what is removed.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

mod common;

use anyhow::Result;
use futures_util::TryStreamExt;
use uuid::Uuid;

use pges_core::{stream, version};
use pges_postgres::{append, delete, load, read, store};

use common::{Fixture, Job, JobEvent, OrderEvent, Placed, v};

async fn job_with_snapshot(
    f: &Fixture,
    key: &str,
) -> Result<(stream::StreamId, append::AppendResult)> {
    let stream = stream::StreamId::from_key(common::JOBS, key);
    let id = stream.id;
    f.store
        .append(
            stream,
            version::ExpectedVersion::NoStream,
            &[JobEvent::created("a")],
        )
        .await?;
    let second = f
        .store
        .append(
            id,
            version::ExpectedVersion::Exact(v(1)),
            &[JobEvent::assigned()],
        )
        .await?;
    let loaded = f.store.load_snapshotted::<Job>(id).await?;
    f.store.save_snapshot(&loaded).await?;

    Ok((id, second))
}

async fn rows(f: &Fixture, id: stream::StreamId) -> Result<(i64, i64, Option<String>, i16)> {
    let commits: i64 = sqlx::query_scalar("SELECT count(*) FROM es.commits WHERE stream_id = $1")
        .bind(id.as_uuid())
        .fetch_one(&f.pool)
        .await?;
    let snapshots: i64 =
        sqlx::query_scalar("SELECT count(*) FROM es.snapshots WHERE stream_id = $1")
            .bind(id.as_uuid())
            .fetch_one(&f.pool)
            .await?;
    let (key, state): (Option<String>, i16) =
        sqlx::query_as("SELECT stream_key, state FROM es.streams WHERE stream_id = $1")
            .bind(id.as_uuid())
            .fetch_one(&f.pool)
            .await?;

    Ok((commits, snapshots, key, state))
}

#[tokio::test]
async fn a_deleted_stream_keeps_only_its_tombstone() -> Result<()> {
    let f = Fixture::new().await?;
    let (id, second) = job_with_snapshot(&f, "job-1").await?;
    assert_eq!(rows(&f, id).await?, (2, 1, Some("job-1".into()), 0));

    // The expected version guards the deletion; a conflict changes nothing.
    let error = f
        .store
        .delete_stream::<JobEvent>(id, v(1))
        .await
        .unwrap_err();
    assert!(
        matches!(
            error,
            delete::DeleteStreamError::Conflict { expected, actual, .. }
                if expected == delete::DeleteExpected::Exact(v(1)) && actual == Some(v(2))
        ),
        "{error:?}"
    );
    let error = f
        .store
        .delete_stream::<OrderEvent>(id, delete::DeleteExpected::Any)
        .await
        .unwrap_err();
    assert!(
        matches!(error, delete::DeleteStreamError::CategoryMismatch { .. }),
        "{error:?}"
    );
    assert_eq!(rows(&f, id).await?, (2, 1, Some("job-1".into()), 0));
    let removed: Vec<(i32, Uuid)> = sqlx::query_as(
        "SELECT category_id, commit_id FROM es.commits WHERE stream_id = $1 ORDER BY commit_id",
    )
    .bind(id.as_uuid())
    .fetch_all(&f.pool)
    .await?;

    let tombstone = f
        .store
        .delete_stream::<JobEvent>(id, second.last_version())
        .await?;
    assert_eq!(tombstone.versions, v(3)..=v(3));
    assert_eq!(tombstone.outcome, append::Outcome::Appended);
    assert_eq!(rows(&f, id).await?, (1, 0, None, 2));
    // Their log rows stay, so readers of the whole log need to know these commits were deleted.
    let recorded: Vec<(i32, Uuid)> =
        sqlx::query_as("SELECT category_id, commit_id FROM es.deleted_commits ORDER BY commit_id")
            .fetch_all(&f.pool)
            .await?;
    assert_eq!(recorded, removed);
    let (commit_id, type_name): (Uuid, String) = sqlx::query_as(
        "SELECT c.commit_id, t.name FROM es.commits c
         JOIN es.event_types t ON t.category_id = c.category_id AND t.name LIKE '$%'
         WHERE c.stream_id = $1",
    )
    .bind(id.as_uuid())
    .fetch_one(&f.pool)
    .await?;
    assert_eq!(commit_id, tombstone.commit_id.as_uuid());
    assert_eq!(type_name, "$stream-deleted");

    // Appends, reads, loads and another deletion all see the deleted state.
    let error = f
        .store
        .append(id, version::ExpectedVersion::Any, &[JobEvent::assigned()])
        .await
        .unwrap_err();
    assert!(
        matches!(error, append::AppendError::StreamDeleted(s) if s == id),
        "{error:?}"
    );
    let error = f
        .store
        .read_stream::<JobEvent>(id, v(1))
        .try_collect::<Vec<_>>()
        .await
        .unwrap_err();
    assert!(
        matches!(error, read::ReadError::StreamDeleted(s) if s == id),
        "{error:?}"
    );
    for error in [
        f.store.load::<Job>(id).await.unwrap_err(),
        f.store.load_snapshotted::<Job>(id).await.unwrap_err(),
    ] {
        assert!(
            matches!(
                error,
                load::LoadError::Read(read::ReadError::StreamDeleted(_))
            ),
            "{error:?}"
        );
    }
    let error = f
        .store
        .delete_stream::<JobEvent>(id, delete::DeleteExpected::Any)
        .await
        .unwrap_err();
    assert!(
        matches!(error, delete::DeleteStreamError::StreamDeleted(s) if s == id),
        "{error:?}"
    );

    f.finish().await
}

#[tokio::test]
async fn a_missing_stream_is_not_created() -> Result<()> {
    let f = Fixture::new().await?;
    let id = stream::StreamId::new_v7();

    for expected in [
        delete::DeleteExpected::Any,
        delete::DeleteExpected::Exact(v(1)),
    ] {
        let error = f
            .store
            .delete_stream::<JobEvent>(id, expected)
            .await
            .unwrap_err();
        assert!(
            matches!(error, delete::DeleteStreamError::NotFound(s) if s == id),
            "{error:?}"
        );
    }
    let streams: i64 = sqlx::query_scalar("SELECT count(*) FROM es.streams")
        .fetch_one(&f.pool)
        .await?;
    assert_eq!(streams, 0);

    // Other streams of the category are untouched by a deletion.
    let order = stream::StreamId::new_v7();
    f.store
        .append(
            order,
            version::ExpectedVersion::NoStream,
            &[OrderEvent::Placed(Placed { total: 3 })],
        )
        .await?;
    let (job, second) = job_with_snapshot(&f, "job-2").await?;
    let (other, _) = job_with_snapshot(&f, "job-3").await?;
    f.store
        .delete_stream::<JobEvent>(job, second.last_version())
        .await?;
    assert_eq!(rows(&f, other).await?, (2, 1, Some("job-3".into()), 0));
    assert_eq!(f.store.load::<Job>(other).await?.version(), Some(v(2)));

    f.finish().await
}

#[tokio::test]
async fn an_outbox_store_enqueues_the_tombstone() -> Result<()> {
    let f = Fixture::with_outbox(store::Outbox::Enabled).await?;
    let (id, second) = job_with_snapshot(&f, "job-4").await?;

    let tombstone = f
        .store
        .delete_stream::<JobEvent>(id, second.last_version())
        .await?;
    let queued: Vec<Uuid> = sqlx::query_scalar("SELECT commit_id FROM es.pending ORDER BY seq")
        .fetch_all(&f.pool)
        .await?;
    assert_eq!(queued.len(), 3);
    assert_eq!(queued[2], tombstone.commit_id.as_uuid());

    f.finish().await
}

/// A snapshot saved from a load made before the stream was deleted is not stored: the deletion removed the
/// stream's snapshot, and a later save must not bring the deleted state back.
#[tokio::test]
async fn a_snapshot_saved_after_the_deletion_is_not_stored() -> Result<()> {
    let f = Fixture::new().await?;
    let id = stream::StreamId::new_v7();
    f.store
        .append(
            id,
            version::ExpectedVersion::NoStream,
            &[JobEvent::created("a"), JobEvent::assigned()],
        )
        .await?;
    let loaded = f.store.load_snapshotted::<Job>(id).await?;

    f.store
        .delete_stream::<JobEvent>(id, delete::DeleteExpected::Any)
        .await?;
    f.store.save_snapshot(&loaded).await?;

    let (_, snapshots, _, state) = rows(&f, id).await?;
    assert_eq!(state, 2, "the stream is deleted");
    assert_eq!(snapshots, 0, "the deleted stream's state was stored again");

    f.finish().await
}

/// A save that overlaps a deletion waits for it, then stores nothing. The deletion is played by hand: it
/// locks the row as `delete_stream` does and marks the stream deleted, and commits only once the save waits.
#[tokio::test]
async fn a_snapshot_saved_during_the_deletion_is_not_stored() -> Result<()> {
    let f = Fixture::new().await?;
    let id = stream::StreamId::new_v7();
    f.store
        .append(
            id,
            version::ExpectedVersion::NoStream,
            &[JobEvent::created("a"), JobEvent::assigned()],
        )
        .await?;
    let loaded = f.store.load_snapshotted::<Job>(id).await?;

    let mut deletion = f.db.connect().await?;
    sqlx::query("BEGIN").execute(&mut deletion).await?;
    sqlx::query("SELECT 1 FROM es.streams WHERE stream_id = $1 FOR UPDATE")
        .bind(id.as_uuid())
        .execute(&mut deletion)
        .await?;
    sqlx::query("UPDATE es.streams SET state = 2 WHERE stream_id = $1")
        .bind(id.as_uuid())
        .execute(&mut deletion)
        .await?;
    let save = tokio::spawn({
        let store = f.store.clone();
        async move { store.save_snapshot(&loaded).await }
    });
    let mut side = f.db.connect().await?;
    for attempt in 0.. {
        assert!(attempt < 2000, "the save did not wait for the deletion");
        let waiting: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM pg_stat_activity
             WHERE datname = current_database() AND wait_event_type = 'Lock'",
        )
        .fetch_one(&mut side)
        .await?;
        if waiting > 0 {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    }
    sqlx::query("COMMIT").execute(&mut deletion).await?;
    save.await??;

    let (_, snapshots, _, _) = rows(&f, id).await?;
    assert_eq!(
        snapshots, 0,
        "the save stored the state of a stream deleted under it"
    );

    f.finish().await
}

/// A save that holds the stream's row when a deletion starts makes the deletion wait, and the deletion then
/// deletes the snapshot the save stored. The save is stopped after it locked the stream's row: another
/// transaction holds the stored snapshot's row, which the save must update.
#[tokio::test]
async fn a_deletion_waits_for_a_save_in_progress_then_deletes_its_snapshot() -> Result<()> {
    let f = Fixture::new().await?;
    let (id, _) = job_with_snapshot(&f, "job-1").await?;
    f.store
        .append(
            id,
            version::ExpectedVersion::Exact(v(2)),
            &[JobEvent::assigned()],
        )
        .await?;
    let loaded = f.store.load_snapshotted::<Job>(id).await?;

    let mut holder = hold_snapshot(&f, id).await?;
    let save = tokio::spawn({
        let store = f.store.clone();
        async move { store.save_snapshot(&loaded).await }
    });
    let saver = lock_waiter(&f, "INSERT INTO es.snapshots").await?;
    let deletion = tokio::spawn({
        let store = f.store.clone();
        async move {
            store
                .delete_stream::<JobEvent>(id, delete::DeleteExpected::Any)
                .await
        }
    });
    let deleter = lock_waiter(&f, "FROM es.streams WHERE stream_id = $1 FOR UPDATE").await?;
    let blockers: Vec<i32> = sqlx::query_scalar("SELECT unnest(pg_blocking_pids($1))")
        .bind(deleter)
        .fetch_all(&f.pool)
        .await?;
    assert_eq!(blockers, [saver], "the deletion waits for the save");

    sqlx::query("COMMIT").execute(&mut holder).await?;
    save.await??;
    deletion.await??;

    let (_, snapshots, _, state) = rows(&f, id).await?;
    assert_eq!(state, 2, "the stream is deleted");
    assert_eq!(snapshots, 0, "the deletion left the saved snapshot");

    f.finish().await
}

/// Relation locks a save holds once it has locked the stream's row and waits to update the stored snapshot:
/// `snapshots`, its partition and their keys, and the stream's partition of `streams` and its key: 6.
/// It names that partition, so a generic plan locks no other: through `streams`, every one.
#[tokio::test]
async fn a_save_locks_one_streams_partition_under_a_generic_plan() -> Result<()> {
    for routing in [store::AppendRouting::Auto, store::AppendRouting::Streams] {
        let mut definition = pges_testing::definition(store::Outbox::Disabled);
        definition.stream_partitions = std::num::NonZeroU16::new(64).unwrap();
        let f = Fixture::with_definition(definition).await?;
        let options: sqlx::postgres::PgConnectOptions = f.db.url().parse()?;
        let generic = sqlx::postgres::PgPoolOptions::new()
            .max_connections(2)
            .connect_with(options.options([("plan_cache_mode", "force_generic_plan")]))
            .await?;
        let mut config = store::StoreConfig::new(common::STORE_NAME);
        config.append_routing = routing;
        let store = store::Store::open(generic.clone(), config).await?;
        let (id, _) = job_with_snapshot(&f, "job-1").await?;
        f.store
            .append(
                id,
                version::ExpectedVersion::Exact(v(2)),
                &[JobEvent::assigned()],
            )
            .await?;
        let loaded = store.load_snapshotted::<Job>(id).await?;

        let mut holder = hold_snapshot(&f, id).await?;
        let save = tokio::spawn({
            let store = store.clone();
            async move { store.save_snapshot(&loaded).await }
        });
        let saver = lock_waiter(&f, "INSERT INTO es.snapshots").await?;
        let relations: Vec<String> = sqlx::query_scalar(
            "SELECT l.relation::regclass::text || ' ' || l.mode FROM pg_locks l
             WHERE l.pid = $1 AND l.locktype = 'relation' AND l.granted ORDER BY 1",
        )
        .bind(saver)
        .fetch_all(&f.pool)
        .await?;
        let partitions: i64 = sqlx::query_scalar(
            "SELECT count(DISTINCT c.relname) FROM pg_locks l JOIN pg_class c ON c.oid = l.relation
             WHERE l.pid = $1 AND c.relnamespace = 'es'::regnamespace
               AND c.relname ~ '^streams_p[0-9]+$'",
        )
        .bind(saver)
        .fetch_one(&f.pool)
        .await?;
        sqlx::query("COMMIT").execute(&mut holder).await?;
        save.await??;

        match routing {
            store::AppendRouting::Auto => {
                assert_eq!(partitions, 1, "{routing:?}");
                assert_eq!(relations.len(), 6, "{routing:?}: {relations:?}");
            }
            _ => assert_eq!(partitions, 64, "{routing:?}"),
        }
        let (_, snapshots, _, _) = rows(&f, id).await?;
        assert_eq!(snapshots, 1, "{routing:?}");

        generic.close().await;
        f.finish().await?;
    }

    Ok(())
}

/// An append's snapshot needs no deleted check: the append's own row lock makes a deletion wait for its
/// transaction, and the deletion then deletes the snapshot.
#[tokio::test]
async fn a_deletion_waits_for_an_append_with_a_snapshot_then_deletes_it() -> Result<()> {
    let f = Fixture::new().await?;
    let (id, _) = job_with_snapshot(&f, "job-1").await?;

    let mut conn = f.pool.acquire().await?;
    let mut tx = sqlx::Connection::begin(&mut *conn).await?;
    let mut work = pges_postgres::unit_of_work::UnitOfWork::wrap(&f.store, &mut tx);
    work.append(
        id,
        version::ExpectedVersion::Exact(v(2)),
        &[JobEvent::assigned()],
    )
    .snapshot_after(&Job::default())
    .await?;
    work.finish().await?;
    let deletion = tokio::spawn({
        let store = f.store.clone();
        async move {
            store
                .delete_stream::<JobEvent>(id, delete::DeleteExpected::Any)
                .await
        }
    });
    lock_waiter(&f, "FROM es.streams WHERE stream_id = $1 FOR UPDATE").await?;
    tx.commit().await?;
    deletion.await??;

    let (_, snapshots, _, state) = rows(&f, id).await?;
    assert_eq!(state, 2, "the stream is deleted");
    assert_eq!(snapshots, 0, "the deletion left the append's snapshot");

    drop(conn);
    f.finish().await
}

/// Begins a transaction that locks the stream's stored snapshot.
async fn hold_snapshot(f: &Fixture, id: stream::StreamId) -> Result<sqlx::PgConnection> {
    let mut holder = f.db.connect().await?;
    sqlx::query("BEGIN").execute(&mut holder).await?;
    sqlx::query("SELECT FROM es.snapshots WHERE stream_id = $1 FOR UPDATE")
        .bind(id.as_uuid())
        .execute(&mut holder)
        .await?;

    Ok(holder)
}

/// The backend that waits for a lock while running a statement containing `statement`.
async fn lock_waiter(f: &Fixture, statement: &str) -> Result<i32> {
    for _ in 0..2000 {
        let pid: Option<i32> = sqlx::query_scalar(
            "SELECT pid FROM pg_stat_activity
             WHERE datname = current_database() AND wait_event_type = 'Lock'
               AND strpos(query, $1) > 0",
        )
        .bind(statement)
        .fetch_optional(&f.pool)
        .await?;
        if let Some(pid) = pid {
            return Ok(pid);
        }
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    }

    anyhow::bail!("no statement containing {statement:?} waited for a lock")
}
