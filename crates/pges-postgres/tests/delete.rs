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
