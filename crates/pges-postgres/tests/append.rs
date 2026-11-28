//! Appends: every case of the "no row returned" classification (`append/classify.sql`), retries,
//! concurrency, append_many and units of work.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

mod common;

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::Result;
use sqlx::Connection;
use uuid::Uuid;

use pges_core::{category, commit, event, interval, stream, version};
use pges_postgres::{append, limit, store, unit_of_work};

use common::{Fixture, JobEvent, OrderEvent, Placed, v};

fn commit_id_at(offset: Duration, ahead: bool) -> commit::CommitId {
    let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
    let at = if ahead { now + offset } else { now - offset };
    let ts = uuid::Timestamp::from_unix(uuid::NoContext, at.as_secs(), at.subsec_nanos());

    commit::CommitId::from_uuid(Uuid::new_v7(ts)).unwrap()
}

async fn stream_with(f: &Fixture, events: &[JobEvent]) -> Result<stream::StreamId> {
    let id = stream::StreamId::new_v7();
    f.store
        .append(id, version::ExpectedVersion::NoStream, events)
        .await?;

    Ok(id)
}

#[tokio::test]
async fn appends_assign_versions_and_store_the_key() -> Result<()> {
    let f = Fixture::new().await?;
    let key = stream::StreamId::from_key(common::JOBS, "JOB-1");

    let first = f
        .store
        .append(
            key.clone(),
            version::ExpectedVersion::NoStream,
            &[JobEvent::created("a"), JobEvent::assigned()],
        )
        .await?;
    let second = f
        .store
        .append(key.id, first.expected(), &[JobEvent::assigned()])
        .await?;
    let any = f
        .store
        .append(
            key.id,
            version::ExpectedVersion::Any,
            &[JobEvent::assigned()],
        )
        .await?;

    assert_eq!(first.versions, v(1)..=v(2));
    assert_eq!(first.outcome, append::Outcome::Appended);
    assert_eq!(second.versions, v(3)..=v(3));
    assert_eq!(any.versions, v(4)..=v(4));
    let stored_key: Option<String> =
        sqlx::query_scalar("SELECT stream_key FROM es.streams WHERE stream_id = $1")
            .bind(key.id.as_uuid())
            .fetch_one(&f.pool)
            .await?;
    assert_eq!(stored_key.as_deref(), Some("JOB-1"));
    f.finish().await
}

/// Row "done is set": a retry with the same commit id returns the original result, for every expectation.
#[tokio::test]
async fn retried_commit_ids_are_deduplicated() -> Result<()> {
    let f = Fixture::new().await?;
    let id = stream::StreamId::new_v7();
    let events = [JobEvent::created("a"), JobEvent::assigned()];

    for expected in [
        version::ExpectedVersion::NoStream,
        version::ExpectedVersion::Exact(v(2)),
        version::ExpectedVersion::Any,
    ] {
        let commit_id = commit::CommitId::generate();
        let first = f
            .store
            .append(id, expected, &events)
            .commit_id(commit_id)
            .await?;
        let retry = f
            .store
            .append(id, expected, &events)
            .commit_id(commit_id)
            .await?;

        assert_eq!(first.outcome, append::Outcome::Appended);
        assert_eq!(retry.outcome, append::Outcome::Deduplicated, "{expected:?}");
        assert_eq!(retry.versions, first.versions);
        assert_eq!(retry.commit_id, commit_id);
    }

    let version: i64 = sqlx::query_scalar("SELECT version FROM es.streams WHERE stream_id = $1")
        .bind(id.as_uuid())
        .fetch_one(&f.pool)
        .await?;
    assert_eq!(version, 6);
    f.finish().await
}

/// A retry after the commit id left the skew window still finds the original result.
#[tokio::test]
async fn retries_past_the_skew_window_are_deduplicated() -> Result<()> {
    let f = Fixture::new().await?;
    let mut config = store::StoreConfig::new(common::STORE_NAME);
    config.max_clock_skew = Some(Duration::from_millis(500));
    let tight = store::Store::open(f.pool.clone(), config).await?;
    let id = stream::StreamId::new_v7();
    let commit_id = commit::CommitId::generate();
    let events = [JobEvent::created("a")];

    tight
        .append(id, version::ExpectedVersion::NoStream, &events)
        .commit_id(commit_id)
        .await?;
    tokio::time::sleep(Duration::from_millis(1200)).await;
    let retry = tight
        .append(id, version::ExpectedVersion::NoStream, &events)
        .commit_id(commit_id)
        .await?;

    assert_eq!(retry.outcome, append::Outcome::Deduplicated);
    assert_eq!(retry.versions, v(1)..=v(1));
    f.finish().await
}

/// A commit id past the skew window that is already stored is deduplicated in every mode: a unit of work, an
/// `append_in` inside a transaction and a snapshot append, which raise no error to classify, as well as
/// autocommit. The unit of work still commits. Without the stream's last commit id in the skew check, the
/// transaction modes failed with `CommitIdSkew` and aborted the transaction.
#[tokio::test]
async fn late_retries_are_deduplicated_in_every_mode() -> Result<()> {
    let f = Fixture::new().await?;
    let mut config = store::StoreConfig::new(common::STORE_NAME);
    config.max_clock_skew = Some(Duration::from_millis(500));
    let tight = store::Store::open(f.pool.clone(), config).await?;
    let events = [JobEvent::created("a")];
    let late = || commit_id_at(Duration::from_secs(3), false);

    let first = late();
    let id = stream::StreamId::new_v7();
    f.store
        .append(id, version::ExpectedVersion::NoStream, &events)
        .commit_id(first)
        .await?;
    let second = late();
    f.store
        .append(id, version::ExpectedVersion::Exact(v(1)), &events)
        .commit_id(second)
        .await?;

    let retry = tight
        .append(id, version::ExpectedVersion::NoStream, &events)
        .commit_id(first)
        .await?;
    assert_eq!(retry.outcome, append::Outcome::Deduplicated, "autocommit");
    assert_eq!(retry.versions, v(1)..=v(1));

    let retry = tight
        .append(id, version::ExpectedVersion::Exact(v(1)), &events)
        .commit_id(second)
        .snapshot_after(&common::Job::default())
        .await?;
    assert_eq!(retry.outcome, append::Outcome::Deduplicated, "snapshot");
    assert_eq!(retry.versions, v(2)..=v(2));

    let other = stream::StreamId::new_v7();
    let mut uow = tight.begin().await?;
    let retry = uow
        .append(id, version::ExpectedVersion::NoStream, &events)
        .commit_id(first)
        .await?;
    assert_eq!(retry.outcome, append::Outcome::Deduplicated, "unit of work");
    assert_eq!(retry.versions, v(1)..=v(1));
    let retry = uow
        .append(id, version::ExpectedVersion::Any, &events)
        .commit_id(second)
        .await?;
    assert_eq!(retry.outcome, append::Outcome::Deduplicated, "unit of work");
    assert_eq!(retry.versions, v(2)..=v(2));
    uow.append(other, version::ExpectedVersion::NoStream, &events)
        .await?;
    uow.commit().await?;

    let mut conn = f.pool.acquire().await?;
    let mut tx = conn.begin().await?;
    let retry = tight
        .append_in(&mut tx, id, version::ExpectedVersion::Exact(v(1)), &events)
        .commit_id(second)
        .await?;
    assert_eq!(retry.outcome, append::Outcome::Deduplicated, "append_in");
    tx.commit().await?;
    drop(conn);

    let versions: Vec<i64> = sqlx::query_scalar(
        "SELECT version FROM es.streams WHERE stream_id = ANY($1) ORDER BY stream_id = $2",
    )
    .bind(vec![id.as_uuid(), other.as_uuid()])
    .bind(id.as_uuid())
    .fetch_all(&f.pool)
    .await?;
    assert_eq!(
        versions,
        [1, 2],
        "the unit of work committed its new stream"
    );
    f.finish().await
}

/// A late commit id the stream has not stored is still refused: below the stream's last commit id it is out
/// of order, and a new id outside the skew window is `CommitIdSkew`, in a unit of work too.
#[tokio::test]
async fn late_commit_ids_that_were_not_stored_are_refused() -> Result<()> {
    let f = Fixture::new().await?;
    let mut config = store::StoreConfig::new(common::STORE_NAME);
    config.max_clock_skew = Some(Duration::from_millis(500));
    let tight = store::Store::open(f.pool.clone(), config).await?;
    let events = [JobEvent::created("a")];
    let behind = commit_id_at(Duration::from_secs(3), false);
    let id = stream_with(&f, &events).await?;

    let error = tight
        .append(id, version::ExpectedVersion::Any, &events)
        .commit_id(behind)
        .await
        .unwrap_err();
    assert!(
        matches!(error, append::AppendError::CommitIdOutOfOrder { commit_id, .. } if commit_id == behind),
        "{error}"
    );

    let mut uow = tight.begin().await?;
    let error = uow
        .append(id, version::ExpectedVersion::Any, &events)
        .commit_id(behind)
        .await
        .unwrap_err();
    assert!(
        matches!(error, append::AppendError::CommitIdOutOfOrder { .. }),
        "{error}"
    );
    let ahead = commit_id_at(Duration::from_secs(60), true);
    let error = uow
        .append(id, version::ExpectedVersion::Any, &events)
        .commit_id(ahead)
        .await
        .unwrap_err();
    assert!(
        matches!(error, append::AppendError::CommitIdSkew(c) if c == ahead),
        "{error}"
    );
    uow.rollback().await?;
    f.finish().await
}

/// Row "no stream, expected = Any": the update's snapshot cannot see a concurrent creation; one retry wins.
#[tokio::test]
async fn any_racing_the_stream_creation_is_retried() -> Result<()> {
    let f = Fixture::new().await?;
    let id = stream::StreamId::new_v7();

    let mut uow = f.store.begin().await?;
    uow.append(
        id,
        version::ExpectedVersion::NoStream,
        &[JobEvent::created("a")],
    )
    .await?;
    let racer = {
        let store = f.store.clone();
        tokio::spawn(async move {
            store
                .append(id, version::ExpectedVersion::Any, &[JobEvent::assigned()])
                .await
        })
    };
    tokio::time::sleep(Duration::from_millis(300)).await;
    uow.commit().await?;

    let result = racer.await??;
    assert_eq!(result.versions, v(2)..=v(2));
    assert_eq!(result.outcome, append::Outcome::Appended);
    f.finish().await
}

/// Row "stream exists for NoStream", reached through a race with the creation.
#[tokio::test]
async fn no_stream_racing_the_stream_creation_conflicts() -> Result<()> {
    let f = Fixture::new().await?;
    let id = stream::StreamId::new_v7();

    let mut uow = f.store.begin().await?;
    uow.append(
        id,
        version::ExpectedVersion::NoStream,
        &[JobEvent::created("a")],
    )
    .await?;
    let racer = {
        let store = f.store.clone();
        tokio::spawn(async move {
            store
                .append(
                    id,
                    version::ExpectedVersion::NoStream,
                    &[JobEvent::assigned()],
                )
                .await
        })
    };
    tokio::time::sleep(Duration::from_millis(300)).await;
    uow.commit().await?;

    let error = racer.await?.unwrap_err();
    assert!(
        matches!(error, append::AppendError::Conflict { actual: Some(a), .. } if a == v(1)),
        "{error}"
    );
    f.finish().await
}

/// Row "no stream, Exact".
#[tokio::test]
async fn exact_on_a_missing_stream_conflicts() -> Result<()> {
    let f = Fixture::new().await?;
    let id = stream::StreamId::new_v7();

    let error = f
        .store
        .append(
            id,
            version::ExpectedVersion::Exact(v(1)),
            &[JobEvent::assigned()],
        )
        .await
        .unwrap_err();

    assert!(
        matches!(error, append::AppendError::Conflict { stream_id, actual: None, .. } if stream_id == id),
        "{error}"
    );
    f.finish().await
}

/// Row "category_id <> $2".
#[tokio::test]
async fn appending_another_category_to_a_stream_fails() -> Result<()> {
    let f = Fixture::new().await?;
    let id = stream_with(&f, &[JobEvent::created("a")]).await?;

    let error = f
        .store
        .append(
            id,
            version::ExpectedVersion::Any,
            &[OrderEvent::Placed(Placed { total: 3 })],
        )
        .await
        .unwrap_err();

    assert!(
        matches!(
            error,
            append::AppendError::CategoryMismatch { stream_category, event_category, .. }
                if stream_category == common::JOBS && event_category == common::ORDERS
        ),
        "{error}"
    );
    f.finish().await
}

/// Rows "state = 1" and "state = 2".
#[tokio::test]
async fn archived_and_deleted_streams_take_no_events() -> Result<()> {
    let f = Fixture::new().await?;
    let archived = stream_with(&f, &[JobEvent::created("a")]).await?;
    let deleted = stream_with(&f, &[JobEvent::created("b")]).await?;
    for (id, state) in [(archived, 1_i16), (deleted, 2)] {
        sqlx::query("UPDATE es.streams SET state = $2 WHERE stream_id = $1")
            .bind(id.as_uuid())
            .bind(state)
            .execute(&f.pool)
            .await?;
    }

    let error = f
        .store
        .append(
            archived,
            version::ExpectedVersion::Exact(v(1)),
            &[JobEvent::assigned()],
        )
        .await
        .unwrap_err();
    assert!(
        matches!(error, append::AppendError::StreamArchived(id) if id == archived),
        "{error}"
    );
    let error = f
        .store
        .append(
            deleted,
            version::ExpectedVersion::Any,
            &[JobEvent::assigned()],
        )
        .await
        .unwrap_err();
    assert!(
        matches!(error, append::AppendError::StreamDeleted(id) if id == deleted),
        "{error}"
    );
    f.finish().await
}

/// Rows "version differs from Exact" and "stream exists for NoStream".
#[tokio::test]
async fn stale_expectations_conflict_with_the_actual_version() -> Result<()> {
    let f = Fixture::new().await?;
    let id = stream_with(&f, &[JobEvent::created("a"), JobEvent::assigned()]).await?;

    for expected in [
        version::ExpectedVersion::Exact(v(1)),
        version::ExpectedVersion::Exact(v(3)),
        version::ExpectedVersion::NoStream,
    ] {
        let error = f
            .store
            .append(id, expected, &[JobEvent::assigned()])
            .await
            .unwrap_err();
        assert!(
            matches!(
                error,
                append::AppendError::Conflict { expected: e, actual: Some(a), .. } if e == expected && a == v(2)
            ),
            "{expected:?}: {error}"
        );
    }
    f.finish().await
}

/// Row "$8 <= last_commit_id" for a caller-supplied id.
#[tokio::test]
async fn caller_commit_ids_must_increase() -> Result<()> {
    let f = Fixture::new().await?;
    let early = commit::CommitId::generate();
    let id = stream_with(&f, &[JobEvent::created("a")]).await?;

    let error = f
        .store
        .append(
            id,
            version::ExpectedVersion::Exact(v(1)),
            &[JobEvent::assigned()],
        )
        .commit_id(early)
        .await
        .unwrap_err();

    assert!(
        matches!(
            error,
            append::AppendError::CommitIdOutOfOrder { stream_id, commit_id } if stream_id == id && commit_id == early
        ),
        "{error}"
    );
    f.finish().await
}

/// Row "$8 <= last_commit_id" for a crate-generated id: another writer's clock is ahead, so the crate runs
/// the append again, under the stream's row lock, with an id just above the stream's last one.
#[tokio::test]
async fn generated_commit_ids_below_the_last_one_are_moved_up() -> Result<()> {
    let f = Fixture::new().await?;
    let id = stream::StreamId::new_v7();
    let ahead = commit_id_at(Duration::from_secs(2), true);
    f.store
        .append(
            id,
            version::ExpectedVersion::NoStream,
            &[JobEvent::created("a")],
        )
        .commit_id(ahead)
        .await?;

    let result = f
        .store
        .append(
            id,
            version::ExpectedVersion::Exact(v(1)),
            &[JobEvent::assigned()],
        )
        .await?;

    assert_eq!(result.versions, v(2)..=v(2));
    assert!(result.commit_id > ahead);
    assert_eq!(
        result.commit_id.recorded_at(),
        ahead.recorded_at(),
        "same millisecond, higher counter"
    );
    f.finish().await
}

#[tokio::test]
async fn commit_ids_outside_the_skew_tolerance_are_rejected() -> Result<()> {
    let f = Fixture::new().await?;
    let id = stream::StreamId::new_v7();

    for skewed in [
        commit_id_at(Duration::from_secs(60), false),
        commit_id_at(Duration::from_secs(60), true),
    ] {
        let error = f
            .store
            .append(id, version::ExpectedVersion::Any, &[JobEvent::created("a")])
            .commit_id(skewed)
            .await
            .unwrap_err();
        assert!(
            matches!(error, append::AppendError::CommitIdSkew(c) if c == skewed),
            "{error}"
        );
    }
    f.finish().await
}

/// A writer that sets no `max_clock_skew` takes the store's recorded one, so raising the store's is enough; a
/// writer that sets one uses it.
#[tokio::test]
async fn a_writer_without_its_own_skew_uses_the_stores() -> Result<()> {
    let f = Fixture::new().await?;
    f.store
        .admin()
        .set_writer_limits(limit::WriterLimits::new(
            Duration::from_secs(120),
            interval!(pges_core::store::DEFAULT_MAX_TXN_AGE),
        ))
        .await?;
    let late = commit_id_at(Duration::from_secs(60), false);

    let own =
        store::Store::open(f.pool.clone(), store::StoreConfig::new(common::STORE_NAME)).await?;
    own.append(
        stream::StreamId::new_v7(),
        version::ExpectedVersion::NoStream,
        &[JobEvent::created("a")],
    )
    .commit_id(late)
    .await?;

    let mut config = store::StoreConfig::new(common::STORE_NAME);
    config.max_clock_skew = Some(Duration::from_secs(5));
    let tight = store::Store::open(f.pool.clone(), config).await?;
    let error = tight
        .append(
            stream::StreamId::new_v7(),
            version::ExpectedVersion::NoStream,
            &[JobEvent::created("b")],
        )
        .commit_id(commit_id_at(Duration::from_secs(60), false))
        .await
        .unwrap_err();
    assert!(
        matches!(error, append::AppendError::CommitIdSkew(_)),
        "{error}"
    );

    f.finish().await
}

/// In a batch, a stream whose last commit id is ahead is retried alone with a higher id, and the rest of the
/// batch stays written.
#[tokio::test]
async fn append_many_moves_generated_commit_ids_up() -> Result<()> {
    let f = Fixture::new().await?;
    let ahead_stream = stream::StreamId::new_v7();
    let ahead = commit_id_at(Duration::from_secs(2), true);
    f.store
        .append(
            ahead_stream,
            version::ExpectedVersion::NoStream,
            &[JobEvent::created("a")],
        )
        .commit_id(ahead)
        .await?;
    let fresh = stream::StreamId::new_v7();
    let jobs = [JobEvent::assigned()];

    let results = f
        .store
        .append_many([
            append::AppendEntry::new(ahead_stream, version::ExpectedVersion::Exact(v(1)), &jobs),
            append::AppendEntry::new(fresh, version::ExpectedVersion::NoStream, &jobs),
        ])
        .await?;

    assert_eq!(results[0].versions, v(2)..=v(2));
    assert!(results[0].commit_id > ahead);
    assert_eq!(results[1].versions, v(1)..=v(1));
    let commits: i64 =
        sqlx::query_scalar("SELECT count(*) FROM es.commits WHERE stream_id = ANY($1)")
            .bind(vec![ahead_stream.as_uuid(), fresh.as_uuid()])
            .fetch_one(&f.pool)
            .await?;
    assert_eq!(commits, 3);
    f.finish().await
}

/// append_many generates its commit ids, so only a database clock far from the client's rejects them. A zero
/// tolerance stands in for that.
#[tokio::test]
async fn append_many_reports_skewed_commit_ids() -> Result<()> {
    let f = Fixture::new().await?;
    let mut config = store::StoreConfig::new(common::STORE_NAME);
    config.max_clock_skew = Some(Duration::ZERO);
    let strict = store::Store::open(f.pool.clone(), config).await?;
    let jobs = [JobEvent::created("a")];

    let error = strict
        .append_many([append::AppendEntry::new(
            stream::StreamId::new_v7(),
            version::ExpectedVersion::NoStream,
            &jobs,
        )])
        .await
        .unwrap_err();

    assert!(
        matches!(error, append::AppendError::CommitIdSkew(_)),
        "{error}"
    );
    f.finish().await
}

/// A commit id reused by a different append (23505 on the commits key).
#[tokio::test]
async fn reusing_a_commit_id_on_another_stream_is_a_duplicate() -> Result<()> {
    let f = Fixture::new().await?;
    let commit_id = commit::CommitId::generate();
    f.store
        .append(
            stream::StreamId::new_v7(),
            version::ExpectedVersion::NoStream,
            &[JobEvent::created("a")],
        )
        .commit_id(commit_id)
        .await?;

    let error = f
        .store
        .append(
            stream::StreamId::new_v7(),
            version::ExpectedVersion::NoStream,
            &[JobEvent::created("b")],
        )
        .commit_id(commit_id)
        .await
        .unwrap_err();
    assert!(
        matches!(error, append::AppendError::DuplicateCommit(c) if c == commit_id),
        "{error}"
    );

    let other = stream::StreamId::new_v7();
    let mut uow = f.store.begin().await?;
    let error = uow
        .append(
            other,
            version::ExpectedVersion::NoStream,
            &[JobEvent::created("c")],
        )
        .commit_id(commit_id)
        .await
        .unwrap_err();
    assert!(
        matches!(error, append::AppendError::DuplicateCommit(_)),
        "{error}"
    );
    let error = uow.commit().await.unwrap_err();
    assert!(
        matches!(error, unit_of_work::TransactionError::Aborted),
        "{error}"
    );
    f.finish().await
}

#[tokio::test]
async fn invalid_batches_are_rejected_before_writing() -> Result<()> {
    struct Invoice;
    impl event::Event for Invoice {
        const CATEGORY: category::CategoryName = category!("invoices");
        const TYPES: &'static [&'static str] = &["invoice.sent"];
        fn type_name(&self) -> &'static str {
            "invoice.sent"
        }
        fn schema_version(&self) -> event::SchemaVersion {
            event::SchemaVersion::FIRST
        }
        fn encode(&self, _: &mut Vec<u8>) -> Result<(), event::CodecError> {
            Ok(())
        }
        fn decode(
            _: &str,
            _: event::SchemaVersion,
            _: &[u8],
        ) -> Result<event::Decoded<Self>, event::CodecError> {
            Ok(event::Decoded::Event(Self))
        }
    }

    let f = Fixture::new().await?;
    let id = stream::StreamId::new_v7();

    let none: [JobEvent; 0] = [];
    let error = f
        .store
        .append(id, version::ExpectedVersion::Any, &none)
        .await
        .unwrap_err();
    assert!(matches!(error, append::AppendError::NoEvents), "{error}");

    let many = vec![JobEvent::assigned(); 32_768];
    let error = f
        .store
        .append(id, version::ExpectedVersion::Any, &many)
        .await
        .unwrap_err();
    assert!(
        matches!(error, append::AppendError::TooManyEvents { max: 32_767 }),
        "{error}"
    );

    let error = f
        .store
        .append(id, version::ExpectedVersion::Any, &[Invoice])
        .await
        .unwrap_err();
    assert!(
        matches!(error, append::AppendError::UnknownCategory(c) if c.as_str() == "invoices"),
        "{error}"
    );
    f.finish().await
}

/// Two writers append to one stream expecting the same version: exactly one wins, every time.
#[tokio::test]
async fn racing_exact_appends_have_exactly_one_winner() -> Result<()> {
    let f = Fixture::new().await?;

    for _ in 0..20 {
        let id = stream_with(&f, &[JobEvent::created("a")]).await?;
        let barrier = std::sync::Arc::new(tokio::sync::Barrier::new(2));
        let writers: Vec<_> = (0..2)
            .map(|_| {
                let store = f.store.clone();
                let barrier = barrier.clone();
                tokio::spawn(async move {
                    barrier.wait().await;
                    store
                        .append(
                            id,
                            version::ExpectedVersion::Exact(v(1)),
                            &[JobEvent::assigned()],
                        )
                        .await
                })
            })
            .collect();

        let mut won = 0;
        let mut lost = 0;
        for writer in writers {
            match writer.await? {
                Ok(result) => {
                    assert_eq!(result.versions, v(2)..=v(2));
                    won += 1;
                }
                Err(append::AppendError::Conflict { actual, .. }) => {
                    assert_eq!(actual, Some(v(2)));
                    lost += 1;
                }
                Err(other) => panic!("{other}"),
            }
        }
        assert_eq!((won, lost), (1, 1));
    }
    f.finish().await
}

#[tokio::test]
async fn append_many_writes_every_stream_or_none() -> Result<()> {
    let f = Fixture::new().await?;
    let job = stream_with(&f, &[JobEvent::created("a")]).await?;
    let order = stream::StreamId::new_v7();
    let jobs = [JobEvent::assigned()];
    let orders = [OrderEvent::Placed(Placed { total: 9 })];

    let results = f
        .store
        .append_many([
            append::AppendEntry::new(job, version::ExpectedVersion::Exact(v(1)), &jobs),
            append::AppendEntry::new(order, version::ExpectedVersion::NoStream, &orders),
        ])
        .await?;
    assert_eq!(results.len(), 2);
    assert_eq!(
        (results[0].stream_id, results[0].versions.clone()),
        (job, v(2)..=v(2))
    );
    assert_eq!(
        (results[1].stream_id, results[1].versions.clone()),
        (order, v(1)..=v(1))
    );

    let fresh = stream::StreamId::new_v7();
    let error = f
        .store
        .append_many([
            append::AppendEntry::new(fresh, version::ExpectedVersion::NoStream, &jobs),
            append::AppendEntry::new(job, version::ExpectedVersion::Exact(v(1)), &jobs),
        ])
        .await
        .unwrap_err();
    assert!(
        matches!(error, append::AppendError::Conflict { stream_id, actual: Some(a), .. } if stream_id == job && a == v(2)),
        "{error}"
    );
    let fresh_exists: bool =
        sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM es.streams WHERE stream_id = $1)")
            .bind(fresh.as_uuid())
            .fetch_one(&f.pool)
            .await?;
    assert!(!fresh_exists, "the failed batch wrote nothing");

    let error = f
        .store
        .append_many([
            append::AppendEntry::new(fresh, version::ExpectedVersion::Any, &jobs),
            append::AppendEntry::new(fresh, version::ExpectedVersion::Any, &jobs),
        ])
        .await
        .unwrap_err();
    assert!(
        matches!(error, append::AppendError::DuplicateStream(id) if id == fresh),
        "{error}"
    );
    f.finish().await
}

#[tokio::test]
async fn units_of_work_commit_or_roll_back_as_one() -> Result<()> {
    let f = Fixture::new().await?;
    let (a, b) = (stream::StreamId::new_v7(), stream::StreamId::new_v7());
    sqlx::query("CREATE TABLE side_effects (stream_id uuid PRIMARY KEY)")
        .execute(&f.pool)
        .await?;
    let count = |id: stream::StreamId| {
        let pool = f.pool.clone();
        async move {
            sqlx::query_scalar::<_, i64>("SELECT count(*) FROM es.commits WHERE stream_id = $1")
                .bind(id.as_uuid())
                .fetch_one(&pool)
                .await
        }
    };

    let mut uow = f.store.begin().await?;
    uow.append(
        a,
        version::ExpectedVersion::NoStream,
        &[JobEvent::created("a")],
    )
    .await?;
    uow.append(
        b,
        version::ExpectedVersion::NoStream,
        &[JobEvent::created("b")],
    )
    .await?;
    drop(uow);
    assert_eq!(
        (count(a).await?, count(b).await?),
        (0, 0),
        "dropped: rolled back"
    );

    let mut uow = f.store.begin().await?;
    uow.append(
        a,
        version::ExpectedVersion::NoStream,
        &[JobEvent::created("a")],
    )
    .await?;
    uow.append(
        b,
        version::ExpectedVersion::NoStream,
        &[JobEvent::created("b")],
    )
    .await?;
    sqlx::query("INSERT INTO side_effects VALUES ($1)")
        .bind(a.as_uuid())
        .execute(uow.conn())
        .await?;
    uow.commit().await?;
    assert_eq!((count(a).await?, count(b).await?), (1, 1), "committed");

    // A failed append_many may have written part of the batch, so the unit of work refuses to commit.
    let c = stream::StreamId::new_v7();
    let mut uow = f.store.begin().await?;
    let jobs = [JobEvent::assigned()];
    let error = uow
        .append_many([
            append::AppendEntry::new(c, version::ExpectedVersion::NoStream, &jobs),
            append::AppendEntry::new(a, version::ExpectedVersion::Exact(v(7)), &jobs),
        ])
        .await
        .unwrap_err();
    assert!(
        matches!(error, append::AppendError::Conflict { .. }),
        "{error}"
    );
    let error = uow.commit().await.unwrap_err();
    assert!(
        matches!(error, unit_of_work::TransactionError::Aborted),
        "{error}"
    );
    assert_eq!(count(c).await?, 0, "aborted: rolled back");

    // Inside a transaction the caller owns.
    let mut conn = f.pool.acquire().await?;
    let mut tx = conn.begin().await?;
    let mut uow = unit_of_work::UnitOfWork::wrap(&f.store, &mut tx);
    uow.append(
        c,
        version::ExpectedVersion::NoStream,
        &[JobEvent::created("c")],
    )
    .await?;
    uow.finish().await?;
    assert_eq!(count(c).await?, 0, "the caller has not committed yet");
    tx.commit().await?;
    assert_eq!(count(c).await?, 1);
    // sqlx 0.9's `Pool::close` waits for every connection to come back.
    drop(conn);
    f.finish().await
}

#[tokio::test]
async fn append_in_uses_the_callers_connection() -> Result<()> {
    let f = Fixture::new().await?;
    let id = stream::StreamId::new_v7();
    let mut conn = f.pool.acquire().await?;
    let mut tx = conn.begin().await?;

    f.store
        .append_in(
            &mut tx,
            id,
            version::ExpectedVersion::NoStream,
            &[JobEvent::created("a")],
        )
        .await?;
    tx.rollback().await?;

    let exists: bool =
        sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM es.streams WHERE stream_id = $1)")
            .bind(id.as_uuid())
            .fetch_one(&f.pool)
            .await?;
    assert!(!exists);
    drop(conn);
    f.finish().await
}

/// The planner's view of `streams` when the appends start.
#[derive(Debug, Clone, Copy)]
enum Regime {
    /// Never analyzed: the planner guesses the partitions' sizes.
    Never,
    /// Analyzed with about 30 streams per partition.
    Small,
    /// Analyzed with 3 streams in all. Through `streams`, the plan cache then keeps a generic plan, which
    /// locks every partition.
    NearlyEmpty,
    /// The session sets `plan_cache_mode = force_generic_plan`.
    ForcedGeneric,
}

/// How the store reaches `streams` in the lock test.
#[derive(Debug, Clone, Copy)]
enum Appends {
    /// sqlx's default statement cache, which holds the 64 per-partition appends: appends are routed.
    Routed,
    /// A statement cache of 99, one short of 64 partition statements and the reserve of 36: appends go
    /// through `streams`.
    SmallCache,
    /// [`store::AppendRouting::Streams`].
    RoutingOff,
}

/// A routed append locks its stream's partition of `streams` and that partition's key, never another
/// partition, and takes the same relation locks whatever the planner's statistics and the session's
/// `plan_cache_mode`: it names the partition, so no plan has partitions to prune. Unrouted appends (a
/// statement cache too small for the partition statements, or routing turned off) go through `streams`
/// with one prepared statement, where a forced generic plan locks all 64 partitions.
#[tokio::test]
async fn an_append_locks_only_the_streams_partition_it_writes() -> Result<()> {
    for (outbox, regime, appends) in [
        (store::Outbox::Disabled, Regime::Never, Appends::Routed),
        (store::Outbox::Enabled, Regime::Never, Appends::Routed),
        (store::Outbox::Disabled, Regime::Small, Appends::Routed),
        (
            store::Outbox::Disabled,
            Regime::NearlyEmpty,
            Appends::Routed,
        ),
        (store::Outbox::Enabled, Regime::NearlyEmpty, Appends::Routed),
        (
            store::Outbox::Disabled,
            Regime::ForcedGeneric,
            Appends::Routed,
        ),
        (
            store::Outbox::Enabled,
            Regime::ForcedGeneric,
            Appends::Routed,
        ),
        (
            store::Outbox::Disabled,
            Regime::NearlyEmpty,
            Appends::SmallCache,
        ),
        (
            store::Outbox::Disabled,
            Regime::ForcedGeneric,
            Appends::SmallCache,
        ),
        (
            store::Outbox::Enabled,
            Regime::ForcedGeneric,
            Appends::RoutingOff,
        ),
    ] {
        let case = format!("{outbox:?}, {regime:?}, {appends:?}");
        let mut definition = pges_testing::definition(outbox);
        definition.stream_partitions = std::num::NonZeroU16::new(64).unwrap();
        let f = Fixture::with_definition(definition).await?;
        let mut config = store::StoreConfig::new(common::STORE_NAME);
        let pool = match appends {
            Appends::Routed => f.pool.clone(),
            Appends::RoutingOff => {
                config.append_routing = store::AppendRouting::Streams;
                f.pool.clone()
            }
            Appends::SmallCache => {
                let options: sqlx::postgres::PgConnectOptions = f.db.url().parse()?;
                sqlx::postgres::PgPoolOptions::new()
                    .max_connections(2)
                    .connect_with(options.statement_cache_capacity(99))
                    .await?
            }
        };
        let store = store::Store::open(pool.clone(), config).await?;
        let id = stream::StreamId::new_v7();
        match regime {
            Regime::Never | Regime::ForcedGeneric => {}
            Regime::Small => {
                sqlx::query(
                    "INSERT INTO es.streams (version, stream_id, first_commit_id, last_commit_id, category_id)
                     SELECT 1, gen_random_uuid(), uuidv7(), uuidv7(), 1 FROM generate_series(1, 2000)",
                )
                .execute(&f.pool)
                .await?;
                sqlx::query("ANALYZE es.streams").execute(&f.pool).await?;
            }
            Regime::NearlyEmpty => {
                for stream_id in [id, stream::StreamId::new_v7(), stream::StreamId::new_v7()] {
                    store
                        .append(
                            stream_id,
                            version::ExpectedVersion::NoStream,
                            &[JobEvent::assigned()],
                        )
                        .await?;
                }
                sqlx::query("ANALYZE es.streams").execute(&f.pool).await?;
            }
        }
        let mut conn = pool.acquire().await?;
        if let Regime::ForcedGeneric = regime {
            sqlx::query("SET plan_cache_mode = force_generic_plan")
                .execute(&mut *conn)
                .await?;
        }

        // Past the sixth execution, from which the plan cache weighs a generic plan against custom ones.
        for _ in 0..10 {
            store
                .append_in(
                    &mut conn,
                    id,
                    version::ExpectedVersion::Any,
                    &[JobEvent::assigned()],
                )
                .await?;
        }
        let mut tx = conn.begin().await?;
        store
            .append_in(
                &mut tx,
                id,
                version::ExpectedVersion::Any,
                &[JobEvent::assigned()],
            )
            .await?;
        let relations: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM pg_locks WHERE pid = pg_backend_pid() AND locktype = 'relation'",
        )
        .fetch_one(&mut *tx)
        .await?;
        let locked: Vec<String> = sqlx::query_scalar(
            "SELECT DISTINCT c.relname::text FROM pg_locks l JOIN pg_class c ON c.oid = l.relation
             WHERE l.pid = pg_backend_pid() AND c.relnamespace = 'es'::regnamespace
               AND c.relname ~ '^streams_p[0-9]+'
             ORDER BY 1",
        )
        .fetch_all(&mut *tx)
        .await?;
        let partition: String = sqlx::query_scalar(
            "SELECT c.relname::text FROM es.streams s JOIN pg_class c ON c.oid = s.tableoid
             WHERE s.stream_id = $1",
        )
        .bind(id.as_uuid())
        .fetch_one(&mut *tx)
        .await?;
        tx.rollback().await?;
        let statements = append_statements(&mut conn).await?;

        assert!(locked.contains(&partition), "{case}: {locked:?}");
        if let Appends::Routed = appends {
            // Its key too when the plan reads the partition through it; small partitions are scanned.
            let key = format!("{partition}_pkey");
            assert!(
                locked.iter().all(|name| *name == partition || *name == key),
                "{case}: {locked:?}"
            );
            let most = match outbox {
                store::Outbox::Disabled => APPEND_RELATION_LOCKS,
                _ => APPEND_OUTBOX_RELATION_LOCKS,
            };
            assert!(relations <= most, "{case}: {relations} relation locks");
            assert_eq!(statements.parent, 0, "{case}");
            assert!(statements.partitions >= 1, "{case}");
        } else {
            assert_eq!(
                statements,
                AppendStatements {
                    parent: 1,
                    partitions: 0
                },
                "{case}"
            );
            if let Regime::ForcedGeneric = regime {
                assert!(locked.len() >= 64, "{case}: {locked:?}");
            }
        }

        sqlx::query("RESET plan_cache_mode")
            .execute(&mut *conn)
            .await?;
        drop(conn);
        if let Appends::SmallCache = appends {
            pool.close().await;
        }
        f.finish().await?;
    }

    Ok(())
}

/// Most relation locks held after an append to an existing stream, counted before any other query in the
/// transaction: the stream's partition of `streams` and its key, each in two modes (the skew check reads the
/// stream's `last_commit_id` before the update), and the category's partition of `commits`, its leaf and their
/// indexes.
const APPEND_RELATION_LOCKS: i64 = 9;

/// [`APPEND_RELATION_LOCKS`] in a store with the outbox, which also inserts into `pending`.
const APPEND_OUTBOX_RELATION_LOCKS: i64 = 11;

/// Relation locks the snapshot upsert of `snapshot_after` adds to an append's: `snapshots`, the stream's
/// partition of it and their keys.
const SNAPSHOT_RELATION_LOCKS: i64 = 4;

/// An append with `snapshot_after` locks only its stream's partition of `streams`, even under
/// `force_generic_plan`: the snapshot upsert does not read `streams`. A generic plan of a statement that reads
/// `streams` locks every partition.
#[tokio::test]
async fn an_append_with_a_snapshot_locks_one_streams_partition() -> Result<()> {
    let mut definition = pges_testing::definition(store::Outbox::Disabled);
    definition.stream_partitions = std::num::NonZeroU16::new(64).unwrap();
    let f = Fixture::with_definition(definition).await?;
    let id = stream::StreamId::new_v7();
    let mut conn = f.pool.acquire().await?;
    sqlx::query("SET plan_cache_mode = force_generic_plan")
        .execute(&mut *conn)
        .await?;
    let events = [JobEvent::assigned()];
    let state = common::Job::default();

    // Past the sixth execution, from which the plan cache weighs a generic plan; the last is counted.
    let mut counted = (0, Vec::new());
    for _ in 0..10 {
        let mut tx = conn.begin().await?;
        let mut work = unit_of_work::UnitOfWork::wrap(&f.store, &mut tx);
        work.append(id, version::ExpectedVersion::Any, &events)
            .snapshot_after(&state)
            .await?;
        work.finish().await?;
        let relations: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM pg_locks WHERE pid = pg_backend_pid() AND locktype = 'relation'",
        )
        .fetch_one(&mut *tx)
        .await?;
        let partitions: Vec<String> = sqlx::query_scalar(
            "SELECT DISTINCT c.relname::text FROM pg_locks l JOIN pg_class c ON c.oid = l.relation
             WHERE l.pid = pg_backend_pid() AND c.relnamespace = 'es'::regnamespace
               AND c.relname ~ '^streams_p[0-9]+$'",
        )
        .fetch_all(&mut *tx)
        .await?;
        tx.commit().await?;
        counted = (relations, partitions);
    }
    let snapshots: i64 =
        sqlx::query_scalar("SELECT max(version) FROM es.snapshots WHERE stream_id = $1")
            .bind(id.as_uuid())
            .fetch_one(&f.pool)
            .await?;

    assert_eq!(snapshots, 10, "every append wrote its snapshot");
    let (relations, partitions) = counted;
    assert_eq!(partitions.len(), 1, "{partitions:?}");
    assert!(
        relations <= APPEND_RELATION_LOCKS + SNAPSHOT_RELATION_LOCKS,
        "{relations} relation locks"
    );

    drop(conn);
    f.finish().await
}

/// A store whose `streams` partitions the client cannot name exactly (here one needs quoting) appends
/// through `streams` itself, to every partition.
#[tokio::test]
async fn appends_go_through_streams_when_its_partitions_cannot_be_named() -> Result<()> {
    let f = Fixture::new().await?;
    sqlx::query(r#"ALTER TABLE es.streams_p003 RENAME TO "Streams_P003""#)
        .execute(&f.pool)
        .await?;
    let store =
        store::Store::open(f.pool.clone(), store::StoreConfig::new(common::STORE_NAME)).await?;
    let ids: Vec<stream::StreamId> = (0..64).map(|_| stream::StreamId::new_v7()).collect();
    let mut conn = f.pool.acquire().await?;

    for id in &ids {
        store
            .append_in(
                &mut conn,
                *id,
                version::ExpectedVersion::NoStream,
                &[JobEvent::assigned()],
            )
            .await?;
        store
            .append_in(
                &mut conn,
                *id,
                version::ExpectedVersion::Exact(v(1)),
                &[JobEvent::assigned()],
            )
            .await?;
    }
    let statements = append_statements(&mut conn).await?;
    let in_renamed: i64 =
        sqlx::query_scalar(r#"SELECT count(*) FROM es."Streams_P003" WHERE version = 2"#)
            .fetch_one(&mut *conn)
            .await?;
    let versions: Vec<i64> = sqlx::query_scalar("SELECT version FROM es.streams")
        .fetch_all(&mut *conn)
        .await?;

    assert_eq!(
        statements,
        AppendStatements {
            parent: 1,
            partitions: 0
        }
    );
    assert!(in_renamed > 0, "no stream hashed to the renamed partition");
    assert_eq!(versions, vec![2; ids.len()]);
    drop(conn);
    f.finish().await
}

/// A role granted `streams` but not one of its partitions may append only through `streams` (privileges on a
/// partitioned table do not cover its partitions named directly), so its store does not route appends.
#[tokio::test]
async fn appends_go_through_streams_when_the_role_may_not_write_a_partition() -> Result<()> {
    let f = Fixture::new().await?;
    let role = format!("pges_writer_{}", Uuid::now_v7().simple());
    sqlx::raw_sql(sqlx::AssertSqlSafe(format!(
        "CREATE ROLE {role};
         GRANT USAGE ON SCHEMA es, pges TO {role};
         GRANT ALL ON ALL TABLES IN SCHEMA es, pges TO {role};
         GRANT ALL ON ALL SEQUENCES IN SCHEMA es, pges TO {role};
         REVOKE ALL ON es.streams_p003 FROM {role};"
    )))
    .execute(&f.pool)
    .await?;
    let set_role = format!("SET ROLE {role}");
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(4)
        .after_connect(move |conn, _| {
            let set_role = set_role.clone();
            Box::pin(async move {
                sqlx::raw_sql(sqlx::AssertSqlSafe(set_role))
                    .execute(conn)
                    .await
                    .map(drop)
            })
        })
        .connect(f.db.url())
        .await?;
    let store =
        store::Store::open(pool.clone(), store::StoreConfig::new(common::STORE_NAME)).await?;
    let ids: Vec<stream::StreamId> = (0..64).map(|_| stream::StreamId::new_v7()).collect();
    let mut conn = pool.acquire().await?;

    for id in &ids {
        store
            .append_in(
                &mut conn,
                *id,
                version::ExpectedVersion::NoStream,
                &[JobEvent::assigned()],
            )
            .await?;
        store
            .append_in(
                &mut conn,
                *id,
                version::ExpectedVersion::Exact(v(1)),
                &[JobEvent::assigned()],
            )
            .await?;
    }
    let statements = append_statements(&mut conn).await?;
    drop(conn);
    pool.close().await;
    let in_revoked: i64 =
        sqlx::query_scalar("SELECT count(*) FROM es.streams_p003 WHERE version = 2")
            .fetch_one(&f.pool)
            .await?;
    sqlx::raw_sql(sqlx::AssertSqlSafe(format!(
        "DROP OWNED BY {role}; DROP ROLE {role};"
    )))
    .execute(&f.pool)
    .await?;

    assert_eq!(
        statements,
        AppendStatements {
            parent: 1,
            partitions: 0
        }
    );
    assert!(in_revoked > 0, "no stream hashed to the revoked partition");
    f.finish().await
}

/// A store whose partitions the client can name prepares one append per partition it writes, and none
/// through `streams`.
#[tokio::test]
async fn appends_name_their_streams_partition() -> Result<()> {
    let f = Fixture::new().await?;
    let mut conn = f.pool.acquire().await?;

    for _ in 0..200 {
        f.store
            .append_in(
                &mut conn,
                stream::StreamId::new_v7(),
                version::ExpectedVersion::NoStream,
                &[JobEvent::assigned()],
            )
            .await?;
    }
    let statements = append_statements(&mut conn).await?;
    let partitions_written: i64 =
        sqlx::query_scalar("SELECT count(DISTINCT tableoid) FROM es.streams")
            .fetch_one(&mut *conn)
            .await?;

    assert_eq!(statements.parent, 0);
    assert_eq!(statements.partitions, partitions_written);
    drop(conn);
    f.finish().await
}

/// A store with 256 partitions on a pool with sqlx's default statement cache (100) appends through `streams`
/// with one prepared statement, as before appends were routed; a cache of 256 + 36 routes them.
#[tokio::test]
async fn appends_go_through_streams_when_the_partition_statements_do_not_fit_the_cache()
-> Result<()> {
    let mut definition = pges_testing::definition(store::Outbox::Disabled);
    definition.stream_partitions = std::num::NonZeroU16::new(256).unwrap();
    let f = Fixture::with_definition(definition).await?;
    let options: sqlx::postgres::PgConnectOptions = f.db.url().parse()?;

    for (capacity, expected) in [
        (
            None,
            AppendStatements {
                parent: 1,
                partitions: 0,
            },
        ),
        (
            Some(292),
            AppendStatements {
                parent: 0,
                partitions: 1,
            },
        ),
    ] {
        let pool_options = match capacity {
            None => options.clone(),
            Some(capacity) => options.clone().statement_cache_capacity(capacity),
        };
        let pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(2)
            .connect_with(pool_options)
            .await?;
        let store =
            store::Store::open(pool.clone(), store::StoreConfig::new(common::STORE_NAME)).await?;
        let mut conn = pool.acquire().await?;
        let id = stream::StreamId::new_v7();

        for expected_version in [
            version::ExpectedVersion::NoStream,
            version::ExpectedVersion::Exact(v(1)),
        ] {
            store
                .append_in(&mut conn, id, expected_version, &[JobEvent::assigned()])
                .await?;
        }
        let statements = append_statements(&mut conn).await?;
        drop(conn);
        pool.close().await;

        assert_eq!(statements, expected, "capacity {capacity:?}");
    }

    f.finish().await
}

/// With [`store::AppendRouting::Streams`], appends go through `streams` though the partition statements
/// would fit the statement cache.
#[tokio::test]
async fn appends_go_through_streams_when_routing_is_off() -> Result<()> {
    let f = Fixture::new().await?;
    let mut config = store::StoreConfig::new(common::STORE_NAME);
    config.append_routing = store::AppendRouting::Streams;
    let store = store::Store::open(f.pool.clone(), config).await?;
    let mut conn = f.pool.acquire().await?;

    for _ in 0..32 {
        store
            .append_in(
                &mut conn,
                stream::StreamId::new_v7(),
                version::ExpectedVersion::NoStream,
                &[JobEvent::assigned()],
            )
            .await?;
    }
    let statements = append_statements(&mut conn).await?;

    assert_eq!(
        statements,
        AppendStatements {
            parent: 1,
            partitions: 0
        }
    );
    drop(conn);
    f.finish().await
}

/// The single-append statements a connection has prepared, by the table they update.
#[derive(Debug, PartialEq, Eq)]
struct AppendStatements {
    parent: i64,
    partitions: i64,
}

async fn append_statements(conn: &mut sqlx::PgConnection) -> Result<AppendStatements> {
    let (parent, partitions): (i64, i64) = sqlx::query_as(
        "SELECT count(*) FILTER (WHERE statement LIKE '%UPDATE es.streams s %'),
                count(*) FILTER (WHERE statement LIKE '%UPDATE es.streams\\_p% s %')
         FROM pg_prepared_statements WHERE statement LIKE 'WITH input%'",
    )
    .fetch_one(&mut *conn)
    .await?;

    Ok(AppendStatements { parent, partitions })
}

#[tokio::test]
async fn outbox_stores_enqueue_every_commit() -> Result<()> {
    let f = Fixture::with_outbox(store::Outbox::Enabled).await?;
    let job = stream_with(&f, &[JobEvent::created("a"), JobEvent::assigned()]).await?;
    let orders = [OrderEvent::Placed(Placed { total: 1 })];
    f.store
        .append_many([
            append::AppendEntry::new(
                job,
                version::ExpectedVersion::Exact(v(2)),
                &[JobEvent::assigned()],
            ),
            append::AppendEntry::new(
                stream::StreamId::new_v7(),
                version::ExpectedVersion::NoStream,
                &orders,
            ),
        ])
        .await?;

    let pending: Vec<i16> = sqlx::query_scalar("SELECT event_count FROM es.pending ORDER BY seq")
        .fetch_all(&f.pool)
        .await?;
    assert_eq!(pending.len(), 3);
    assert_eq!(pending[0], 2);
    f.finish().await
}

/// `append_many` locks stream rows in stream id order whatever join the planner picks. Writers on a pool that
/// forbids nested loops and merge joins (so the update scans `streams` in physical order) race writers on a
/// normal pool, over overlapping stream sets given in opposite orders. None may fail with a deadlock.
#[tokio::test]
async fn concurrent_append_many_never_deadlocks() -> Result<()> {
    let f = Fixture::new().await?;
    let hashed_pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(8)
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
    let hashed = store::Store::open(
        hashed_pool.clone(),
        store::StoreConfig::new(common::STORE_NAME),
    )
    .await?;
    f.store.register::<JobEvent>().await?;
    hashed.register::<JobEvent>().await?;

    let mut streams = Vec::new();
    for _ in 0..8 {
        streams.push(stream_with(&f, &[JobEvent::created("a")]).await?);
    }
    let streams = std::sync::Arc::new(streams);

    let writers: Vec<_> = (0..16)
        .map(|writer| {
            let store = if writer % 2 == 0 {
                f.store.clone()
            } else {
                hashed.clone()
            };
            let streams = streams.clone();
            tokio::spawn(async move {
                let jobs = [JobEvent::assigned()];
                let mut written = 0;
                for round in 0..15 {
                    let mut ids: Vec<stream::StreamId> =
                        streams.iter().copied().skip((writer + round) % 3).collect();
                    if (writer / 2 + round) % 2 == 1 {
                        ids.reverse();
                    }
                    let entries = ids.iter().map(|id| {
                        append::AppendEntry::new(*id, version::ExpectedVersion::Any, &jobs)
                    });
                    store.append_many(entries).await?;
                    written += 1;
                }

                Ok::<_, append::AppendError>(written)
            })
        })
        .collect();

    let mut written = 0;
    for writer in writers {
        match writer.await? {
            Ok(n) => written += n,
            Err(error) => panic!("append_many failed: {error}"),
        }
    }
    assert_eq!(written, 16 * 15);

    hashed_pool.close().await;
    f.finish().await
}

/// How a contended writer appends with `ExpectedVersion::Any`.
#[derive(Debug, Clone, Copy)]
enum AnyWriter {
    Append,
    AppendMany,
    UnitOfWork,
}

const ANY_WRITERS: usize = 16;
const ANY_APPENDS: usize = 25;

/// Many writers append with `Any` to one new stream at once, each through `writer` (`append_many` also
/// appends to a second new stream). Every append succeeds: a generated commit id that falls behind the
/// stream's last one is replaced under the stream's row lock, and a batch whose retry would wait for a lock
/// out of stream id order starts over (`append/try_lock_stream.sql`). Commit ids increase with the
/// versions.
async fn contended_any_appends_succeed(writer: AnyWriter) -> Result<()> {
    let f = Fixture::new().await?;
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(ANY_WRITERS as u32)
        .connect(f.db.url())
        .await?;
    let store =
        store::Store::open(pool.clone(), store::StoreConfig::new(common::STORE_NAME)).await?;
    store.register::<JobEvent>().await?;
    let id = stream::StreamId::new_v7();
    let other = stream::StreamId::new_v7();

    let writers: Vec<_> = (0..ANY_WRITERS)
        .map(|_| {
            let store = store.clone();
            tokio::spawn(async move {
                let jobs = [JobEvent::assigned()];
                for _ in 0..ANY_APPENDS {
                    match writer {
                        AnyWriter::Append => {
                            store
                                .append(id, version::ExpectedVersion::Any, &jobs)
                                .await?;
                        }
                        AnyWriter::AppendMany => {
                            store
                                .append_many([
                                    append::AppendEntry::new(
                                        id,
                                        version::ExpectedVersion::Any,
                                        &jobs,
                                    ),
                                    append::AppendEntry::new(
                                        other,
                                        version::ExpectedVersion::Any,
                                        &jobs,
                                    ),
                                ])
                                .await?;
                        }
                        AnyWriter::UnitOfWork => {
                            let mut uow = store.begin().await?;
                            uow.append(id, version::ExpectedVersion::Any, &jobs).await?;
                            uow.commit().await?;
                        }
                    }
                }

                Ok::<_, anyhow::Error>(())
            })
        })
        .collect();
    for writer in writers {
        writer.await??;
    }

    let commit_ids: Vec<Uuid> = sqlx::query_scalar(
        "SELECT commit_id FROM es.commits WHERE stream_id = $1 ORDER BY last_version",
    )
    .bind(id.as_uuid())
    .fetch_all(&pool)
    .await?;
    assert_eq!(commit_ids.len(), ANY_WRITERS * ANY_APPENDS);
    assert!(commit_ids.windows(2).all(|pair| pair[0] < pair[1]));

    pool.close().await;
    f.finish().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn contended_any_appends_all_succeed() -> Result<()> {
    contended_any_appends_succeed(AnyWriter::Append).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn contended_any_append_many_all_succeed() -> Result<()> {
    contended_any_appends_succeed(AnyWriter::AppendMany).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn contended_any_appends_in_units_of_work_all_succeed() -> Result<()> {
    contended_any_appends_succeed(AnyWriter::UnitOfWork).await
}

/// A retried append on a hot stream does not make the appends queued behind it retry too. It takes the id
/// just above the stream's last one, so the queued appends, whose ids were generated later, stay above it
/// and succeed on their first run. With a fresh id instead, nearly every append of 16 writers ran twice.
/// Appends that overtake a queued one still make it retry, so the bound is loose: about 60–73% ran twice
/// on a loaded box, against 97% with fresh ids. Counts the append statement's runs through the function
/// statistics of `pges.check_commit_id`, which each run calls once.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_retried_append_does_not_make_the_queued_ones_retry() -> Result<()> {
    const WRITERS: usize = 16;
    const APPENDS: usize = 25;

    let f = Fixture::new().await?;
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "ALTER DATABASE {} SET track_functions = 'pl'",
        f.db.name()
    )))
    .execute(&f.pool)
    .await?;
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(WRITERS as u32)
        .connect(f.db.url())
        .await?;
    let store =
        store::Store::open(pool.clone(), store::StoreConfig::new(common::STORE_NAME)).await?;
    store.register::<JobEvent>().await?;
    let id = stream::StreamId::new_v7();
    store
        .append(
            id,
            version::ExpectedVersion::NoStream,
            &[JobEvent::assigned()],
        )
        .await?;

    let writers: Vec<_> = (0..WRITERS)
        .map(|_| {
            let store = store.clone();
            tokio::spawn(async move {
                for _ in 0..APPENDS {
                    store
                        .append(id, version::ExpectedVersion::Any, &[JobEvent::assigned()])
                        .await?;
                }

                Ok::<_, anyhow::Error>(())
            })
        })
        .collect();
    for writer in writers {
        writer.await??;
    }
    pool.close().await;

    let appends = (WRITERS * APPENDS + 1) as i64;
    let runs = statement_runs_after_backends_exit(&f, appends).await?;
    let retried = runs - appends;
    assert!(
        retried * 8 < appends * 7,
        "{retried} of {appends} appends ran the statement again"
    );

    f.finish().await
}

/// The calls of `pges.check_commit_id` once the closed connections' backends have reported them: at least
/// `at_least`, and unchanged for a while.
async fn statement_runs_after_backends_exit(f: &Fixture, at_least: i64) -> Result<i64> {
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    let mut previous = -1;
    loop {
        let mut conn = f.db.connect().await?;
        let calls: i64 = sqlx::query_scalar(
            "SELECT coalesce(sum(calls), 0)::int8 FROM pg_stat_user_functions \
             WHERE schemaname = 'pges' AND funcname = 'check_commit_id'",
        )
        .fetch_one(&mut conn)
        .await?;
        conn.close().await?;
        if calls >= at_least && calls == previous {
            return Ok(calls);
        }
        assert!(
            std::time::Instant::now() < deadline,
            "check_commit_id calls stayed at {calls}, below {at_least} or still changing"
        );

        previous = calls;
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

/// Registering an event type needs a second pooled connection. A unit of work holding the only connection of
/// its pool fails at once instead of waiting for the pool's acquire timeout, and works once the type is
/// registered.
#[tokio::test]
async fn units_of_work_on_a_single_connection_pool_need_registered_types() -> Result<()> {
    let f = Fixture::new().await?;
    let single = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .connect(f.db.url())
        .await?;
    let s = store::Store::open(single.clone(), store::StoreConfig::new(common::STORE_NAME)).await?;
    let id = stream::StreamId::new_v7();

    let mut uow = s.begin().await?;
    let events = [JobEvent::created("a")];
    let attempt = uow.append(id, version::ExpectedVersion::NoStream, &events);
    let error = tokio::time::timeout(Duration::from_secs(5), attempt)
        .await?
        .unwrap_err();
    assert!(
        matches!(error, append::AppendError::NoSpareConnection(c) if c == common::JOBS),
        "{error}"
    );
    uow.rollback().await?;

    s.register::<JobEvent>().await?;
    let mut uow = s.begin().await?;
    uow.append(
        id,
        version::ExpectedVersion::NoStream,
        &[JobEvent::created("a")],
    )
    .await?;
    uow.commit().await?;

    // A store opened after the types were registered (a restarted process) loads their ids and needs no
    // second connection either.
    let reopened =
        store::Store::open(single.clone(), store::StoreConfig::new(common::STORE_NAME)).await?;
    let mut uow = reopened.begin().await?;
    uow.append(
        stream::StreamId::new_v7(),
        version::ExpectedVersion::NoStream,
        &[JobEvent::created("b")],
    )
    .await?;
    uow.commit().await?;

    single.close().await;
    f.finish().await
}

/// A savepoint begin on an aborted transaction fails, and sqlx then ends the whole transaction: what follows
/// would run in autocommit. A unit of work refuses to append after it, and fails to commit or finish.
#[tokio::test]
async fn units_of_work_fail_after_a_failed_savepoint_begin() -> Result<()> {
    let f = Fixture::new().await?;
    let ids = [(); 4].map(|_| stream::StreamId::new_v7());
    let created = [JobEvent::created("a")];
    let none = version::ExpectedVersion::NoStream;

    let mut uow = f.store.begin().await?;
    uow.append(ids[0], none, &created).await?;
    sqlx::query("SELECT 1 / 0")
        .execute(uow.conn())
        .await
        .unwrap_err();
    uow.conn().begin().await.unwrap_err();
    let error = uow.append(ids[1], none, &created).await.unwrap_err();
    assert!(
        matches!(error, append::AppendError::TransactionEnded),
        "{error}"
    );
    let error = uow.commit().await.unwrap_err();
    assert!(
        matches!(error, unit_of_work::TransactionError::Aborted),
        "{error}"
    );

    let mut conn = f.pool.acquire().await?;
    let mut tx = conn.begin().await?;
    let mut uow = unit_of_work::UnitOfWork::wrap(&f.store, &mut tx);
    uow.append(ids[2], none, &created).await?;
    sqlx::query("SELECT 1 / 0")
        .execute(uow.conn())
        .await
        .unwrap_err();
    uow.conn().begin().await.unwrap_err();
    let error = uow.append(ids[3], none, &created).await.unwrap_err();
    assert!(
        matches!(error, append::AppendError::TransactionEnded),
        "{error}"
    );
    let error = uow.finish().await.unwrap_err();
    assert!(
        matches!(error, unit_of_work::TransactionError::Aborted),
        "{error}"
    );
    tx.rollback().await?;
    drop(conn);

    let ids: Vec<Uuid> = ids.iter().map(|id| id.as_uuid()).collect();
    let written: i64 =
        sqlx::query_scalar("SELECT count(*) FROM es.streams WHERE stream_id = ANY($1)")
            .bind(&ids)
            .fetch_one(&f.pool)
            .await?;
    assert_eq!(written, 0, "the ended units of work wrote nothing");
    f.finish().await
}

/// PostgreSQL turns the COMMIT of a transaction aborted by a failed statement into a ROLLBACK and reports
/// success. A unit of work whose caller statement failed must not.
#[tokio::test]
async fn units_of_work_refuse_to_commit_after_a_failed_caller_statement() -> Result<()> {
    let f = Fixture::new().await?;
    let id = stream::StreamId::new_v7();

    let mut uow = f.store.begin().await?;
    uow.append(
        id,
        version::ExpectedVersion::NoStream,
        &[JobEvent::created("a")],
    )
    .await?;
    let failed = sqlx::query("SELECT 1 / 0").execute(uow.conn()).await;
    assert!(failed.is_err());
    let error = uow.commit().await.unwrap_err();
    assert!(
        matches!(error, unit_of_work::TransactionError::Aborted),
        "{error}"
    );

    let mut conn = f.pool.acquire().await?;
    let mut tx = conn.begin().await?;
    let mut uow = unit_of_work::UnitOfWork::wrap(&f.store, &mut tx);
    let failed = sqlx::query("SELECT 1 / 0").execute(uow.conn()).await;
    assert!(failed.is_err());
    let error = uow.finish().await.unwrap_err();
    assert!(
        matches!(error, unit_of_work::TransactionError::Aborted),
        "{error}"
    );
    tx.rollback().await?;
    drop(conn);

    let exists: bool =
        sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM es.streams WHERE stream_id = $1)")
            .bind(id.as_uuid())
            .fetch_one(&f.pool)
            .await?;
    assert!(!exists, "the aborted unit of work wrote nothing");
    f.finish().await
}

/// `pges.check_commit_id` refuses an id that is not a UUIDv7 with SQLSTATE 22023, even with the infinite
/// tolerance an append gives an id at or below its stream's last one; the crate only sends v7 ids.
#[tokio::test]
async fn the_commit_id_check_refuses_ids_that_are_not_uuidv7() -> Result<()> {
    let f = Fixture::new().await?;
    let nil = "00000000-0000-0000-0000-000000000000";
    let v1 = "6ba7b810-9dad-11d1-80b4-00c04fd430c8";
    let v4 = "6ba7b810-9dad-41d1-80b4-00c04fd430c8";

    for id in [nil, v1, v4] {
        let error = sqlx::query("SELECT pges.check_commit_id($1::uuid, 'infinity')")
            .bind(id)
            .execute(&f.pool)
            .await
            .unwrap_err();
        assert_eq!(
            error.as_database_error().and_then(|e| e.code()).as_deref(),
            Some("22023"),
            "{id}: {error}"
        );
    }
    let v7 = commit::CommitId::generate().as_uuid();
    let checked: Uuid = sqlx::query_scalar("SELECT pges.check_commit_id($1, '5 seconds')")
        .bind(v7)
        .fetch_one(&f.pool)
        .await?;
    assert_eq!(checked, v7);
    f.finish().await
}
