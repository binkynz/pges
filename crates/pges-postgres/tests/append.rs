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

/// Whether `streams` has planner statistics when the appends start.
#[derive(Debug, Clone, Copy)]
enum StreamStats {
    /// Never analyzed: the planner guesses the partitions' sizes.
    Never,
    /// Analyzed with about 30 streams per partition.
    Small,
}

/// An append locks the stream's partition of `streams` and its index, not every partition: with 64
/// partitions, locking them all would take 128 of the 256 fast-path lock slots of a backend. The plan cache
/// keeps the custom plans that prune, with or without statistics. (Analyzed while nearly empty, a few streams
/// per partition, the generic plan looks cheaper and it locks every partition, though not their indexes.)
#[tokio::test]
async fn an_append_locks_only_the_streams_partition_it_writes() -> Result<()> {
    for (outbox, stats) in [
        (store::Outbox::Disabled, StreamStats::Never),
        (store::Outbox::Enabled, StreamStats::Never),
        (store::Outbox::Disabled, StreamStats::Small),
    ] {
        let mut definition = pges_testing::definition(outbox);
        definition.stream_partitions = std::num::NonZeroU16::new(64).unwrap();
        let f = Fixture::with_definition(definition).await?;
        if let StreamStats::Small = stats {
            sqlx::query(
                "INSERT INTO es.streams (version, stream_id, first_commit_id, last_commit_id, category_id)
                 SELECT 1, gen_random_uuid(), uuidv7(), uuidv7(), 1 FROM generate_series(1, 2000)",
            )
            .execute(&f.pool)
            .await?;
            sqlx::query("ANALYZE es.streams").execute(&f.pool).await?;
        }
        let id = stream::StreamId::new_v7();
        let mut conn = f.pool.acquire().await?;

        // The plan cache plans a generic plan once, at the sixth execution, to weigh it against the custom
        // ones; that planning locks every partition. The appends after it get custom plans.
        for _ in 0..10 {
            f.store
                .append_in(
                    &mut conn,
                    id,
                    version::ExpectedVersion::Any,
                    &[JobEvent::assigned()],
                )
                .await?;
        }
        let mut tx = conn.begin().await?;
        f.store
            .append_in(
                &mut tx,
                id,
                version::ExpectedVersion::Any,
                &[JobEvent::assigned()],
            )
            .await?;
        let locked: Vec<String> = sqlx::query_scalar(
            "SELECT DISTINCT c.relname::text FROM pg_locks l JOIN pg_class c ON c.oid = l.relation
             WHERE l.pid = pg_backend_pid() AND c.relnamespace = 'es'::regnamespace
               AND c.relname ~ '^streams_p[0-9]+'
             ORDER BY 1",
        )
        .fetch_all(&mut *tx)
        .await?;
        tx.rollback().await?;

        assert_eq!(locked.len(), 2, "{outbox:?}, {stats:?}: {locked:?}");
        assert_eq!(
            format!("{}_pkey", locked[0]),
            locked[1],
            "{outbox:?}, {stats:?}"
        );
        drop(conn);
        f.finish().await?;
    }

    Ok(())
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
