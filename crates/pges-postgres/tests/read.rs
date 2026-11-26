//! Reading streams and loading aggregates: round trips, snapshots and events from newer writers.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

mod common;

use anyhow::Result;
use futures_util::TryStreamExt;

use pges_core::{event, metadata, stream, version};
use pges_postgres::{append, load, read};

use common::{
    BusyJob, BusyJobV2, Closed, Fixture, Job, JobEvent, NewerJobEvent, OrderEvent, Placed, v,
};

async fn read_all<E: event::Event>(
    f: &Fixture,
    id: stream::StreamId,
    from: version::Version,
) -> Result<Vec<stream::StreamEvent<E>>, read::ReadError> {
    f.store.read_stream::<E>(id, from).try_collect().await
}

#[tokio::test]
async fn read_stream_returns_events_with_versions_and_metadata() -> Result<()> {
    let f = Fixture::new().await?;
    let id = stream::StreamId::new_v7();
    let mut meta = metadata::Metadata::default();
    meta.actor = Some("user:7".into());
    let first = f
        .store
        .append(
            id,
            version::ExpectedVersion::NoStream,
            &[JobEvent::created("a"), JobEvent::assigned()],
        )
        .metadata(meta.clone())
        .await?;
    let second = f
        .store
        .append(id, first.expected(), &[JobEvent::created("b")])
        .await?;

    let events = read_all::<JobEvent>(&f, id, version::Version::FIRST).await?;

    assert_eq!(
        events.iter().map(|e| e.version).collect::<Vec<_>>(),
        vec![v(1), v(2), v(3)]
    );
    assert_eq!(events[0].event, JobEvent::created("a"));
    assert_eq!(events[2].event, JobEvent::created("b"));
    assert_eq!(events[0].commit_id, first.commit_id);
    assert_eq!(events[1].commit_id, first.commit_id);
    assert_eq!(events[2].commit_id, second.commit_id);
    assert_eq!(events[0].metadata.actor.as_deref(), Some("user:7"));
    assert_eq!(events[2].metadata.actor, None);
    assert_eq!(events[0].category, common::JOBS);
    assert_eq!(events[0].recorded_at, first.commit_id.recorded_at());

    let from_two = read_all::<JobEvent>(&f, id, v(2)).await?;
    assert_eq!(
        from_two.iter().map(|e| e.version).collect::<Vec<_>>(),
        vec![v(2), v(3)],
        "starts inside the first commit"
    );
    let missing =
        read_all::<JobEvent>(&f, stream::StreamId::new_v7(), version::Version::FIRST).await?;
    assert!(missing.is_empty());
    f.finish().await
}

#[tokio::test]
async fn long_streams_are_read_across_pages() -> Result<()> {
    let f = Fixture::new().await?;
    let id = stream::StreamId::new_v7();
    let mut expected = version::ExpectedVersion::NoStream;
    for i in 0..600 {
        let result = f
            .store
            .append(id, expected, &[JobEvent::created(&i.to_string())])
            .await?;
        expected = result.expected();
    }

    let events = read_all::<JobEvent>(&f, id, version::Version::FIRST).await?;
    let loaded = f.store.load::<Job>(id).await?;

    assert_eq!(events.len(), 600);
    assert!(events.windows(2).all(|w| w[0].version < w[1].version));
    assert_eq!(loaded.version(), Some(v(600)));
    assert_eq!(loaded.state().titles.len(), 600);
    assert_eq!(loaded.state().titles[599], "599");
    f.finish().await
}

/// A read ends at the stream's version when it began, even when a later page is fetched after more appends.
#[tokio::test]
async fn a_read_ends_at_the_version_it_began_at() -> Result<()> {
    let f = Fixture::new().await?;
    let id = stream::StreamId::new_v7();
    let mut expected = version::ExpectedVersion::NoStream;
    for i in 0..300 {
        let result = f
            .store
            .append(id, expected, &[JobEvent::created(&i.to_string())])
            .await?;
        expected = result.expected();
    }

    let mut read = std::pin::pin!(f.store.read_stream::<JobEvent>(id, version::Version::FIRST));
    let first = read.try_next().await?.unwrap();
    f.store
        .append(id, expected, &[JobEvent::created("late")])
        .await?;
    let rest: Vec<_> = read.try_collect().await?;

    assert_eq!(first.version, v(1));
    assert_eq!(rest.len(), 299);
    assert_eq!(rest.last().unwrap().version, v(300));
    assert_eq!(read_all::<JobEvent>(&f, id, v(300)).await?.len(), 2);
    f.finish().await
}

#[tokio::test]
async fn reading_a_stream_as_another_category_fails() -> Result<()> {
    let f = Fixture::new().await?;
    let id = stream::StreamId::new_v7();
    f.store
        .append(
            id,
            version::ExpectedVersion::NoStream,
            &[JobEvent::created("a")],
        )
        .await?;

    let error = read_all::<OrderEvent>(&f, id, version::Version::FIRST)
        .await
        .unwrap_err();

    assert!(
        matches!(error, read::ReadError::CategoryMismatch { stream_category, .. } if stream_category == common::JOBS),
        "{error}"
    );
    f.finish().await
}

#[tokio::test]
async fn unknown_events_fail_or_are_skipped() -> Result<()> {
    let f = Fixture::new().await?;
    let id = stream::StreamId::new_v7();
    f.store
        .append(
            id,
            version::ExpectedVersion::NoStream,
            &[
                NewerJobEvent::Known(JobEvent::created("a")),
                NewerJobEvent::Closed(Closed {
                    reason: "done".into(),
                }),
                NewerJobEvent::Known(JobEvent::assigned()),
            ],
        )
        .await?;

    let error = read_all::<JobEvent>(&f, id, version::Version::FIRST)
        .await
        .unwrap_err();
    assert!(
        matches!(
            &error,
            read::ReadError::UnknownEvent { version, type_name, schema_version, .. }
                if *version == v(2) && type_name == "job.closed" && schema_version.get() == 1
        ),
        "{error}"
    );

    let skipped: Vec<_> = f
        .store
        .read_stream_with::<JobEvent>(id, version::Version::FIRST, event::OnUnknown::Skip)
        .try_collect()
        .await?;
    assert_eq!(
        skipped.iter().map(|e| e.version).collect::<Vec<_>>(),
        vec![v(1), v(3)]
    );

    let error = f.store.load::<Job>(id).await.unwrap_err();
    assert!(
        matches!(
            error,
            load::LoadError::Read(read::ReadError::UnknownEvent { .. })
        ),
        "{error}"
    );

    let newer = read_all::<NewerJobEvent>(&f, id, version::Version::FIRST).await?;
    assert_eq!(newer.len(), 3);
    f.finish().await
}

/// A store opened before a writer registered a new event type learns its id on read.
#[tokio::test]
async fn event_types_registered_elsewhere_are_reloaded() -> Result<()> {
    let f = Fixture::new().await?;
    let reader = pges_postgres::store::Store::open(
        f.pool.clone(),
        pges_postgres::store::StoreConfig::new(common::STORE_NAME),
    )
    .await?;
    reader.register::<JobEvent>().await?;
    let id = stream::StreamId::new_v7();
    f.store
        .append(
            id,
            version::ExpectedVersion::NoStream,
            &[NewerJobEvent::Closed(Closed { reason: "x".into() })],
        )
        .await?;

    let events: Vec<stream::StreamEvent<NewerJobEvent>> = reader
        .read_stream::<NewerJobEvent>(id, version::Version::FIRST)
        .try_collect()
        .await?;

    assert_eq!(events.len(), 1);
    f.finish().await
}

#[tokio::test]
async fn load_folds_the_stream_and_reports_the_expected_version() -> Result<()> {
    let f = Fixture::new().await?;
    let id = stream::StreamId::new_v7();

    let empty = f.store.load::<Job>(id).await?;
    assert_eq!(empty.version(), None);
    assert_eq!(empty.expected(), version::ExpectedVersion::NoStream);
    assert_eq!(empty.state(), &Job::default());

    let result = f
        .store
        .append(
            id,
            empty.expected(),
            &[
                JobEvent::created("a"),
                JobEvent::assigned(),
                JobEvent::assigned(),
            ],
        )
        .await?;
    let job = f.store.load::<Job>(id).await?;

    assert_eq!(job.version(), Some(v(3)));
    assert_eq!(job.expected(), version::ExpectedVersion::Exact(v(3)));
    assert_eq!(job.commit_id(), Some(result.commit_id));
    assert_eq!(job.stream_id(), id);
    assert_eq!(job.state().titles, vec!["a".to_string()]);
    assert_eq!(job.state().assignments, 2);
    f.finish().await
}

#[tokio::test]
async fn snapshots_are_saved_and_loaded_from() -> Result<()> {
    let f = Fixture::new().await?;
    let id = stream::StreamId::new_v7();
    f.store
        .append(
            id,
            version::ExpectedVersion::NoStream,
            &[JobEvent::created("a"), JobEvent::assigned()],
        )
        .await?;
    let job = f.store.load_snapshotted::<Job>(id).await?;
    f.store.save_snapshot(&job).await?;
    f.store
        .append(id, job.expected(), &[JobEvent::created("b")])
        .await?;

    // Change the snapshot behind the store's back, to prove the load starts from it.
    sqlx::query(
        r#"UPDATE es.snapshots SET state = '{"titles":["from-snapshot"],"assignments":5}' WHERE stream_id = $1"#,
    )
    .bind(id.as_uuid())
    .execute(&f.pool)
    .await?;
    let loaded = f.store.load_snapshotted::<Job>(id).await?;

    assert_eq!(loaded.version(), Some(v(3)));
    assert_eq!(
        loaded.state().titles,
        vec!["from-snapshot".to_string(), "b".to_string()]
    );
    assert_eq!(loaded.state().assignments, 5);
    f.finish().await
}

#[tokio::test]
async fn snapshot_after_writes_in_the_append_transaction() -> Result<()> {
    let f = Fixture::new().await?;
    let id = stream::StreamId::new_v7();
    let events = [JobEvent::created("a")];
    let mut state = Job::default();
    for event in &events {
        pges_core::aggregate::Aggregate::apply(&mut state, event);
    }

    let result = f
        .store
        .append(id, version::ExpectedVersion::NoStream, &events)
        .snapshot_after(&state)
        .await?;

    let (version, commit_id): (i64, uuid::Uuid) =
        sqlx::query_as("SELECT version, commit_id FROM es.snapshots WHERE stream_id = $1")
            .bind(id.as_uuid())
            .fetch_one(&f.pool)
            .await?;
    assert_eq!(version, 1);
    assert_eq!(commit_id, result.commit_id.as_uuid());
    let loaded = f.store.load_snapshotted::<Job>(id).await?;
    assert_eq!(loaded.state(), &state);

    // The append fails, so no snapshot is written either.
    let error = f
        .store
        .append(id, version::ExpectedVersion::NoStream, &events)
        .snapshot_after(&state)
        .await
        .unwrap_err();
    assert!(
        matches!(error, append::AppendError::Conflict { .. }),
        "{error}"
    );
    f.finish().await
}

#[tokio::test]
async fn every_n_policy_snapshots_on_load_and_ignores_other_state_versions() -> Result<()> {
    let f = Fixture::new().await?;
    let id = stream::StreamId::new_v7();
    let snapshot_version = |pool: sqlx::PgPool| async move {
        sqlx::query_scalar::<_, i64>("SELECT version FROM es.snapshots WHERE stream_id = $1")
            .bind(id.as_uuid())
            .fetch_optional(&pool)
            .await
    };
    f.store
        .append(
            id,
            version::ExpectedVersion::NoStream,
            &[JobEvent::created("a"), JobEvent::assigned()],
        )
        .await?;

    let loaded = f.store.load_snapshotted::<BusyJob>(id).await?;
    assert_eq!(loaded.state().events, 2);
    assert_eq!(
        snapshot_version(f.pool.clone()).await?,
        None,
        "2 replayed < 3"
    );

    f.store
        .append(id, loaded.expected(), &[JobEvent::assigned()])
        .await?;
    let loaded = f.store.load_snapshotted::<BusyJob>(id).await?;
    assert_eq!(loaded.state().events, 3);
    assert_eq!(
        snapshot_version(f.pool.clone()).await?,
        Some(3),
        "3 replayed"
    );

    let rebuilt = f.store.load_snapshotted::<BusyJobV2>(id).await?;
    assert_eq!(
        rebuilt.state().events,
        3,
        "a snapshot of another state version is ignored"
    );
    f.finish().await
}

#[tokio::test]
async fn a_snapshot_is_replaced_only_by_a_newer_or_a_higher_state_version() -> Result<()> {
    let f = Fixture::new().await?;
    let id = stream::StreamId::new_v7();
    let stored = |pool: sqlx::PgPool| async move {
        sqlx::query_as::<_, (i32, i64)>(
            "SELECT state_version, version FROM es.snapshots WHERE stream_id = $1",
        )
        .bind(id.as_uuid())
        .fetch_one(&pool)
        .await
    };
    f.store
        .append(
            id,
            version::ExpectedVersion::NoStream,
            &[
                JobEvent::created("a"),
                JobEvent::assigned(),
                JobEvent::assigned(),
            ],
        )
        .await?;
    let stale_v2 = f.store.load_snapshotted::<BusyJobV2>(id).await?;
    let stale_v1 = f.store.load_snapshotted::<BusyJob>(id).await?;
    assert_eq!(stored(f.pool.clone()).await?, (1, 3), "saved by the policy");
    f.store
        .append(id, stale_v1.expected(), &[JobEvent::assigned()])
        .await?;
    let v1 = f.store.load_snapshotted::<BusyJob>(id).await?;
    f.store.save_snapshot(&v1).await?;
    assert_eq!(stored(f.pool.clone()).await?, (1, 4));

    f.store.save_snapshot(&stale_v2).await?;
    assert_eq!(
        stored(f.pool.clone()).await?,
        (1, 4),
        "a higher state version at a lower version does not replace it"
    );
    let v2 = f.store.load_snapshotted::<BusyJobV2>(id).await?;
    f.store.save_snapshot(&v2).await?;
    assert_eq!(stored(f.pool.clone()).await?, (2, 4));

    f.store.save_snapshot(&v1).await?;
    f.store.save_snapshot(&stale_v1).await?;
    f.store.save_snapshot(&stale_v2).await?;
    assert_eq!(
        stored(f.pool.clone()).await?,
        (2, 4),
        "a lower state version, or an older snapshot, never replaces it"
    );

    // Truncated up to the snapshot, as retention would: the current code still loads.
    sqlx::query("UPDATE es.streams SET floor_version = 4 WHERE stream_id = $1")
        .bind(id.as_uuid())
        .execute(&f.pool)
        .await?;
    let loaded = f.store.load_snapshotted::<BusyJobV2>(id).await?;
    assert_eq!((loaded.version(), loaded.state().events), (Some(v(4)), 4));
    f.finish().await
}

#[tokio::test]
async fn deleted_and_truncated_streams_are_reported() -> Result<()> {
    let f = Fixture::new().await?;
    let deleted = stream::StreamId::new_v7();
    let truncated = stream::StreamId::new_v7();
    for id in [deleted, truncated] {
        f.store
            .append(
                id,
                version::ExpectedVersion::NoStream,
                &[JobEvent::created("a"), JobEvent::assigned()],
            )
            .await?;
    }
    sqlx::query("UPDATE es.streams SET state = 2 WHERE stream_id = $1")
        .bind(deleted.as_uuid())
        .execute(&f.pool)
        .await?;
    sqlx::query("UPDATE es.streams SET floor_version = 1 WHERE stream_id = $1")
        .bind(truncated.as_uuid())
        .execute(&f.pool)
        .await?;

    let error = read_all::<JobEvent>(&f, deleted, version::Version::FIRST)
        .await
        .unwrap_err();
    assert!(
        matches!(error, read::ReadError::StreamDeleted(id) if id == deleted),
        "{error}"
    );
    let error = f.store.load::<Job>(truncated).await.unwrap_err();
    assert!(
        matches!(error, load::LoadError::Read(read::ReadError::Truncated { floor, .. }) if floor == v(1)),
        "{error}"
    );
    let after_floor = read_all::<JobEvent>(&f, truncated, v(2)).await?;
    assert_eq!(after_floor.len(), 1);
    f.finish().await
}

#[tokio::test]
async fn orders_round_trip_through_their_own_partition() -> Result<()> {
    let f = Fixture::new().await?;
    let id = stream::StreamId::from_key(common::ORDERS, "ORDER-1");
    f.store
        .append(
            id.clone(),
            version::ExpectedVersion::NoStream,
            &[OrderEvent::Placed(Placed { total: 12 })],
        )
        .await?;

    let events = read_all::<OrderEvent>(&f, id.id, version::Version::FIRST).await?;

    assert_eq!(events.len(), 1);
    assert_eq!(events[0].event, OrderEvent::Placed(Placed { total: 12 }));
    assert_eq!(events[0].category, common::ORDERS);
    f.finish().await
}
