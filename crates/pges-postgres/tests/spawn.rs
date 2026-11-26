//! Every operation's future can run on another task: `tokio::spawn` needs it to be `Send` for any
//! lifetime, which sqlx futures holding borrowed connections do not always satisfy.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

mod common;

use anyhow::Result;
use futures_util::TryStreamExt;

use pges_core::{metadata, stream, version};
use pges_postgres::{admin, ambient, append, limit, migrate, retention, store, unit_of_work};

use common::{Fixture, Job, JobEvent};

#[tokio::test]
async fn operations_run_on_spawned_tasks() -> Result<()> {
    let f = Fixture::new().await?;
    let id = stream::StreamId::new_v7();

    let pool = f.pool.clone();
    let opened = tokio::spawn(async move {
        let mut conn = pool.acquire().await.unwrap();
        migrate::run(
            &mut conn,
            &common::STORE_NAME,
            &store::StoreDefinition::default(),
        )
        .await
        .unwrap();
        store::Store::open(pool, store::StoreConfig::new(common::STORE_NAME)).await
    })
    .await??;

    let s = opened.clone();
    tokio::spawn(async move {
        s.register::<JobEvent>().await.unwrap();
        let events = [JobEvent::created("a")];
        let created = s
            .append(id, version::ExpectedVersion::NoStream, &events)
            .await
            .unwrap();

        let mut uow = s.begin().await.unwrap();
        uow.append(id, created.expected(), &[JobEvent::assigned()])
            .await
            .unwrap();
        let jobs = [JobEvent::assigned()];
        uow.append_many([append::AppendEntry::new(
            stream::StreamId::new_v7(),
            version::ExpectedVersion::NoStream,
            &jobs,
        )])
        .await
        .unwrap();
        uow.commit().await.unwrap();

        let mut conn = s.pool().acquire().await.unwrap();
        let mut wrapped = unit_of_work::UnitOfWork::wrap(&s, &mut conn);
        wrapped
            .append(id, version::ExpectedVersion::Any, &[JobEvent::assigned()])
            .await
            .unwrap();
        wrapped.finish().await.unwrap();
        s.append_in(&mut conn, id, version::ExpectedVersion::Any, &jobs)
            .await
            .unwrap();
        s.append_many([append::AppendEntry::new(
            id,
            version::ExpectedVersion::Any,
            &jobs,
        )])
        .await
        .unwrap();

        let job = s.load_snapshotted::<Job>(id).await.unwrap();
        s.save_snapshot(&job).await.unwrap();
        let _ = s.load::<Job>(id).await.unwrap();
        let events: Vec<_> = s
            .read_stream::<JobEvent>(id, version::Version::FIRST)
            .try_collect()
            .await
            .unwrap();
        assert_eq!(events.len(), 5);

        s.admin().maintain().await.unwrap();
        s.admin()
            .register_category(common::JOBS, admin::CategoryConfig::default())
            .await
            .unwrap();
        s.admin()
            .change_bucket(common::JOBS, admin::Bucket::Month)
            .await
            .unwrap();
        s.admin()
            .set_retention(common::JOBS, retention::Retention::Forever)
            .await
            .unwrap();
        s.admin()
            .set_default_retention(retention::Retention::Forever)
            .await
            .unwrap();
        let config = retention::RetentionConfig::new(limit::WriterRoles::one("pges_no_such_role"));
        let error = s.admin().apply_retention(&config).await.unwrap_err();
        assert!(matches!(
            error,
            retention::RetentionError::WriterRole(limit::WriterRoleError::UnknownRole(_))
        ));
        s.admin()
            .set_writer_limits(limit::WriterLimits::default())
            .await
            .unwrap();
        s.admin().prune_log(common::JOBS).await.unwrap();
        s.admin().freeze_closed_partitions().await.unwrap();
        assert!(!s.admin().partition_ages().await.unwrap().is_empty());

        let deleted = s
            .append(
                stream::StreamId::new_v7(),
                version::ExpectedVersion::NoStream,
                &jobs,
            )
            .await
            .unwrap();
        s.delete_stream::<JobEvent>(deleted.stream_id, deleted.last_version())
            .await
            .unwrap();
    })
    .await?;

    let s = opened.clone();
    let mut request = metadata::Metadata::default();
    request.actor = Some("user:7".into());
    tokio::spawn(ambient::scope(request, async move {
        let events = [JobEvent::created("a")];
        let id = stream::StreamId::new_v7();
        ambient::scope(
            ambient::current(),
            s.append(id, version::ExpectedVersion::NoStream, &events),
        )
        .await
        .unwrap();
        let mut uow = s.begin().await.unwrap();
        uow.append(id, version::ExpectedVersion::Any, &events)
            .await
            .unwrap();
        uow.commit().await.unwrap();
    }))
    .await?;

    f.finish().await
}
