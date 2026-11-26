//! Every public future can run on another task: `tokio::spawn` needs it to be `Send` for any lifetime,
//! which sqlx futures holding borrowed connections do not always satisfy.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

mod common;

use std::num::NonZeroUsize;

use anyhow::Result;

use pges_sequencer::{outbox, sequencer, source};

use common::Fixture;

#[tokio::test]
async fn futures_run_on_spawned_tasks() -> Result<()> {
    let f = Fixture::new().await?;

    let pool = f.pool.clone();
    tokio::spawn(async move {
        let mut outbox = outbox::OutboxSource::new(pool.clone(), common::outbox_config());
        source::Source::open(&mut outbox, None).await.unwrap();
        source::Source::ready(&mut outbox).await.unwrap();
        let mut tx = pool.begin().await.unwrap();
        let batch = source::Source::next(&mut outbox, &mut tx, NonZeroUsize::MIN)
            .await
            .unwrap();
        tx.rollback().await.unwrap();
        source::Source::ack(&mut outbox, &batch.token)
            .await
            .unwrap();

        let mut boxed: Box<dyn source::DynSource> = Box::new(outbox);
        source::Source::open(&mut boxed, None).await.unwrap();
        source::Source::ready(&mut boxed).await.unwrap();
        let mut tx = pool.begin().await.unwrap();
        let batch = source::Source::next(&mut boxed, &mut tx, NonZeroUsize::MIN)
            .await
            .unwrap();
        tx.rollback().await.unwrap();
        source::Source::ack(&mut boxed, &batch.token).await.unwrap();
        source::DynSource::ack(&mut *boxed, &[]).await.unwrap();
        source::Source::close(&mut boxed).await;
        source::DynSource::close(&mut *boxed).await;
    })
    .await?;

    let generic = sequencer::Sequencer::new(
        f.pool.clone(),
        outbox::OutboxSource::new(f.pool.clone(), common::outbox_config()),
        common::fast(),
    );
    tokio::spawn(generic.run(std::future::ready(()))).await??;

    let boxed: Box<dyn source::DynSource> = Box::new(outbox::OutboxSource::new(
        f.pool.clone(),
        common::outbox_config(),
    ));
    let boxed = sequencer::Sequencer::new(f.pool.clone(), boxed, common::fast());
    let watch = boxed.watch();
    tokio::spawn(boxed.run(async {
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    }))
    .await??;
    assert!(matches!(
        *watch.status().borrow(),
        sequencer::Status::Stopped
    ));

    #[cfg(feature = "watch-grpc")]
    {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let service = pges_sequencer::grpc::WatchService::new(watch);
        tokio::spawn(service.serve(listener, std::future::ready(()))).await??;
    }

    f.finish().await
}

#[cfg(feature = "source-logical")]
#[tokio::test]
async fn logical_futures_run_on_spawned_tasks() -> Result<()> {
    let f = Fixture::logical().await?;

    let pool = f.pool.clone();
    let mut logical = f.logical_source(pool.clone())?;
    let appender = f.store.clone();
    let logical = tokio::spawn(async move {
        logical.bootstrap().await.unwrap();
        source::Source::open(&mut logical, None).await.unwrap();
        appender
            .append(
                pges_core::stream::StreamId::new_v7(),
                pges_core::version::ExpectedVersion::Any,
                &[common::JobEvent::Noted { n: 1 }],
            )
            .await
            .unwrap();
        source::Source::ready(&mut logical).await.unwrap();
        let mut tx = pool.begin().await.unwrap();
        let batch = source::Source::next(&mut logical, &mut tx, NonZeroUsize::MIN)
            .await
            .unwrap();
        tx.rollback().await.unwrap();
        assert_eq!(batch.commits.len(), 1);
        source::Source::ack(&mut logical, &batch.token)
            .await
            .unwrap();
        // Refused: nothing is halted. The future must still be Send.
        let refused = logical
            .resequence(&pges_sequencer::logical::ResequenceConfig::new(
                pges_postgres::limit::WriterRoles::one("pges_no_such_role"),
            ))
            .await;
        assert!(refused.is_err());
        logical
    })
    .await?;

    let pool = f.pool.clone();
    tokio::spawn(async move {
        let mut boxed: Box<dyn source::DynSource> = Box::new(logical);
        source::Source::open(&mut boxed, None).await.unwrap();
        let mut tx = pool.begin().await.unwrap();
        let batch = source::Source::next(&mut boxed, &mut tx, NonZeroUsize::MIN)
            .await
            .unwrap();
        tx.rollback().await.unwrap();
        source::DynSource::ack(&mut *boxed, &batch.token)
            .await
            .unwrap();
        source::Source::close(&mut boxed).await;
    })
    .await?;

    let generic = sequencer::Sequencer::new(
        f.pool.clone(),
        f.logical_source(f.pool.clone())?,
        common::fast(),
    );
    tokio::spawn(generic.run(std::future::ready(()))).await??;

    let boxed: Box<dyn source::DynSource> = Box::new(f.logical_source(f.pool.clone())?);
    let boxed = sequencer::Sequencer::new(f.pool.clone(), boxed, common::fast());
    tokio::spawn(boxed.run(async {
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    }))
    .await??;

    f.finish().await
}
