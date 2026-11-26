//! Every public future can run on another task: `tokio::spawn` needs it to be `Send` for any lifetime,
//! which sqlx futures holding borrowed connections do not always satisfy.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

mod common;

use std::time::Duration;

use anyhow::Result;

#[cfg(all(feature = "wake-inproc", feature = "wake-poll"))]
use pges_core::high_water;
use pges_core::{category, interval, non_empty, stream};
use pges_subscribe::{
    backfill, batch, handler, replica, subscription::StoreExt, subscription_name,
};

use common::{Cat, Fixture, Recorder};

struct Ignore;

impl handler::Handler for Ignore {
    async fn handle(&mut self, _batch: &batch::Batch) -> Result<(), handler::HandlerError> {
        Ok(())
    }
}

#[tokio::test]
async fn futures_run_on_spawned_tasks() -> Result<()> {
    let f = Fixture::new(pges_postgres::store::Outbox::Disabled).await?;
    f.append(Cat::Jobs, stream::StreamId::new_v7(), 1).await?;
    f.sequence_all().await?;

    let run_pg = f
        .store
        .subscription(subscription_name!("spawned-pg"))
        .categories(non_empty![category!("jobs")])
        .run_pg(Recorder::new("spawned"));
    let status = run_pg.status();
    let task = tokio::spawn(run_pg.run_until(async {
        tokio::time::sleep(Duration::from_millis(200)).await;
    }));
    common::wait_position(status, 1).await?;
    task.await??;

    let handle = f
        .store
        .subscription(subscription_name!("spawned"))
        .categories(non_empty![category!("jobs")])
        .run(Ignore)
        .spawn();
    common::wait_position(handle.status_changes(), 1).await?;
    tokio::spawn(handle.shutdown()).await??;

    let joined = f
        .store
        .subscription(subscription_name!("joined"))
        .categories(non_empty![category!("nope")])
        .run(Ignore);
    let errors = common::track(joined.status());
    let handle = joined.spawn();
    assert!(tokio::spawn(handle.join()).await?.is_err());
    assert!(errors.lock().unwrap().is_empty(), "{errors:?}");

    // The store's own pool as the "replica": not in recovery, so it is read without waiting.
    let replicated = f
        .store
        .subscription(subscription_name!("spawned-replica"))
        .categories(non_empty![category!("jobs")])
        .replica(
            replica::Replica::new(f.pool.clone())
                .fallback(interval!(1 s), replica::FallbackLimit::new(1)),
        )
        .run_pg(Recorder::new("spawned-replica"))
        .spawn();
    common::wait_position(replicated.status_changes(), 1).await?;
    tokio::spawn(replicated.shutdown()).await??;

    let backfilled = tokio::spawn(
        f.store
            .backfill(
                subscription_name!("spawned-backfill"),
                backfill::Until::Head,
            )
            .categories(non_empty![category!("jobs")])
            .parallelism(pges_subscribe::lanes!(2))
            .run_pg(Recorder::new("spawned-backfill")),
    )
    .await??;
    assert_eq!(backfilled.events, 1);

    #[cfg(all(feature = "wake-inproc", feature = "wake-poll"))]
    wakers(&f).await?;
    #[cfg(feature = "wake-grpc")]
    grpc_waker(&f).await?;

    f.finish().await
}

#[cfg(feature = "wake-grpc")]
async fn grpc_waker(f: &Fixture) -> Result<()> {
    use pges_core::high_water;
    use pges_subscribe::waker::{Waker, grpc};

    let sequencer = common::Sequencer::start(f).await?;
    sequencer.wait_leading().await?;
    let server = common::GrpcServer::start(sequencer.watch.clone()).await?;
    let mut waker =
        grpc::GrpcWaker::new(f.store.store_id(), non_empty![server.endpoint().parse()?]);
    tokio::spawn(async move {
        assert!(
            waker
                .changed(&high_water::HighWater::default())
                .await
                .is_ok()
        );
    })
    .await?;

    sequencer.stop().await?;
    server.stop().await
}

#[cfg(all(feature = "wake-inproc", feature = "wake-poll"))]
async fn wakers(f: &Fixture) -> Result<()> {
    use pges_subscribe::waker::{self, Waker, inproc, poll};

    let known = high_water::HighWater::default();
    let (sender, receiver) = tokio::sync::watch::channel(high_water::HighWater::default());
    drop(sender);
    let mut inproc = inproc::InprocWaker::new(receiver.clone());
    let mut poll = poll::PollWaker::new(&f.store, poll::PollConfig::default());
    let mut boxed: Box<dyn waker::DynWaker> = Box::new(inproc::InprocWaker::new(receiver));
    let mut any = waker::Wakers::new(poll.clone());
    tokio::spawn(async move {
        assert!(inproc.changed(&known).await.is_err());
        assert!(poll.changed(&known).await.is_ok());
        assert!(Waker::changed(&mut boxed, &known).await.is_err());
        assert!(waker::DynWaker::changed(&mut *boxed, &known).await.is_err());
        assert!(any.changed(&known).await.is_ok());
    })
    .await?;

    Ok(())
}
