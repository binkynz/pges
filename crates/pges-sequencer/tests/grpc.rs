//! The gRPC watch server: the leader streams each store's high-water mark with its fence, a standby answers
//! `UNAVAILABLE`, streams end when the leader stops or the server shuts down, and a takeover's values carry a
//! higher fence.

#![cfg(feature = "watch-grpc")]
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

mod common;

use std::{net::SocketAddr, time::Duration};

use anyhow::{Context, Result, ensure};
use tokio::{sync::oneshot, task::JoinHandle};

use pges_core::stream;
use pges_sequencer::{grpc, sequencer, watch};
use pges_testing::proxy;
use pges_watch_proto::v1;

use common::{Fixture, Target};

type Client = v1::watch_client::WatchClient<tonic::transport::Channel>;
type Stream = tonic::codec::Streaming<v1::WatchResponse>;

struct Server {
    addr: SocketAddr,
    stop: oneshot::Sender<()>,
    task: JoinHandle<Result<(), grpc::ServeError>>,
}

impl Server {
    async fn start(watch: watch::Watch) -> Result<Self> {
        Self::start_with(watch, grpc::WatchConfig::default()).await
    }

    async fn start_with(watch: watch::Watch, config: grpc::WatchConfig) -> Result<Self> {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;
        let (stop, stopped) = oneshot::channel::<()>();
        let service = grpc::WatchService::with_config(watch, config);
        let task = tokio::spawn(service.serve(listener, async {
            let _ = stopped.await;
        }));
        Ok(Self { addr, stop, task })
    }

    async fn client(&self) -> Result<Client> {
        let channel = tonic::transport::Endpoint::from_shared(format!("http://{}", self.addr))?
            .connect()
            .await?;
        Ok(v1::watch_client::WatchClient::new(channel))
    }

    async fn stop(self) -> Result<()> {
        let _ = self.stop.send(());
        tokio::time::timeout(Duration::from_secs(10), self.task)
            .await
            .context("the server did not stop")???;
        Ok(())
    }
}

async fn watch(client: &mut Client, store_id: i32) -> Result<Stream, tonic::Status> {
    Ok(client
        .watch(v1::WatchRequest { store_id })
        .await?
        .into_inner())
}

async fn next(stream: &mut Stream) -> Result<v1::WatchResponse> {
    tokio::time::timeout(Duration::from_secs(10), stream.message())
        .await
        .context("no value")??
        .context("the stream ended")
}

/// Reads values until one reaches `store`; positions never go down on the way.
async fn until(stream: &mut Stream, store: u64) -> Result<v1::WatchResponse> {
    let mut last = 0;
    loop {
        let message = next(stream).await?;
        ensure!(message.store >= last, "{} after {last}", message.store);
        last = message.store;
        if message.store >= store {
            return Ok(message);
        }
    }
}

/// The status a stream ended with.
async fn ended(stream: &mut Stream) -> Result<tonic::Status> {
    loop {
        match tokio::time::timeout(Duration::from_secs(10), stream.message())
            .await
            .context("the stream did not end")?
        {
            Ok(Some(_)) => {}
            Ok(None) => anyhow::bail!("the stream ended without a status"),
            Err(status) => return Ok(status),
        }
    }
}

async fn state(client: &mut Client) -> Result<(v1::SequencerState, Option<u64>)> {
    let status = client.status(v1::StatusRequest {}).await?.into_inner();
    Ok((status.state(), status.fence))
}

fn fence(running: &common::Running) -> u64 {
    match &*running.watch.status().borrow() {
        sequencer::Status::Leading(fence) => fence.get(),
        other => panic!("not leading: {other:?}"),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_leader_streams_the_current_value_then_each_change() -> Result<()> {
    let f = Fixture::new().await?;
    let running = f.start(common::fast()).await?;
    running.wait_leading().await?;
    let leading = fence(&running);
    let server = Server::start(running.watch.clone()).await?;
    let mut client = server.client().await?;
    assert_eq!(
        state(&mut client).await?,
        (v1::SequencerState::Leading, Some(leading))
    );

    let mut stream = watch(&mut client, f.store_id.get()).await?;
    let first = next(&mut stream).await?;
    assert_eq!((first.fence, first.store), (leading, 0));

    f.append(Target::Job(stream::StreamId::new_v7()), 1).await?;
    f.append(Target::Order(stream::StreamId::new_v7()), 2)
        .await?;
    let high = until(&mut stream, 3)
        .await?
        .high_water()
        .context("an invalid position")?;
    assert_eq!(high.category(f.jobs).get(), 1);
    assert_eq!(high.category(f.orders).get(), 3);
    assert!(high.lsn().is_some(), "{high:?}");
    assert_eq!(high, *running.watch.high_water(f.store_id).borrow());

    // A healthy leader renews its lease, so its stream outlives several lease lengths (800 ms here).
    tokio::time::sleep(Duration::from_millis(2500)).await;
    f.append(Target::Job(stream::StreamId::new_v7()), 1).await?;
    until(&mut stream, 4).await?;

    // A store no leader announced reads as zero, as the in-process watch does.
    let mut unknown = watch(&mut client, 9999).await?;
    let zero = next(&mut unknown).await?;
    assert_eq!((zero.store, zero.categories.len(), zero.lsn), (0, 0, None));

    // The stream ends when the sequencer stops leading.
    running.stop().await?;
    let status = ended(&mut stream).await?;
    assert_eq!(status.code(), tonic::Code::Unavailable, "{status:?}");
    assert!(status.message().contains("stopped"), "{status:?}");
    assert_eq!(
        state(&mut client).await?,
        (v1::SequencerState::Stopped, None)
    );
    assert_eq!(
        watch(&mut client, f.store_id.get())
            .await
            .unwrap_err()
            .code(),
        tonic::Code::Unavailable
    );

    server.stop().await?;
    f.finish().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_standby_answers_unavailable_until_it_takes_over() -> Result<()> {
    let f = Fixture::new().await?;
    let first = f.start(common::fast()).await?;
    first.wait_leading().await?;
    let second = f.start(common::fast()).await?;
    let first_server = Server::start(first.watch.clone()).await?;
    let second_server = Server::start(second.watch.clone()).await?;
    let mut first_client = first_server.client().await?;
    let mut second_client = second_server.client().await?;

    assert_eq!(
        state(&mut second_client).await?,
        (v1::SequencerState::Standby, None)
    );
    let refused = watch(&mut second_client, f.store_id.get())
        .await
        .unwrap_err();
    assert_eq!(refused.code(), tonic::Code::Unavailable, "{refused:?}");
    assert!(refused.message().contains("standing by"), "{refused:?}");

    let mut stream = watch(&mut first_client, f.store_id.get()).await?;
    f.append(Target::Job(stream::StreamId::new_v7()), 2).await?;
    let before = until(&mut stream, 2).await?;

    // A graceful shutdown releases the lease: the old leader's stream ends, the standby leads.
    first.stop().await?;
    assert_eq!(ended(&mut stream).await?.code(), tonic::Code::Unavailable);
    second.wait_leading().await?;
    let mut stream = watch(&mut second_client, f.store_id.get()).await?;
    let after = next(&mut stream).await?;
    ensure!(after.fence > before.fence, "{after:?} after {before:?}");
    ensure!(after.store >= before.store, "{after:?} after {before:?}");
    f.append(Target::Job(stream::StreamId::new_v7()), 1).await?;
    assert_eq!(until(&mut stream, 3).await?.fence, after.fence);

    second.stop().await?;
    first_server.stop().await?;
    second_server.stop().await?;
    f.finish().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_crashed_leader_stops_serving_and_a_standby_takes_over() -> Result<()> {
    let f = Fixture::new().await?;
    let first = f.start(common::fast()).await?;
    first.wait_leading().await?;
    let second = f.start(common::fast()).await?;
    let first_server = Server::start(first.watch.clone()).await?;
    let second_server = Server::start(second.watch.clone()).await?;
    let mut first_client = first_server.client().await?;
    let mut second_client = second_server.client().await?;
    let mut stream = watch(&mut first_client, f.store_id.get()).await?;
    f.append(Target::Job(stream::StreamId::new_v7()), 2).await?;
    let before = until(&mut stream, 2).await?;

    // The run's future is dropped while it leads, without releasing the lease. Its server, still up, must
    // not keep serving it as the leader.
    let errors = first.errors();
    ensure!(
        errors.is_empty(),
        "the first sequencer recovered: {errors:?}"
    );
    let first_watch = first.watch.clone();
    first.kill().await;
    let status = first_watch.status().borrow().clone();
    assert!(matches!(status, sequencer::Status::Abandoned), "{status:?}");
    let end = ended(&mut stream).await?;
    assert_eq!(end.code(), tonic::Code::Unavailable, "{end:?}");
    assert!(end.message().contains("abandoned"), "{end:?}");
    assert_eq!(
        state(&mut first_client).await?,
        (v1::SequencerState::Abandoned, None)
    );
    let refused = watch(&mut first_client, f.store_id.get())
        .await
        .unwrap_err();
    assert_eq!(refused.code(), tonic::Code::Unavailable, "{refused:?}");
    assert!(refused.message().contains("abandoned"), "{refused:?}");

    // The standby leads once the lease expired.
    second.wait_leading().await?;
    let mut stream = watch(&mut second_client, f.store_id.get()).await?;
    let after = next(&mut stream).await?;
    ensure!(after.fence > before.fence, "{after:?} after {before:?}");
    ensure!(after.store >= before.store, "{after:?} after {before:?}");

    second.stop().await?;
    first_server.stop().await?;
    second_server.stop().await?;
    f.finish().await
}

/// A leader whose database calls hang keeps saying `Leading`. Once its lease passes without renewal, its
/// streams end with `UNAVAILABLE` and new ones are refused, so clients move on to the sequencer that takes
/// over.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_stalled_leaders_streams_end_once_its_lease_expires() -> Result<()> {
    let f = Fixture::new().await?;
    let proxy = proxy::Proxy::start(pges_testing::server().await?).await?;
    let proxied = sqlx::postgres::PgPoolOptions::new()
        .max_connections(8)
        .connect_lazy(&proxy.url(&f.db)?)?;
    let first = common::Running::start(proxied.clone(), f.source(proxied)?, common::fast())?;
    first.wait_leading().await?;
    let second = f.start(common::fast()).await?;
    let first_server = Server::start(first.watch.clone()).await?;
    let second_server = Server::start(second.watch.clone()).await?;
    let mut first_client = first_server.client().await?;
    let mut second_client = second_server.client().await?;
    let mut stream = watch(&mut first_client, f.store_id.get()).await?;
    f.append(Target::Job(stream::StreamId::new_v7()), 1).await?;
    let before = until(&mut stream, 1).await?;

    proxy.hold(proxy::Direction::Both);
    let end = ended(&mut stream).await?;
    assert_eq!(end.code(), tonic::Code::Unavailable, "{end:?}");
    assert!(end.message().contains("lease expired"), "{end:?}");
    assert!(
        first.is_leading(),
        "the stalled leader no longer says Leading"
    );
    let refused = watch(&mut first_client, f.store_id.get())
        .await
        .unwrap_err();
    assert_eq!(refused.code(), tonic::Code::Unavailable, "{refused:?}");
    assert!(refused.message().contains("lease expired"), "{refused:?}");

    second.wait_leading().await?;
    let mut stream = watch(&mut second_client, f.store_id.get()).await?;
    let after = next(&mut stream).await?;
    ensure!(after.fence > before.fence, "{after:?} after {before:?}");
    f.append(Target::Job(stream::StreamId::new_v7()), 1).await?;
    until(&mut stream, 2).await?;

    // Released, the old leader finds its lease taken and stands by.
    proxy.release();
    let mut status = first.watch.status();
    tokio::time::timeout(
        Duration::from_secs(10),
        status.wait_for(|status| matches!(status, sequencer::Status::Standby)),
    )
    .await
    .context("the old leader did not stand by")??;
    drop(status);

    first.stop_after_errors().await?;
    second.stop().await?;
    first_server.stop().await?;
    second_server.stop().await?;
    f.finish().await
}

/// One stream more than `max_streams` is refused with `RESOURCE_EXHAUSTED`, over any connection; a stream
/// that ends gives its place back.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn streams_beyond_the_limit_are_refused() -> Result<()> {
    let f = Fixture::new().await?;
    let running = f.start(common::fast()).await?;
    running.wait_leading().await?;
    let mut config = grpc::WatchConfig::default();
    config.max_streams = std::num::NonZeroUsize::new(2).unwrap();
    let server = Server::start_with(running.watch.clone(), config).await?;
    let mut client = server.client().await?;
    let mut other = server.client().await?;

    let mut first = watch(&mut client, f.store_id.get()).await?;
    next(&mut first).await?;
    let mut second = watch(&mut other, 4242).await?;
    next(&mut second).await?;
    for client in [&mut client, &mut other] {
        let refused = watch(client, f.store_id.get()).await.unwrap_err();
        assert_eq!(
            refused.code(),
            tonic::Code::ResourceExhausted,
            "{refused:?}"
        );
    }

    drop(second);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    let mut third = loop {
        match watch(&mut client, f.store_id.get()).await {
            Ok(stream) => break stream,
            Err(status) if status.code() == tonic::Code::ResourceExhausted => {
                ensure!(
                    tokio::time::Instant::now() < deadline,
                    "the dropped stream never gave its place back"
                );
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            Err(status) => return Err(status.into()),
        }
    };
    next(&mut third).await?;
    f.append(Target::Job(stream::StreamId::new_v7()), 1).await?;
    until(&mut first, 1).await?;
    until(&mut third, 1).await?;

    running.stop().await?;
    server.stop().await?;
    f.finish().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shutting_the_server_down_ends_open_streams() -> Result<()> {
    let f = Fixture::new().await?;
    let running = f.start(common::fast()).await?;
    running.wait_leading().await?;
    let server = Server::start(running.watch.clone()).await?;
    let mut client = server.client().await?;
    let mut stream = watch(&mut client, f.store_id.get()).await?;
    next(&mut stream).await?;

    // The sequencer keeps leading; the server must not wait for the stream.
    server.stop().await?;
    let status = ended(&mut stream).await?;
    assert_eq!(status.code(), tonic::Code::Unavailable, "{status:?}");
    assert!(running.is_leading());

    running.stop().await?;
    f.finish().await
}
