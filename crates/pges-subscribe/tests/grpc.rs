//! The gRPC waker against the real sequencer's gRPC watch: a subscription woken end to end by a logical
//! sequencer, reconnects after a server restart and a dropped or silent connection, a leader handover, and
//! standbys.

#![cfg(feature = "wake-grpc")]
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

mod common;

use std::time::Duration;

use anyhow::{Context, Result, ensure};

use pges_core::{category, high_water, interval, non_empty, position, stream};
use pges_sequencer::sequencer;
use pges_subscribe::{
    subscription::StoreExt,
    subscription_name,
    waker::{self, Waker, grpc},
};
use pges_testing::proxy;

use common::{Cat, Fixture, Recorder};

fn at(n: u64) -> position::Position {
    pges_testing::position(n)
}

fn known(store: u64, categories: &[category::CategoryId]) -> high_water::HighWater {
    high_water::HighWater::new(
        at(store),
        categories.iter().map(|id| (*id, at(store))).collect(),
        None,
    )
}

/// Whether `changed` is still waiting after `wait`. The call is dropped then, which the waker must survive.
async fn pending(
    waker: &mut grpc::GrpcWaker,
    known: &high_water::HighWater,
    wait: Duration,
) -> bool {
    tokio::time::timeout(wait, waker.changed(known))
        .await
        .is_err()
}

async fn woken(
    waker: &mut grpc::GrpcWaker,
    known: &high_water::HighWater,
) -> Result<high_water::HighWater> {
    Ok(
        tokio::time::timeout(Duration::from_secs(10), waker.changed(known))
            .await
            .context("the waker did not fire")??,
    )
}

/// Waits until the waker has seen `category` reach `position`, firing as often as it likes on the way.
async fn woken_at(
    waker: &mut grpc::GrpcWaker,
    category: category::CategoryId,
    position: u64,
) -> Result<high_water::HighWater> {
    let watching = known(position - 1, &[category]);
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let high = waker.changed(&watching).await?;
            if high.category(category) >= at(position) {
                return anyhow::Ok(high);
            }
        }
    })
    .await
    .with_context(|| format!("the waker did not see {category} reach {position}"))?
}

const SHORT: Duration = Duration::from_millis(300);

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_logical_sequencer_wakes_a_subscription_through_grpc() -> Result<()> {
    let f = Fixture::new(pges_postgres::store::Outbox::Disabled).await?;
    let (job, part) = (stream::StreamId::new_v7(), stream::StreamId::new_v7());
    let sequencer = common::Sequencer::start_logical(&f, common::Sequencer::config()).await?;
    sequencer.wait_leading().await?;
    let server = common::GrpcServer::start(sequencer.watch.clone()).await?;

    // No recheck interval applies: the waker is the only wake-up.
    let running = common::Running::spawn(
        f.store
            .subscription(subscription_name!("streamed"))
            .categories(non_empty![category!("jobs")])
            .waker(grpc::GrpcWaker::new(
                f.store.store_id(),
                non_empty![server.endpoint().parse()?],
            ))
            .run_pg(Recorder::new("streamed")),
    );
    for _ in 0..5 {
        f.append(Cat::Jobs, job, 1).await?;
        f.append(Cat::Parts, part, 2).await?;
        let jobs = f.expected(&[f.id(Cat::Jobs)]).await?;
        if let Some((last, _)) = jobs.last() {
            running.wait_position(*last as u64).await?;
        }
    }
    common::wait_sequenced(&f.pool, 15).await?;
    let expected = f.expected(&[f.id(Cat::Jobs)]).await?;
    ensure!(expected.len() == 5, "{expected:?}");
    running.wait_position(expected[4].0 as u64).await?;

    running.stop().await?;
    sequencer.stop().await?;
    server.stop().await?;
    common::check_delivery(&f.delivered("streamed").await?, &expected)?;

    f.finish_with_slots().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fires_on_values_for_watched_categories_and_after_a_server_restart() -> Result<()> {
    let f = Fixture::new(pges_postgres::store::Outbox::Enabled).await?;
    let (jobs, orders) = (f.id(Cat::Jobs), f.id(Cat::Orders));
    let sequencer = common::Sequencer::start(&f).await?;
    sequencer.wait_leading().await?;
    let server = common::GrpcServer::start(sequencer.watch.clone()).await?;
    let mut waker =
        grpc::GrpcWaker::new(f.store.store_id(), non_empty![server.endpoint().parse()?]);

    // The first call connects and fires: nothing was streamed to the waker before.
    let watching_jobs = known(0, &[jobs]);
    woken(&mut waker, &watching_jobs).await?;
    assert!(pending(&mut waker, &watching_jobs, SHORT).await);

    // Another category moving does not wake a subscriber of jobs, but the whole store moved.
    f.append(Cat::Orders, stream::StreamId::new_v7(), 2).await?;
    common::wait_sequenced(&f.pool, 2).await?;
    assert!(pending(&mut waker, &watching_jobs, SHORT).await);
    assert_eq!(
        woken(&mut waker, &known(0, &[])).await?.category(orders),
        at(2)
    );

    f.append(Cat::Jobs, stream::StreamId::new_v7(), 1).await?;
    let high = woken_at(&mut waker, jobs, 3).await?;
    assert_eq!(high.store(), at(3));
    assert!(high.lsn().is_some(), "{high:?}");
    // Level-triggered: a value already seen wakes every call it has passed, and no other.
    woken(&mut waker, &watching_jobs).await?;
    let caught_up = known(3, &[jobs]);
    assert!(pending(&mut waker, &caught_up, SHORT).await);

    // While the server is down the waker waits; it does not fail.
    let addr = server.addr;
    server.stop().await?;
    assert!(pending(&mut waker, &caught_up, SHORT).await);

    // Back on the same address: values sent meanwhile are gone, so the waker fires once on the new stream.
    let server = common::GrpcServer::start_on(sequencer.watch.clone(), addr).await?;
    woken(&mut waker, &caught_up).await?;
    assert!(pending(&mut waker, &caught_up, SHORT).await);
    f.append(Cat::Jobs, stream::StreamId::new_v7(), 1).await?;
    woken_at(&mut waker, jobs, 4).await?;

    drop(waker);
    sequencer.stop().await?;
    server.stop().await?;
    f.finish().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_dropped_or_silent_connection_fires_once_after_reconnecting() -> Result<()> {
    let f = Fixture::new(pges_postgres::store::Outbox::Enabled).await?;
    let jobs = f.id(Cat::Jobs);
    let sequencer = common::Sequencer::start(&f).await?;
    sequencer.wait_leading().await?;
    let server = common::GrpcServer::start(sequencer.watch.clone()).await?;
    let proxy = proxy::Proxy::to(server.addr).await?;
    let mut config = grpc::GrpcConfig::default();
    config.connect_timeout = interval!(500 ms);
    config.keepalive_interval = interval!(200 ms);
    config.keepalive_timeout = interval!(300 ms);
    let mut waker = grpc::GrpcWaker::with_config(
        f.store.store_id(),
        non_empty![format!("http://{}", proxy.addr()).parse()?],
        config,
    );
    let watching = known(0, &[jobs]);
    woken(&mut waker, &watching).await?;
    assert!(pending(&mut waker, &watching, SHORT).await);

    // A broken connection: the waker cannot know what it missed, so it fires once on the new one.
    proxy.drop_connections();
    woken(&mut waker, &watching).await?;
    assert!(pending(&mut waker, &watching, SHORT).await);

    // A connection that goes silent, as behind a firewall that drops packets: keepalive pings go unanswered,
    // and the waker opens a new connection while the old one still looks open.
    let accepted = proxy.accepted();
    proxy.hold(proxy::Direction::Both);
    assert!(pending(&mut waker, &watching, Duration::from_millis(1500)).await);
    ensure!(
        proxy.accepted() > accepted,
        "the waker did not notice the silent connection"
    );
    proxy.release();
    woken(&mut waker, &watching).await?;

    // The new stream carries new values.
    f.append(Cat::Jobs, stream::StreamId::new_v7(), 1).await?;
    woken_at(&mut waker, jobs, 1).await?;

    drop(waker);
    drop(proxy);
    sequencer.stop().await?;
    server.stop().await?;
    f.finish().await
}

/// Two logical sequencers, each with a server; the subscription's waker knows both. The leader shuts down,
/// the standby takes over, and the subscription keeps being woken without recovering from anything.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_subscription_follows_a_leader_handover() -> Result<()> {
    let f = Fixture::new(pges_postgres::store::Outbox::Disabled).await?;
    let job = stream::StreamId::new_v7();
    let first = common::Sequencer::start_logical(&f, common::Sequencer::config()).await?;
    let first_fence = first.wait_leading().await?;
    let second = common::Sequencer::start_logical(&f, common::Sequencer::config()).await?;
    let first_server = common::GrpcServer::start(first.watch.clone()).await?;
    let second_server = common::GrpcServer::start(second.watch.clone()).await?;

    let running = common::Running::spawn(
        f.store
            .subscription(subscription_name!("handed-over"))
            .categories(non_empty![category!("jobs")])
            .waker(grpc::GrpcWaker::new(
                f.store.store_id(),
                non_empty![
                    second_server.endpoint().parse()?,
                    first_server.endpoint().parse()?
                ],
            ))
            .run_pg(Recorder::new("handed-over")),
    );
    f.append(Cat::Jobs, job, 1).await?;
    running.wait_position(1).await?;

    // A graceful shutdown releases the lease; the standby takes over.
    first.stop().await?;
    first_server.stop().await?;
    let second_fence = second.wait_leading().await?;
    ensure!(
        second_fence > first_fence,
        "{second_fence} after {first_fence}"
    );

    for _ in 0..3 {
        f.append(Cat::Jobs, job, 1).await?;
    }
    running.wait_position(4).await?;

    running.stop().await?;
    second.stop().await?;
    second_server.stop().await?;
    common::check_delivery(
        &f.delivered("handed-over").await?,
        &f.expected(&[f.id(Cat::Jobs)]).await?,
    )?;

    f.finish_with_slots().await
}

/// The leader crashes without releasing its lease while its server stays up. The waker leaves it, waits for
/// the standby to take over once the lease expired, fires on the new leader's stream and follows it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_waker_follows_a_crashed_leaders_successor() -> Result<()> {
    let f = Fixture::new(pges_postgres::store::Outbox::Enabled).await?;
    let jobs = f.id(Cat::Jobs);
    let mut config = common::Sequencer::config();
    config.lease =
        sequencer::Lease::with_renew(pges_core::interval!(800 ms), pges_core::interval!(200 ms))
            .unwrap();
    config.acquire_interval = pges_core::interval!(50 ms);
    let first = common::Sequencer::start_with(&f, config.clone()).await?;
    let first_fence = first.wait_leading().await?;
    let second = common::Sequencer::start_with(&f, config).await?;
    let first_server = common::GrpcServer::start(first.watch.clone()).await?;
    let second_server = common::GrpcServer::start(second.watch.clone()).await?;
    let mut waker = grpc::GrpcWaker::new(
        f.store.store_id(),
        non_empty![
            first_server.endpoint().parse()?,
            second_server.endpoint().parse()?
        ],
    );
    woken(&mut waker, &known(0, &[jobs])).await?;
    f.append(Cat::Jobs, stream::StreamId::new_v7(), 2).await?;
    woken_at(&mut waker, jobs, 2).await?;

    let first_watch = first.watch.clone();
    first.kill().await?;
    let status = first_watch.status().borrow().clone();
    assert!(matches!(status, sequencer::Status::Abandoned), "{status:?}");
    let second_fence = second.wait_leading().await?;
    ensure!(
        second_fence > first_fence,
        "{second_fence} after {first_fence}"
    );
    let caught_up = known(2, &[jobs]);
    let high = woken(&mut waker, &caught_up).await?;
    assert_eq!(high.category(jobs), at(2));
    assert!(pending(&mut waker, &caught_up, SHORT).await);
    f.append(Cat::Jobs, stream::StreamId::new_v7(), 1).await?;
    woken_at(&mut waker, jobs, 3).await?;

    drop(waker);
    second.stop().await?;
    first_server.stop().await?;
    second_server.stop().await?;
    f.finish().await
}

/// The leader's database calls hang: it still says `Leading` and its server stays up. Once its lease expires
/// its stream ends; the waker leaves it for the standby that takes over, fires on the new stream and follows
/// it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_waker_leaves_a_stalled_leader_for_its_successor() -> Result<()> {
    let f = Fixture::new(pges_postgres::store::Outbox::Enabled).await?;
    let jobs = f.id(Cat::Jobs);
    let proxy = proxy::Proxy::start(pges_testing::server().await?).await?;
    let proxied = sqlx::postgres::PgPoolOptions::new()
        .max_connections(8)
        .connect_lazy(&proxy.url(&f.db)?)?;
    let mut config = common::Sequencer::config();
    config.lease =
        sequencer::Lease::with_renew(pges_core::interval!(800 ms), pges_core::interval!(200 ms))
            .unwrap();
    config.acquire_interval = pges_core::interval!(50 ms);
    let first = common::Sequencer::start_on(proxied, config.clone())?;
    let first_fence = first.wait_leading().await?;
    let second = common::Sequencer::start_with(&f, config).await?;
    let first_server = common::GrpcServer::start(first.watch.clone()).await?;
    let second_server = common::GrpcServer::start(second.watch.clone()).await?;
    let mut waker = grpc::GrpcWaker::new(
        f.store.store_id(),
        non_empty![
            first_server.endpoint().parse()?,
            second_server.endpoint().parse()?
        ],
    );
    woken(&mut waker, &known(0, &[jobs])).await?;
    f.append(Cat::Jobs, stream::StreamId::new_v7(), 2).await?;
    woken_at(&mut waker, jobs, 2).await?;

    proxy.hold(proxy::Direction::Both);
    let second_fence = second.wait_leading().await?;
    ensure!(
        second_fence > first_fence,
        "{second_fence} after {first_fence}"
    );
    let caught_up = known(2, &[jobs]);
    let high = woken(&mut waker, &caught_up).await?;
    assert_eq!(high.category(jobs), at(2));
    f.append(Cat::Jobs, stream::StreamId::new_v7(), 1).await?;
    woken_at(&mut waker, jobs, 3).await?;
    let stalled = first.watch.status().borrow().clone();
    assert!(
        matches!(stalled, sequencer::Status::Leading(_)),
        "{stalled:?}"
    );

    drop(waker);
    proxy.release();
    let mut status = first.watch.status();
    tokio::time::timeout(
        Duration::from_secs(10),
        status.wait_for(|status| matches!(status, sequencer::Status::Standby)),
    )
    .await
    .context("the stalled leader did not stand by")??;
    drop(status);
    first.stop().await?;
    second.stop().await?;
    first_server.stop().await?;
    second_server.stop().await?;
    f.finish().await
}

/// A standby answers `UNAVAILABLE`: the waker moves on to the leader, and with only standbys it waits, then
/// reports them unreachable.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn standbys_are_skipped_and_reported_once_unreachable_for_long() -> Result<()> {
    let f = Fixture::new(pges_postgres::store::Outbox::Enabled).await?;
    let jobs = f.id(Cat::Jobs);
    let leader = common::Sequencer::start(&f).await?;
    leader.wait_leading().await?;
    let standby = common::Sequencer::start(&f).await?;
    let leader_server = common::GrpcServer::start(leader.watch.clone()).await?;
    let standby_server = common::GrpcServer::start(standby.watch.clone()).await?;
    let watching = known(0, &[jobs]);

    let mut both = grpc::GrpcWaker::new(
        f.store.store_id(),
        non_empty![
            standby_server.endpoint().parse()?,
            leader_server.endpoint().parse()?
        ],
    );
    woken(&mut both, &watching).await?;
    f.append(Cat::Jobs, stream::StreamId::new_v7(), 1).await?;
    woken_at(&mut both, jobs, 1).await?;

    let mut standby_only = grpc::GrpcWaker::new(
        f.store.store_id(),
        non_empty![standby_server.endpoint().parse()?],
    );
    assert!(pending(&mut standby_only, &watching, SHORT).await);

    let mut config = grpc::GrpcConfig::default();
    config.unreachable_after = Duration::from_millis(300);
    let mut reported = grpc::GrpcWaker::with_config(
        f.store.store_id(),
        non_empty![standby_server.endpoint().parse()?],
        config,
    );
    let error = tokio::time::timeout(Duration::from_secs(10), reported.changed(&watching))
        .await
        .context("the waker did not give up")?
        .expect_err("a standby does not serve");
    let waker::WakeError::Other(error) = error else {
        panic!("not an Unreachable error: {error:?}");
    };
    let unreachable = error
        .downcast_ref::<grpc::Unreachable>()
        .expect("an Unreachable error");
    assert!(unreachable.unreachable() >= Duration::from_millis(300));
    assert!(
        unreachable.to_string().contains("not leading"),
        "{unreachable}"
    );

    drop((both, standby_only, reported));
    standby.stop().await?;
    leader.stop().await?;
    standby_server.stop().await?;
    leader_server.stop().await?;
    f.finish().await
}

/// The watch over TLS, with client certificates: a subscription is woken through an `https` endpoint by name
/// and by address; a waker that trusts another CA, or shows no client certificate, is refused.
#[cfg(feature = "wake-grpc-tls")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_subscription_is_woken_over_tls() -> Result<()> {
    use pges_sequencer::grpc as server_grpc;

    let pki = pges_testing::tls::Pki::generate()?;
    let f = Fixture::new(pges_postgres::store::Outbox::Enabled).await?;
    let jobs = f.id(Cat::Jobs);
    let sequencer = common::Sequencer::start(&f).await?;
    sequencer.wait_leading().await?;
    let mut server_config = server_grpc::WatchConfig::default();
    server_config.tls =
        Some(server_grpc::ServerTls::new(&pki.server_cert, &pki.server_key)?.client_ca(&pki.ca)?);
    let server = common::GrpcServer::start_with(
        sequencer.watch.clone(),
        "127.0.0.1:0".parse()?,
        server_config,
    )
    .await?;
    let by_name = format!("https://localhost:{}", server.addr.port());
    let mut trusted = grpc::GrpcConfig::default();
    trusted.tls = grpc::ClientTls::ca(&pki.ca)?.identity(&pki.client_cert, &pki.client_key)?;

    let running = common::Running::spawn(
        f.store
            .subscription(subscription_name!("over-tls"))
            .categories(non_empty![category!("jobs")])
            .waker(grpc::GrpcWaker::with_config(
                f.store.store_id(),
                non_empty![by_name.parse()?],
                trusted.clone(),
            ))
            .run_pg(Recorder::new("over-tls")),
    );
    for expected in 1..=3 {
        f.append(Cat::Jobs, stream::StreamId::new_v7(), 1).await?;
        running.wait_position(expected).await?;
    }
    let mut by_address = grpc::GrpcWaker::with_config(
        f.store.store_id(),
        non_empty![server.endpoint().parse()?],
        trusted,
    );
    woken_at(&mut by_address, jobs, 3).await?;

    // A server certificate the waker does not trust is reported at once, not retried for 30 s.
    let mut config = grpc::GrpcConfig::default();
    config.tls = grpc::ClientTls::ca(&pki.other_ca)?.identity(&pki.client_cert, &pki.client_key)?;
    let refused = refusal(&f, &by_name, config).await?;
    let rejected = refused
        .downcast_ref::<grpc::Rejected>()
        .with_context(|| format!("not a Rejected error: {refused}"))?;
    assert_eq!(rejected.rejection(), grpc::Rejection::Tls, "{rejected}");
    ensure!(rejected.to_string().contains("certificate"), "{rejected}");

    // TLS 1.3 sends the server's refusal after the client's handshake ended, so the client may see a broken
    // connection rather than the reason, which is retried until `unreachable_after`.
    let mut config = grpc::GrpcConfig::default();
    config.unreachable_after = Duration::from_millis(300);
    config.tls = grpc::ClientTls::ca(&pki.ca)?;
    refusal(&f, &by_name, config).await?;

    // A plain client gets no stream from a TLS server.
    let mut config = grpc::GrpcConfig::default();
    config.unreachable_after = Duration::from_millis(300);
    let plain = format!("http://localhost:{}", server.addr.port());
    let refused = refusal(&f, &plain, config).await?;
    ensure!(
        refused.downcast_ref::<grpc::Unreachable>().is_some(),
        "{refused}"
    );

    // A certificate the trusted CA signed for another host is refused like an untrusted one.
    let mut elsewhere_config = server_grpc::WatchConfig::default();
    elsewhere_config.tls = Some(
        server_grpc::ServerTls::new(&pki.elsewhere_cert, &pki.elsewhere_key)?.client_ca(&pki.ca)?,
    );
    let elsewhere = common::GrpcServer::start_with(
        sequencer.watch.clone(),
        "127.0.0.1:0".parse()?,
        elsewhere_config,
    )
    .await?;
    let mut config = grpc::GrpcConfig::default();
    config.tls = grpc::ClientTls::ca(&pki.ca)?.identity(&pki.client_cert, &pki.client_key)?;
    let elsewhere_by_name = format!("https://localhost:{}", elsewhere.addr.port());
    let refused = refusal(&f, &elsewhere_by_name, config).await?;
    let rejected = refused
        .downcast_ref::<grpc::Rejected>()
        .with_context(|| format!("not a Rejected error: {refused}"))?;
    assert_eq!(rejected.rejection(), grpc::Rejection::Tls, "{rejected}");
    elsewhere.stop().await?;

    drop(by_address);
    running.stop().await?;
    sequencer.stop().await?;
    server.stop().await?;
    common::check_delivery(&f.delivered("over-tls").await?, &f.expected(&[jobs]).await?)?;

    f.finish().await
}

/// How a waker with `config` fails on `endpoint`, which must refuse it.
#[cfg(feature = "wake-grpc-tls")]
async fn refusal(f: &Fixture, endpoint: &str, config: grpc::GrpcConfig) -> Result<waker::BoxError> {
    let mut waker =
        grpc::GrpcWaker::with_config(f.store.store_id(), non_empty![endpoint.parse()?], config);
    let error = tokio::time::timeout(Duration::from_secs(10), waker.changed(&known(0, &[])))
        .await
        .context("the waker did not give up")?
        .expect_err("the server must refuse the waker");
    let waker::WakeError::Other(error) = error else {
        anyhow::bail!("not a gRPC waker's error: {error:?}");
    };

    Ok(error)
}
