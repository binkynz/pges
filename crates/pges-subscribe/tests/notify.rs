//! The `LISTEN/NOTIFY` waker: notices for watched categories, the overflow marker, unreadable payloads, a
//! lost connection, and the real sequencer's notifications end to end.

#![cfg(feature = "wake-notify")]
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

mod common;

use std::time::Duration;

use anyhow::{Context, Result, ensure};
use sqlx::PgPool;

use pges_core::{category, high_water, non_empty, position, stream};
use pges_sequencer::sequencer;
use pges_subscribe::{
    subscription::StoreExt,
    subscription_name,
    waker::{Waker, notify},
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
    waker: &mut notify::NotifyWaker,
    known: &high_water::HighWater,
    wait: Duration,
) -> bool {
    tokio::time::timeout(wait, waker.changed(known))
        .await
        .is_err()
}

async fn woken(
    waker: &mut notify::NotifyWaker,
    known: &high_water::HighWater,
) -> Result<high_water::HighWater> {
    Ok(
        tokio::time::timeout(Duration::from_secs(10), waker.changed(known))
            .await
            .context("the waker did not fire")??,
    )
}

async fn send(pool: &PgPool, payload: &str) -> Result<()> {
    sqlx::query("SELECT pg_notify('pges_es', $1)")
        .bind(payload)
        .execute(pool)
        .await?;
    Ok(())
}

const SHORT: Duration = Duration::from_millis(150);

#[tokio::test]
async fn wakes_on_notices_for_watched_categories() -> Result<()> {
    let f = Fixture::new(pges_postgres::store::Outbox::Disabled).await?;
    let (jobs, orders) = (f.id(Cat::Jobs), f.id(Cat::Orders));
    let mut waker = notify::NotifyWaker::new(&f.store);
    let watching_jobs = known(5, &[jobs]);

    // The first call opens the LISTEN connection and fires: nothing was listening before.
    woken(&mut waker, &watching_jobs).await?;
    assert!(pending(&mut waker, &watching_jobs, SHORT).await);

    send(&f.pool, &format!("1;9;0/10;{orders}:9")).await?;
    assert!(pending(&mut waker, &watching_jobs, SHORT).await);
    // The whole store moved, though.
    assert_eq!(woken(&mut waker, &known(5, &[])).await?.store(), at(9));

    send(&f.pool, &format!("1;10;0/20;{jobs}:10")).await?;
    let high = woken(&mut waker, &watching_jobs).await?;
    assert_eq!(high.category(jobs), at(10));
    assert_eq!(high.category(orders), at(9));
    assert_eq!(high.store(), at(10));
    // Level-triggered: the notice already seen wakes every call it has passed, and no other.
    woken(&mut waker, &watching_jobs).await?;
    assert!(pending(&mut waker, &known(10, &[jobs]), SHORT).await);

    // Too many categories for the payload: any category may be at the notice's store position.
    send(&f.pool, "1;30;;*").await?;
    assert_eq!(
        woken(&mut waker, &known(10, &[orders]))
            .await?
            .category(orders),
        at(30)
    );
    assert!(pending(&mut waker, &known(30, &[orders]), SHORT).await);

    // A payload it cannot read: wake once, so the subscriber reads the log.
    for payload in ["2;31;;", "not a notice"] {
        send(&f.pool, payload).await?;
        woken(&mut waker, &known(30, &[jobs])).await?;
        assert!(pending(&mut waker, &known(30, &[jobs]), SHORT).await);
    }

    drop(waker);
    f.finish().await
}

#[tokio::test]
async fn a_lost_connection_fires_once_after_listening_again() -> Result<()> {
    let f = Fixture::new(pges_postgres::store::Outbox::Disabled).await?;
    let jobs = f.id(Cat::Jobs);
    let proxy = proxy::Proxy::start(pges_testing::server().await?).await?;
    let proxied = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .connect(&proxy.url(&f.db)?)
        .await?;
    let mut waker = notify::NotifyWaker::with_separate_pool(&f.store, proxied.clone());
    let watching = known(5, &[jobs]);
    woken(&mut waker, &watching).await?;
    assert!(pending(&mut waker, &watching, SHORT).await);

    // The notice reaches the old connection's socket and is lost with it.
    proxy.hold(proxy::Direction::ToClient);
    send(&f.pool, &format!("1;6;;{jobs}:6")).await?;
    tokio::time::sleep(Duration::from_millis(50)).await;
    proxy.drop_connections();
    proxy.release();

    // The waker cannot know what it missed, so it fires once on the new connection.
    let high = woken(&mut waker, &watching).await?;
    assert_eq!(high.category(jobs), at(0), "the notice was lost: {high:?}");
    assert!(pending(&mut waker, &watching, SHORT).await);

    // The new connection listens.
    send(&f.pool, &format!("1;7;;{jobs}:7")).await?;
    assert_eq!(woken(&mut waker, &watching).await?.category(jobs), at(7));

    drop(waker);
    proxied.close().await;
    drop(proxy);
    f.finish().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_sequencer_wakes_a_subscription_through_notify() -> Result<()> {
    let f = Fixture::new(pges_postgres::store::Outbox::Enabled).await?;
    let (job, part) = (stream::StreamId::new_v7(), stream::StreamId::new_v7());
    let sequencer = common::Sequencer::start(&f).await?;

    // No recheck interval applies: the waker is the only wake-up.
    let running = common::Running::spawn(
        f.store
            .subscription(subscription_name!("notified"))
            .categories(non_empty![category!("jobs")])
            .waker(notify::NotifyWaker::new(&f.store))
            .run_pg(Recorder::new("notified")),
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
    common::check_delivery(&f.delivered("notified").await?, &expected)?;

    f.finish().await
}

/// With the sequencer's notifications off, only the reconnect wakes the subscription: a lost `LISTEN`
/// connection must make it read the log once, or it would wait for a notice that never comes.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_subscription_reads_again_after_its_listen_connection_was_lost() -> Result<()> {
    let f = Fixture::new(pges_postgres::store::Outbox::Enabled).await?;
    let job = stream::StreamId::new_v7();
    let mut config = common::Sequencer::config();
    config.notify = sequencer::Notify::Disabled;
    let sequencer = common::Sequencer::start_with(&f, config).await?;
    let proxy = proxy::Proxy::start(pges_testing::server().await?).await?;
    let proxied = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .connect(&proxy.url(&f.db)?)
        .await?;

    let running = common::Running::spawn(
        f.store
            .subscription(subscription_name!("reconnected"))
            .categories(non_empty![category!("jobs")])
            .waker(notify::NotifyWaker::with_separate_pool(
                &f.store,
                proxied.clone(),
            ))
            .run_pg(Recorder::new("reconnected")),
    );
    f.append(Cat::Jobs, job, 1).await?;
    common::wait_sequenced(&f.pool, 1).await?;
    // Nothing notifies: the subscription read the log when the waker first listened, maybe before the
    // append was sequenced.
    tokio::time::sleep(Duration::from_millis(300)).await;
    let before = running.handle.status().position;

    f.append(Cat::Jobs, job, 1).await?;
    common::wait_sequenced(&f.pool, 2).await?;
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(
        running.handle.status().position,
        before,
        "woken without a notice"
    );

    proxy.drop_connections();
    running.wait_position(2).await?;

    running.stop().await?;
    sequencer.stop().await?;
    common::check_delivery(
        &f.delivered("reconnected").await?,
        &f.expected(&[f.id(Cat::Jobs)]).await?,
    )?;
    proxied.close().await;
    drop(proxy);

    f.finish().await
}

/// A server that closes every new connection must not make the waker reconnect in a tight loop, even when
/// it is called again at once after each failure (as `Wakers::any` does).
#[tokio::test]
async fn a_refused_connection_is_retried_with_backoff() -> Result<()> {
    let f = Fixture::new(pges_postgres::store::Outbox::Disabled).await?;
    let jobs = f.id(Cat::Jobs);
    let proxy = proxy::Proxy::start(pges_testing::server().await?).await?;
    proxy.refuse();
    let proxied = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .connect_lazy(&proxy.url(&f.db)?)?;
    let mut waker = notify::NotifyWaker::with_separate_pool(&f.store, proxied.clone());
    let watching = known(5, &[jobs]);

    let start = tokio::time::Instant::now();
    let mut failures = 0;
    while start.elapsed() < Duration::from_millis(1600) {
        match tokio::time::timeout(Duration::from_secs(10), waker.changed(&watching)).await {
            Ok(Err(_)) => failures += 1,
            Ok(Ok(high)) => anyhow::bail!("woken while refused: {high:?}"),
            Err(_) => anyhow::bail!("the call did not return"),
        }
    }
    // Attempts at about 0, 0.1, 0.3, 0.7, 1.5 and 3.1 s.
    let attempts = proxy.accepted();
    ensure!(
        (4..=7).contains(&attempts),
        "{attempts} attempts, {failures} failures"
    );
    ensure!(
        failures == attempts,
        "{attempts} attempts, {failures} failures"
    );

    // A call dropped during the wait does not connect early.
    assert!(pending(&mut waker, &watching, SHORT).await);
    ensure!(proxy.accepted() == attempts, "connected during the backoff");

    // Once connections are admitted again it listens, fires once, and wakes on notices.
    proxy.admit();
    woken(&mut waker, &watching).await?;
    send(&f.pool, &format!("1;6;;{jobs}:6")).await?;
    assert_eq!(woken(&mut waker, &watching).await?.category(jobs), at(6));

    drop(waker);
    proxied.close().await;
    drop(proxy);
    f.finish().await
}
