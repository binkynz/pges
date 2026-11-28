//! Leadership: one writer per database, takeover after expiry or release, fencing in the statement.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

mod common;

use std::time::Duration;

use anyhow::Result;
use sqlx::PgPool;
use tokio::time::Instant;

use pges_core::{interval, stream};
use pges_sequencer::{outbox, sequencer};

use common::{Fixture, Running, Target};

async fn wait_for_leader(a: &Running, b: &Running) -> Result<()> {
    let deadline = Instant::now() + Duration::from_secs(10);
    while !a.is_leading() && !b.is_leading() {
        anyhow::ensure!(Instant::now() < deadline, "nobody leads");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    Ok(())
}

#[tokio::test]
async fn one_of_two_sequencers_writes_and_the_other_takes_over() -> Result<()> {
    let f = Fixture::new().await?;
    let config = common::fast();
    let a = f.start(config.clone()).await?;
    let b = f.start(config.clone()).await?;
    wait_for_leader(&a, &b).await?;

    let job = stream::StreamId::new_v7();
    let mut count = 0;
    let sampling = Instant::now();
    while sampling.elapsed() < Duration::from_millis(400) {
        assert!(!(a.is_leading() && b.is_leading()), "two leaders");
        f.append(Target::Job(job), 1).await?;
        count += 1;
    }
    common::wait_sequenced(&f.pool, count).await?;
    let (leader, standby) = if a.is_leading() { (a, b) } else { (b, a) };
    assert!(!standby.is_leading());

    // The leader crashes: the standby takes over only once the lease has expired.
    let crashed = Instant::now();
    leader.kill().await;
    for _ in 0..5 {
        f.append(Target::Order(stream::StreamId::new_v7()), 2)
            .await?;
        count += 1;
    }
    standby.wait_leading().await?;
    let takeover = crashed.elapsed();
    assert!(
        takeover >= config.lease.ttl().get() - config.lease.renew() - Duration::from_millis(100),
        "took over after {takeover:?}, before the lease could expire"
    );
    common::wait_sequenced(&f.pool, count).await?;

    // A graceful stop releases the lease: the next standby does not wait for expiry.
    let next = f.start(config.clone()).await?;
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(!next.is_leading());
    let released = Instant::now();
    standby.stop().await?;
    next.wait_leading().await?;
    assert!(
        released.elapsed() < config.lease.ttl().get(),
        "took {:?}",
        released.elapsed()
    );
    f.append(Target::Job(job), 1).await?;
    count += 1;
    common::wait_sequenced(&f.pool, count).await?;

    let log = common::check_log(&f.pool).await?;
    assert_eq!(log.len(), count);
    next.stop().await?;
    f.finish().await
}

/// A leader that shuts down reports `Stopped` and releases its lease; one whose run is dropped reports
/// `Abandoned` and keeps the lease until it expires.
#[tokio::test]
async fn a_shutdown_reports_stopped_and_a_dropped_run_abandoned() -> Result<()> {
    let f = Fixture::new().await?;
    let mut config = common::fast();
    config.lease = sequencer::Lease::with_renew(interval!(60 s), interval!(1 s)).unwrap();

    let stopped = f.start(config.clone()).await?;
    stopped.wait_leading().await?;
    let watch = stopped.watch.clone();
    stopped.stop().await?;
    let status = watch.status().borrow().clone();
    assert!(matches!(status, sequencer::Status::Stopped), "{status:?}");
    assert!(!lease_held(&f.pool).await?, "the lease was not released");

    let dropped = f.start(config).await?;
    dropped.wait_leading().await?;
    let watch = dropped.watch.clone();
    let errors = dropped.errors();
    anyhow::ensure!(errors.is_empty(), "the sequencer recovered: {errors:?}");
    dropped.kill().await;
    let status = watch.status().borrow().clone();
    assert!(matches!(status, sequencer::Status::Abandoned), "{status:?}");
    assert!(
        lease_held(&f.pool).await?,
        "a dropped run released the lease"
    );

    f.finish().await
}

/// The sequencer still believes it leads (its renewal is far off), but another one has bumped the fence.
/// Its next batch fails the fence check inside the transaction and writes nothing.
#[tokio::test]
async fn a_fenced_sequencer_cannot_write() -> Result<()> {
    let f = Fixture::new().await?;
    let mut config = common::fast();
    config.lease = sequencer::Lease::new(interval!(60 s));
    let stale = f.start(config).await?;
    stale.wait_leading().await?;

    sqlx::query(
        "UPDATE pges.sequencer SET fence = fence + 1, lease_until = now() + interval '1 hour'",
    )
    .execute(&f.pool)
    .await?;
    f.append(Target::Job(stream::StreamId::new_v7()), 1).await?;

    let mut status = stale.watch.status();
    tokio::time::timeout(
        Duration::from_secs(5),
        status.wait_for(|s| matches!(s, sequencer::Status::Standby)),
    )
    .await??;
    tokio::time::sleep(Duration::from_millis(100)).await;
    let (logged, pending) = counts(&f.pool).await?;
    assert_eq!((logged, pending), (0, 1));

    // Once the other lease is gone, the stood-down sequencer takes over and drains what is left.
    sqlx::query("UPDATE pges.sequencer SET lease_until = NULL")
        .execute(&f.pool)
        .await?;
    stale.wait_leading().await?;
    common::wait_sequenced(&f.pool, 1).await?;
    common::check_log(&f.pool).await?;

    stale.stop().await?;
    f.finish().await
}

#[tokio::test]
async fn a_halted_database_stops_the_sequencer() -> Result<()> {
    let f = Fixture::new().await?;
    sqlx::query("UPDATE pges.sequencer SET halted = 'slot invalidated'")
        .execute(&f.pool)
        .await?;

    let running = f.start(common::fast()).await?;
    let result = running.result().await;
    assert!(
        matches!(result.as_ref().map_err(|error| &**error), Err(sequencer::RunError::Halted(reason)) if reason == "slot invalidated"),
        "{result:?}"
    );
    let released: bool = sqlx::query_scalar("SELECT lease_until IS NULL FROM pges.sequencer")
        .fetch_one(&f.pool)
        .await?;
    assert!(released);

    f.finish().await
}

#[tokio::test]
async fn a_token_of_another_source_kind_is_refused() -> Result<()> {
    let f = Fixture::new().await?;
    sqlx::query("UPDATE pges.sequencer SET source = 'logical', source_token = '\\x0102'")
        .execute(&f.pool)
        .await?;

    let running = f.start(common::fast()).await?;
    let result = running.result().await;
    assert!(
        matches!(
            result.as_ref().map_err(|error| &**error),
            Err(sequencer::RunError::SourceMismatch { stored, configured }) if stored == "logical" && configured.as_str() == "outbox"
        ),
        "{result:?}"
    );

    f.finish().await
}

#[test]
fn a_lease_is_renewed_before_it_expires() {
    assert_eq!(sequencer::Lease::DEFAULT.ttl(), interval!(10 s));
    assert_eq!(sequencer::Lease::DEFAULT.renew(), Duration::from_secs(5));
    assert_eq!(
        sequencer::Lease::new(interval::Interval::MIN).renew(),
        Duration::from_micros(500)
    );
    assert_eq!(
        sequencer::Lease::with_renew(interval!(1 s), interval!(1 s)),
        None
    );
    assert_eq!(
        sequencer::Lease::with_renew(interval!(1 s), interval!(2 s)),
        None
    );
    let lease = sequencer::Lease::with_renew(interval!(1 s), interval!(999 ms)).unwrap();
    assert_eq!(lease.renew(), Duration::from_millis(999));
}

#[tokio::test]
async fn every_configuration_is_valid() -> Result<()> {
    let pool = PgPool::connect_lazy("postgres://localhost/unused")?;
    let source = outbox::OutboxSource::new(pool.clone(), outbox::OutboxConfig::default());

    let mut config = sequencer::SequencerConfig::default();
    config.lease = sequencer::Lease::new(interval::Interval::MIN);
    config.acquire_interval = interval::Interval::MAX;
    config.retry_interval = interval::Interval::MIN;
    config.token_interval = Duration::ZERO;
    let _sequencer = sequencer::Sequencer::new(pool, source, config);

    Ok(())
}

async fn counts(pool: &PgPool) -> Result<(i64, i64)> {
    Ok(
        sqlx::query_as("SELECT (SELECT count(*) FROM es.log), (SELECT count(*) FROM es.pending)")
            .fetch_one(pool)
            .await?,
    )
}

async fn lease_held(pool: &PgPool) -> Result<bool> {
    Ok(
        sqlx::query_scalar("SELECT coalesce(lease_until > now(), false) FROM pges.sequencer")
            .fetch_one(pool)
            .await?,
    )
}

/// A migration that drops support for this crate while the sequencer leads (here the `min_compatible` of a
/// store, or of its outbox scope, raised past the crate's schema version) stops it at its next lease renewal
/// with `CrateTooOld`, and its lease is released for a newer sequencer.
#[tokio::test]
async fn a_migration_past_the_crate_stops_the_leader_at_renewal() -> Result<()> {
    for raised in ["es", "es.outbox"] {
        let f = Fixture::new().await?;
        let running = f.start(common::fast()).await?;
        running.wait_leading().await?;
        f.append(Target::Job(stream::StreamId::new_v7()), 1).await?;
        common::wait_sequenced(&f.pool, 1).await?;

        let updated = sqlx::query(
            "UPDATE pges.scopes SET schema_version = 9, min_compatible = 9 WHERE scope = $1",
        )
        .bind(raised)
        .execute(&f.pool)
        .await?;
        assert_eq!(updated.rows_affected(), 1, "{raised}");
        let result = running.result().await;

        assert!(
            matches!(
                result.as_ref().map_err(|error| &**error),
                Err(sequencer::RunError::CrateTooOld { scope, min_compatible, crate_version })
                    if scope == raised && min_compatible.get() == 9 && crate_version.get() == 1
            ),
            "{raised}: {result:?}"
        );
        let released: bool = sqlx::query_scalar("SELECT lease_until IS NULL FROM pges.sequencer")
            .fetch_one(&f.pool)
            .await?;
        assert!(released, "{raised}");
        f.finish().await?;
    }
    Ok(())
}

/// A sequencer of a release a migration no longer supports (here the `min_compatible` of the shared scope,
/// a store or its outbox scope raised past the crate's schema version) does not take a free lease: it stops
/// with `CrateTooOld`, and the lease row is left as it was.
#[tokio::test]
async fn a_sequencer_past_its_crate_does_not_take_a_free_lease() -> Result<()> {
    for raised in ["pges", "es", "es.outbox"] {
        let f = Fixture::new().await?;
        let updated = sqlx::query(
            "UPDATE pges.scopes SET schema_version = 9, min_compatible = 9 WHERE scope = $1",
        )
        .bind(raised)
        .execute(&f.pool)
        .await?;
        assert_eq!(updated.rows_affected(), 1, "{raised}");
        let lease_row = || {
            sqlx::query_as::<_, (i64, Option<String>, bool)>(
                "SELECT fence, leader, lease_until IS NULL FROM pges.sequencer",
            )
            .fetch_one(&f.pool)
        };
        let before = lease_row().await?;
        assert!(before.2, "the lease is free");

        let result = f.start(common::fast()).await?.result().await;

        assert!(
            matches!(
                result.as_ref().map_err(|error| &**error),
                Err(sequencer::RunError::CrateTooOld { scope, min_compatible, crate_version })
                    if scope == raised && min_compatible.get() == 9 && crate_version.get() == 1
            ),
            "{raised}: {result:?}"
        );
        assert_eq!(
            lease_row().await?,
            before,
            "{raised}: the lease was not touched"
        );
        f.finish().await?;
    }
    Ok(())
}
