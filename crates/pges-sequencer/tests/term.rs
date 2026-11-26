//! A leadership term around its source: the lease is renewed while the source opens, shutdown is watched
//! during the open, and the source is closed whenever a term ends. A fake source counts the calls.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

mod common;

use std::{
    num::NonZeroUsize,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use anyhow::{Result, bail, ensure};
use sqlx::{PgConnection, PgPool};
use tokio::sync::Notify;

use pges_core::interval;
use pges_sequencer::{sequencer, source, source_kind};

use common::{Fixture, Running};

#[derive(Debug, Clone, Default)]
struct Calls {
    opens: Arc<AtomicUsize>,
    closes: Arc<AtomicUsize>,
}

impl Calls {
    fn opens(&self) -> usize {
        self.opens.load(Ordering::SeqCst)
    }

    fn closes(&self) -> usize {
        self.closes.load(Ordering::SeqCst)
    }
}

/// How the fake source behaves.
#[derive(Debug, Clone, Copy)]
enum Behaviour {
    /// Opens at once and never has commits.
    Idle,
    /// Takes this long to open, then never has commits.
    SlowOpen(Duration),
    /// Never finishes opening.
    StuckOpen,
    /// Opens at once, then fails fatally.
    Broken,
    /// Opens at once, then halts once `Fake::halt` is notified.
    HaltWhenTold,
}

struct Fake {
    calls: Calls,
    behaviour: Behaviour,
    halt: Arc<Notify>,
}

impl source::Source for Fake {
    fn kind(&self) -> source::SourceKind {
        source_kind!("fake")
    }

    async fn open(&mut self, _token: Option<&[u8]>) -> Result<(), source::SourceError> {
        self.calls.opens.fetch_add(1, Ordering::SeqCst);
        match self.behaviour {
            Behaviour::Idle | Behaviour::Broken | Behaviour::HaltWhenTold => {}
            Behaviour::SlowOpen(duration) => tokio::time::sleep(duration).await,
            Behaviour::StuckOpen => std::future::pending().await,
        }
        Ok(())
    }

    async fn ready(&mut self) -> Result<(), source::SourceError> {
        match self.behaviour {
            Behaviour::Broken => Err(source::SourceError::Fatal("broken".into())),
            Behaviour::HaltWhenTold => {
                self.halt.notified().await;
                Err(source::SourceError::Halt("told to halt".into()))
            }
            _ => std::future::pending().await,
        }
    }

    async fn next(
        &mut self,
        _tx: &mut PgConnection,
        _limit: NonZeroUsize,
    ) -> Result<source::SourceBatch, source::SourceError> {
        Ok(source::SourceBatch::default())
    }

    async fn ack(&mut self, _token: &[u8]) -> Result<(), source::SourceError> {
        Ok(())
    }

    async fn close(&mut self) {
        self.calls.closes.fetch_add(1, Ordering::SeqCst);
    }
}

async fn start(f: &Fixture, behaviour: Behaviour) -> Result<(Running, Calls)> {
    let calls = Calls::default();
    let source = Fake {
        calls: calls.clone(),
        behaviour,
        halt: Arc::new(Notify::new()),
    };
    let running = Running::start(f.db.pool().await?, source, common::fast())?;
    Ok((running, calls))
}

#[tokio::test]
async fn shutdown_ends_an_open_that_does_not_finish() -> Result<()> {
    let f = Fixture::new().await?;
    let (running, calls) = start(&f, Behaviour::StuckOpen).await?;
    let watch = running.watch.clone();
    wait_until(|| calls.opens() == 1).await?;
    assert!(lease_held(&f.pool).await?);

    running.stop().await?;
    assert_eq!(calls.closes(), 1);
    assert!(!lease_held(&f.pool).await?, "the lease was not released");
    assert!(matches!(
        *watch.status().borrow(),
        sequencer::Status::Stopped
    ));

    f.finish().await
}

/// The source takes several lease TTLs to open (as the logical source can, while a previous leader's
/// walsender holds the slot). The leader renews meanwhile, so the standby never takes over.
#[tokio::test]
async fn the_lease_is_renewed_while_the_source_opens() -> Result<()> {
    let f = Fixture::new().await?;
    let open_for = Duration::from_secs(3);
    assert!(open_for > common::fast().lease.ttl().get() * 3);
    let (slow, slow_calls) = start(&f, Behaviour::SlowOpen(open_for)).await?;
    wait_until(|| slow_calls.opens() == 1).await?;
    let (standby, standby_calls) = start(&f, Behaviour::Idle).await?;

    let opened_by = tokio::time::Instant::now() + open_for;
    while tokio::time::Instant::now() + Duration::from_millis(200) < opened_by {
        ensure!(
            !standby.is_leading(),
            "the standby took over during the open"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    slow.wait_leading().await?;
    assert_eq!(standby_calls.opens(), 0);

    slow.stop().await?;
    assert_eq!(slow_calls.closes(), 1);
    standby.wait_leading().await?;
    standby.stop().await?;
    assert_eq!(standby_calls.closes(), 1);
    f.finish().await
}

#[tokio::test]
async fn the_source_is_closed_when_the_lease_is_lost() -> Result<()> {
    let f = Fixture::new().await?;
    let (running, calls) = start(&f, Behaviour::Idle).await?;
    running.wait_leading().await?;
    assert_eq!(calls.closes(), 0);

    sqlx::query(
        "UPDATE pges.sequencer SET fence = fence + 1, lease_until = now() + interval '1 hour'",
    )
    .execute(&f.pool)
    .await?;
    let mut status = running.watch.status();
    tokio::time::timeout(
        Duration::from_secs(10),
        status.wait_for(|s| matches!(s, sequencer::Status::Standby)),
    )
    .await??;
    assert_eq!(calls.closes(), 1);

    // Leading again opens the source again.
    sqlx::query("UPDATE pges.sequencer SET lease_until = NULL")
        .execute(&f.pool)
        .await?;
    running.wait_leading().await?;
    assert_eq!(calls.opens(), 2);

    running.stop().await?;
    assert_eq!(calls.closes(), 2);
    f.finish().await
}

#[tokio::test]
async fn a_fatal_error_closes_the_source_and_halts_the_status() -> Result<()> {
    let f = Fixture::new().await?;
    let (running, calls) = start(&f, Behaviour::Broken).await?;

    let result = running.result().await;
    assert!(
        matches!(
            &result,
            Err(sequencer::RunError::Source(source::SourceError::Fatal(_)))
        ),
        "{result:?}"
    );
    assert_eq!(calls.closes(), 1);
    assert!(!lease_held(&f.pool).await?, "the lease was not released");

    f.finish().await
}

/// The term's connection dies while the leader idles; on shutdown the lease is released on a fresh one, so
/// a standby need not wait for it to expire.
#[tokio::test]
async fn a_dead_term_connection_still_releases_the_lease() -> Result<()> {
    let f = Fixture::new().await?;
    let name = format!("t_term_{}", f.db.name());
    let (running, _) = start_named(&f, &name, Behaviour::Idle).await?;
    running.wait_leading().await?;

    terminate(&f.pool, &name).await?;
    running.stop().await?;
    assert!(!lease_held(&f.pool).await?, "the lease was not released");

    f.finish().await
}

/// The term's connection dies, then the source halts: the halt is recorded on a fresh connection.
#[tokio::test]
async fn a_dead_term_connection_still_records_the_halt() -> Result<()> {
    let f = Fixture::new().await?;
    let name = format!("t_term_{}", f.db.name());
    let halt = Arc::new(Notify::new());
    let source = Fake {
        calls: Calls::default(),
        behaviour: Behaviour::HaltWhenTold,
        halt: halt.clone(),
    };
    let running = Running::start(f.named_pool(&name).await?, source, long_lease())?;
    running.wait_leading().await?;

    terminate(&f.pool, &name).await?;
    halt.notify_one();
    let result = running.result().await;
    assert!(
        matches!(
            &result,
            Err(sequencer::RunError::Source(source::SourceError::Halt(_)))
        ),
        "{result:?}"
    );
    let halted: Option<String> = sqlx::query_scalar("SELECT halted FROM pges.sequencer")
        .fetch_one(&f.pool)
        .await?;
    assert_eq!(halted.as_deref(), Some("told to halt"));
    assert!(!lease_held(&f.pool).await?, "the lease was not released");

    f.finish().await
}

/// A lease that is not renewed within a test, so the term's connection is not used until the test acts.
fn long_lease() -> sequencer::SequencerConfig {
    let mut config = common::fast();
    config.lease = sequencer::Lease::new(interval!(60 s));
    config
}

async fn start_named(f: &Fixture, name: &str, behaviour: Behaviour) -> Result<(Running, Calls)> {
    let calls = Calls::default();
    let source = Fake {
        calls: calls.clone(),
        behaviour,
        halt: Arc::new(Notify::new()),
    };
    let running = Running::start(f.named_pool(name).await?, source, long_lease())?;
    Ok((running, calls))
}

/// Ends every session of `application_name` and waits until they are gone.
async fn terminate(pool: &PgPool, application_name: &str) -> Result<()> {
    let ended: Vec<bool> = sqlx::query_scalar(
        "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE application_name = $1",
    )
    .bind(application_name)
    .fetch_all(pool)
    .await?;
    ensure!(!ended.is_empty(), "no session of {application_name}");
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let left: i64 =
            sqlx::query_scalar("SELECT count(*) FROM pg_stat_activity WHERE application_name = $1")
                .bind(application_name)
                .fetch_one(pool)
                .await?;
        if left == 0 {
            return Ok(());
        }
        ensure!(
            tokio::time::Instant::now() < deadline,
            "{left} sessions left"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

async fn lease_held(pool: &PgPool) -> Result<bool> {
    Ok(
        sqlx::query_scalar("SELECT lease_until IS NOT NULL FROM pges.sequencer")
            .fetch_one(pool)
            .await?,
    )
}

async fn wait_until(done: impl Fn() -> bool) -> Result<()> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while !done() {
        if tokio::time::Instant::now() > deadline {
            bail!("timed out");
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    Ok(())
}
