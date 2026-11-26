//! Cancellation: drop a future after a given number of polls, then check that no connection it used is left
//! inside a transaction or holding a lock.
//!
//! [`poll_then_drop`] polls a future, waits for its waker between polls, and drops it after the given number
//! of polls, so each count stops it at a later await point. [`check_released`] then looks at every connection
//! of the pool's application. [`drop_at_every_await`] does both for 1, 2, 3, ... polls until the future
//! completes. [`poll_check_drop`] with [`wait_until_waiting`] checks, before the drop, that a statement whose
//! reply a proxy holds reached the server.

use std::{
    future::Future,
    pin::pin,
    str::FromStr,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    task::{Context, Poll, Wake, Waker},
    time::Duration,
};

use anyhow::{Context as _, Result, bail};
use sqlx::{PgPool, postgres::PgConnectOptions};
use tokio::sync::Notify;

use pges_postgres::sql::typed::Query as _;

use crate::sql::statement;

/// The `application_name` of [`pool`]'s connections, which [`check_released`] looks for.
pub const APPLICATION: &str = "pges-cancel";

/// How long [`poll_then_drop`] waits for a wake-up before it gives up.
const WAKE_TIMEOUT: Duration = Duration::from_secs(30);

/// How long [`check_released`] waits for a closed connection's server process to end.
const RELEASE_TIMEOUT: Duration = Duration::from_secs(10);

/// How long [`poll_then_drop`] waits after a wake-up before it polls.
const QUIET: Duration = Duration::from_millis(2);

/// The most polls [`drop_at_every_await`] tries.
const MAX_POLLS: usize = 10_000;

/// The most yields in a row [`poll_then_drop`] polls through before it counts one as a poll.
const MAX_YIELDS: usize = 100_000;

/// A pool of `connections` to the database at `url`, named [`APPLICATION`].
pub async fn pool(url: &str, connections: u32) -> Result<PgPool> {
    // No statement cache and no ping on acquire: a run dropped before a statement was cached, or during the
    // ping (which closes the connection), would change the path of the next run, so the same number of polls
    // would stop it somewhere else.
    let options = PgConnectOptions::from_str(url)?
        .application_name(APPLICATION)
        .statement_cache_capacity(0);

    sqlx::postgres::PgPoolOptions::new()
        .max_connections(connections)
        .test_before_acquire(false)
        .acquire_timeout(Duration::from_secs(10))
        .connect_with(options)
        .await
        .context("connecting the cancellation pool")
}

/// Polls `future` up to `polls` times, waiting for its waker between polls, and drops it unless it completed.
/// Returns its output if it completed within `polls` polls.
///
/// A poll after which the future has already woken itself is a yield, not a wait: it is polled again at once
/// and not counted. sqlx 0.9 yields every few hashes while it derives a SCRAM key, about 800 times per new
/// connection; counted, they made a run that connects need that many polls more.
pub async fn poll_then_drop<F: Future>(future: F, polls: usize) -> Result<Option<F::Output>> {
    poll_check_drop(future, polls, async { Ok(()) }).await
}

/// [`poll_then_drop`], awaiting `before_drop` after the last poll when the future did not complete, and
/// dropping it only then: to check what it sent, for example with [`wait_until_waiting`].
pub async fn poll_check_drop<F, C>(
    future: F,
    polls: usize,
    before_drop: C,
) -> Result<Option<F::Output>>
where
    F: Future,
    C: Future<Output = Result<()>>,
{
    let signal = Arc::new(Signal {
        notify: Notify::new(),
        wakes: AtomicU64::new(0),
    });
    let waker = Waker::from(Arc::clone(&signal));
    let mut future = pin!(future);
    let mut poll = 1;
    let mut yields = 0;

    loop {
        let before = signal.wakes();
        if let Poll::Ready(output) = future.as_mut().poll(&mut Context::from_waker(&waker)) {
            return Ok(Some(output));
        }
        // Tokio defers a yield's wake to the end of the scheduler tick, so it shows only after we yield too.
        tokio::task::yield_now().await;
        if signal.wakes() != before && yields < MAX_YIELDS {
            yields += 1;
            continue;
        }
        yields = 0;
        if poll == polls {
            before_drop.await?;
            return Ok(None);
        }
        poll += 1;

        while signal.wakes() == before {
            tokio::time::timeout(WAKE_TIMEOUT, signal.notify.notified())
                .await
                .with_context(|| format!("the future did not wake within {WAKE_TIMEOUT:?}"))?;
        }
        // Let the rest of a reply arrive, so the next poll runs on to the next round trip whatever the
        // timing, and a number of polls stops every run at the same await point.
        tokio::time::sleep(QUIET).await;
    }
}

/// Checks that no other connection of the pool's application is inside a transaction or holds a lock, and
/// that the connection it checks from is not inside a transaction and holds no advisory lock. Waits for
/// connections that are closing to end; a pooled connection inside a transaction fails at once.
pub async fn check_released(pool: &PgPool) -> Result<()> {
    let deadline = tokio::time::Instant::now() + RELEASE_TIMEOUT;

    loop {
        let mut conn = pool.acquire().await.context("acquiring a connection")?;
        let first = connections(&mut conn).await?;
        let second = connections(&mut conn).await?;
        drop(conn);

        if first.started_micros == second.started_micros {
            bail!("a pooled connection is inside a transaction that nothing ends");
        }
        let (in_transaction, locks) = (
            second.in_transaction.unwrap_or(0),
            second.locks.unwrap_or(0),
        );
        if in_transaction == 0 && locks == 0 {
            return settled(pool, deadline).await;
        }
        if tokio::time::Instant::now() > deadline {
            bail!(
                "after {RELEASE_TIMEOUT:?}, {in_transaction} other connection(s) are inside a transaction \
                 and {locks} lock(s) are held"
            );
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

/// Waits until another connection to `conn`'s database has run `statement` and waits inside its transaction,
/// as one does whose reply a [`proxy`](crate::proxy) holds. Pass it to [`poll_check_drop`] to check that the
/// statement reached the server before the future that sent it is dropped.
pub async fn wait_until_waiting(conn: &mut sqlx::PgConnection, statement: &str) -> Result<()> {
    let deadline = tokio::time::Instant::now() + RELEASE_TIMEOUT;

    loop {
        let row = statement::waiting::Query::fixed()
            .bind(statement::waiting::Params { statement })
            .fetch_one(&mut *conn)
            .await
            .context("reading the waiting connections")?;
        if row.waiting == Some(true) {
            return Ok(());
        }
        if tokio::time::Instant::now() > deadline {
            bail!("after {RELEASE_TIMEOUT:?}, no connection waits after `{statement}`");
        }
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
}

/// Runs the future `make` returns once for each number of polls from 1, dropping it after that many polls
/// and calling [`check_released`] on `pool`, until a run completes. Returns how many polls the completed run
/// took, and its output.
pub async fn drop_at_every_await<Fut, M>(pool: &PgPool, mut make: M) -> Result<(usize, Fut::Output)>
where
    Fut: Future,
    M: FnMut() -> Fut,
{
    check_released(pool).await.context("before the first run")?;
    for polls in 1..=MAX_POLLS {
        let output = poll_then_drop(make(), polls).await?;
        check_released(pool)
            .await
            .with_context(|| format!("dropped after {polls} poll(s)"))?;
        if let Some(output) = output {
            return Ok((polls, output));
        }
    }

    bail!("the future did not complete within {MAX_POLLS} polls")
}

struct Signal {
    notify: Notify,
    wakes: AtomicU64,
}

impl Signal {
    fn wakes(&self) -> u64 {
        self.wakes.load(Ordering::Acquire)
    }
}

impl Wake for Signal {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.wakes.fetch_add(1, Ordering::AcqRel);
        self.notify.notify_one();
    }
}

/// Waits until the pool holds all its connections, open and idle, so each run starts from the same state.
async fn settled(pool: &PgPool, deadline: tokio::time::Instant) -> Result<()> {
    let max = pool.options().get_max_connections();
    if pool.size() < max {
        let mut opened = Vec::with_capacity(max as usize);
        for _ in 0..max {
            opened.push(pool.acquire().await.context("refilling the pool")?);
        }
    }

    while pool.size() < max || pool.num_idle() < max as usize {
        if tokio::time::Instant::now() > deadline {
            bail!("the pool's connections did not come back within {RELEASE_TIMEOUT:?}");
        }
        tokio::time::sleep(Duration::from_millis(1)).await;
    }

    Ok(())
}

async fn connections(conn: &mut sqlx::PgConnection) -> Result<statement::connections::Row> {
    statement::connections::Query::fixed()
        .bind(statement::connections::Params {})
        .fetch_one(conn)
        .await
        .context("reading the connections")
}
