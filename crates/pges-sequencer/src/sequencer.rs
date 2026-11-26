//! The sequencer: one leader per database writes every store's log.

use std::{
    collections::BTreeSet, fmt, future::Future, num::NonZeroUsize, pin::Pin, sync::Arc,
    time::Duration,
};

use pges_postgres::sql::typed::Query as _;
use sqlx::{Connection, PgPool, Postgres, pool::PoolConnection, postgres::types::PgInterval};
use tokio::time::Instant;

use pges_core::{interval, lsn, store};

#[cfg(feature = "watch-notify")]
use crate::notify;
use crate::{lease, log, source, sql::statement, watch};

/// Orders committed appends into each store's log, gap-free, as the single leader of its database.
///
/// Run one or more sequencers per database; they elect a leader through the lease row `pges.sequencer`. The
/// others wait in standby and take over once the leader's lease expires, or at once when the leader shuts
/// down gracefully.
///
/// Each batch is one transaction that starts with a fence check (`SELECT … FOR SHARE` on the lease row with
/// this term's fence), so a sequencer that lost its lease cannot write: a takeover bumps the fence, and
/// waits for a batch that already holds the row lock. Positions are assigned per store in source order and
/// carry on from the newest log row, so they stay gap-free and unique across takeovers and restarts.
///
/// About once a second ([`SequencerConfig::token_interval`]) a batch also stores the source's resume token
/// and the coarse high-water marks (`pges.stores.last_position`, `categories.last_position`). After a
/// restart, commits the source delivers again are dropped against the log rows written since then.
///
/// The live high-water marks are announced in process through [`Sequencer::watch`].
///
/// ```no_run
/// # async fn example(pool: sqlx::PgPool) -> Result<(), Box<dyn std::error::Error>> {
/// use pges_sequencer::{outbox, sequencer};
///
/// let source = outbox::OutboxSource::new(pool.clone(), outbox::OutboxConfig::default());
/// let sequencer = sequencer::Sequencer::new(pool, source, sequencer::SequencerConfig::default());
/// let watch = sequencer.watch();
/// let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
/// let running = tokio::spawn(sequencer.run(async {
///     let _ = stopped.await;
/// }));
///
/// // ... later: finish the current batch, store the token, release the lease.
/// let _ = stop.send(());
/// running.await??;
/// # let _ = watch;
/// # Ok(())
/// # }
/// ```
pub struct Sequencer<S> {
    pool: PgPool,
    source: S,
    config: SequencerConfig,
    ttl: PgInterval,
    watch: watch::Watch,
}

impl<S: source::Source> Sequencer<S> {
    /// A sequencer reading from `source` and writing through `pool`. The pool needs one connection for the
    /// leader, plus whatever the source uses. Every [`SequencerConfig`] is valid.
    pub fn new(pool: PgPool, source: S, config: SequencerConfig) -> Self {
        Self {
            pool,
            source,
            ttl: pg_interval(config.lease.ttl()),
            config,
            watch: watch::Watch::new(),
        }
    }

    /// The channels this sequencer announces on. Take it before [`Sequencer::run`].
    pub fn watch(&self) -> watch::Watch {
        self.watch.clone()
    }

    /// Runs until `shutdown` resolves or a fatal error occurs.
    ///
    /// The sequencer waits in standby while another one holds the lease, leads once it acquires it, and
    /// returns to standby if it loses it. Database and transient source errors are retried every
    /// [`SequencerConfig::retry_interval`] and reported through [`watch::Watch::status`] as
    /// [`Status::Recovering`]; they do not end the run.
    ///
    /// On shutdown a leader finishes its current batch, stores its token and high-water marks, closes the
    /// source and releases the lease, so a standby takes over at once. `shutdown` is checked between
    /// batches, and while the source opens.
    ///
    /// Fails with the [`RunError`]s that need an operator: [`RunError::Halted`],
    /// [`RunError::SourceMismatch`], a fatal, halting or invalid [`source::SourceError`],
    /// [`RunError::UnknownStore`], [`RunError::InvalidStoreName`] and [`RunError::PositionOverflow`]. The
    /// status then becomes [`Status::Halted`]; after a shutdown it becomes [`Status::Stopped`]. When the
    /// future is dropped before it returns (a panic, an aborted task), it becomes [`Status::Abandoned`].
    pub async fn run<F>(self, shutdown: F) -> Result<(), RunError>
    where
        F: Future<Output = ()> + Send,
    {
        let Self {
            pool,
            mut source,
            config,
            ttl,
            watch,
        } = self;
        let mut ending = Ending {
            watch: &watch,
            status: Status::Abandoned,
        };
        #[cfg(feature = "watch-notify")]
        let notifier = notify::Notifier::default();
        let ctx = Ctx {
            pool: &pool,
            config: &config,
            ttl,
            watch: &watch,
            #[cfg(feature = "watch-notify")]
            notifier: match config.notify {
                Notify::Enabled => Some(&notifier),
                Notify::Disabled => None,
            },
        };
        let mut shutdown = std::pin::pin!(shutdown);

        let sequencing = standby(&ctx, &mut source, shutdown.as_mut());
        #[cfg(feature = "watch-notify")]
        let result = tokio::select! {
            result = sequencing => result,
            never = notifier.run(&pool) => match never {},
        };
        #[cfg(not(feature = "watch-notify"))]
        let result = sequencing.await;
        ending.status = match &result {
            Ok(()) => Status::Stopped,
            Err(error) => Status::Halted(error.to_string().into()),
        };

        result
    }
}

impl<S> fmt::Debug for Sequencer<S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Sequencer")
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

/// How a [`Sequencer`] batches, leases and retries. Start from [`SequencerConfig::default`] and change
/// fields.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct SequencerConfig {
    /// Most commits per batch transaction. Default 1000.
    pub batch_size: NonZeroUsize,
    /// How long the lease lasts without renewal, and how often the leader renews it. A standby takes over
    /// [`Lease::ttl`] after the leader stopped renewing. Default [`Lease::DEFAULT`]: 10 s, renewed every 5 s.
    pub lease: Lease,
    /// How often a standby tries to acquire the lease. Default 1 s.
    pub acquire_interval: interval::Interval,
    /// How often the resume token and the coarse high-water marks are stored. A restart replays at most
    /// about this much. Default 1 s; zero stores them with every batch.
    pub token_interval: Duration,
    /// Pause before retrying after an error. Default 1 s.
    pub retry_interval: interval::Interval,
    /// How many ids of each store's commits the dedupe window keeps beyond those sequenced since the last
    /// stored token: a term starts with up to this many of the store's newest log rows, and after each
    /// stored token the window shrinks back to this many. Covers sources that replay further back than
    /// their token. Default 1024.
    pub dedupe_tail: usize,
    /// The address announced in `pges.sequencer.leader` while leading (the watch endpoint). Default none.
    pub leader: Option<String>,
    /// Whether each batch is announced with `NOTIFY pges_<store>` (one per store the batch wrote to, payload
    /// [`Notice`](pges_core::high_water::Notice)). Default [`Notify::Enabled`].
    #[cfg(feature = "watch-notify")]
    pub notify: Notify,
}

impl Default for SequencerConfig {
    fn default() -> Self {
        Self {
            batch_size: NonZeroUsize::new(1000).unwrap_or(NonZeroUsize::MIN),
            lease: Lease::DEFAULT,
            acquire_interval: interval!(1 s),
            token_interval: Duration::from_secs(1),
            retry_interval: interval!(1 s),
            dedupe_tail: 1024,
            leader: None,
            #[cfg(feature = "watch-notify")]
            notify: Notify::Enabled,
        }
    }
}

/// A lease's time to live, and how often its holder renews it: always more often than it expires.
///
/// ```
/// use pges_core::interval;
/// use pges_sequencer::sequencer;
///
/// let lease = sequencer::Lease::new(interval!(10 s));
/// assert_eq!(lease.renew(), std::time::Duration::from_secs(5));
///
/// const FAST: sequencer::Lease = match sequencer::Lease::with_renew(interval!(800 ms), interval!(200 ms)) {
///     Some(lease) => lease,
///     None => panic!("renew must be shorter than the lease"),
/// };
/// assert_eq!(FAST.ttl(), interval!(800 ms));
///
/// assert_eq!(sequencer::Lease::with_renew(interval!(1 s), interval!(1 s)), None);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Lease {
    ttl: interval::Interval,
    renew: Duration,
}

impl Lease {
    /// 10 s, renewed every 5 s.
    pub const DEFAULT: Self = Self::new(interval!(10 s));

    /// A lease of `ttl`, renewed every half of it.
    pub const fn new(ttl: interval::Interval) -> Self {
        Self {
            ttl,
            renew: match ttl.get().checked_div(2) {
                Some(half) => half,
                None => ttl.get(),
            },
        }
    }

    /// A lease of `ttl`, renewed every `renew`; `None` unless `renew` is shorter than `ttl`.
    pub const fn with_renew(ttl: interval::Interval, renew: interval::Interval) -> Option<Self> {
        if renew.get().as_nanos() >= ttl.get().as_nanos() {
            return None;
        }

        Some(Self {
            ttl,
            renew: renew.get(),
        })
    }

    /// How long the lease lasts without renewal.
    pub const fn ttl(self) -> interval::Interval {
        self.ttl
    }

    /// How often the holder renews it: shorter than [`Lease::ttl`], and never zero.
    pub const fn renew(self) -> Duration {
        self.renew
    }
}

impl Default for Lease {
    fn default() -> Self {
        Self::DEFAULT
    }
}

/// Whether a [`Sequencer`] sends `NOTIFY pges_<store>` after each batch that wrote to the store's log.
///
/// The notification is sent after the batch committed, in a statement and transaction of its own on another
/// connection, so it never holds up or fails the log write: a failure is logged and the notification
/// dropped. Its payload is a [`Notice`](pges_core::high_water::Notice). Notifications of batches that
/// follow each other quickly may be merged into one.
#[cfg(feature = "watch-notify")]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum Notify {
    /// Notify after every batch that wrote rows.
    Enabled,
    /// Do not notify.
    Disabled,
}

/// What a [`Sequencer`] is doing, announced through [`watch::Watch::status`].
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum Status {
    /// Waiting for the lease; another sequencer may hold it.
    Standby,
    /// Holding the lease with this fence and sequencing: the source is open and the stores are loaded.
    Leading(Fence),
    /// Retrying after an error; it keeps its lease if it can renew it in time.
    Recovering(Arc<RunError>),
    /// [`Sequencer::run`] returned the [`RunError`] with this text: sequencing stopped on an error that needs
    /// an operator. Alert on it. If the source halted (for example its replication slot was lost),
    /// `pges.sequencer.halted` holds the reason, and every sequencer refuses to start until it is cleared.
    Halted(Arc<str>),
    /// [`Sequencer::run`] returned after a shutdown. A leader released its lease, so a standby takes over at
    /// once.
    Stopped,
    /// The future of [`Sequencer::run`] was dropped before it returned: a panic, or an aborted task. The
    /// lease is not released, so a standby takes over only once it expires. Alert on it.
    Abandoned,
}

/// A leadership term's fence: `pges.sequencer.fence`, bumped by every acquisition. Higher is newer.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Fence(u64);

impl Fence {
    /// The number.
    pub const fn get(self) -> u64 {
        self.0
    }

    pub(crate) fn new(fence: i64) -> Self {
        Self(u64::try_from(fence).unwrap_or(0))
    }

    pub(crate) fn sql(self) -> i64 {
        i64::try_from(self.0).unwrap_or(i64::MAX)
    }
}

impl fmt::Display for Fence {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

/// Why sequencing failed.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum RunError {
    /// A database error. Retried.
    #[error(transparent)]
    Database(#[from] pges_postgres::error::DbError),
    /// The source failed. Retried for database and transient source errors; fatal otherwise.
    #[error(transparent)]
    Source(#[from] source::SourceError),
    /// `pges.sequencer.halted` is set: sequencing stopped for a reason an operator must resolve (for
    /// example an invalidated replication slot, then `resequence`). Fatal.
    #[error("sequencing is halted: {0}")]
    Halted(String),
    /// The stored token was written by another kind of source. Fatal: switching sources needs an operator.
    #[error("the stored token belongs to source {stored:?}, not {configured:?}")]
    SourceMismatch {
        /// The kind stored in `pges.sequencer.source`.
        stored: String,
        /// The configured source's [`source::Source::kind`].
        configured: source::SourceKind,
    },
    /// The source delivered a commit for a store that `pges.stores` does not list. Fatal.
    #[error("the source delivered a commit for unknown store {0}")]
    UnknownStore(store::StoreId),
    /// `pges.stores` lists a schema name that is not a valid store name. Fatal.
    #[error("pges.stores lists an invalid store name {0:?}")]
    InvalidStoreName(String),
    /// A store's positions would pass `i64::MAX`. Fatal.
    #[error("store {0} ran out of positions")]
    PositionOverflow(store::StoreId),
}

impl RunError {
    fn is_fatal(&self) -> bool {
        match self {
            Self::Database(_) => false,
            Self::Source(error) => !matches!(
                error,
                source::SourceError::Database(_) | source::SourceError::Transient(_)
            ),
            _ => true,
        }
    }
}

impl From<sqlx::Error> for RunError {
    fn from(error: sqlx::Error) -> Self {
        Self::Database(error.into())
    }
}

struct Ctx<'a> {
    pool: &'a PgPool,
    config: &'a SequencerConfig,
    ttl: PgInterval,
    watch: &'a watch::Watch,
    #[cfg(feature = "watch-notify")]
    notifier: Option<&'a notify::Notifier>,
}

/// The lease as one sequencer holds it.
struct Term {
    conn: PoolConnection<Postgres>,
    fence: Fence,
    /// Past this local instant the lease may have expired; never sequence after it without renewing.
    deadline: Instant,
    renew_at: Instant,
}

/// Sets a run's final status when it ends, and [`Status::Abandoned`] when its future is dropped before it
/// returns: a watcher (an in-process waker, the gRPC watch server) must not take a dropped run for a live
/// leader.
struct Ending<'a> {
    watch: &'a watch::Watch,
    status: Status,
}

impl Drop for Ending<'_> {
    fn drop(&mut self) {
        self.watch
            .set_status(std::mem::replace(&mut self.status, Status::Abandoned));
    }
}

enum Ended {
    Shutdown,
    LeaseLost,
}

enum Wake {
    Shutdown,
    Renew,
    Persist,
    Ready(Result<(), source::SourceError>),
}

/// Waits for the lease, leads while holding it, and waits again after losing it.
async fn standby<S: source::Source>(
    ctx: &Ctx<'_>,
    source: &mut S,
    mut shutdown: Pin<&mut impl Future<Output = ()>>,
) -> Result<(), RunError> {
    loop {
        ctx.watch.set_status(Status::Standby);

        // Only waiting for a connection races the shutdown. The acquire statement runs to completion: if
        // it were dropped after the lease was taken, standbys would wait for that lease to expire.
        let conn = tokio::select! {
            biased;
            () = shutdown.as_mut() => return Ok(()),
            conn = ctx.pool.acquire() => conn,
        };
        let acquired = match conn {
            Ok(conn) => acquire(ctx, conn).await,
            Err(error) => Err(error.into()),
        };
        match acquired {
            Ok(Some(mut term)) => match lead(ctx, source, &mut term, shutdown.as_mut()).await? {
                Ended::Shutdown => return Ok(()),
                Ended::LeaseLost => ctx.watch.set_status(Status::Standby),
            },
            Ok(None) => {}
            Err(error) if error.is_fatal() => return Err(error),
            Err(error) => ctx.watch.set_status(Status::Recovering(Arc::new(error))),
        }

        tokio::select! {
            biased;
            () = shutdown.as_mut() => return Ok(()),
            () = tokio::time::sleep(ctx.config.acquire_interval.get()) => {}
        }
    }
}

async fn acquire(
    ctx: &Ctx<'_>,
    mut conn: PoolConnection<Postgres>,
) -> Result<Option<Term>, RunError> {
    let sent = Instant::now();
    let Some(acquired) = lease::acquire(&mut conn, ctx.ttl, ctx.config.leader.as_deref()).await?
    else {
        return Ok(None);
    };
    if let Some(reason) = acquired.halted {
        let _ = lease::release(&mut conn, acquired.fence).await;
        return Err(RunError::Halted(reason));
    }

    let deadline = sent + ctx.config.lease.ttl().get();
    ctx.watch.set_lease_deadline(deadline);

    Ok(Some(Term {
        conn,
        fence: acquired.fence,
        deadline,
        renew_at: sent + ctx.config.lease.renew(),
    }))
}

/// Leads one term, then closes the source and, unless the lease was lost, releases it.
async fn lead<S: source::Source>(
    ctx: &Ctx<'_>,
    source: &mut S,
    term: &mut Term,
    shutdown: Pin<&mut impl Future<Output = ()>>,
) -> Result<Ended, RunError> {
    let ended = lead_term(ctx, source, term, shutdown).await;

    // Closed before the lease is released, so the next leader finds the slot free.
    source.close().await;
    match &ended {
        Ok(Ended::LeaseLost) => {}
        Ok(Ended::Shutdown) | Err(_) => release(ctx, term).await,
    }

    ended
}

/// Sequences while holding the lease, recovering from errors while the lease can still be renewed.
async fn lead_term<S: source::Source>(
    ctx: &Ctx<'_>,
    source: &mut S,
    term: &mut Term,
    mut shutdown: Pin<&mut impl Future<Output = ()>>,
) -> Result<Ended, RunError> {
    loop {
        let error = match sequence(ctx, source, term, shutdown.as_mut()).await {
            Ok(ended) => return Ok(ended),
            Err(error) if error.is_fatal() => {
                if let RunError::Source(source::SourceError::Halt(reason)) = &error {
                    record_halt(ctx, term, &reason.to_string()).await;
                }
                return Err(error);
            }
            Err(error) => error,
        };
        ctx.watch.set_status(Status::Recovering(Arc::new(error)));

        loop {
            tokio::select! {
                biased;
                () = shutdown.as_mut() => return Ok(Ended::Shutdown),
                () = tokio::time::sleep(ctx.config.retry_interval.get()) => {}
            }
            if Instant::now() >= term.deadline {
                return Ok(Ended::LeaseLost);
            }

            match reconnect(ctx, term).await {
                Ok(true) => break,
                Ok(false) => return Ok(Ended::LeaseLost),
                Err(error) => ctx.watch.set_status(Status::Recovering(Arc::new(error))),
            }
        }
    }
}

/// Gives the lease up, on the term's connection or, if that one failed, on a new one.
async fn release(ctx: &Ctx<'_>, term: &mut Term) {
    if lease::release(&mut term.conn, term.fence).await.is_ok() {
        return;
    }

    if let Ok(mut conn) = ctx.pool.acquire().await {
        let _ = lease::release(&mut conn, term.fence).await;
    }
}

/// Records the halt under the term's fence, on the term's connection or, if that one failed, on a new one.
async fn record_halt(ctx: &Ctx<'_>, term: &mut Term, reason: &str) {
    if lease::halt(&mut term.conn, term.fence, reason)
        .await
        .is_ok()
    {
        return;
    }

    match ctx.pool.acquire().await {
        Ok(mut conn) => {
            if let Err(error) = lease::halt(&mut conn, term.fence, reason).await {
                tracing::error!(%error, reason, "the halt could not be recorded");
            }
        }
        Err(error) => tracing::error!(%error, reason, "the halt could not be recorded"),
    }
}

/// Replaces the term's connection and renews the lease on it. `false`: the lease is gone.
async fn reconnect(ctx: &Ctx<'_>, term: &mut Term) -> Result<bool, RunError> {
    term.conn = ctx.pool.acquire().await?;

    renew(ctx, term).await
}

async fn renew(ctx: &Ctx<'_>, term: &mut Term) -> Result<bool, RunError> {
    let sent = Instant::now();
    if !lease::renew(&mut term.conn, term.fence, ctx.ttl).await? {
        return Ok(false);
    }

    term.deadline = sent + ctx.config.lease.ttl().get();
    ctx.watch.set_lease_deadline(term.deadline);
    term.renew_at = sent + ctx.config.lease.renew();
    Ok(true)
}

/// The state of a term since it (re)started: everything is read again from the database after an error.
struct Leader {
    logs: log::Logs,
    /// The token stored in `pges.sequencer`, if it belongs to this kind of source.
    stored: Option<Vec<u8>>,
    /// The token of the newest committed batch.
    latest: Option<Vec<u8>>,
    persisted_at: Instant,
}

impl Leader {
    fn is_dirty(&self) -> bool {
        self.logs.is_dirty() || self.latest != self.stored
    }
}

/// Sequences batches until shutdown, a lost lease, or an error.
async fn sequence<S: source::Source>(
    ctx: &Ctx<'_>,
    source: &mut S,
    term: &mut Term,
    mut shutdown: Pin<&mut impl Future<Output = ()>>,
) -> Result<Ended, RunError> {
    let Some(stored) = lease::stored_token(&mut term.conn, term.fence).await? else {
        return Ok(Ended::LeaseLost);
    };
    let kind = source.kind();
    let token = match (stored.source, stored.token) {
        (Some(stored_kind), token) if stored_kind == kind.as_str() => token,
        (Some(stored_kind), Some(token)) if !token.is_empty() => {
            return Err(RunError::SourceMismatch {
                stored: stored_kind,
                configured: kind,
            });
        }
        _ => None,
    };
    if let Some(ended) = open(ctx, source, term, shutdown.as_mut(), token.as_deref()).await? {
        return Ok(ended);
    }
    let mut leader = Leader {
        logs: log::Logs::load(&mut term.conn, ctx.config.dedupe_tail).await?,
        latest: token.clone(),
        stored: token,
        persisted_at: Instant::now(),
    };
    publish(ctx, term, &leader.logs, leader.logs.store_ids()).await?;
    ctx.watch.set_status(Status::Leading(term.fence));

    loop {
        if Instant::now() >= term.renew_at {
            if !renew(ctx, term).await? {
                return Ok(Ended::LeaseLost);
            }
            continue;
        }

        // A token interval too long for an `Instant` never comes due.
        let persist_at = leader
            .is_dirty()
            .then(|| leader.persisted_at.checked_add(ctx.config.token_interval))
            .flatten();
        let wake = tokio::select! {
            biased;
            () = shutdown.as_mut() => Wake::Shutdown,
            () = tokio::time::sleep_until(term.renew_at) => Wake::Renew,
            () = tokio::time::sleep_until(persist_at.unwrap_or(term.renew_at)), if persist_at.is_some() => {
                Wake::Persist
            }
            ready = source.ready() => Wake::Ready(ready),
        };

        let kept = match wake {
            Wake::Shutdown => {
                if leader.is_dirty() {
                    let _ = persist(source, term, &mut leader).await;
                }
                return Ok(Ended::Shutdown);
            }
            Wake::Renew => continue,
            Wake::Persist => persist(source, term, &mut leader).await?,
            Wake::Ready(ready) => {
                ready?;
                batch(ctx, source, term, &mut leader).await?
            }
        };
        if !kept {
            return Ok(Ended::LeaseLost);
        }
    }
}

/// Opens the source while renewing the lease and watching for shutdown: opening can take long, for example
/// while a previous leader's walsender still holds the slot. `Some`: the term ended before the source opened.
async fn open<S: source::Source>(
    ctx: &Ctx<'_>,
    source: &mut S,
    term: &mut Term,
    mut shutdown: Pin<&mut impl Future<Output = ()>>,
    token: Option<&[u8]>,
) -> Result<Option<Ended>, RunError> {
    let mut opening = std::pin::pin!(source.open(token));

    loop {
        tokio::select! {
            biased;
            () = shutdown.as_mut() => return Ok(Some(Ended::Shutdown)),
            opened = &mut opening => {
                opened?;
                return Ok(None);
            }
            () = tokio::time::sleep_until(term.renew_at) => {
                if !renew(ctx, term).await? {
                    return Ok(Some(Ended::LeaseLost));
                }
            }
        }
    }
}

/// One batch transaction: fence, source, log rows, and the token when it is due. `false`: fenced out.
async fn batch<S: source::Source>(
    ctx: &Ctx<'_>,
    source: &mut S,
    term: &mut Term,
    leader: &mut Leader,
) -> Result<bool, RunError> {
    let mut tx = term.conn.begin().await?;
    if !lease::hold_fence(&mut tx, term.fence).await? {
        tx.rollback().await?;
        return Ok(false);
    }

    let batch = source.next(&mut tx, ctx.config.batch_size).await?;
    let kind = source.kind();
    let fence = term.fence;
    let write = async {
        leader.logs.load_missing(&mut tx, &batch.commits).await?;
        let touched = leader.logs.write(&mut tx, &batch.commits).await?;
        leader.latest = Some(batch.token);

        let persist_now =
            leader.is_dirty() && leader.persisted_at.elapsed() >= ctx.config.token_interval;
        if persist_now && !store_token(kind, fence, &mut tx, leader).await? {
            tx.rollback().await?;
            return Ok(None);
        }
        tx.commit().await?;

        Ok(Some((touched, persist_now)))
    };
    let Some((touched, persist_now)) = while_busy(source, write).await? else {
        return Ok(false);
    };

    if persist_now {
        persisted(source, leader).await?;
    }
    if !touched.is_empty() {
        publish(ctx, term, &leader.logs, touched.keys().copied()).await?;
        #[cfg(feature = "watch-notify")]
        queue_notices(ctx, &leader.logs, &touched);
    }

    Ok(true)
}

/// Stores the newest token and the coarse high-water marks in a transaction of their own. `false`: fenced
/// out.
async fn persist<S: source::Source>(
    source: &mut S,
    term: &mut Term,
    leader: &mut Leader,
) -> Result<bool, RunError> {
    let kind = source.kind();
    let fence = term.fence;
    let mut tx = term.conn.begin().await?;
    let write = async {
        if !store_token(kind, fence, &mut tx, leader).await? {
            tx.rollback().await?;
            return Ok(false);
        }
        tx.commit().await?;

        Ok(true)
    };
    if !while_busy(source, write).await? {
        return Ok(false);
    }

    persisted(source, leader).await?;
    Ok(true)
}

/// Runs a batch's database work while the source keeps its own connection alive
/// ([`source::Source::keep_alive`]), however long the work takes.
async fn while_busy<S: source::Source, T>(
    source: &mut S,
    work: impl Future<Output = Result<T, RunError>>,
) -> Result<T, RunError> {
    let mut work = std::pin::pin!(work);

    tokio::select! {
        biased;
        result = &mut work => result,
        Err(error) = source.keep_alive() => Err(error.into()),
    }
}

async fn store_token(
    kind: source::SourceKind,
    fence: Fence,
    tx: &mut sqlx::PgConnection,
    leader: &Leader,
) -> Result<bool, RunError> {
    let token = leader.latest.as_deref().unwrap_or_default();
    if !lease::store_token(tx, fence, kind.as_str(), token).await? {
        return Ok(false);
    }
    leader.logs.persist(tx).await?;

    Ok(true)
}

/// After the transaction holding [`store_token`] committed: everything up to the token is durable.
async fn persisted<S: source::Source>(source: &mut S, leader: &mut Leader) -> Result<(), RunError> {
    leader.logs.persisted();
    leader.stored = leader.latest.clone();
    leader.persisted_at = Instant::now();

    let token = leader.stored.clone().unwrap_or_default();
    source.ack(&token).await?;

    Ok(())
}

/// Queues a notification for every store the batch wrote to, with the marks [`publish`] just announced.
#[cfg(feature = "watch-notify")]
fn queue_notices(ctx: &Ctx<'_>, logs: &log::Logs, touched: &log::Touched) {
    let Some(notifier) = ctx.notifier else {
        return;
    };

    for (id, categories) in touched {
        let high_water = ctx.watch.high_water(*id).borrow().clone();
        if let Some(schema) = logs.schema(*id) {
            notifier.queue(*id, schema, high_water, categories);
        }
    }
}

/// Announces the stores' high-water marks with the WAL position read after their log rows committed.
async fn publish(
    ctx: &Ctx<'_>,
    term: &mut Term,
    logs: &log::Logs,
    stores: impl IntoIterator<Item = store::StoreId>,
) -> Result<(), RunError> {
    let row = statement::wal_insert_lsn::Query::fixed()
        .bind(statement::wal_insert_lsn::Params {})
        .fetch_one(&mut *term.conn)
        .await?;
    let lsn = u64::try_from(row.lsn).ok().map(lsn::Lsn::new);

    for id in stores.into_iter().collect::<BTreeSet<_>>() {
        if let Some(high_water) = logs.high_water(id, lsn) {
            ctx.watch.publish(id, high_water);
        }
    }

    Ok(())
}

/// A lease time to live as a PostgreSQL `interval`. [`interval::Interval::MAX`] fits in its microseconds.
pub(crate) fn pg_interval(ttl: interval::Interval) -> PgInterval {
    PgInterval {
        months: 0,
        days: 0,
        microseconds: i64::try_from(ttl.get().as_micros()).unwrap_or(i64::MAX),
    }
}
