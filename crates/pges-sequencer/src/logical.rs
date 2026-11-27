//! The logical replication source: commits decoded from the write-ahead log through a replication slot.

use std::{
    collections::{HashMap, VecDeque},
    num::NonZeroUsize,
    time::Duration,
};

use sqlx::{Connection, Executor, PgConnection, PgPool};
use tokio::time::Instant;
use uuid::Uuid;

use pges_core::{category, commit, interval, lsn, position, store};
use pges_postgres::{
    limit,
    sql::{template, typed::Query as _},
};
use pges_replication::{config, connection, error, name, pgoutput, stream};

use crate::{lease, log, sequencer, source, source_kind, sql::statement};

mod resequence;

/// Sequences every store without the outbox option, in WAL commit order, from the publication `pges` through
/// a logical replication slot (pgoutput, `streaming 'off'`).
///
/// - The slot is named `pges_<oid of the database>` unless [`LogicalConfig::slot`] names another, so several
///   databases can share a server. [`Source::open`](source::Source::open) creates it, and the publication,
///   if they are missing.
/// - [`Source::ready`](source::Source::ready) reads the stream into a buffer of whole transactions and
///   answers keepalives. [`Source::next`](source::Source::next) hands out buffered transactions and ignores
///   its transaction argument. A status update goes out at least every [`LogicalConfig::status_interval`],
///   also while the sequencer works through a backlog or waits on a slow batch
///   ([`Source::keep_alive`](source::Source::keep_alive)), so the server does not end the connection after
///   `wal_sender_timeout`.
/// - The token is the end LSN of the last transaction of a batch (eight bytes, big-endian). Opening from a
///   token drops every transaction that ends at or below it. [`Source::ack`](source::Source::ack) confirms
///   the token to the slot, so the server can release the WAL before it. Nothing else confirms the slot:
///   it is never confirmed past a token stored in `pges.sequencer`.
/// - While idle, with nothing buffered, it hands out the WAL position of the server's keepalives as the
///   token of an empty batch, at most once per [`LogicalConfig::status_interval`]. Once the sequencer
///   stores that token it is confirmed like any other, so the slot does not hold back WAL that carried
///   nothing for the publication.
/// - A slot it creates gets its consistent point stored as the token at once, if no token is stored.
/// - [`Source::close`](source::Source::close) ends streaming when the sequencer stops leading, so the slot is
///   free for the next leader at once.
/// - Stores created with the outbox option are left to [`OutboxSource`](crate::outbox::OutboxSource): their
///   commits are skipped. A database is sequenced by one kind of source.
///
/// # Slot loss
///
/// An invalidated slot, or a missing slot while a token is stored, means commits may have been skipped. So
/// does a slot confirmed past the stored token ([`SlotLost::Ahead`]): the server would silently start
/// after the slot's confirmed position. That happens when a physical standby is promoted that has the slot
/// (a failover slot) but not the sequencer's last transactions: their log rows and token are lost, and the
/// commits they sequenced would never be delivered again. The source then fails with
/// [`source::SourceError::Halt`] carrying a [`SlotLost`]; the sequencer records it in
/// `pges.sequencer.halted` and stops. It never recreates the slot by itself. The checks run every time the
/// source opens, before `START_REPLICATION`.
///
/// A slot created without a stored token only sees commits from its creation on. When the source creates
/// the slot, it checks the stores in the snapshot exported at the slot's consistent point: if they hold any
/// commit, the slot would never deliver it, so the source drops the slot again and fails with
/// [`source::SourceError::Halt`] carrying [`SlotLost::CommitsBeforeSlot`] instead of skipping them.
/// Create the slot before the first append: run [`LogicalSource::bootstrap`] (`pges-sequencer bootstrap`)
/// before writers start, or start the sequencer first.
///
/// [`LogicalSource::resequence`] (`pges-sequencer resequence`) recovers from each of these halts: it
/// creates a new slot, sequences the commits the sequencers never saw, and lets them resume from the new
/// slot.
///
/// # Requirements
///
/// `wal_level = logical`; a role with the `REPLICATION` attribute for `replication`, allowed by
/// `pg_hba.conf` to connect to the database; `max_replication_slots` and `max_wal_senders` with room for the
/// slot. Terminating the walsender of a previous leader needs the same role or `pg_signal_backend`. Writers
/// with `synchronous_commit = off` add up to `wal_writer_delay` to sequencing latency, because decoding sees
/// only flushed WAL.
pub struct LogicalSource {
    pool: PgPool,
    replication: config::Config,
    config: LogicalConfig,
    slot: Option<name::SlotName>,
    stream: Option<stream::ReplicationStream>,
    relations: pgoutput::Relations,
    routes: HashMap<String, Route>,
    held: Option<stream::ReplicationMessage>,
    open: Option<Vec<source::CommitRef>>,
    buffer: VecDeque<Transaction>,
    buffered: usize,
    resume: lsn::Lsn,
    delivered: lsn::Lsn,
    acked: lsn::Lsn,
    /// The newest WAL position up to which the stream carried nothing to sequence, seen while nothing was
    /// buffered: the token of the next empty batch.
    idle: lsn::Lsn,
    idle_reported: Instant,
    status_sent: Instant,
}

impl LogicalSource {
    /// A logical source that reads the database through `pool` and streams it over a replication connection
    /// made from `replication`. Both must reach the same database; [`Source::open`](source::Source::open)
    /// checks it.
    pub fn new(pool: PgPool, replication: config::Config, config: LogicalConfig) -> Self {
        Self {
            pool,
            replication,
            slot: config.slot.clone(),
            config,
            stream: None,
            relations: pgoutput::Relations::new(),
            routes: HashMap::new(),
            held: None,
            open: None,
            buffer: VecDeque::new(),
            buffered: 0,
            resume: lsn::Lsn::ZERO,
            delivered: lsn::Lsn::ZERO,
            acked: lsn::Lsn::ZERO,
            idle: lsn::Lsn::ZERO,
            idle_reported: Instant::now(),
            status_sent: Instant::now(),
        }
    }

    /// The slot this source streams from: [`LogicalConfig::slot`], or `pges_<oid>` once the source has been
    /// opened or bootstrapped.
    pub fn slot(&self) -> Option<&name::SlotName> {
        self.slot.as_ref()
    }

    /// Creates the publication and the slot if they are missing, without streaming and without taking the
    /// sequencer's lease. Run it once when deploying, after the migrations and before any writer starts, so
    /// the slot exists before the first append and no sequencer start can find commits from before it.
    ///
    /// - An existing slot is checked as [`Source::open`](source::Source::open) checks it, and kept:
    ///   [`Bootstrap::Exists`]. Its failover setting is kept unless [`LogicalConfig::failover`] is
    ///   [`SlotFailover::Set`] to a different value; then it is changed with `ALTER_REPLICATION_SLOT`
    ///   (PostgreSQL 17+): [`Bootstrap::FailoverChanged`]. That fails with
    ///   [`source::SourceError::Transient`] while a process (a sequencer) streams from the slot.
    /// - A missing slot is created ([`Bootstrap::Created`]) if no token is stored, after the same check of
    ///   the stores in the snapshot exported at the slot's consistent point, and the consistent point is
    ///   stored as the token. If the stores already hold commits, the slot is dropped again and this fails
    ///   with [`source::SourceError::Halt`] carrying [`SlotLost::CommitsBeforeSlot`]. Bootstrap records that
    ///   halt in `pges.sequencer.halted`, as a sequencer would, unless a sequencer holds the lease, so
    ///   [`LogicalSource::resequence`] can recover at once.
    /// - A missing slot with a stored token fails with [`source::SourceError::Halt`] carrying
    ///   [`SlotLost::Missing`], an invalidated slot with [`SlotLost::Invalidated`], and a slot confirmed
    ///   past the stored token with [`SlotLost::Ahead`]. Bootstrap does not record these halts in
    ///   `pges.sequencer.halted`: a sequencer already ran, and meets them itself. A slot of another database
    ///   or plugin, a database whose stores use the outbox, or a replication connection to another database
    ///   is [`source::SourceError::Invalid`].
    ///
    /// Safe to run again. Run it before sequencers and writers start: run during a sequencer's first start, the
    /// sequencer can end the bootstrap's walsender before its check finishes. It needs the same grants as the source.
    pub async fn bootstrap(&mut self) -> Result<Bootstrap, source::SourceError> {
        let prepared = self.prepare().await?;
        if self.routes.values().any(|route| *route == Route::Skipped)
            && !self
                .routes
                .values()
                .any(|route| matches!(route, Route::Store(_)))
        {
            return Err(source::SourceError::Invalid(
                "the database's stores use the outbox; the logical source needs no slot".to_owned(),
            ));
        }
        if let Some(state) = prepared.existing {
            state.check(&prepared.slot, &prepared.database.name)?;
            let token = stored_token(&self.pool)
                .await?
                .map(|token| decode_token(&token))
                .transpose()?
                .flatten();
            self.check_confirmed(&state, &prepared.slot, token).await?;
            let failover = match self.config.failover {
                SlotFailover::Set(failover) if failover != state.failover => failover,
                _ => return Ok(Bootstrap::Exists),
            };

            let mut replication = self.connect_checked(&prepared.database).await?;
            let altered = self
                .alter_failover(&mut replication, &prepared.slot, failover)
                .await;
            let _ = replication.close().await;

            return altered;
        }
        if stored_token(&self.pool).await?.is_some() {
            return Err(halt(SlotLost::Missing {
                slot: prepared.slot.to_string(),
            }));
        }

        let mut replication = self.connect_checked(&prepared.database).await?;
        let created = self.create_slot(&mut replication, &prepared.slot).await;
        let _ = replication.close().await;

        // Recorded like a sequencer's halt, so `resequence` can recover before any sequencer started.
        if let Err(source::SourceError::Halt(lost)) = &created
            && matches!(
                lost.downcast_ref::<SlotLost>(),
                Some(SlotLost::CommitsBeforeSlot { .. })
            )
        {
            lease::halt_unleased(&self.pool, &lost.to_string()).await?;
        }

        created
    }

    /// Recovers from a halt ([`SlotLost`]): creates a new slot, sequences the commits the sequencers never
    /// saw, and stores a token that makes them resume from the new slot. Stop every sequencer first; start
    /// them again once this returns.
    ///
    /// It refuses with [`ResequenceError::WriterRole`] unless every role in
    /// [`ResequenceConfig::writer_roles`] caps its transactions at the stores' `max_txn_age`, with
    /// [`ResequenceError::NotHalted`] unless `pges.sequencer.halted` is set, and with
    /// [`ResequenceError::LeaseHeld`] while a sequencer holds the lease. Otherwise it takes the lease
    /// itself, so no sequencer starts meanwhile, and then:
    ///
    /// 1. drops the old slot if it still exists (a slot in use is [`ResequenceError::SlotInUse`]) and
    ///    creates a new one, with the old slot's failover setting unless [`LogicalConfig::failover`] sets
    ///    one. The new slot delivers every commit after its consistent point;
    /// 2. finds, as of that consistent point, every commit of each store that has no log row, among the
    ///    commits whose id is at most the store's `max_txn_age` plus twice its `max_clock_skew` older than
    ///    its newest sequenced commit, or newer (both recorded in `pges.stores`, [`pges_postgres::limit`]);
    /// 3. writes their log rows in commit id order, continuing the store's positions without gaps, records
    ///    the positions each category got (`categories.resequenced_from`, `resequenced_through`, which
    ///    retention scans whole), and in the same transaction stores the consistent point as the token,
    ///    clears `pges.sequencer.halted` and releases the lease.
    ///
    /// The recovered commits are ordered by commit id, not by when they committed: within a stream they
    /// keep their order; across streams causal order is best effort. A writer transaction open for longer
    /// than `max_txn_age` can make a commit be missed or sequenced twice, which is why the writer roles are
    /// checked first; a session that overrides `transaction_timeout` with `SET` is not seen.
    ///
    /// If it fails before the last step, nothing is written, the new slot is dropped (unless the lease was
    /// lost, [`ResequenceError::LeaseLost`]) and the lease is released; run it again. [`ResequenceError::CommitUnknown`] means the last step may have committed.
    /// Running it again is safe either way: it refuses with `NotHalted` if the step committed.
    pub async fn resequence(
        &mut self,
        config: &ResequenceConfig,
    ) -> Result<Resequenced, ResequenceError> {
        resequence::run(self, config).await
    }
}

impl source::Source for LogicalSource {
    fn kind(&self) -> source::SourceKind {
        KIND
    }

    async fn open(&mut self, token: Option<&[u8]>) -> Result<(), source::SourceError> {
        self.reset();
        let token = token.map(decode_token).transpose()?.flatten();
        self.resume = token.unwrap_or(lsn::Lsn::ZERO);
        self.delivered = self.resume;
        self.acked = self.resume;
        self.idle = self.resume;
        self.idle_reported = Instant::now();

        let prepared = self.prepare().await?;
        let create = match &prepared.existing {
            Some(state) => {
                state.check(&prepared.slot, &prepared.database.name)?;
                self.check_confirmed(state, &prepared.slot, token).await?;
                false
            }
            None if token.is_some() => {
                return Err(halt(SlotLost::Missing {
                    slot: prepared.slot.to_string(),
                }));
            }
            None => true,
        };

        let mut replication = self.connect_checked(&prepared.database).await?;
        if create {
            self.create_slot(&mut replication, &prepared.slot).await?;
        }

        let stream = self.start(replication, &prepared.slot, token).await?;
        self.stream = Some(stream);
        self.status_sent = Instant::now();

        Ok(())
    }

    async fn ready(&mut self) -> Result<(), source::SourceError> {
        loop {
            let waiting = self.buffer.is_empty();
            if !waiting && self.buffered >= self.config.read_ahead.get() {
                return self.heartbeat().await;
            }
            if waiting && self.idle_due() {
                return Ok(());
            }

            let message = match self.held.take() {
                Some(message) => message,
                None => {
                    // While transactions are buffered, read only what has already arrived.
                    let wait = if waiting {
                        self.idle_wait()
                    } else {
                        Duration::ZERO
                    };
                    let stream = self.stream()?;
                    match tokio::time::timeout(wait, stream.next()).await {
                        Ok(message) => message
                            .map_err(transient)?
                            .ok_or_else(|| transient_message("the server ended streaming"))?,
                        Err(_) if waiting => {
                            self.heartbeat().await?;
                            continue;
                        }
                        Err(_) => return self.heartbeat().await,
                    }
                }
            };

            if let Handled::NeedsRoutes { message, schema } = self.handle(message).await? {
                // Held until the stores are read: if this is cancelled, the next call handles it again.
                self.held = Some(message);
                self.refresh_routes().await?;
                self.routes.entry(schema).or_insert(Route::Skipped);
            }
        }
    }

    async fn next(
        &mut self,
        _tx: &mut PgConnection,
        limit: NonZeroUsize,
    ) -> Result<source::SourceBatch, source::SourceError> {
        self.heartbeat().await?;

        // Recorded only while nothing was buffered, so every buffered transaction ends after it.
        if self.idle > self.delivered {
            self.delivered = self.idle;
            self.idle_reported = Instant::now();
        }
        let mut commits = Vec::new();
        while let Some(front) = self.buffer.front() {
            if !commits.is_empty() && commits.len() + front.commits.len() > limit.get() {
                break;
            }
            let Some(transaction) = self.buffer.pop_front() else {
                break;
            };
            self.buffered -= transaction.commits.len();
            commits.extend(transaction.commits);
            self.delivered = self.delivered.max(transaction.end);
        }

        Ok(source::SourceBatch::new(
            commits,
            encode_token(self.delivered),
        ))
    }

    async fn ack(&mut self, token: &[u8]) -> Result<(), source::SourceError> {
        let Some(acked) = decode_token(token)? else {
            return Ok(());
        };
        self.acked = self.acked.max(acked);

        if let Some(stream) = &mut self.stream {
            stream.confirm(self.acked).await.map_err(transient)?;
            self.status_sent = Instant::now();
        }

        Ok(())
    }

    async fn keep_alive(&mut self) -> Result<(), source::SourceError> {
        if self.stream.is_none() {
            return Ok(());
        }

        loop {
            tokio::time::sleep_until(self.status_sent + self.config.status_interval.get()).await;
            self.heartbeat().await?;
        }
    }

    /// Ends streaming (`CopyDone`) and closes the replication connection, so the walsender lets go of the
    /// slot at once instead of after `wal_sender_timeout`. A server that does not answer within a second
    /// is left to notice the closed socket.
    async fn close(&mut self) {
        let Some(stream) = self.stream.take() else {
            return;
        };
        self.reset();

        let stopped = tokio::time::timeout(CLOSE_TIMEOUT, async {
            let connection = stream.stop().await.map_err(transient)?;
            connection.close().await.map_err(transient)
        })
        .await;
        match stopped {
            Ok(Ok(())) => {}
            Ok(Err(error)) => tracing::debug!(%error, "closing the replication stream failed"),
            Err(_) => tracing::debug!("closing the replication stream timed out"),
        }
    }
}

impl std::fmt::Debug for LogicalSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LogicalSource")
            .field("slot", &self.slot)
            .field("delivered", &self.delivered)
            .field("acked", &self.acked)
            .field("buffered", &self.buffered)
            .finish_non_exhaustive()
    }
}

/// How the [`LogicalSource`] names its slot and talks to the server.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct LogicalConfig {
    /// The replication slot. Default `None`: `pges_<oid of the database>`. Slot names are server-wide.
    pub slot: Option<name::SlotName>,
    /// Whether the slot is synchronised to physical standbys, so it survives a failover (PostgreSQL 17+,
    /// with `sync_replication_slots` on the standby). [`Source::open`](source::Source::open) and
    /// [`LogicalSource::bootstrap`] apply it to a slot they create; only `bootstrap` changes an existing
    /// slot, and only for [`SlotFailover::Set`]. Default [`SlotFailover::Keep`].
    ///
    /// Promote a synchronous standby (in `synchronous_standby_names` and `synchronized_standby_slots`). An
    /// asynchronous one can have the slot's position without the sequencer's last transactions; the
    /// source then halts with [`SlotLost::Ahead`] until [`LogicalSource::resequence`] runs.
    pub failover: SlotFailover,
    /// The longest time between status updates to the server. Keep it well below the server's
    /// `wal_sender_timeout` (60 s by default), which ends a replication connection that stays silent for
    /// longer. While idle, it is also the shortest time between two empty batches that move the token, and
    /// so the slot, past WAL with nothing to sequence. Default 10 s.
    pub status_interval: interval::Interval,
    /// How many commits [`Source::ready`](source::Source::ready) buffers ahead while messages keep arriving.
    /// A transaction is always buffered whole. Default 1000.
    pub read_ahead: NonZeroUsize,
    /// How long opening keeps retrying while another process still streams from the slot (a previous leader
    /// whose walsender is being terminated). Default 30 s.
    pub start_timeout: interval::Interval,
}

impl Default for LogicalConfig {
    fn default() -> Self {
        Self {
            slot: None,
            failover: SlotFailover::Keep,
            status_interval: interval!(10 s),
            read_ahead: NonZeroUsize::new(1000).unwrap_or(NonZeroUsize::MIN),
            start_timeout: interval!(30 s),
        }
    }
}

/// The failover setting [`LogicalConfig::failover`] asks for.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[non_exhaustive]
pub enum SlotFailover {
    /// An existing slot keeps its setting; a new slot is created without failover.
    #[default]
    Keep,
    /// A new slot is created with this setting, and [`LogicalSource::bootstrap`] changes an existing slot
    /// to it.
    Set(connection::Failover),
}

/// What [`LogicalSource::bootstrap`] found.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum Bootstrap {
    /// It created the slot.
    Created,
    /// The slot existed and is valid; it was kept as it was.
    Exists,
    /// The slot existed and is valid; its failover setting was changed to the one
    /// [`SlotFailover::Set`] asked for.
    FailoverChanged,
}

/// How [`LogicalSource::resequence`] runs. The window it searches comes from each store's writer limits
/// (`pges.stores.max_txn_age` and `max_clock_skew`, [`pges_postgres::limit`]), not from here.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct ResequenceConfig {
    /// The roles that append. Each must cap its transactions (`transaction_timeout`) at the shortest
    /// `max_txn_age` of the stores, as [`pges_postgres::limit::check_writer_roles`] checks.
    pub writer_roles: limit::WriterRoles,
    /// The lease taken while resequencing. Default [`sequencer::Lease::DEFAULT`]: 10 s, renewed every 5 s.
    pub lease: sequencer::Lease,
}

impl ResequenceConfig {
    /// Resequencing for writers in `writer_roles`, with the default lease.
    pub fn new(writer_roles: limit::WriterRoles) -> Self {
        Self {
            writer_roles,
            lease: sequencer::Lease::DEFAULT,
        }
    }
}

/// What [`LogicalSource::resequence`] did.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct Resequenced {
    /// The new slot.
    pub slot: name::SlotName,
    /// The new slot's consistent point, now the stored token: the slot delivers every commit after it.
    pub consistent_point: lsn::Lsn,
    /// Every store the logical source sequences, with the commits recovered in it.
    pub stores: Vec<Recovered>,
}

/// The commits [`LogicalSource::resequence`] sequenced in one store.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub struct Recovered {
    /// The store.
    pub store: store::StoreId,
    /// How many commits had no log row and got one.
    pub commits: usize,
    /// The store's newest position afterwards.
    pub last_position: position::Position,
}

/// Why [`LogicalSource::resequence`] did not finish.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum ResequenceError {
    /// `pges.sequencer.halted` is not set: sequencing did not halt, so nothing needs recovering.
    #[error("sequencing is not halted (pges.sequencer.halted is not set); nothing to resequence")]
    NotHalted,
    /// A sequencer holds the lease. Stop every sequencer, or wait for the lease to expire.
    #[error("a sequencer holds the lease; stop every sequencer first")]
    LeaseHeld,
    /// Another process took the lease over while resequencing. Nothing was written; the new slot may be
    /// left behind, and the next resequence drops it.
    #[error("the lease was taken over while resequencing; nothing was written")]
    LeaseLost,
    /// A process streams from the old slot, so it cannot be dropped.
    #[error("replication slot {slot} is in use; stop every sequencer first")]
    SlotInUse {
        /// The slot.
        slot: String,
    },
    /// A store's positions would pass `i64::MAX`. Nothing was written.
    #[error("the positions of store {0} would overflow")]
    PositionOverflow(store::StoreId),
    /// A role in [`ResequenceConfig::writer_roles`] does not exist or does not cap its transactions at the
    /// stores' `max_txn_age`. Nothing was done.
    #[error(transparent)]
    WriterRole(pges_postgres::limit::WriterRoleError),
    /// The commit of the last step failed, so it may or may not have committed. If
    /// `pges.sequencer.halted` is clear, it did; otherwise run resequence again.
    #[error("the resequence commit may have failed: {0}")]
    CommitUnknown(pges_postgres::error::DbError),
    /// Reading the database, or replacing the slot, failed. Nothing was written.
    #[error(transparent)]
    Source(#[from] source::SourceError),
}

impl From<sqlx::Error> for ResequenceError {
    fn from(error: sqlx::Error) -> Self {
        Self::Source(error.into())
    }
}

/// Why the [`LogicalSource`] cannot resume from its slot. Carried by [`source::SourceError::Halt`]; recover
/// with [`LogicalSource::resequence`].
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum SlotLost {
    /// The server invalidated the slot (`pg_replication_slots.invalidation_reason`), for example because it
    /// passed `max_slot_wal_keep_size` or `idle_replication_slot_timeout`.
    #[error("replication slot {slot} was invalidated: {reason}")]
    Invalidated {
        /// The slot.
        slot: String,
        /// The server's invalidation reason, e.g. `wal_removed` or `idle_timeout`.
        reason: String,
    },
    /// The slot does not exist, but a token is stored: it was dropped after sequencing began.
    #[error("replication slot {slot} is missing although a token is stored")]
    Missing {
        /// The slot.
        slot: String,
    },
    /// The slot was confirmed past the stored token, so the server would start streaming after commits that
    /// were never sequenced, or whose log rows were lost. This happens after promoting an asynchronous
    /// standby that had the slot (synchronised as a failover slot) but not the sequencer's last
    /// transactions. A missing token counts as `0/0`, when the stores hold commits.
    #[error(
        "replication slot {slot} is confirmed up to {confirmed}, past the stored token {token}; commits in \
         between may never have been sequenced"
    )]
    Ahead {
        /// The slot.
        slot: String,
        /// The stored token.
        token: lsn::Lsn,
        /// The slot's `confirmed_flush_lsn`.
        confirmed: lsn::Lsn,
    },
    /// No slot and no token exist yet, but the store already holds commits: a new slot would never deliver
    /// them. The new slot was dropped again.
    #[error("store {store} holds commits from before replication slot {slot} existed")]
    CommitsBeforeSlot {
        /// The slot that would be created.
        slot: String,
        /// The store's schema name.
        store: String,
    },
}

const KIND: source::SourceKind = source_kind!("logical");
const PUBLICATION: name::PublicationName = pges_replication::publication_name!("pges");
const COMMITS: &str = "commits";
const TOKEN_LEN: usize = 8;
const CLOSE_TIMEOUT: Duration = Duration::from_secs(1);

/// How the commits table of a schema is sequenced.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Route {
    Store(store::StoreId),
    /// An outbox store, or a schema that is not a store.
    Skipped,
}

/// One committed transaction with the commits it inserted.
struct Transaction {
    commits: Vec<source::CommitRef>,
    end: lsn::Lsn,
}

enum Handled {
    Done,
    /// The insert names a schema this source has not seen yet; read the stores again, then handle it. A
    /// schema that is still not a store is skipped.
    NeedsRoutes {
        message: stream::ReplicationMessage,
        schema: String,
    },
}

struct Database {
    oid: i64,
    name: String,
}

/// What every open and bootstrap reads first.
struct Prepared {
    database: Database,
    slot: name::SlotName,
    existing: Option<SlotState>,
}

struct SlotState {
    database: Option<String>,
    plugin: Option<String>,
    invalidation_reason: Option<String>,
    failover: connection::Failover,
    activity: SlotActivity,
    confirmed_flush: Option<lsn::Lsn>,
}

/// Whether a process streams from a slot (`pg_replication_slots.active`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SlotActivity {
    Active,
    Inactive,
}

impl SlotState {
    fn invalidated(&self, slot: &name::SlotName) -> Option<SlotLost> {
        self.invalidation_reason
            .as_ref()
            .map(|reason| SlotLost::Invalidated {
                slot: slot.to_string(),
                reason: reason.clone(),
            })
    }

    /// [`SlotLost::Ahead`] if the slot was confirmed past `token`, the stored token.
    fn ahead_of(&self, slot: &name::SlotName, token: lsn::Lsn) -> Option<SlotLost> {
        let confirmed = self.confirmed_flush?;

        (confirmed > token).then(|| SlotLost::Ahead {
            slot: slot.to_string(),
            token,
            confirmed,
        })
    }

    fn check(&self, slot: &name::SlotName, database: &str) -> Result<(), source::SourceError> {
        if let Some(lost) = self.invalidated(slot) {
            return Err(halt(lost));
        }
        if self.database.as_deref() != Some(database) || self.plugin.as_deref() != Some("pgoutput")
        {
            return Err(source::SourceError::Invalid(format!(
                "replication slot {slot} belongs to database {:?} with plugin {:?}, not {database:?} with pgoutput",
                self.database, self.plugin
            )));
        }

        Ok(())
    }
}

impl SlotFailover {
    /// The setting a new slot is created with.
    fn for_new_slot(self) -> connection::Failover {
        match self {
            Self::Keep => connection::Failover::Disabled,
            Self::Set(failover) => failover,
        }
    }
}

impl LogicalSource {
    /// Names the slot, reads the stores, repairs the publication and reads the slot's state.
    async fn prepare(&mut self) -> Result<Prepared, source::SourceError> {
        let database = database(&self.pool).await?;
        let slot = match &self.slot {
            Some(slot) => slot.clone(),
            None => {
                let slot = slot_name(database.oid)?;
                self.slot = Some(slot.clone());
                slot
            }
        };
        self.refresh_routes().await?;
        ensure_publication(&self.pool, &self.routes).await?;
        let existing = slot_state(&self.pool, &slot).await?;

        Ok(Prepared {
            database,
            slot,
            existing,
        })
    }

    /// A replication connection, checked to reach the pool's database.
    async fn connect_checked(
        &self,
        database: &Database,
    ) -> Result<connection::ReplicationConnection, source::SourceError> {
        let mut replication = connect(&self.replication).await?;
        let identity = replication.identify_system().await.map_err(transient)?;
        if identity.database.as_deref() != Some(database.name.as_str()) {
            return Err(source::SourceError::Invalid(format!(
                "the replication connection reaches database {:?}, the pool {:?}",
                identity.database, database.name
            )));
        }

        Ok(replication)
    }

    /// Sets the slot's failover setting. ALTER_REPLICATION_SLOT waits while another process uses the slot,
    /// which a running sequencer never stops doing, so a slot in use is refused instead. The check runs once
    /// the replication connection is open, so a sequencer has little time to take the slot before the ALTER.
    async fn alter_failover(
        &self,
        replication: &mut connection::ReplicationConnection,
        slot: &name::SlotName,
        failover: connection::Failover,
    ) -> Result<Bootstrap, source::SourceError> {
        let activity = slot_state(&self.pool, slot)
            .await?
            .map(|state| state.activity);
        if activity == Some(SlotActivity::Active) {
            return Err(transient_message(&format!(
                "replication slot {slot} is in use; stop the sequencers to change its failover setting"
            )));
        }

        replication
            .alter_slot_failover(slot, failover)
            .await
            .map_err(transient)?;

        Ok(Bootstrap::FailoverChanged)
    }

    /// Creates the slot and checks, in the snapshot exported at its consistent point, that no store holds a
    /// commit yet: those committed before the slot and would never be delivered. Then stores the consistent
    /// point as the token, unless one is stored. If a store holds a commit, or the check or the store
    /// fails, the slot is dropped again. [`Bootstrap::Exists`]: another process created it meanwhile.
    async fn create_slot(
        &self,
        replication: &mut connection::ReplicationConnection,
        slot: &name::SlotName,
    ) -> Result<Bootstrap, source::SourceError> {
        let created = match replication
            .create_slot_with(
                slot,
                self.config.failover.for_new_slot(),
                connection::SlotSnapshot::Export,
            )
            .await
        {
            Ok(created) => created,
            Err(error::CommandError::Server(server))
                if server.code == error::SqlState::DUPLICATE_OBJECT =>
            {
                return Ok(Bootstrap::Exists);
            }
            Err(error) => return Err(transient(error)),
        };

        let checked = match &created.snapshot {
            Some(snapshot) => commits_before_slot(&self.pool, snapshot, &self.routes).await,
            None => Err(transient_message("the new slot exported no snapshot")),
        };
        let checked = match checked {
            Ok(None) => store_initial_token(&self.pool, created.consistent_point)
                .await
                .map(|()| None),
            other => other,
        };
        let error = match checked {
            Ok(None) => return Ok(Bootstrap::Created),
            Ok(Some(store)) => halt(SlotLost::CommitsBeforeSlot {
                slot: slot.to_string(),
                store,
            }),
            Err(error) => error,
        };
        // The exported snapshot ends with the next command on this connection, so the drop comes after the
        // check.
        if let Err(dropped) = replication
            .drop_slot(slot, connection::DropWait::NoWait)
            .await
        {
            return Err(source::SourceError::Fatal(
                format!(
                    "replication slot {slot} was created but could not be checked ({error}) nor dropped \
                     ({dropped}); drop it before sequencing"
                )
                .into(),
            ));
        }

        Err(error)
    }

    /// Halts with [`SlotLost::Ahead`] if the slot was confirmed past the stored token. The source confirms
    /// only stored tokens, so on a healthy server the slot is at or behind the token (behind after a
    /// crash, when the slot's position was not saved yet).
    ///
    /// A slot the source creates has its consistent point stored as the token at once. A slot without a
    /// token therefore comes from a source that failed before storing it, or the token's transaction was
    /// lost together with the commits after it; only the stores tell. Without commits there is nothing to
    /// lose.
    async fn check_confirmed(
        &self,
        state: &SlotState,
        slot: &name::SlotName,
        token: Option<lsn::Lsn>,
    ) -> Result<(), source::SourceError> {
        let Some(lost) = state.ahead_of(slot, token.unwrap_or(lsn::Lsn::ZERO)) else {
            return Ok(());
        };
        if token.is_none() && !stores_hold_commits(&self.pool, &self.routes).await? {
            return Ok(());
        }

        Err(halt(lost))
    }

    /// Whether the idle position goes out as an empty batch: it moved past the last token, and the last
    /// one went out at least [`LogicalConfig::status_interval`] ago. Each empty batch writes WAL of its own,
    /// which the next keepalive reports, so they are spaced out.
    fn idle_due(&self) -> bool {
        self.idle > self.delivered
            && self.idle_reported.elapsed() >= self.config.status_interval.get()
    }

    /// How long to wait for a message while nothing is buffered: until the next status update, or until
    /// the idle position is due.
    fn idle_wait(&self) -> Duration {
        let interval = self.config.status_interval.get();
        if self.idle <= self.delivered {
            return interval;
        }

        interval.saturating_sub(self.idle_reported.elapsed())
    }

    /// Records `lsn` as the idle position if everything received before it is handed out: nothing
    /// buffered and no transaction open.
    fn note_idle(&mut self, lsn: lsn::Lsn) {
        if self.buffer.is_empty() && self.open.is_none() && lsn > self.delivered.max(self.idle) {
            self.idle = lsn;
        }
    }

    /// Forgets everything read from the previous stream; the server sends it again from the token.
    fn reset(&mut self) {
        self.stream = None;
        self.relations = pgoutput::Relations::new();
        self.held = None;
        self.open = None;
        self.buffer.clear();
        self.buffered = 0;
    }

    fn stream(&mut self) -> Result<&mut stream::ReplicationStream, source::SourceError> {
        self.stream
            .as_mut()
            .ok_or_else(|| transient_message("the logical source is not open"))
    }

    /// `START_REPLICATION` from the token. While the slot is still active for another process (a previous
    /// leader, or this source's previous connection), ends that process and tries again.
    async fn start(
        &self,
        mut replication: connection::ReplicationConnection,
        slot: &name::SlotName,
        token: Option<lsn::Lsn>,
    ) -> Result<stream::ReplicationStream, source::SourceError> {
        let options = connection::StartOptions::new(slot.clone(), PUBLICATION)
            .start_at(token.unwrap_or(lsn::Lsn::ZERO));
        let deadline = Instant::now() + self.config.start_timeout.get();
        let mut pause = Duration::from_millis(10);

        loop {
            terminate_holder(&self.pool, slot).await?;

            let error = match replication.start(&options).await {
                Ok(stream) => return Ok(stream),
                Err(error) => error,
            };
            let in_use = matches!(
                &error,
                error::CommandError::Server(server) if server.code == error::SqlState::OBJECT_IN_USE
            );
            if !in_use || Instant::now() >= deadline {
                return Err(self.start_failed(slot, token, error).await);
            }

            tokio::time::sleep(pause).await;
            pause = (pause * 2).min(Duration::from_secs(1));
            replication = connect(&self.replication).await?;
        }
    }

    /// Turns a failed `START_REPLICATION` into a halt if the slot was lost meanwhile.
    async fn start_failed(
        &self,
        slot: &name::SlotName,
        token: Option<lsn::Lsn>,
        error: error::CommandError,
    ) -> source::SourceError {
        match slot_state(&self.pool, slot).await {
            Ok(Some(state)) => state
                .invalidated(slot)
                .map_or_else(|| transient(error), halt),
            Ok(None) if token.is_some() => halt(SlotLost::Missing {
                slot: slot.to_string(),
            }),
            _ => transient(error),
        }
    }

    /// Reads `pges.stores` again. Outbox stores are skipped, and so is a schema that is not a store.
    async fn refresh_routes(&mut self) -> Result<(), source::SourceError> {
        let rows = statement::logical_stores::Query::fixed()
            .bind(statement::logical_stores::Params {})
            .fetch_all(&self.pool)
            .await?;

        let mut routes = HashMap::with_capacity(rows.len());
        for row in rows {
            let statement::logical_stores::Row {
                store_id: id,
                schema_name: schema,
                outbox,
            } = row;
            if store::StoreName::parse(schema.as_str()).is_err() {
                return Err(source::SourceError::Invalid(format!(
                    "store {id} has an invalid schema name {schema:?}"
                )));
            }

            let route = if outbox {
                if !self.routes.contains_key(&schema) {
                    tracing::warn!(
                        store = %schema,
                        "the logical source skips outbox stores; sequence this database with one kind of source"
                    );
                }
                Route::Skipped
            } else {
                Route::Store(store::StoreId::new(id))
            };
            routes.insert(schema, route);
        }
        // A schema the publication sends that is not a store stays skipped until it becomes one.
        for (schema, route) in &self.routes {
            routes.entry(schema.clone()).or_insert(*route);
        }

        self.routes = routes;
        Ok(())
    }

    async fn handle(
        &mut self,
        message: stream::ReplicationMessage,
    ) -> Result<Handled, source::SourceError> {
        let data = match message {
            stream::ReplicationMessage::XLogData(data) => data,
            stream::ReplicationMessage::Keepalive(keepalive) => {
                self.note_idle(keepalive.wal_end);
                return Ok(Handled::Done);
            }
            _ => return Ok(Handled::Done),
        };

        match data.message {
            pgoutput::Message::Begin(_) => self.open = Some(Vec::new()),
            pgoutput::Message::Relation(relation) => self.relations.insert(relation),
            pgoutput::Message::Insert(ref insert) => {
                let Some(relation) = self.relations.get(insert.relation) else {
                    return Err(transient_message(
                        "an insert arrived before its relation message",
                    ));
                };
                if relation.name != COMMITS {
                    return Ok(Handled::Done);
                }
                let store = match self.routes.get(&relation.namespace) {
                    Some(Route::Store(store)) => *store,
                    Some(Route::Skipped) => return Ok(Handled::Done),
                    None => {
                        let schema = relation.namespace.clone();
                        let message = stream::ReplicationMessage::XLogData(data);
                        return Ok(Handled::NeedsRoutes { message, schema });
                    }
                };
                let commit = commit_ref(store, relation, &insert.new)?;
                let Some(open) = &mut self.open else {
                    return Err(transient_message("an insert arrived outside a transaction"));
                };
                open.push(commit);
            }
            pgoutput::Message::Commit(commit) => {
                let Some(commits) = self.open.take() else {
                    return Err(transient_message("a commit arrived without its begin"));
                };
                if commit.end_lsn <= self.resume {
                    return Ok(Handled::Done);
                }
                if commits.is_empty() {
                    self.note_idle(commit.end_lsn);
                    return Ok(Handled::Done);
                }

                self.buffered += commits.len();
                self.buffer.push_back(Transaction {
                    commits,
                    end: commit.end_lsn,
                });
            }
            _ => {}
        }

        Ok(Handled::Done)
    }

    /// Sends a status update if none went out for [`LogicalConfig::status_interval`], so the server does not
    /// time the connection out while the sequencer is busy.
    async fn heartbeat(&mut self) -> Result<(), source::SourceError> {
        if self.status_sent.elapsed() < self.config.status_interval.get() {
            return Ok(());
        }

        self.stream()?
            .send_status(stream::Reply::NotRequested)
            .await
            .map_err(transient)?;
        self.status_sent = Instant::now();
        Ok(())
    }
}

fn slot_name(oid: i64) -> Result<name::SlotName, source::SourceError> {
    name::SlotName::new(format!("pges_{oid}"))
        .map_err(|error| source::SourceError::Invalid(error.to_string()))
}

fn encode_token(lsn: lsn::Lsn) -> Vec<u8> {
    if lsn == lsn::Lsn::ZERO {
        return Vec::new();
    }

    lsn.get().to_be_bytes().to_vec()
}

/// An empty token means none: nothing was delivered before it was stored.
fn decode_token(token: &[u8]) -> Result<Option<lsn::Lsn>, source::SourceError> {
    if token.is_empty() {
        return Ok(None);
    }

    let bytes: [u8; TOKEN_LEN] = token.try_into().map_err(|_| {
        source::SourceError::Invalid(format!(
            "a logical token has {TOKEN_LEN} bytes, not {}",
            token.len()
        ))
    })?;
    Ok(Some(lsn::Lsn::new(u64::from_be_bytes(bytes))))
}

/// The commit an insert into a store's `commits` describes. Columns are looked up by name: the publication's
/// column list decides their order.
fn commit_ref(
    store: store::StoreId,
    relation: &pgoutput::Relation,
    row: &pgoutput::Tuple,
) -> Result<source::CommitRef, source::SourceError> {
    let column = |name: &str| {
        relation
            .columns
            .iter()
            .position(|column| column.name == name)
            .and_then(|index| row.text(index))
            .ok_or_else(|| {
                source::SourceError::Invalid(format!(
                    "{}.{} has no text column {name}",
                    relation.namespace, relation.name
                ))
            })
    };
    let invalid = |name: &str, text: &str| {
        source::SourceError::Invalid(format!(
            "{}.{}: invalid {name} {text:?}",
            relation.namespace, relation.name
        ))
    };

    let text = column("commit_id")?;
    let commit_id = Uuid::parse_str(text)
        .ok()
        .and_then(commit::CommitId::from_uuid)
        .ok_or_else(|| invalid("commit_id", text))?;
    let text = column("category_id")?;
    let category = text
        .parse::<i32>()
        .map_err(|_| invalid("category_id", text))?;
    let text = column("event_count")?;
    let event_count = text
        .parse::<i16>()
        .ok()
        .and_then(log::event_count_from_sql)
        .ok_or_else(|| invalid("event_count", text))?;

    Ok(source::CommitRef::new(
        store,
        commit_id,
        category::CategoryId::new(category),
        event_count,
    ))
}

async fn database(pool: &PgPool) -> Result<Database, source::SourceError> {
    let row = statement::logical_database::Query::fixed()
        .bind(statement::logical_database::Params {})
        .fetch_one(pool)
        .await?;

    Ok(Database {
        oid: row.oid,
        name: row.name,
    })
}

async fn slot_state(
    pool: &PgPool,
    slot: &name::SlotName,
) -> Result<Option<SlotState>, source::SourceError> {
    let row = statement::slot::Query::fixed()
        .bind(statement::slot::Params {
            slot: slot.as_str(),
        })
        .fetch_optional(pool)
        .await?;

    row.map(|row| {
        Ok(SlotState {
            database: row.database,
            plugin: row.plugin,
            invalidation_reason: row.invalidation_reason,
            failover: if not_null(row.failover, "failover")? {
                connection::Failover::Enabled
            } else {
                connection::Failover::Disabled
            },
            activity: if not_null(row.active, "active")? {
                SlotActivity::Active
            } else {
                SlotActivity::Inactive
            },
            confirmed_flush: row
                .confirmed_flush
                .and_then(|confirmed| u64::try_from(confirmed).ok())
                .map(lsn::Lsn::new),
        })
    })
    .transpose()
}

/// A column of `pg_replication_slots` that PostgreSQL describes as nullable, though the view never returns
/// NULL in it. NULL fails with the error sqlx gives a NULL read as a non-`Option` type.
fn not_null<T>(value: Option<T>, column: &str) -> Result<T, source::SourceError> {
    value.ok_or_else(|| {
        sqlx::Error::ColumnDecode {
            index: format!("{column:?}"),
            source: Box::new(sqlx::error::UnexpectedNullError),
        }
        .into()
    })
}

/// Creates the publication if it is missing, and adds the `commits` table of every store this source
/// sequences that it does not publish yet, as the migrations do.
async fn ensure_publication(
    pool: &PgPool,
    routes: &HashMap<String, Route>,
) -> Result<(), source::SourceError> {
    let exists = statement::publication_exists::Query::fixed()
        .bind(statement::publication_exists::Params {
            publication: PUBLICATION.as_str(),
        })
        .fetch_optional(pool)
        .await?
        .is_some();
    if !exists {
        tracing::warn!("the publication pges is missing; creating it");
        pool.execute(statement::CREATE_PUBLICATION.sql()).await?;
    }

    let published: Vec<String> = statement::published::Query::fixed()
        .bind(statement::published::Params {
            publication: PUBLICATION.as_str(),
            table: COMMITS,
        })
        .fetch_all(pool)
        .await?
        .into_iter()
        .map(|row| row.schemaname)
        .collect();
    for (schema, route) in routes {
        if *route == Route::Skipped || published.contains(schema) {
            continue;
        }

        tracing::warn!(store = %schema, "the publication pges misses the store's commits; adding them");
        let values = template::Values::new().store(&store_name(schema)?);
        pool.execute(statement::PUBLISH_STORE.render_text(&values))
            .await?;
    }

    Ok(())
}

/// The first store that holds a commit in `snapshot`, the snapshot exported at a new slot's consistent
/// point: the slot never delivers the commits visible in it.
async fn commits_before_slot(
    pool: &PgPool,
    snapshot: &name::SnapshotName,
    routes: &HashMap<String, Route>,
) -> Result<Option<String>, source::SourceError> {
    let mut conn = pool.acquire().await?;
    let mut tx = conn.begin().await?;
    import_snapshot(&mut tx, snapshot).await?;

    for (schema, route) in routes {
        if *route == Route::Skipped {
            continue;
        }

        let values = template::Values::new().store(&store_name(schema)?);
        let found = statement::store_has_commits::Query::render(&values)
            .bind(statement::store_has_commits::Params {})
            .fetch_optional(&mut *tx)
            .await?;
        if found.is_some() {
            return Ok(Some(schema.clone()));
        }
    }
    tx.rollback().await?;

    Ok(None)
}

/// Whether any store this source sequences holds a commit.
async fn stores_hold_commits(
    pool: &PgPool,
    routes: &HashMap<String, Route>,
) -> Result<bool, source::SourceError> {
    for (schema, route) in routes {
        if *route == Route::Skipped {
            continue;
        }

        let values = template::Values::new().store(&store_name(schema)?);
        let found = statement::store_has_commits::Query::render(&values)
            .bind(statement::store_has_commits::Params {})
            .fetch_optional(pool)
            .await?;
        if found.is_some() {
            return Ok(true);
        }
    }

    Ok(false)
}

/// Stores a new slot's consistent point as the token, unless a token is stored.
async fn store_initial_token(
    pool: &PgPool,
    consistent_point: lsn::Lsn,
) -> Result<(), source::SourceError> {
    statement::store_initial_token::Query::fixed()
        .bind(statement::store_initial_token::Params {
            source: KIND.as_str(),
            token: &encode_token(consistent_point),
        })
        .execute(pool)
        .await?;

    Ok(())
}

/// The logical token stored in `pges.sequencer`, if any.
async fn stored_token(pool: &PgPool) -> Result<Option<Vec<u8>>, source::SourceError> {
    let row = statement::logical_stored_token::Query::fixed()
        .bind(statement::logical_stored_token::Params {})
        .fetch_optional(pool)
        .await?;
    let Some(statement::logical_stored_token::Row {
        source,
        source_token: token,
    }) = row
    else {
        return Ok(None);
    };

    Ok(token.filter(|token| source.as_deref() == Some(KIND.as_str()) && !token.is_empty()))
}

/// Ends the walsender still holding the slot, which would otherwise keep it until `wal_sender_timeout`.
async fn terminate_holder(pool: &PgPool, slot: &name::SlotName) -> Result<(), source::SourceError> {
    statement::terminate_holder::Query::fixed()
        .bind(statement::terminate_holder::Params {
            slot: slot.as_str(),
        })
        .execute(pool)
        .await?;

    Ok(())
}

/// Makes the transaction `tx` just began see the snapshot a new slot exported. Nothing may run in `tx`
/// before.
pub(super) async fn import_snapshot(
    tx: &mut PgConnection,
    snapshot: &name::SnapshotName,
) -> Result<(), source::SourceError> {
    let snapshot = template::SnapshotName::new(snapshot.as_str()).ok_or_else(|| {
        source::SourceError::Invalid(format!("{snapshot:?} is not a snapshot name"))
    })?;

    tx.execute(statement::REPEATABLE_READ.sql()).await?;
    tx.execute(statement::SET_SNAPSHOT.render_text(&template::Values::new().snapshot(&snapshot)))
        .await?;

    Ok(())
}

/// The store name of a schema in the routes. Every schema routed to a store was checked against the
/// store-name rule when the routes were read.
pub(super) fn store_name(schema: &str) -> Result<store::StoreName, source::SourceError> {
    store::StoreName::parse(schema)
        .map_err(|_| source::SourceError::Invalid(format!("{schema:?} is not a store name")))
}

async fn connect(
    replication: &config::Config,
) -> Result<connection::ReplicationConnection, source::SourceError> {
    connection::ReplicationConnection::connect(replication)
        .await
        .map_err(|error| match error {
            error::ConnectError::InvalidConfig(error) => {
                source::SourceError::Fatal(Box::new(error))
            }
            error => transient(error),
        })
}

fn halt(lost: SlotLost) -> source::SourceError {
    source::SourceError::Halt(Box::new(lost))
}

fn transient(error: impl std::error::Error + Send + Sync + 'static) -> source::SourceError {
    source::SourceError::Transient(Box::new(error))
}

fn transient_message(message: &str) -> source::SourceError {
    source::SourceError::Transient(message.into())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tokens_round_trip_and_empty_means_none() {
        let lsn = lsn::Lsn::new(0x16_B374_D848);
        assert_eq!(decode_token(&encode_token(lsn)).unwrap(), Some(lsn));
        assert_eq!(encode_token(lsn).len(), TOKEN_LEN);
        assert_eq!(encode_token(lsn::Lsn::ZERO), Vec::<u8>::new());
        assert_eq!(decode_token(&[]).unwrap(), None);
        assert!(matches!(
            decode_token(&[1, 2, 3]),
            Err(source::SourceError::Invalid(_))
        ));
    }

    #[test]
    fn an_invalidated_slot_halts_and_a_foreign_slot_is_invalid() {
        let slot = slot_name(7).unwrap();
        let state = |database: &str, plugin: &str, reason: Option<&str>| SlotState {
            database: Some(database.to_owned()),
            plugin: Some(plugin.to_owned()),
            failover: connection::Failover::Disabled,
            activity: SlotActivity::Inactive,
            invalidation_reason: reason.map(str::to_owned),
            confirmed_flush: None,
        };

        assert!(state("app", "pgoutput", None).check(&slot, "app").is_ok());
        let Err(source::SourceError::Halt(error)) =
            state("app", "pgoutput", Some("wal_removed")).check(&slot, "app")
        else {
            panic!("an invalidated slot must halt");
        };
        assert_eq!(
            error.downcast_ref::<SlotLost>(),
            Some(&SlotLost::Invalidated {
                slot: "pges_7".to_owned(),
                reason: "wal_removed".to_owned(),
            })
        );
        assert!(matches!(
            state("other", "pgoutput", None).check(&slot, "app"),
            Err(source::SourceError::Invalid(_))
        ));
        assert!(matches!(
            state("app", "test_decoding", None).check(&slot, "app"),
            Err(source::SourceError::Invalid(_))
        ));
    }

    #[test]
    fn a_slot_confirmed_past_the_token_is_ahead() {
        let slot = slot_name(7).unwrap();
        let state = |confirmed: Option<u64>| SlotState {
            database: Some("app".to_owned()),
            plugin: Some("pgoutput".to_owned()),
            failover: connection::Failover::Enabled,
            activity: SlotActivity::Inactive,
            invalidation_reason: None,
            confirmed_flush: confirmed.map(lsn::Lsn::new),
        };
        let token = lsn::Lsn::new(0x200);

        assert_eq!(state(Some(0x200)).ahead_of(&slot, token), None);
        assert_eq!(state(Some(0x100)).ahead_of(&slot, token), None);
        assert_eq!(state(None).ahead_of(&slot, token), None);
        assert_eq!(
            state(Some(0x201)).ahead_of(&slot, token),
            Some(SlotLost::Ahead {
                slot: "pges_7".to_owned(),
                token,
                confirmed: lsn::Lsn::new(0x201),
            })
        );
    }

    #[test]
    fn a_new_slot_gets_failover_only_when_asked() {
        assert_eq!(LogicalConfig::default().failover, SlotFailover::Keep);
        assert_eq!(
            SlotFailover::Keep.for_new_slot(),
            connection::Failover::Disabled
        );
        for failover in [
            connection::Failover::Enabled,
            connection::Failover::Disabled,
        ] {
            assert_eq!(SlotFailover::Set(failover).for_new_slot(), failover);
        }
    }

    #[test]
    fn slot_names_follow_the_database_oid() {
        assert_eq!(slot_name(16_384).unwrap().as_str(), "pges_16384");
    }
}
