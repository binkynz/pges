//! Appending events (`sql/append/`).
//!
//! An append is one statement. When it returns no row, one follow-up query classifies why: a retry of an
//! append that already succeeded (returned as [`Outcome::Deduplicated`]), a version conflict, a stream in
//! another category or state, or a commit id at or below the stream's last one. Commit ids are always
//! generated on the client, so a lost response can be retried safely.

use std::{
    any::TypeId,
    collections::HashSet,
    future::{Future, IntoFuture},
    ops::RangeInclusive,
    pin::Pin,
    sync::Arc,
    time::Duration,
};

use serde::Serialize;
use sqlx::{Connection, PgConnection, Postgres, pool::PoolConnection};
use uuid::Uuid;

use pges_core::{aggregate, category, commit, event, frame, metadata, stream, version};

use crate::{ambient, catalog, error, load, sql::statement, store, unit_of_work};

impl store::Store {
    /// Appends `events` to one stream, checking `expected` first.
    ///
    /// Returns a builder; `.await` it to run the append. Outside a [`unit_of_work::UnitOfWork`] the append
    /// commits on its own, and after a lost connection it is retried once on a new one: a retry of an append
    /// that already committed returns its original result with [`Outcome::Deduplicated`].
    pub fn append<'a, E: event::Event>(
        &'a self,
        stream: impl Into<stream::StreamRef>,
        expected: version::ExpectedVersion,
        events: &'a [E],
    ) -> Append<'a, E> {
        Append::new(Target::Pool(self), stream.into(), expected, events)
    }

    /// Appends on `conn`, which may be inside a transaction the caller owns (for example a projection's).
    ///
    /// The append is not retried after a lost connection, and after an error the caller must roll the
    /// transaction back. A crate-generated commit id that fell behind the stream's last one is replaced while
    /// the transaction holds the stream's row lock; outside a transaction that lock ends with its statement,
    /// so the append can still fail with [`AppendError::CommitIdOutOfOrder`] under heavy contention.
    pub fn append_in<'a, E: event::Event>(
        &'a self,
        conn: &'a mut PgConnection,
        stream: impl Into<stream::StreamRef>,
        expected: version::ExpectedVersion,
        events: &'a [E],
    ) -> Append<'a, E> {
        Append::new(
            Target::Connection {
                store: self,
                conn,
                state: None,
                lease: PoolLease::Unknown,
            },
            stream.into(),
            expected,
            events,
        )
    }

    /// Appends to several streams in one statement and one transaction: all succeed or none do.
    ///
    /// The entries are sorted by stream id, so concurrent calls lock streams in the same order. A batch
    /// naming a stream twice fails with [`AppendError::DuplicateStream`]. The results are in entry order.
    pub fn append_many<'a>(
        &'a self,
        entries: impl IntoIterator<Item = AppendEntry<'a>>,
    ) -> AppendMany<'a> {
        AppendMany {
            target: Target::Pool(self),
            entries: entries.into_iter().collect(),
            metadata: metadata::Metadata::default(),
        }
    }
}

/// An append to one stream, run when awaited. Built by [`store::Store::append`],
/// [`store::Store::append_in`] and [`unit_of_work::UnitOfWork::append`].
#[must_use = "an append does nothing until it is awaited"]
pub struct Append<'a, E> {
    target: Target<'a>,
    stream: stream::StreamRef,
    expected: version::ExpectedVersion,
    events: &'a [E],
    metadata: metadata::Metadata,
    commit_id: Option<commit::CommitId>,
    snapshot: Option<SnapshotWrite>,
}

impl<'a, E: event::Event> Append<'a, E> {
    /// The commit's metadata, shared by its events. Fields it leaves unset are filled from the
    /// [`ambient::scope`] and, with the `otel` feature, the trace from the current span.
    pub fn metadata(mut self, metadata: metadata::Metadata) -> Self {
        self.metadata = metadata;
        self
    }

    /// Uses this commit id instead of generating one, so the caller can retry the append itself: an append
    /// with an id that already committed on this stream returns the original result with
    /// [`Outcome::Deduplicated`]. The id must be above the stream's last commit id, or the append fails with
    /// [`AppendError::CommitIdOutOfOrder`]; unlike a generated id, it is never replaced.
    pub fn commit_id(mut self, commit_id: commit::CommitId) -> Self {
        self.commit_id = Some(commit_id);
        self
    }

    /// Writes a snapshot of `state` (the aggregate after these events) in the same transaction, when the
    /// aggregate's [`aggregate::SnapshotPolicy`] fires: always for `Manual`, and for `EveryN(n)` when the
    /// stored snapshot is at least `n` versions behind.
    pub fn snapshot_after<A>(mut self, state: &A) -> Self
    where
        A: aggregate::Snapshotted<Event = E>,
    {
        self.snapshot = Some(SnapshotWrite::new(state));
        self
    }
}

impl<'a, E: event::Event> IntoFuture for Append<'a, E> {
    type Output = Result<AppendResult, AppendError>;
    type IntoFuture = Pin<Box<dyn Future<Output = Self::Output> + Send + 'a>>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(self.run())
    }
}

/// The outcome of a successful append.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct AppendResult {
    /// The stream appended to.
    pub stream_id: stream::StreamId,
    /// The commit holding the events.
    pub commit_id: commit::CommitId,
    /// The versions the events took.
    pub versions: RangeInclusive<version::Version>,
    /// Whether this call wrote the events or found them already written.
    pub outcome: Outcome,
}

impl AppendResult {
    /// The stream's version after the append.
    pub fn last_version(&self) -> version::Version {
        *self.versions.end()
    }

    /// What to expect for the next append to this stream.
    pub fn expected(&self) -> version::ExpectedVersion {
        version::ExpectedVersion::Exact(self.last_version())
    }
}

/// Whether an append wrote its events.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum Outcome {
    /// The events were written by this call.
    Appended,
    /// The commit id had already been written to this stream, by an earlier attempt of the same append;
    /// nothing was written and the original result is returned.
    Deduplicated,
}

/// One stream's part of [`store::Store::append_many`].
pub struct AppendEntry<'a> {
    stream: stream::StreamRef,
    expected: version::ExpectedVersion,
    events: Box<dyn EventBatch + 'a>,
}

impl<'a> AppendEntry<'a> {
    /// Appends `events` to `stream`, checking `expected` first.
    pub fn new<E: event::Event>(
        stream: impl Into<stream::StreamRef>,
        expected: version::ExpectedVersion,
        events: &'a [E],
    ) -> Self {
        Self {
            stream: stream.into(),
            expected,
            events: Box::new(Events(events)),
        }
    }
}

/// Appends to several streams, run when awaited. Built by [`store::Store::append_many`] and
/// [`unit_of_work::UnitOfWork::append_many`].
#[must_use = "an append does nothing until it is awaited"]
pub struct AppendMany<'a> {
    target: Target<'a>,
    entries: Vec<AppendEntry<'a>>,
    metadata: metadata::Metadata,
}

impl AppendMany<'_> {
    /// The metadata of every commit in the batch. Fields it leaves unset are filled as for
    /// [`Append::metadata`].
    pub fn metadata(mut self, metadata: metadata::Metadata) -> Self {
        self.metadata = metadata;
        self
    }
}

impl<'a> IntoFuture for AppendMany<'a> {
    type Output = Result<Vec<AppendResult>, AppendError>;
    type IntoFuture = Pin<Box<dyn Future<Output = Self::Output> + Send + 'a>>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(self.run())
    }
}

/// An append failed. Nothing was written.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum AppendError {
    /// The stream is not at the expected version. `actual` is `None` when the stream does not exist.
    #[error("stream {stream_id} is at version {actual:?}, expected {expected:?}")]
    Conflict {
        /// The stream.
        stream_id: stream::StreamId,
        /// What the append expected.
        expected: version::ExpectedVersion,
        /// The stream's version.
        actual: Option<version::Version>,
    },
    /// The stream exists in another category than the events'.
    #[error("stream {stream_id} belongs to category {stream_category}, not {event_category}")]
    CategoryMismatch {
        /// The stream.
        stream_id: stream::StreamId,
        /// The stream's category.
        stream_category: category::CategoryName,
        /// The events' category.
        event_category: category::CategoryName,
    },
    /// The stream was deleted.
    #[error("stream {0} was deleted")]
    StreamDeleted(stream::StreamId),
    /// The stream was archived and takes no more events.
    #[error("stream {0} is archived")]
    StreamArchived(stream::StreamId),
    /// A caller-supplied commit id is at or below the stream's last commit id.
    ///
    /// A commit id the crate generated is replaced instead: the append runs again with an id above the
    /// stream's last one, chosen while its transaction holds the stream's row lock, so the new id cannot
    /// fall behind. Only [`store::Store::append_in`] on a connection outside a transaction, where that lock
    /// ends with its statement, can still fail with this error for a generated id, after five new ids.
    /// Nothing was written, so the append can be retried.
    #[error("commit id {commit_id} is not above the last commit id of stream {stream_id}")]
    CommitIdOutOfOrder {
        /// The stream.
        stream_id: stream::StreamId,
        /// The rejected id.
        commit_id: commit::CommitId,
    },
    /// The commit id's timestamp is further from the database clock than `StoreConfig::max_clock_skew`.
    #[error("commit id {0} is outside the clock skew tolerance")]
    CommitIdSkew(commit::CommitId),
    /// A unit of work's transaction ended before the append: a savepoint begin on its connection failed or
    /// was dropped, which rolls back the whole transaction (see [`unit_of_work::UnitOfWork`]). Nothing was
    /// written.
    #[error("the unit of work's transaction has ended")]
    TransactionEnded,
    /// Two different appends used the same commit id. Inside a transaction the transaction is aborted.
    #[error("commit id {0} was already used by another append")]
    DuplicateCommit(commit::CommitId),
    /// More events than one commit can hold.
    #[error("a commit holds at most {max} events")]
    TooManyEvents {
        /// The limit, 32767.
        max: u16,
    },
    /// An append needs at least one event.
    #[error("an append needs at least one event")]
    NoEvents,
    /// [`store::Store::append_many`] was given the same stream twice.
    #[error("stream {0} appears twice in one append_many")]
    DuplicateStream(stream::StreamId),
    /// The events' category is not registered in the store.
    #[error("category {0} is not registered in the store")]
    UnknownCategory(category::CategoryName),
    /// An event could not be encoded.
    #[error(transparent)]
    Encode(event::CodecError),
    /// The snapshot passed to [`Append::snapshot_after`] could not be encoded.
    #[error("could not encode the snapshot: {0}")]
    SnapshotEncode(event::BoxError),
    /// The append inside a [`unit_of_work::UnitOfWork`] needed to register its event type, which takes a
    /// second connection from the store's pool, but the pool has a single connection and the unit of work
    /// holds it. Call [`store::Store::register`] for the type before beginning, or use a larger pool.
    #[error(
        "registering the event types of category {0} needs a second pooled connection, and the pool has one"
    )]
    NoSpareConnection(category::CategoryName),
    /// A database error.
    #[error(transparent)]
    Database(error::DbError),
}

impl From<sqlx::Error> for AppendError {
    fn from(source: sqlx::Error) -> Self {
        Self::Database(source.into())
    }
}

impl From<store::RegisterError> for AppendError {
    fn from(error: store::RegisterError) -> Self {
        match error {
            store::RegisterError::UnknownCategory(name) => Self::UnknownCategory(name),
            store::RegisterError::Database(error) => Self::Database(error),
        }
    }
}

/// Where an append runs.
pub(crate) enum Target<'a> {
    /// A pooled connection, in autocommit.
    Pool(&'a store::Store),
    /// A connection that may be inside a transaction. `state` is the unit of work to mark failed when an
    /// error leaves the transaction unusable or partly written.
    Connection {
        store: &'a store::Store,
        conn: &'a mut PgConnection,
        state: Option<&'a mut unit_of_work::State>,
        lease: PoolLease,
    },
}

/// Whether `conn` of a [`Target::Connection`] is known to come from the store's pool.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PoolLease {
    /// A unit of work started by [`store::Store::begin`] holds one of the pool's connections.
    Held,
    /// The caller's own connection, from this pool or another.
    Unknown,
}

impl Target<'_> {
    fn store(&self) -> &store::Store {
        match self {
            Self::Pool(store) => store,
            Self::Connection { store, .. } => store,
        }
    }

    fn lease(&self) -> PoolLease {
        match self {
            Self::Pool(_) => PoolLease::Unknown,
            Self::Connection { lease, .. } => *lease,
        }
    }

    /// Refuses to run in autocommit what the unit of work meant to run in its transaction.
    fn check_open(&self) -> Result<(), AppendError> {
        match self {
            Self::Connection {
                state: Some(unit_of_work::State::Ended),
                ..
            } => Err(AppendError::TransactionEnded),
            _ => Ok(()),
        }
    }
}

impl<'a, E: event::Event> Append<'a, E> {
    pub(crate) fn new(
        target: Target<'a>,
        stream: stream::StreamRef,
        expected: version::ExpectedVersion,
        events: &'a [E],
    ) -> Self {
        Self {
            target,
            stream,
            expected,
            events,
            metadata: metadata::Metadata::default(),
            commit_id: None,
            snapshot: None,
        }
    }

    async fn run(self) -> Result<AppendResult, AppendError> {
        let Self {
            target,
            stream,
            expected,
            events,
            metadata,
            commit_id,
            snapshot,
        } = self;
        let metadata = ambient::resolve(metadata);
        if let Some(Err(error)) = snapshot.as_ref().map(|s| &s.state) {
            return Err(AppendError::SnapshotEncode(error.to_string().into()));
        }
        target.check_open()?;

        let store = target.store().clone();
        let mut prepared = prepare(
            &store,
            target.lease(),
            &Events(events),
            stream,
            expected,
            &metadata,
            commit_id,
        )
        .await?;

        match target {
            Target::Pool(store) => append_pooled(store, &mut prepared, snapshot.as_ref()).await,
            Target::Connection {
                store, conn, state, ..
            } => {
                let result =
                    append_in_transaction(store, conn, &mut prepared, snapshot.as_ref()).await;
                if let (Err(error), Some(state)) = (&result, state)
                    && error.aborts_transaction()
                {
                    *state = unit_of_work::State::Failed;
                }

                result
            }
        }
    }
}

impl<'a> AppendMany<'a> {
    pub(crate) fn new(target: Target<'a>, entries: Vec<AppendEntry<'a>>) -> Self {
        Self {
            target,
            entries,
            metadata: metadata::Metadata::default(),
        }
    }

    async fn run(self) -> Result<Vec<AppendResult>, AppendError> {
        let Self {
            target,
            entries,
            metadata,
        } = self;
        let metadata = ambient::resolve(metadata);
        if entries.is_empty() {
            return Ok(Vec::new());
        }
        target.check_open()?;

        let mut seen = HashSet::with_capacity(entries.len());
        for entry in &entries {
            if !seen.insert(entry.stream.id) {
                return Err(AppendError::DuplicateStream(entry.stream.id));
            }
        }

        let store = target.store().clone();
        let lease = target.lease();
        let mut prepared = Vec::with_capacity(entries.len());
        for entry in entries {
            prepared.push(
                prepare(
                    &store,
                    lease,
                    entry.events.as_ref(),
                    entry.stream,
                    entry.expected,
                    &metadata,
                    None,
                )
                .await?,
            );
        }

        match target {
            Target::Pool(store) => append_many_pooled(store, &mut prepared).await,
            Target::Connection {
                store, conn, state, ..
            } => {
                let result = match append_many_on(store, conn, &mut prepared, LockWait::Wait).await
                {
                    Ok(Batch::Written(results)) => Ok(results),
                    // Waiting for locks never asks for a restart.
                    Ok(Batch::Restart) => Err(out_of_order(&prepared[0])),
                    Err(error) => Err(error),
                };
                // A failed batch may have written some of its streams; only a rollback undoes that.
                if let (Err(_), Some(state)) = (&result, state) {
                    *state = unit_of_work::State::Failed;
                }

                result
            }
        }
    }
}

impl AppendError {
    /// Whether the error left the surrounding transaction aborted.
    fn aborts_transaction(&self) -> bool {
        matches!(
            self,
            Self::CommitIdSkew(_) | Self::DuplicateCommit(_) | Self::Database(_)
        )
    }
}

/// The events of one append, with their type erased so one batch can mix event types.
pub(crate) trait EventBatch: Send + Sync {
    fn type_key(&self) -> TypeId;
    fn category(&self) -> category::CategoryName;
    fn types(&self) -> &'static [&'static str];
    fn len(&self) -> usize;
    fn encode(
        &self,
        types: &catalog::TypeTable,
        metadata: &metadata::Metadata,
        format: frame::Format,
    ) -> Result<Vec<u8>, AppendError>;
}

struct Events<'a, E>(&'a [E]);

impl<E: event::Event> EventBatch for Events<'_, E> {
    fn type_key(&self) -> TypeId {
        TypeId::of::<E>()
    }

    fn category(&self) -> category::CategoryName {
        E::CATEGORY
    }

    fn types(&self) -> &'static [&'static str] {
        let () = E::__CHECK;

        E::TYPES
    }

    fn len(&self) -> usize {
        self.0.len()
    }

    fn encode(
        &self,
        types: &catalog::TypeTable,
        metadata: &metadata::Metadata,
        format: frame::Format,
    ) -> Result<Vec<u8>, AppendError> {
        let mut data = Vec::with_capacity(self.0.len());
        for event in self.0 {
            let mut out = Vec::new();
            event.encode(&mut out).map_err(AppendError::Encode)?;
            data.push(out);
        }

        let mut frame_events = Vec::with_capacity(self.0.len());
        for (event, data) in self.0.iter().zip(&data) {
            let type_name = event.type_name();
            let Some(type_id) = types.id(type_name) else {
                return Err(AppendError::Encode(event::CodecError::Encode {
                    type_name,
                    source: "the type name is not listed in Event::TYPES".into(),
                }));
            };
            frame_events.push(frame::FrameEvent::new(
                type_id,
                event.schema_version(),
                data,
            ));
        }

        frame::encode(format, metadata, &frame_events).map_err(|error| match error {
            frame::FrameError::NoEvents => AppendError::NoEvents,
            frame::FrameError::TooManyEvents(_) => AppendError::TooManyEvents {
                max: frame::MAX_EVENTS as u16,
            },
            other => AppendError::Encode(event::CodecError::Encode {
                type_name: "frame",
                source: other.into(),
            }),
        })
    }
}

/// A snapshot to write with an append, encoded when the append was built.
pub(crate) struct SnapshotWrite {
    state_version: i32,
    /// How many versions the new snapshot must be ahead of the stored one.
    min_distance: i64,
    state: Result<Vec<u8>, serde_json::Error>,
}

impl SnapshotWrite {
    fn new<A: aggregate::Snapshotted + Serialize>(state: &A) -> Self {
        let min_distance = match A::policy() {
            aggregate::SnapshotPolicy::EveryN(n) => i64::from(n.get()),
            _ => 1,
        };

        Self {
            state_version: i32::from(A::SNAPSHOT_VERSION.get()),
            min_distance,
            state: serde_json::to_vec(state),
        }
    }
}

/// A commit ready to send.
pub(crate) struct Prepared {
    stream_id: stream::StreamId,
    key: Option<Arc<str>>,
    category: catalog::Category,
    expected: version::ExpectedVersion,
    event_count: i16,
    payload: Vec<u8>,
    format: frame::Format,
    commit_id: commit::CommitId,
    origin: CommitIdOrigin,
    retries: Retries,
}

impl Prepared {
    pub(crate) fn category_id(&self) -> category::CategoryId {
        self.category.id
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CommitIdOrigin {
    Caller,
    Generated,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Mode {
    /// Each statement commits on its own, so a failed statement leaves the connection usable.
    Autocommit,
    /// Inside a transaction: a failed statement aborts it.
    Transaction,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Retried {
    No,
    Yes,
}

/// Whether a retry waits for a stream's row lock that another transaction holds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LockWait {
    Wait,
    /// Waiting could deadlock: the transaction already holds the locks of other streams, taken in stream
    /// id order, and this stream may sort before them.
    Skip,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Lock {
    Held,
    /// Another transaction holds the row.
    Busy,
}

/// The outcome of a batch in a transaction.
enum Batch {
    Written(Vec<AppendResult>),
    /// A stream to retry is locked by another transaction; the batch must roll back and start over.
    Restart,
}

/// How many times an append with a crate-generated commit id may run again with a new id because the
/// stream's last commit id was at or above it. The new id is chosen while the transaction holds the stream's
/// row lock, so one is enough; more are needed only by [`store::Store::append_in`] on a connection outside a
/// transaction, where the lock ends with its statement.
const NEW_COMMIT_ID_ATTEMPTS: u32 = 5;

/// The reruns one append has used (see [`decide`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Retries {
    /// After another append changed the stream while the statement ran.
    rerun: Retried,
    /// With a new crate-generated commit id.
    new_commit_ids: u32,
}

impl Retries {
    const NONE: Self = Self {
        rerun: Retried::No,
        new_commit_ids: 0,
    };
}

/// What the classification query (`append/classify.sql`) saw.
#[derive(Debug, Clone, Default)]
struct Classified {
    version: Option<i64>,
    state: Option<i16>,
    category_id: Option<category::CategoryId>,
    last_commit_id: Option<Uuid>,
    done: Option<i64>,
}

#[derive(Debug)]
enum Decision {
    Done(i64),
    Fail(AppendError),
    CategoryMismatch(category::CategoryId),
    Retry(Retry),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Retry {
    /// Run the statement again as it is.
    Rerun,
    /// Run the statement again with a new crate-generated commit id, above the stream's last one.
    NewCommitId,
}

/// `streams.state`, mirrored from its CHECK constraint.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum StreamState {
    Active,
    Archived,
    Deleted,
}

impl StreamState {
    pub(crate) fn from_code(code: i16) -> Option<Self> {
        match code {
            0 => Some(Self::Active),
            1 => Some(Self::Archived),
            2 => Some(Self::Deleted),
            _ => None,
        }
    }
}

pub(crate) async fn prepare(
    store: &store::Store,
    lease: PoolLease,
    events: &dyn EventBatch,
    stream: stream::StreamRef,
    expected: version::ExpectedVersion,
    metadata: &metadata::Metadata,
    commit_id: Option<commit::CommitId>,
) -> Result<Prepared, AppendError> {
    let count = events.len();
    if count == 0 {
        return Err(AppendError::NoEvents);
    }
    let Ok(event_count) = i16::try_from(count) else {
        return Err(AppendError::TooManyEvents {
            max: frame::MAX_EVENTS as u16,
        });
    };

    // With a pool of one connection held by the unit of work, registering would wait for the pool's
    // acquire timeout and then fail; fail now instead.
    if lease == PoolLease::Held
        && store.pool().options().get_max_connections() <= 1
        && !store.is_registered_locally(events.type_key(), events.category(), events.types())
    {
        return Err(AppendError::NoSpareConnection(events.category()));
    }
    let category = store
        .ensure_registered(events.type_key(), events.category(), events.types())
        .await?;
    let format = store.inner.write_format;
    let payload = events.encode(&category.types, metadata, format)?;
    let (commit_id, origin) = match commit_id {
        Some(id) => (id, CommitIdOrigin::Caller),
        None => (commit::CommitId::generate(), CommitIdOrigin::Generated),
    };

    Ok(Prepared {
        stream_id: stream.id,
        key: stream.key,
        category,
        expected,
        event_count,
        payload,
        format,
        commit_id,
        origin,
        retries: Retries::NONE,
    })
}

/// Appends on a pooled connection, retrying once on a new connection if the first is lost.
async fn append_pooled(
    store: &store::Store,
    prepared: &mut Prepared,
    snapshot: Option<&SnapshotWrite>,
) -> Result<AppendResult, AppendError> {
    let mut retried = Retried::No;

    loop {
        let mut conn = store.inner.pool.acquire().await?;
        let result = match snapshot {
            None => append_on(store, &mut conn, prepared).await,
            Some(snapshot) => append_with_snapshot(store, &mut conn, prepared, snapshot).await,
        };

        match result {
            Err(AppendError::Database(error))
                if error.is_connection_loss() && retried == Retried::No =>
            {
                conn.close_on_drop();
                retried = Retried::Yes;
            }
            other => return other,
        }
    }
}

/// Appends and writes the snapshot in one transaction.
async fn append_with_snapshot(
    store: &store::Store,
    conn: &mut PoolConnection<Postgres>,
    prepared: &mut Prepared,
    snapshot: &SnapshotWrite,
) -> Result<AppendResult, AppendError> {
    let mut tx = conn.begin().await?;
    let result = append_in_transaction(store, &mut tx, prepared, Some(snapshot)).await;

    match result {
        Ok(result) => {
            tx.commit().await?;
            Ok(result)
        }
        Err(error @ (AppendError::DuplicateCommit(_) | AppendError::CommitIdSkew(_))) => {
            tx.rollback().await?;
            // In autocommit these lead to a lookup: the id may belong to an append that already committed.
            match decide(&classify(store, conn, prepared).await?, prepared) {
                Decision::Done(last) => Ok(result_of(prepared, last, Outcome::Deduplicated)),
                _ => Err(error),
            }
        }
        Err(error) => Err(error),
    }
}

/// Appends inside the caller's transaction, then writes the snapshot if one is given and the events were
/// written now.
pub(crate) async fn append_in_transaction(
    store: &store::Store,
    conn: &mut PgConnection,
    prepared: &mut Prepared,
    snapshot: Option<&SnapshotWrite>,
) -> Result<AppendResult, AppendError> {
    let result = match attempt(store, conn, Mode::Transaction, prepared).await? {
        Attempt::Finished(result) => result,
        // A transaction never asks for the lock: it takes it itself.
        Attempt::NeedsLock => return Err(out_of_order(prepared)),
    };

    if let (Some(snapshot), Outcome::Appended) = (snapshot, result.outcome) {
        write_snapshot(store, conn, prepared, &result, snapshot).await?;
    }

    Ok(result)
}

async fn write_snapshot(
    store: &store::Store,
    conn: &mut PgConnection,
    prepared: &Prepared,
    result: &AppendResult,
    snapshot: &SnapshotWrite,
) -> Result<(), AppendError> {
    let state = match &snapshot.state {
        Ok(state) => state.as_slice(),
        Err(error) => return Err(AppendError::SnapshotEncode(error.to_string().into())),
    };

    store
        .inner
        .sql
        .snapshot_upsert
        .bind(statement::snapshot_upsert::Params {
            version: to_i64(result.last_version()),
            stream_id: prepared.stream_id.as_uuid(),
            commit_id: result.commit_id.as_uuid(),
            category_id: prepared.category.id.get(),
            state_version: snapshot.state_version,
            format: load::SNAPSHOT_FORMAT_JSON,
            state,
            min_distance: snapshot.min_distance,
        })
        .execute(conn)
        .await?;

    Ok(())
}

/// Runs an autocommit append and classifies a "no row" outcome, retrying where the table says so.
///
/// An append whose generated commit id fell behind the stream's last one runs again in a transaction that
/// locks the stream before choosing the new id, so the new id cannot fall behind again.
async fn append_on(
    store: &store::Store,
    conn: &mut PoolConnection<Postgres>,
    prepared: &mut Prepared,
) -> Result<AppendResult, AppendError> {
    match attempt(store, conn, Mode::Autocommit, prepared).await? {
        Attempt::Finished(result) => Ok(result),
        Attempt::NeedsLock => append_locked(store, conn, prepared).await,
    }
}

enum Attempt {
    Finished(AppendResult),
    /// A generated commit id fell behind in autocommit: the next run needs a transaction holding the lock.
    NeedsLock,
}

/// The statement and its classification, run again while the table says so. Inside a transaction a
/// generated commit id that fell behind is replaced under the stream's row lock; in autocommit the caller
/// does that.
async fn attempt(
    store: &store::Store,
    conn: &mut PgConnection,
    mode: Mode,
    prepared: &mut Prepared,
) -> Result<Attempt, AppendError> {
    loop {
        match run_append(store, conn, prepared).await {
            Ok(Some(last)) => {
                return Ok(Attempt::Finished(result_of(
                    prepared,
                    last,
                    Outcome::Appended,
                )));
            }
            Ok(None) => {}
            Err(error) => {
                return statement_failed(store, conn, mode, prepared, error)
                    .await
                    .map(Attempt::Finished);
            }
        }

        let classified = classify(store, conn, prepared).await?;
        match decide(&classified, prepared) {
            Decision::Done(last) => {
                return Ok(Attempt::Finished(result_of(
                    prepared,
                    last,
                    Outcome::Deduplicated,
                )));
            }
            Decision::Fail(error) => return Err(error),
            Decision::CategoryMismatch(category_id) => {
                return Err(category_mismatch(store, prepared, category_id).await);
            }
            Decision::Retry(Retry::NewCommitId) if mode == Mode::Autocommit => {
                return Ok(Attempt::NeedsLock);
            }
            Decision::Retry(Retry::NewCommitId) => {
                take_retry(prepared, Retry::NewCommitId, classified.version)?;
                lock_for_retry(
                    store,
                    conn,
                    prepared,
                    Retry::NewCommitId,
                    classified.last_commit_id,
                    LockWait::Wait,
                )
                .await?;
            }
            Decision::Retry(Retry::Rerun) => {
                take_retry(prepared, Retry::Rerun, classified.version)?;
            }
        }
    }
}

/// Runs an autocommit append again in a transaction that first locks the stream and moves the generated
/// commit id above the stream's last one.
async fn append_locked(
    store: &store::Store,
    conn: &mut PoolConnection<Postgres>,
    prepared: &mut Prepared,
) -> Result<AppendResult, AppendError> {
    take_retry(prepared, Retry::NewCommitId, None)?;
    let mut tx = conn.begin().await?;
    lock_for_retry(
        store,
        &mut tx,
        prepared,
        Retry::NewCommitId,
        None,
        LockWait::Wait,
    )
    .await?;

    let result = match attempt(store, &mut tx, Mode::Transaction, prepared).await {
        Ok(Attempt::Finished(result)) => result,
        // A transaction never asks for the lock: it takes it itself.
        Ok(Attempt::NeedsLock) => return Err(out_of_order(prepared)),
        Err(error) => return Err(error),
    };
    tx.commit().await?;

    Ok(result)
}

/// Locks the stream's row before the append runs again and, for [`Retry::NewCommitId`], replaces the
/// generated commit id with one above the stream's last commit id. The lock lasts until the transaction
/// ends, so no other append can move the stream past the new id. `seen` is the last commit id the
/// classification read, used when the row is not locked.
///
/// With [`LockWait::Skip`] a row another transaction holds is not waited for: the result is
/// [`Lock::Busy`], and the commit id is moved above `seen`.
async fn lock_for_retry(
    store: &store::Store,
    conn: &mut PgConnection,
    prepared: &mut Prepared,
    retry: Retry,
    seen: Option<Uuid>,
    wait: LockWait,
) -> Result<Lock, AppendError> {
    let sql = match wait {
        LockWait::Wait => &store.inner.sql.lock_stream,
        LockWait::Skip => &store.inner.sql.try_lock_stream,
    };
    let locked = sql
        .bind(statement::lock_stream::Params {
            stream_id: prepared.stream_id.as_uuid(),
        })
        .fetch_optional(&mut *conn)
        .await?
        .map(|row| row.last_commit_id);

    if retry == Retry::NewCommitId
        && let Some(last) = locked.or(seen)
    {
        prepared.commit_id = commit_id_above(last, recent_last_commit(store));
    }

    Ok(match (wait, locked) {
        (LockWait::Skip, None) => Lock::Busy,
        _ => Lock::Held,
    })
}

/// Counts one more run of the append, or returns the error to report when its retries are used up.
fn take_retry(
    prepared: &mut Prepared,
    retry: Retry,
    actual: Option<i64>,
) -> Result<(), AppendError> {
    match retry {
        Retry::Rerun if prepared.retries.rerun == Retried::No => {
            prepared.retries.rerun = Retried::Yes;
            Ok(())
        }
        Retry::Rerun => Err(conflict(prepared, actual)),
        Retry::NewCommitId if prepared.retries.new_commit_ids < NEW_COMMIT_ID_ATTEMPTS => {
            prepared.retries.new_commit_ids += 1;
            Ok(())
        }
        Retry::NewCommitId => Err(out_of_order(prepared)),
    }
}

fn out_of_order(prepared: &Prepared) -> AppendError {
    AppendError::CommitIdOutOfOrder {
        stream_id: prepared.stream_id,
        commit_id: prepared.commit_id,
    }
}

/// Maps an error from the append statement. In autocommit the connection is still usable, so a skewed or
/// duplicate commit id is first looked up: it may belong to an append that already committed.
async fn statement_failed(
    store: &store::Store,
    conn: &mut PgConnection,
    mode: Mode,
    prepared: &Prepared,
    error: sqlx::Error,
) -> Result<AppendResult, AppendError> {
    let mapped = match error::sqlstate(&error).as_deref() {
        Some("PGE01") => AppendError::CommitIdSkew(prepared.commit_id),
        Some("23505") if is_commit_key(&error) => AppendError::DuplicateCommit(prepared.commit_id),
        _ => return Err(error.into()),
    };
    if mode == Mode::Transaction {
        return Err(mapped);
    }

    match decide(&classify(store, conn, prepared).await?, prepared) {
        Decision::Done(last) => Ok(result_of(prepared, last, Outcome::Deduplicated)),
        _ => Err(mapped),
    }
}

/// Whether a unique violation is on the commits primary key (a leaf's `<leaf>_pkey`), not the per-leaf
/// `(stream_id, last_version)` index.
fn is_commit_key(error: &sqlx::Error) -> bool {
    error::constraint(error).is_some_and(|name| name.ends_with("_pkey"))
}

async fn run_append(
    store: &store::Store,
    conn: &mut PgConnection,
    prepared: &Prepared,
) -> Result<Option<i64>, sqlx::Error> {
    let row = store
        .inner
        .sql
        .append(prepared.stream_id.as_uuid())
        .bind(statement::append::Params {
            stream_id: prepared.stream_id.as_uuid(),
            category_id: prepared.category.id.get(),
            expected: expected_code(prepared.expected),
            event_count: prepared.event_count,
            format: prepared.format.code(),
            payload: prepared.payload.as_slice(),
            key: prepared.key.as_deref(),
            commit_id: prepared.commit_id.as_uuid(),
            max_clock_skew: store.inner.max_clock_skew,
        })
        .fetch_optional(conn)
        .await?;

    Ok(row.map(|row| row.last_version))
}

async fn classify(
    store: &store::Store,
    conn: &mut PgConnection,
    prepared: &Prepared,
) -> Result<Classified, sqlx::Error> {
    let row = store
        .inner
        .sql
        .classify
        .bind(statement::classify::Params {
            stream_id: prepared.stream_id.as_uuid(),
            category_id: prepared.category.id.get(),
            commit_id: prepared.commit_id.as_uuid(),
        })
        .fetch_one(conn)
        .await?;

    Ok(Classified {
        version: row.version,
        state: row.state,
        category_id: row.category_id.map(category::CategoryId::new),
        last_commit_id: row.last_commit_id,
        done: row.done,
    })
}

/// What to do after an append returned no row, from what the classification query saw. In order:
///
/// - `done` is set: a retry of an append that already succeeded; return its result as deduplicated.
/// - No stream, expected `Any` or `NoStream`: the append raced the stream's creation (the statement's
///   snapshot could not see the other insert); run it again, once.
/// - No stream, expected `Exact`: a conflict with no actual version.
/// - Another category: a category mismatch.
/// - Archived or deleted: `StreamArchived` or `StreamDeleted`.
/// - Version differs from `Exact`, or the stream exists for `NoStream`: a conflict with the actual version.
/// - Commit id at or below `last_commit_id`: `CommitIdOutOfOrder` for a caller's id; for a generated one,
///   lock the stream and run again with a new id above `last_commit_id` (at most
///   [`NEW_COMMIT_ID_ATTEMPTS`]).
/// - Otherwise another append changed the stream while the statement ran (for example `Exact` racing the
///   creation): run it again, once.
fn decide(classified: &Classified, prepared: &Prepared) -> Decision {
    if let Some(last) = classified.done {
        return Decision::Done(last);
    }

    let (Some(version), Some(state), Some(category_id), Some(last_commit_id)) = (
        classified.version,
        classified.state,
        classified.category_id,
        classified.last_commit_id,
    ) else {
        return match prepared.expected {
            // The stream's creation raced this append: the statement's snapshot could not see it.
            version::ExpectedVersion::Any | version::ExpectedVersion::NoStream => {
                Decision::Retry(Retry::Rerun)
            }
            _ => Decision::Fail(conflict(prepared, None)),
        };
    };

    if category_id != prepared.category.id {
        return Decision::CategoryMismatch(category_id);
    }
    match StreamState::from_code(state) {
        Some(StreamState::Archived) => {
            return Decision::Fail(AppendError::StreamArchived(prepared.stream_id));
        }
        Some(StreamState::Deleted) => {
            return Decision::Fail(AppendError::StreamDeleted(prepared.stream_id));
        }
        Some(StreamState::Active) | None => {}
    }
    match prepared.expected {
        version::ExpectedVersion::Exact(expected) if to_i64(expected) != version => {
            return Decision::Fail(conflict(prepared, Some(version)));
        }
        version::ExpectedVersion::NoStream => {
            return Decision::Fail(conflict(prepared, Some(version)));
        }
        _ => {}
    }
    if prepared.commit_id.as_uuid() <= last_commit_id {
        return match prepared.origin {
            CommitIdOrigin::Caller => Decision::Fail(out_of_order(prepared)),
            CommitIdOrigin::Generated => Decision::Retry(Retry::NewCommitId),
        };
    }

    // The stream now matches what the append expected, so another append changed it while the statement
    // ran (for example an Exact append racing the stream's creation). Running it again decides.
    Decision::Retry(Retry::Rerun)
}

async fn category_mismatch(
    store: &store::Store,
    prepared: &Prepared,
    category_id: category::CategoryId,
) -> AppendError {
    match store.category_by_id(category_id).await {
        Ok(Some(stream_category)) => AppendError::CategoryMismatch {
            stream_id: prepared.stream_id,
            stream_category: stream_category.name,
            event_category: prepared.category.name,
        },
        // Categories are never deleted, so the id is always found.
        Ok(None) => AppendError::UnknownCategory(prepared.category.name),
        Err(error) => error.into(),
    }
}

fn conflict(prepared: &Prepared, actual: Option<i64>) -> AppendError {
    AppendError::Conflict {
        stream_id: prepared.stream_id,
        expected: prepared.expected,
        actual: actual.and_then(to_version),
    }
}

fn result_of(prepared: &Prepared, last_version: i64, outcome: Outcome) -> AppendResult {
    let last = to_version(last_version).unwrap_or(version::Version::FIRST);
    let first_number = last.get().saturating_sub(prepared.event_count as u64 - 1);
    let first = version::Version::new(first_number).unwrap_or(version::Version::FIRST);

    AppendResult {
        stream_id: prepared.stream_id,
        commit_id: prepared.commit_id,
        versions: first..=last,
        outcome,
    }
}

/// `$3` of the append statement: -1 NoStream, -2 Any, n >= 1 Exact(n).
fn expected_code(expected: version::ExpectedVersion) -> i64 {
    match expected {
        version::ExpectedVersion::NoStream => -1,
        version::ExpectedVersion::Exact(version) => to_i64(version),
        _ => -2,
    }
}

pub(crate) fn to_i64(version: version::Version) -> i64 {
    version.to_i64()
}

pub(crate) fn to_version(value: i64) -> Option<version::Version> {
    version::Version::from_i64(value)
}

/// A new commit id for an append whose generated one was at or below the stream's last commit id `last`.
///
/// When `last` is less than `recent` older than a fresh id (or ahead of it), the new id is just above `last`,
/// in its millisecond. On a hot stream the appends queued for its row lock hold ids generated a moment ago,
/// mostly above `last`; a fresh id would jump past all of them, so each would fail its recheck and retry in
/// turn. Staying just above `last` lets them succeed. It also never moves commit ids further into the future
/// than a writer whose clock is ahead.
///
/// When `last` is older, the new id is fresh, so the stream's ids keep up with the clock.
fn commit_id_above(last: Uuid, recent: Duration) -> commit::CommitId {
    commit_id_above_at(last, recent, commit::CommitId::generate())
}

/// [`commit_id_above`] with `fresh` as the id generated now.
fn commit_id_above_at(last: Uuid, recent: Duration, fresh: commit::CommitId) -> commit::CommitId {
    let above = random_commit_id_above(last);

    let recent =
        unix_millis(last).saturating_add(recent.as_millis()) >= unix_millis(fresh.as_uuid());
    if recent { above } else { fresh.max(above) }
}

/// How far behind a fresh id a stream's last commit id may be for a retry to take the id just above it: 1 s,
/// or half the writer's clock skew if that is smaller. The new id is then at most that far in the past, well
/// inside the skew the append statement checks.
fn recent_last_commit(store: &store::Store) -> Duration {
    const AT_MOST: Duration = Duration::from_secs(1);

    AT_MOST.min(store::duration(store.inner.max_clock_skew) / 2)
}

/// The UUIDv7 timestamp: milliseconds since the Unix epoch, in the top 48 bits.
fn unix_millis(id: Uuid) -> u128 {
    id.as_u128() >> 80
}

/// A UUIDv7 above `id` by a random step of 1 to 2^60, in `id`'s millisecond unless its 74 counter and random
/// bits overflow.
///
/// The step is random because two appends can pick an id above the same last id: one that chose its id under
/// a row lock its transaction then gave up (an `append_many` batch that starts over, a commit whose reply was
/// lost), or one that did not get the lock (`SKIP LOCKED`). The same id for both would make the second look
/// like a retry of the first, and it would be reported as deduplicated without being written.
fn random_commit_id_above(id: Uuid) -> commit::CommitId {
    const STEP_BITS: u32 = 60;
    const RANDOM_BITS: u32 = 32;

    // Only the low 32 bits of a fresh UUIDv7 are random: the 42 above them are a counter, which two ids
    // of one process in one millisecond share but for its last bits. Two ids give 64 random bits.
    let random = || commit::CommitId::generate().as_uuid().as_u128() & ((1 << RANDOM_BITS) - 1);
    let random = (random() << RANDOM_BITS) | random();
    let step = (random & ((1 << STEP_BITS) - 1)) + 1;

    commit_id_above_by(id, step)
}

/// The UUIDv7 `step` above `id`, counting its 74 bits after the version and variant as one number. On
/// overflow it moves to the next millisecond, `step` into it.
fn commit_id_above_by(id: Uuid, step: u128) -> commit::CommitId {
    const RAND_B_BITS: u32 = 62;
    const RAND_B: u128 = (1 << RAND_B_BITS) - 1;
    const COUNTER_LIMIT: u128 = 1 << 74;

    let value = id.as_u128();
    let millis = value >> 80;
    let counter = (((value >> 64) & 0xfff) << RAND_B_BITS) | (value & RAND_B);
    let (millis, counter) = match counter.checked_add(step) {
        Some(sum) if sum < COUNTER_LIMIT => (millis, sum),
        _ => (millis + 1, step.min(COUNTER_LIMIT - 1)),
    };
    let next = (millis << 80)
        | (0x7 << 76)
        | ((counter >> RAND_B_BITS) << 64)
        | (0b10 << RAND_B_BITS)
        | (counter & RAND_B);

    commit::CommitId::from_uuid(Uuid::from_u128(next)).unwrap_or_else(commit::CommitId::generate)
}

/// Appends a batch on a pooled connection in its own transaction, retrying once on a new connection if the
/// first is lost.
async fn append_many_pooled(
    store: &store::Store,
    prepared: &mut [Prepared],
) -> Result<Vec<AppendResult>, AppendError> {
    let mut retried = Retried::No;

    loop {
        let mut conn = store.inner.pool.acquire().await?;
        let result = async {
            loop {
                let mut tx = conn.begin().await?;
                match append_many_on(store, &mut tx, prepared, LockWait::Skip).await? {
                    Batch::Written(results) => {
                        tx.commit().await?;
                        return Ok(results);
                    }
                    Batch::Restart => tx.rollback().await?,
                }
            }
        }
        .await;

        match result {
            Err(AppendError::Database(error))
                if error.is_connection_loss() && retried == Retried::No =>
            {
                conn.close_on_drop();
                retried = Retried::Yes;
            }
            other => return other,
        }
    }
}

/// Runs the batch statement, then classifies each stream it did not write. Streams that should be retried
/// are locked and sent again on their own; the rest of the batch is already written in the transaction.
///
/// The batch statement locks the streams that existed when it started in stream id order. A stream created
/// by another transaction after that is not locked, and locking it for the retry could deadlock. With
/// [`LockWait::Skip`] such a stream ends the attempt with [`Batch::Restart`]; every retry counts against the
/// stream's retries, so restarts are bounded.
async fn append_many_on(
    store: &store::Store,
    conn: &mut PgConnection,
    prepared: &mut [Prepared],
    wait: LockWait,
) -> Result<Batch, AppendError> {
    let mut results: Vec<Option<AppendResult>> = vec![None; prepared.len()];
    let mut pending: Vec<usize> = (0..prepared.len()).collect();

    loop {
        let written = run_append_many(store, conn, prepared, &pending)
            .await
            .map_err(|error| batch_failed(prepared, &pending, error))?;
        for index in &pending {
            if let Some(last) = written.get(&prepared[*index].stream_id.as_uuid()) {
                results[*index] = Some(result_of(&prepared[*index], *last, Outcome::Appended));
            }
        }

        let mut retry = Vec::new();
        for index in pending {
            if results[index].is_some() {
                continue;
            }

            let entry = &mut prepared[index];
            let classified = classify(store, conn, entry).await?;
            match decide(&classified, entry) {
                Decision::Done(last) => {
                    results[index] = Some(result_of(entry, last, Outcome::Deduplicated));
                }
                Decision::Fail(error) => return Err(error),
                Decision::CategoryMismatch(category_id) => {
                    return Err(category_mismatch(store, entry, category_id).await);
                }
                Decision::Retry(next) => {
                    take_retry(entry, next, classified.version)?;
                    let lock =
                        lock_for_retry(store, conn, entry, next, classified.last_commit_id, wait)
                            .await?;
                    if lock == Lock::Busy {
                        return Ok(Batch::Restart);
                    }
                    retry.push(index);
                }
            }
        }

        if retry.is_empty() {
            return Ok(Batch::Written(results.into_iter().flatten().collect()));
        }
        pending = retry;
    }
}

/// Maps an error from the batch statement. The commit ids of one batch are generated together, so a skew
/// error applies to all of them; it names the first.
fn batch_failed(prepared: &[Prepared], pending: &[usize], error: sqlx::Error) -> AppendError {
    let first = pending.first().map(|index| prepared[*index].commit_id);

    match (error::sqlstate(&error).as_deref(), first) {
        (Some("PGE01"), Some(commit_id)) => AppendError::CommitIdSkew(commit_id),
        _ => error.into(),
    }
}

async fn run_append_many(
    store: &store::Store,
    conn: &mut PgConnection,
    prepared: &[Prepared],
    indexes: &[usize],
) -> Result<std::collections::HashMap<Uuid, i64>, sqlx::Error> {
    let entries: Vec<&Prepared> = indexes.iter().map(|i| &prepared[*i]).collect();
    let stream_ids: Vec<Uuid> = entries.iter().map(|p| p.stream_id.as_uuid()).collect();
    let category_ids: Vec<i32> = entries.iter().map(|p| p.category.id.get()).collect();
    let expected: Vec<i64> = entries.iter().map(|p| expected_code(p.expected)).collect();
    let event_counts: Vec<i16> = entries.iter().map(|p| p.event_count).collect();
    let formats: Vec<i16> = entries.iter().map(|p| p.format.code()).collect();
    let payloads: Vec<&[u8]> = entries.iter().map(|p| p.payload.as_slice()).collect();
    let keys: Vec<Option<&str>> = entries.iter().map(|p| p.key.as_deref()).collect();
    let commit_ids: Vec<Uuid> = entries.iter().map(|p| p.commit_id.as_uuid()).collect();

    let rows = store
        .inner
        .sql
        .append_many
        .bind(statement::append_many::Params {
            stream_ids,
            category_ids,
            expected,
            event_counts,
            formats,
            payloads,
            keys,
            commit_ids,
            max_clock_skew: store.inner.max_clock_skew,
        })
        .fetch_all(conn)
        .await?;

    Ok(rows
        .into_iter()
        .map(|row| (row.stream_id, row.last_version))
        .collect())
}

#[cfg(test)]
mod tests {
    use sqlx::Connection;

    use super::*;

    const JOBS_ID: category::CategoryId = category::CategoryId::new(3);
    const OTHER_ID: category::CategoryId = category::CategoryId::new(4);

    fn prepared(expected: version::ExpectedVersion, origin: CommitIdOrigin) -> Prepared {
        let name = pges_core::category!("jobs");

        Prepared {
            stream_id: stream::StreamId::new_v7(),
            key: None,
            category: catalog::Category {
                id: JOBS_ID,
                name,
                read_stream: Arc::new(crate::sql::read_stream(
                    &pges_core::store_name!("ops_es"),
                    name,
                )),
                types: Arc::default(),
            },
            expected,
            event_count: 1,
            payload: Vec::new(),
            format: frame::Format::LATEST,
            commit_id: commit::CommitId::generate(),
            origin,
            retries: Retries::NONE,
        }
    }

    fn existing(version: i64, state: i16, last_commit_id: Uuid) -> Classified {
        Classified {
            version: Some(version),
            state: Some(state),
            category_id: Some(JOBS_ID),
            last_commit_id: Some(last_commit_id),
            done: None,
        }
    }

    fn exact(n: u64) -> version::ExpectedVersion {
        version::ExpectedVersion::Exact(version::Version::new(n).unwrap())
    }

    /// [`decide`], one assertion per case, in order.
    #[test]
    fn classification_follows_the_table() {
        let older = Uuid::now_v7();
        let p = prepared(exact(2), CommitIdOrigin::Caller);

        let done = Classified {
            done: Some(7),
            ..existing(9, 2, older)
        };
        assert!(matches!(decide(&done, &p), Decision::Done(7)));

        let missing = Classified::default();
        let any = prepared(version::ExpectedVersion::Any, CommitIdOrigin::Generated);
        assert!(matches!(
            decide(&missing, &any),
            Decision::Retry(Retry::Rerun)
        ));
        assert!(matches!(
            decide(&missing, &p),
            Decision::Fail(AppendError::Conflict { actual: None, .. })
        ));

        let other_category = Classified {
            category_id: Some(OTHER_ID),
            ..existing(2, 0, older)
        };
        assert!(matches!(
            decide(&other_category, &p),
            Decision::CategoryMismatch(id) if id == OTHER_ID
        ));

        assert!(matches!(
            decide(&existing(2, 1, older), &p),
            Decision::Fail(AppendError::StreamArchived(_))
        ));
        assert!(matches!(
            decide(&existing(2, 2, older), &p),
            Decision::Fail(AppendError::StreamDeleted(_))
        ));

        assert!(matches!(
            decide(&existing(5, 0, older), &p),
            Decision::Fail(AppendError::Conflict { actual: Some(a), .. }) if a.get() == 5
        ));
        let no_stream = prepared(version::ExpectedVersion::NoStream, CommitIdOrigin::Caller);
        assert!(matches!(
            decide(&existing(1, 0, older), &no_stream),
            Decision::Fail(AppendError::Conflict { actual: Some(a), .. }) if a.get() == 1
        ));

        let generated = prepared(exact(2), CommitIdOrigin::Generated);
        let newer = Uuid::now_v7();
        assert!(matches!(
            decide(&existing(2, 0, newer), &p),
            Decision::Fail(AppendError::CommitIdOutOfOrder { .. })
        ));
        assert!(matches!(
            decide(&existing(2, 0, newer), &generated),
            Decision::Retry(Retry::NewCommitId)
        ));

        // Matches every expectation: a concurrent append changed the stream while the statement ran.
        assert!(matches!(
            decide(&existing(2, 0, older), &p),
            Decision::Retry(Retry::Rerun)
        ));
    }

    /// A generated id gets a new one at most `NEW_COMMIT_ID_ATTEMPTS` times, and a rerun after a race is
    /// taken once; neither budget uses up the other.
    #[test]
    fn retries_are_bounded() {
        let mut p = prepared(version::ExpectedVersion::Any, CommitIdOrigin::Generated);

        take_retry(&mut p, Retry::Rerun, None).unwrap();
        for _ in 0..NEW_COMMIT_ID_ATTEMPTS {
            take_retry(&mut p, Retry::NewCommitId, Some(1)).unwrap();
        }

        assert!(matches!(
            take_retry(&mut p, Retry::NewCommitId, Some(1)),
            Err(AppendError::CommitIdOutOfOrder { .. })
        ));
        assert!(matches!(
            take_retry(&mut p, Retry::Rerun, Some(1)),
            Err(AppendError::Conflict { .. })
        ));
    }

    /// A new id is just above a recent or future last id, and fresh when the last id is older than `recent`.
    #[test]
    fn new_commit_ids_are_just_above_a_recent_last_one_or_fresh() {
        // The clock's millisecond, with rand_b at 2^61: a step of at most 2^60 then never carries into rand_a,
        // so the step is the plain difference of the ids. (A generated rand_b carries for about one step in
        // eight, which adds the two variant bits to the difference.)
        let now_ms = unix_millis(commit::CommitId::generate().as_uuid());
        let fresh = commit::CommitId::from_uuid(Uuid::from_u128(
            (now_ms << 80) | 0x7800_a000_0000_0000_0000,
        ))
        .unwrap();
        let now = fresh.as_uuid().as_u128();
        let at = |offset_ms: i128| Uuid::from_u128((now as i128 + (offset_ms << 80)) as u128);
        let second = Duration::from_secs(1);

        let old = Uuid::from_u128(0x0190_0000_0000_7000_8000_0000_0000_0000);
        for last in [old, at(-1_001), at(-1_500)] {
            assert_eq!(commit_id_above_at(last, second, fresh), fresh);
        }

        for last in [at(-1_000), at(-500), at(-10), at(0), at(10_000)] {
            let above = commit_id_above_at(last, second, fresh).as_uuid();
            assert!(above > last);
            assert!(above.as_u128() - last.as_u128() <= 1 << 60);
        }

        let narrow = commit_id_above_at(at(-100), Duration::from_millis(50), fresh);
        assert_eq!(narrow, fresh);

        // With the clock: an old last id gets an id generated now.
        let generated = commit_id_above(old, second);
        assert!(unix_millis(generated.as_uuid()) >= now_ms);
    }

    /// Two ids picked above the same last id differ.
    #[test]
    fn ids_picked_above_the_same_id_differ() {
        let last = Uuid::now_v7();
        let picked: HashSet<Uuid> = (0..1000)
            .map(|_| random_commit_id_above(last).as_uuid())
            .collect();

        assert_eq!(picked.len(), 1000);
        assert!(
            picked
                .iter()
                .all(|id| *id > last && id.get_version_num() == 7)
        );
    }

    #[test]
    fn ids_above_carry_into_rand_a_and_the_timestamp() {
        let id = Uuid::now_v7();
        assert_eq!(
            commit_id_above_by(id, 1).as_uuid().as_u128(),
            id.as_u128() + 1
        );

        let rand_b_full = Uuid::from_u128(0x0190_0000_0000_7000_bfff_ffff_ffff_ffff);
        assert_eq!(
            commit_id_above_by(rand_b_full, 1).as_uuid(),
            Uuid::from_u128(0x0190_0000_0000_7001_8000_0000_0000_0000)
        );

        let all_full = Uuid::from_u128(0x0190_0000_0000_7fff_bfff_ffff_ffff_ffff);
        let next = commit_id_above_by(all_full, 3).as_uuid();
        assert_eq!(
            next,
            Uuid::from_u128(0x0190_0000_0001_7000_8000_0000_0000_0003)
        );
        assert_eq!(next.get_version_num(), 7);
    }

    /// `streams.state` accepts exactly the codes `StreamState` knows.
    #[tokio::test]
    async fn stream_states_mirror_the_check_constraint() -> anyhow::Result<()> {
        assert_eq!(StreamState::from_code(0), Some(StreamState::Active));
        assert_eq!(StreamState::from_code(1), Some(StreamState::Archived));
        assert_eq!(StreamState::from_code(2), Some(StreamState::Deleted));

        let db = pges_testing::TestDb::new().await?;
        let mut conn = db.connect().await?;
        // pges-testing links its own build of this crate, whose `store::Outbox` is another type here.
        pges_testing::create_store(
            &mut conn,
            &pges_core::store_name!("parity"),
            &pges_testing::definition(Default::default()),
        )
        .await?;
        for code in -1_i16..=3 {
            let id = Uuid::now_v7();
            let inserted = sqlx::query(
                "INSERT INTO parity.streams
                   (version, stream_id, first_commit_id, last_commit_id, category_id, state)
                 VALUES (1, $1, $1, $1, 1, $2)",
            )
            .bind(id)
            .bind(code)
            .execute(&mut conn)
            .await;

            assert_eq!(
                inserted.is_ok(),
                StreamState::from_code(code).is_some(),
                "state {code}"
            );
        }

        conn.close().await?;
        db.cleanup().await
    }

    #[test]
    fn expected_versions_have_documented_codes() {
        assert_eq!(expected_code(version::ExpectedVersion::NoStream), -1);
        assert_eq!(expected_code(version::ExpectedVersion::Any), -2);
        assert_eq!(
            expected_code(version::ExpectedVersion::Exact(
                version::Version::new(7).unwrap()
            )),
            7
        );
    }
}
