//! Where the sequencer learns which commits exist: the [`Source`] trait.

use std::{error, fmt, future::Future, num::NonZeroUsize};

use futures_util::future::BoxFuture;
use sqlx::PgConnection;

use pges_core::{category, commit, store};

/// A boxed error from a source.
pub type BoxError = Box<dyn error::Error + Send + Sync>;

/// Delivers committed appends to the [`Sequencer`](crate::sequencer::Sequencer), in an order that respects
/// causality.
///
/// The sequencer drives a source through one leadership term like this:
///
/// 1. [`Source::open`] with the durable token stored in `pges.sequencer.source_token` (if it was written by a
///    source of the same [`Source::kind`]);
/// 2. [`Source::ready`], outside any transaction, until commits may be available;
/// 3. [`Source::next`] inside the batch transaction, after the fence check. The log rows for the returned
///    commits are written in the same transaction;
/// 4. [`Source::ack`] with a token once that token and every log row it covers are durable. The sequencer
///    persists tokens at most about once a second (the coarse token), so not every batch's token is acked.
/// 5. [`Source::close`] when the term ends: the sequencer lost its lease, shuts down, or stops on an error.
///    A new term starts again with [`Source::open`].
///
/// If a batch transaction does not commit, the sequencer calls [`Source::open`] again with the last durable
/// token before the next [`Source::next`]. A source that changes state in [`Source::next`] through the
/// transaction (the outbox) gets that rolled back with it; a source that buffers (logical replication)
/// replays from the token.
///
/// Contract for every source:
///
/// - Every committed append is delivered at least once; rolled-back appends never are.
/// - If append B could have seen append A's commit, A is delivered before B.
/// - Resuming from a token replays nothing already covered by it. Commits delivered again after the token
///   (for example after a crash before the next coarse token) are dropped by the sequencer, which keeps
///   the ids of the commits it sequenced since the last durable token.
///
/// `Source` uses `impl Future` returns, so it is not dyn-compatible; [`DynSource`] is its object-safe twin.
pub trait Source: Send {
    /// A short, stable name for this kind of source, stored in `pges.sequencer.source` next to the token.
    /// A token written by another kind of source is never passed to [`Source::open`]. Write it with
    /// [`source_kind!`](crate::source_kind!).
    fn kind(&self) -> SourceKind;

    /// Starts or restarts delivery after `token` (`None`: from wherever the source starts).
    ///
    /// The sequencer renews its lease while this runs. It drops the future on shutdown or when it loses the
    /// lease, and then calls [`Source::close`]; the source must open again cleanly after that.
    fn open(
        &mut self,
        token: Option<&[u8]>,
    ) -> impl Future<Output = Result<(), SourceError>> + Send;

    /// Resolves when [`Source::next`] may return commits. It runs outside any transaction and must be
    /// cancel safe: the sequencer drops it to renew its lease or to shut down, and calls it again.
    fn ready(&mut self) -> impl Future<Output = Result<(), SourceError>> + Send;

    /// The next batch of committed appends, in source order, with the token that resumes after it.
    ///
    /// `tx` is the sequencer's batch transaction, already fenced; a source may read or change its own
    /// tables through it, and those changes commit or roll back with the log rows. It returns at most
    /// `limit` commits, except that a source may exceed it to keep one source transaction whole. An empty
    /// batch is allowed.
    fn next(
        &mut self,
        tx: &mut PgConnection,
        limit: NonZeroUsize,
    ) -> impl Future<Output = Result<SourceBatch, SourceError>> + Send;

    /// Called once `token`, and the log rows of every commit delivered up to it, are durable. A source
    /// that keeps state elsewhere (a replication slot) may release everything up to the token.
    fn ack(&mut self, token: &[u8]) -> impl Future<Output = Result<(), SourceError>> + Send;

    /// Runs while the sequencer writes a batch, after [`Source::next`] returned, and is dropped when the
    /// batch transaction ends. A source whose connection times out when left alone (a replication stream)
    /// keeps it alive here; an error aborts the batch. It must be cancel safe.
    ///
    /// The default never resolves.
    fn keep_alive(&mut self) -> impl Future<Output = Result<(), SourceError>> + Send {
        std::future::pending()
    }

    /// Called when the sequencer stops leading: it lost its lease, shuts down, or stops on an error. A
    /// source that holds something on the server while it is open (a replication slot) gives it up here, so
    /// the next leader can take it over at once. It cannot fail and should finish within a second or so: the
    /// sequencer waits for it before it releases the lease. [`Source::open`] follows if this sequencer leads
    /// again.
    ///
    /// The default does nothing.
    fn close(&mut self) -> impl Future<Output = ()> + Send {
        std::future::ready(())
    }
}

/// The object-safe twin of [`Source`], for configuration that picks a source at run time
/// (`Box<dyn DynSource>`). Every [`Source`] implements it, and `Box<dyn DynSource>` implements [`Source`].
pub trait DynSource: Send {
    /// See [`Source::kind`].
    fn kind(&self) -> SourceKind;

    /// See [`Source::open`].
    fn open<'a>(&'a mut self, token: Option<&'a [u8]>) -> BoxFuture<'a, Result<(), SourceError>>;

    /// See [`Source::ready`].
    fn ready(&mut self) -> BoxFuture<'_, Result<(), SourceError>>;

    /// See [`Source::next`].
    fn next<'a>(
        &'a mut self,
        tx: &'a mut PgConnection,
        limit: NonZeroUsize,
    ) -> BoxFuture<'a, Result<SourceBatch, SourceError>>;

    /// See [`Source::ack`].
    fn ack<'a>(&'a mut self, token: &'a [u8]) -> BoxFuture<'a, Result<(), SourceError>>;

    /// See [`Source::keep_alive`].
    fn keep_alive(&mut self) -> BoxFuture<'_, Result<(), SourceError>>;

    /// See [`Source::close`].
    fn close(&mut self) -> BoxFuture<'_, ()>;
}

impl<T: Source> DynSource for T {
    fn kind(&self) -> SourceKind {
        Source::kind(self)
    }

    fn open<'a>(&'a mut self, token: Option<&'a [u8]>) -> BoxFuture<'a, Result<(), SourceError>> {
        Box::pin(Source::open(self, token))
    }

    fn ready(&mut self) -> BoxFuture<'_, Result<(), SourceError>> {
        Box::pin(Source::ready(self))
    }

    fn next<'a>(
        &'a mut self,
        tx: &'a mut PgConnection,
        limit: NonZeroUsize,
    ) -> BoxFuture<'a, Result<SourceBatch, SourceError>> {
        Box::pin(Source::next(self, tx, limit))
    }

    fn ack<'a>(&'a mut self, token: &'a [u8]) -> BoxFuture<'a, Result<(), SourceError>> {
        Box::pin(Source::ack(self, token))
    }

    fn keep_alive(&mut self) -> BoxFuture<'_, Result<(), SourceError>> {
        Box::pin(Source::keep_alive(self))
    }

    fn close(&mut self) -> BoxFuture<'_, ()> {
        Box::pin(Source::close(self))
    }
}

impl Source for Box<dyn DynSource> {
    fn kind(&self) -> SourceKind {
        DynSource::kind(&**self)
    }

    async fn open(&mut self, token: Option<&[u8]>) -> Result<(), SourceError> {
        DynSource::open(&mut **self, token).await
    }

    async fn ready(&mut self) -> Result<(), SourceError> {
        DynSource::ready(&mut **self).await
    }

    async fn next(
        &mut self,
        tx: &mut PgConnection,
        limit: NonZeroUsize,
    ) -> Result<SourceBatch, SourceError> {
        DynSource::next(&mut **self, tx, limit).await
    }

    async fn ack(&mut self, token: &[u8]) -> Result<(), SourceError> {
        DynSource::ack(&mut **self, token).await
    }

    async fn keep_alive(&mut self) -> Result<(), SourceError> {
        DynSource::keep_alive(&mut **self).await
    }

    async fn close(&mut self) {
        DynSource::close(&mut **self).await;
    }
}

/// The name of a kind of [`Source`], [`Source::kind`]: 1 to 40 characters from `a-z`, `0-9` and `_`,
/// starting with a letter.
///
/// Write a literal with [`source_kind!`](crate::source_kind!), which checks it at compile time.
///
/// ```
/// use pges_sequencer::{source, source_kind};
///
/// const FAKE: source::SourceKind = source_kind!("fake");
///
/// assert_eq!(FAKE.as_str(), "fake");
/// assert_eq!(source::SourceKind::new("Fake"), None);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct SourceKind(&'static str);

impl SourceKind {
    /// The longest kind name: 40 characters.
    pub const MAX_LEN: usize = 40;

    /// `name`, or `None` if it does not match `^[a-z][a-z0-9_]{0,39}$`.
    pub const fn new(name: &'static str) -> Option<Self> {
        let bytes = name.as_bytes();
        if bytes.is_empty() || bytes.len() > Self::MAX_LEN || !bytes[0].is_ascii_lowercase() {
            return None;
        }

        let mut at = 1;
        while at < bytes.len() {
            if !matches!(bytes[at], b'a'..=b'z' | b'0'..=b'9' | b'_') {
                return None;
            }
            at += 1;
        }

        Some(Self(name))
    }

    /// The name.
    pub const fn as_str(self) -> &'static str {
        self.0
    }
}

impl fmt::Display for SourceKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.0)
    }
}

impl AsRef<str> for SourceKind {
    fn as_ref(&self) -> &str {
        self.0
    }
}

/// A [`SourceKind`](crate::source::SourceKind) from a literal, checked at compile time.
///
/// The macro expands to a block with a `const` item that checks the name, so an invalid name is a compile
/// error (`cargo check` reports it too) and never a run-time panic. The argument must be a constant.
///
/// ```
/// use pges_sequencer::source_kind;
///
/// assert_eq!(source_kind!("replaying").as_str(), "replaying");
/// ```
///
/// ```compile_fail
/// let kind = pges_sequencer::source_kind!("Not a kind");
/// ```
#[macro_export]
macro_rules! source_kind {
    ($name:expr $(,)?) => {{
        const KIND: $crate::source::SourceKind = match $crate::source::SourceKind::new($name) {
            ::core::option::Option::Some(kind) => kind,
            #[allow(clippy::panic)]
            ::core::option::Option::None => {
                ::core::panic!("invalid source kind: it must match ^[a-z][a-z0-9_]{{0,39}}$")
            }
        };

        KIND
    }};
}

/// Commits from one [`Source::next`] call, in source order.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct SourceBatch {
    /// The commits, in the order they get positions.
    pub commits: Vec<CommitRef>,
    /// Resumes after the last commit. Empty for a source that needs no token (the outbox).
    pub token: Vec<u8>,
}

impl SourceBatch {
    /// A batch.
    pub fn new(commits: Vec<CommitRef>, token: Vec<u8>) -> Self {
        Self { commits, token }
    }
}

/// One committed append, as a source sees it: enough to write its log row.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub struct CommitRef {
    /// The store it was appended to.
    pub store: store::StoreId,
    /// The commit.
    pub commit_id: commit::CommitId,
    /// The category of its stream.
    pub category: category::CategoryId,
    /// How many events it holds; it takes that many positions.
    pub event_count: commit::EventCount,
}

impl CommitRef {
    /// A commit reference.
    pub fn new(
        store: store::StoreId,
        commit_id: commit::CommitId,
        category: category::CategoryId,
        event_count: commit::EventCount,
    ) -> Self {
        Self {
            store,
            commit_id,
            category,
            event_count,
        }
    }
}

/// Why a source failed.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum SourceError {
    /// A database error. The sequencer retries after a pause, reopening the source.
    #[error(transparent)]
    Database(#[from] pges_postgres::error::DbError),
    /// A failure that may pass, such as a lost connection to a broker. The sequencer retries after a
    /// pause, reopening the source.
    #[error("source failed: {0}")]
    Transient(BoxError),
    /// A failure that needs an operator, such as a missing grant. The sequencer stops.
    #[error("source failed permanently: {0}")]
    Fatal(BoxError),
    /// A failure after which sequencing must not resume by itself, such as a lost replication slot: commits
    /// may have been skipped. The sequencer records the error in `pges.sequencer.halted`, releases the lease
    /// and stops; every sequencer then refuses to start ([`RunError::Halted`](crate::sequencer::RunError))
    /// until an operator recovers and clears it.
    #[error("sequencing halted: {0}")]
    Halt(BoxError),
    /// The source delivered something that cannot be sequenced, such as a malformed row. The sequencer
    /// stops.
    #[error("source delivered an invalid commit: {0}")]
    Invalid(String),
}

impl From<sqlx::Error> for SourceError {
    fn from(error: sqlx::Error) -> Self {
        Self::Database(error.into())
    }
}
