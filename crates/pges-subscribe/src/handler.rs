//! What a subscription hands its batches to: [`PgHandler`] for exactly-once projections in the same
//! database, [`Handler`] for at-least-once delivery to anything else.

use std::{error, future::Future};

use futures_util::future::BoxFuture;
use sqlx::PgConnection;

use crate::batch;

/// A boxed error from a handler.
pub type BoxError = Box<dyn error::Error + Send + Sync + 'static>;

/// A projection that writes to the store's database.
///
/// [`PgHandler::handle`] runs inside a transaction that the subscription opened; after it returns `Ok`, the
/// subscription moves its checkpoint in the same transaction and commits. The read model's writes and the
/// cursor therefore commit together or not at all: a batch's effect is applied exactly once, also across
/// crashes and lease takeovers. Write only through `tx`, and do not commit or roll it back.
///
/// If the handler returns an error, or leaves the transaction aborted (a failed statement whose error it
/// ignored), the transaction is rolled back and the batch is retried according to the subscription's
/// [`RetryPolicy`](crate::subscription::RetryPolicy). So is an error the server reports for the checkpoint
/// or the COMMIT, which rolled the batch back: it is transient (a lock or statement timeout, a serialization
/// failure, a read-only transaction during a failover), so a batch that can never commit still stops once
/// its attempts run out, except an integrity constraint violation or a data exception (SQLSTATE class 23 or
/// 22, such as a deferred unique constraint), which is fatal.
///
/// Keep the transaction at READ COMMITTED, the default. Under REPEATABLE READ or SERIALIZABLE, a lease
/// renewal during a batch slower than half the lease changes the subscription's row after the handler's
/// snapshot, and the checkpoint fails with a serialization failure on every attempt.
///
/// Savepoints (`tx.begin()`) are allowed, but a savepoint begin that fails (on an aborted transaction) or is
/// dropped before it completes makes sqlx roll back the whole transaction; the handler's statements after it
/// run in autocommit. The subscription sees the transaction gone, moves no checkpoint, and retries the batch
/// like an aborted one.
pub trait PgHandler: Send {
    /// Applies one batch.
    fn handle(
        &mut self,
        tx: &mut PgConnection,
        batch: &batch::Batch,
    ) -> impl Future<Output = Result<(), HandlerError>> + Send;
}

/// A subscriber that writes somewhere else: a search index, a broker, another service.
///
/// The checkpoint is written after [`Handler::handle`] returns `Ok`, in its own transaction. A crash or a
/// lost lease in between delivers the batch again, so delivery is at least once: make the effect idempotent
/// (for example upserts keyed by stream id that skip older versions).
pub trait Handler: Send {
    /// Applies one batch.
    fn handle(
        &mut self,
        batch: &batch::Batch,
    ) -> impl Future<Output = Result<(), HandlerError>> + Send;
}

/// Why a handler could not apply a batch.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum HandlerError {
    /// Worth retrying: a timeout, a deadlock, an unavailable sink. Retried according to the subscription's
    /// [`RetryPolicy`](crate::subscription::RetryPolicy); running out of attempts stops the subscription.
    #[error("transient handler error: {0}")]
    Transient(BoxError),
    /// Retrying cannot help. Stops the subscription.
    #[error("fatal handler error: {0}")]
    Fatal(BoxError),
}

impl From<batch::DecodeError> for HandlerError {
    fn from(error: batch::DecodeError) -> Self {
        Self::Fatal(Box::new(error))
    }
}

/// A database error is transient when retrying may succeed ([`pges_postgres::error::DbError::is_retryable`]), otherwise fatal.
impl From<sqlx::Error> for HandlerError {
    fn from(error: sqlx::Error) -> Self {
        let error = pges_postgres::error::DbError::from(error);
        if error.is_retryable() {
            Self::Transient(Box::new(error))
        } else {
            Self::Fatal(Box::new(error))
        }
    }
}

/// The object-safe twin of [`PgHandler`], so a subscription is not generic over its handler.
pub(crate) trait DynPgHandler: Send {
    fn handle<'a>(
        &'a mut self,
        tx: &'a mut PgConnection,
        batch: &'a batch::Batch,
    ) -> BoxFuture<'a, Result<(), HandlerError>>;
}

impl<T: PgHandler> DynPgHandler for T {
    fn handle<'a>(
        &'a mut self,
        tx: &'a mut PgConnection,
        batch: &'a batch::Batch,
    ) -> BoxFuture<'a, Result<(), HandlerError>> {
        Box::pin(PgHandler::handle(self, tx, batch))
    }
}

/// The object-safe twin of [`Handler`].
pub(crate) trait DynHandler: Send {
    fn handle<'a>(&'a mut self, batch: &'a batch::Batch)
    -> BoxFuture<'a, Result<(), HandlerError>>;
}

impl<T: Handler> DynHandler for T {
    fn handle<'a>(
        &'a mut self,
        batch: &'a batch::Batch,
    ) -> BoxFuture<'a, Result<(), HandlerError>> {
        Box::pin(Handler::handle(self, batch))
    }
}
