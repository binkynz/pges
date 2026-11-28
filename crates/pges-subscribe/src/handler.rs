//! What a subscription hands its batches to: [`PgHandler`] for exactly-once projections in the same
//! database, [`Handler`] for at-least-once delivery to anything else.

use std::{error, future::Future};

use futures_util::future::BoxFuture;
use pges_postgres::sql::template;
use sqlx::{
    Connection, Executor as _, PgConnection, Postgres, Transaction,
    postgres::{PgDatabaseError, PgSeverity},
};

use crate::{batch, sql::statement};

/// A boxed error from a handler.
pub type BoxError = Box<dyn error::Error + Send + Sync + 'static>;

/// A projection that writes to the store's database.
///
/// [`PgHandler::handle`] runs inside a transaction that the subscription opened; after it returns `Ok`, the
/// subscription moves its checkpoint in the same transaction and commits. The read model's writes and the
/// cursor therefore commit together or not at all: a batch's effect is applied exactly once, also across
/// crashes and lease takeovers. Write only through `tx`, and do not commit or roll it back.
///
/// A raw `COMMIT` or `ROLLBACK` sent through `tx` ends the transaction behind sqlx's back. The subscription
/// finds out whatever the handler returns: the checkpoint then runs outside the batch's transaction and moves
/// nothing, and on an error or an aborted transaction a probe finds the batch's savepoint gone. The batch
/// fails with a fatal [`HandlerError`], also when the handler returned a transient one: the subscription or
/// backfill stops. The handler's writes before a `COMMIT` stay committed, so retrying would apply them
/// twice, and the batch's effect may be applied twice when it runs again after a fix.
///
/// If the handler returns an error, or leaves the transaction aborted (a failed statement whose error it
/// ignored), the transaction is rolled back and the batch is retried according to the subscription's
/// [`RetryPolicy`](crate::subscription::RetryPolicy). So is an error the server reports for the checkpoint
/// or the COMMIT, which rolled the batch back: it is transient (a lock or statement timeout, a serialization
/// failure, a read-only transaction during a failover), so a batch that can never commit still stops once
/// its attempts run out, except an integrity constraint violation or a data exception (SQLSTATE class 23 or
/// 22, such as a deferred unique constraint), which is fatal. An error locking the subscription's row before
/// the handler runs (such as a serialization failure under REPEATABLE READ) is classified the same way and
/// counts as one of the batch's attempts, although the handler was not called.
///
/// The transaction runs at the level [`PgHandler::isolation`] asks for. It is already under way when
/// `handle` is called, so `SET TRANSACTION ISOLATION LEVEL` to another level fails there (the same level
/// passes), and `SET TRANSACTION READ ONLY` passes but makes the checkpoint fail (SQLSTATE 25006), retried
/// as transient until the attempts run out.
///
/// The transaction holds a savepoint taken by its `BEGIN`, so the handler's statements run in a
/// subtransaction. Savepoints of its own (`tx.begin()`) are allowed, but a savepoint begin that fails (on an
/// aborted transaction) or is dropped before it completes makes sqlx roll back the whole transaction; the
/// handler's statements after it run in autocommit, and may have committed. The subscription sees the
/// transaction gone, moves no checkpoint, and fails the batch with a fatal error, as after a raw `COMMIT`.
pub trait PgHandler: Send {
    /// Applies one batch.
    fn handle(
        &mut self,
        tx: &mut PgConnection,
        batch: &batch::Batch,
    ) -> impl Future<Output = Result<(), HandlerError>> + Send;

    /// The isolation level of the batch's transaction. Default: [`Isolation::Default`].
    fn isolation(&self) -> Isolation {
        Isolation::Default
    }
}

/// The isolation level of a [`PgHandler`]'s transaction.
///
/// Any level keeps the batch exactly once. The subscription locks its own row in the transaction before
/// the handler runs, so a serialization failure comes only from the handler's own tables (or from the
/// COMMIT), and follows the subscription's [`RetryPolicy`](crate::subscription::RetryPolicy) like any
/// retryable error.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[non_exhaustive]
pub enum Isolation {
    /// The session's `default_transaction_isolation`: READ COMMITTED unless the role or the server says
    /// otherwise.
    #[default]
    Default,
    /// READ COMMITTED, whatever `default_transaction_isolation` says: each statement sees what committed
    /// before it began.
    ReadCommitted,
    /// REPEATABLE READ: one snapshot for the whole batch.
    RepeatableRead,
    /// SERIALIZABLE.
    Serializable,
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

    fn isolation(&self) -> Isolation;
}

impl<T: PgHandler> DynPgHandler for T {
    fn handle<'a>(
        &'a mut self,
        tx: &'a mut PgConnection,
        batch: &'a batch::Batch,
    ) -> BoxFuture<'a, Result<(), HandlerError>> {
        Box::pin(PgHandler::handle(self, tx, batch))
    }

    fn isolation(&self) -> Isolation {
        PgHandler::isolation(self)
    }
}

/// Begins a batch's transaction at `isolation`, with the savepoint [`ended_by_handler`] looks for.
pub(crate) async fn begin(
    conn: &mut PgConnection,
    isolation: Isolation,
) -> Result<Transaction<'_, Postgres>, sqlx::Error> {
    match isolation {
        Isolation::Default => conn.begin_with(statement::BEGIN.sql()).await,
        Isolation::ReadCommitted => conn.begin_with(statement::BEGIN_READ_COMMITTED.sql()).await,
        Isolation::RepeatableRead => {
            conn.begin_with(statement::BEGIN_REPEATABLE_READ.sql())
                .await
        }
        Isolation::Serializable => conn.begin_with(statement::BEGIN_SERIALIZABLE.sql()).await,
    }
}

/// Whether the handler ended the batch's transaction itself (a raw `COMMIT` or `ROLLBACK`): the savepoint
/// [`begin`] took is gone. Works in an aborted transaction too. Only for a failure path: it undoes the batch's
/// work back to the savepoint, and the caller rolls the batch back next.
pub(crate) async fn ended_by_handler(tx: &mut PgConnection) -> Result<bool, sqlx::Error> {
    match tx
        .execute(template::text(statement::ROLLBACK_TO_BATCH.sql()))
        .await
    {
        Ok(_) => Ok(false),
        // No transaction block (25P01), or one without the savepoint (3B001). A FATAL error or a lost
        // connection tells nothing.
        Err(error)
            if error
                .as_database_error()
                .and_then(|error| error.try_downcast_ref::<PgDatabaseError>())
                .is_some_and(|error| matches!(error.severity(), PgSeverity::Error)) =>
        {
            Ok(true)
        }
        Err(error) => Err(error),
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
