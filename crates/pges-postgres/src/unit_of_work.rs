//! Units of work: appends to several streams and the caller's own writes in one transaction.

use sqlx::{Connection, Executor, PgConnection, Postgres, Transaction};

use pges_core::{event, stream, version};

use crate::{append, error, sql, store};

/// One transaction holding appends to several streams and any other writes made through
/// [`UnitOfWork::conn`]. Nothing is visible to others until [`UnitOfWork::commit`]; dropping it without
/// committing rolls back. [`UnitOfWork::wrap`] gives a [`WrappedUnitOfWork`] in a transaction the caller
/// owns instead.
///
/// Separate [`UnitOfWork::append`] calls lock streams in call order, so two units of work appending to the
/// same streams in different orders can deadlock; the loser fails with a retryable database error.
/// [`UnitOfWork::append_many`] locks in stream id order.
///
/// After an append fails in a way that aborts or partly writes the transaction (a database error, a
/// duplicate or skewed commit id, or any [`UnitOfWork::append_many`] failure), or a statement of the caller's
/// through [`UnitOfWork::conn`] fails, [`UnitOfWork::commit`] rolls back and fails with
/// [`TransactionError::Aborted`]. PostgreSQL itself would turn that COMMIT into a ROLLBACK and report
/// success. Conflicts from [`UnitOfWork::append`] write nothing and leave the unit of work usable.
///
/// Savepoints on [`UnitOfWork::conn`] (`conn.begin()`) are allowed, but a savepoint begin that fails (on an
/// aborted transaction) or is dropped before it completes makes sqlx roll back the whole transaction. The
/// unit of work then fails: later appends with [`append::AppendError::TransactionEnded`] and `commit` with
/// [`TransactionError::Aborted`]. The caller's own statements after it run in autocommit.
///
/// The first append of an event type the store has not registered yet registers it on a second pooled
/// connection, committed on its own (see [`store::Store::register`]): the type ids must be committed before a
/// payload refers to them. A unit of work started by [`store::Store::begin`] on a pool of size 1 cannot get
/// that connection and fails with [`append::AppendError::NoSpareConnection`]; with a larger pool that is
/// exhausted, the append waits for the pool's acquire timeout. Call [`store::Store::register`] for every event
/// type up front to avoid both.
pub struct UnitOfWork<'a> {
    work: Work<'a, Transaction<'static, Postgres>>,
}

impl<'a> UnitOfWork<'a> {
    /// A unit of work inside a transaction the caller already owns, such as a `sqlx::Transaction`.
    ///
    /// The caller commits or rolls back its own transaction; [`WrappedUnitOfWork::finish`] only checks that
    /// it is still usable. If `conn` is inside a transaction here and no longer is later (a savepoint begin
    /// on it failed or was dropped, see [`UnitOfWork`]), appends and `finish` fail.
    pub fn wrap(store: &'a store::Store, conn: &'a mut PgConnection) -> WrappedUnitOfWork<'a> {
        let outer = Outer::of(conn);

        WrappedUnitOfWork {
            work: Work {
                store,
                conn,
                lease: append::PoolLease::Unknown,
                state: State::Open,
                caller: CallerStatements::Possible,
                outer,
            },
        }
    }

    /// The transaction's connection, for the caller's own statements.
    ///
    /// If one of them fails, the transaction is aborted and [`UnitOfWork::commit`] fails with
    /// [`TransactionError::Aborted`]. Savepoints are allowed; a failed or dropped savepoint begin ends the
    /// transaction (see [`UnitOfWork`]).
    pub fn conn(&mut self) -> &mut PgConnection {
        self.work.conn()
    }

    /// Appends `events` to one stream inside the transaction. See [`store::Store::append`].
    pub fn append<'b, E: event::Event>(
        &'b mut self,
        stream: impl Into<stream::StreamRef>,
        expected: version::ExpectedVersion,
        events: &'b [E],
    ) -> append::Append<'b, E> {
        self.work.append(stream.into(), expected, events)
    }

    /// Appends to several streams in one statement inside the transaction. See
    /// [`store::Store::append_many`].
    pub fn append_many<'b>(
        &'b mut self,
        entries: impl IntoIterator<Item = append::AppendEntry<'b>>,
    ) -> append::AppendMany<'b> {
        self.work.append_many(entries.into_iter().collect())
    }

    /// Commits the transaction.
    ///
    /// Fails with [`TransactionError::Aborted`], after rolling back, if a failed append or a failed statement
    /// of the caller's aborted the transaction. If that rollback fails too, the error is still `Aborted`.
    pub async fn commit(mut self) -> Result<(), TransactionError> {
        if let Err(error) = self.work.check().await {
            if let TransactionError::Aborted = error {
                // The transaction is lost either way: a failed rollback (for example on a lost connection)
                // must not hide that it was aborted.
                let _ = self.work.conn.rollback().await;
            }
            return Err(error);
        }

        self.work.conn.commit().await?;

        Ok(())
    }

    /// Rolls the transaction back.
    pub async fn rollback(self) -> Result<(), TransactionError> {
        self.work.conn.rollback().await?;

        Ok(())
    }

    pub(crate) async fn begin(store: &'a store::Store) -> Result<Self, TransactionError> {
        let tx = store.inner.pool.begin().await?;

        Ok(Self {
            work: Work {
                store,
                conn: tx,
                lease: append::PoolLease::Held,
                state: State::Open,
                caller: CallerStatements::None,
                outer: Outer::InTransaction,
            },
        })
    }
}

impl std::fmt::Debug for UnitOfWork<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.work.debug("UnitOfWork", f)
    }
}

/// A unit of work inside a transaction the caller owns, from [`UnitOfWork::wrap`].
///
/// It has no `commit` or `rollback`: the caller ends its own transaction. Call
/// [`WrappedUnitOfWork::finish`] before committing it, to learn whether an append aborted it. The other rules
/// of [`UnitOfWork`] apply; an append that registers an event type waits for a second pooled connection
/// whatever the pool size.
pub struct WrappedUnitOfWork<'a> {
    work: Work<'a, &'a mut PgConnection>,
}

impl WrappedUnitOfWork<'_> {
    /// The transaction's connection, for the caller's own statements.
    ///
    /// If one of them fails, the transaction is aborted and [`WrappedUnitOfWork::finish`] fails with
    /// [`TransactionError::Aborted`]. Savepoints are allowed; a failed or dropped savepoint begin ends the
    /// transaction (see [`UnitOfWork`]).
    pub fn conn(&mut self) -> &mut PgConnection {
        self.work.conn()
    }

    /// Appends `events` to one stream inside the transaction. See [`store::Store::append`].
    pub fn append<'b, E: event::Event>(
        &'b mut self,
        stream: impl Into<stream::StreamRef>,
        expected: version::ExpectedVersion,
        events: &'b [E],
    ) -> append::Append<'b, E> {
        self.work.append(stream.into(), expected, events)
    }

    /// Appends to several streams in one statement inside the transaction. See
    /// [`store::Store::append_many`].
    pub fn append_many<'b>(
        &'b mut self,
        entries: impl IntoIterator<Item = append::AppendEntry<'b>>,
    ) -> append::AppendMany<'b> {
        self.work.append_many(entries.into_iter().collect())
    }

    /// Checks that the transaction is still usable, and gives the connection back to the caller, who commits
    /// or rolls back. Ends nothing itself.
    ///
    /// Fails with [`TransactionError::Aborted`] if a failed append or a failed statement of the caller's
    /// aborted the transaction, or a failed or dropped savepoint begin ended it; roll it back then.
    pub async fn finish(mut self) -> Result<(), TransactionError> {
        self.work.check().await
    }
}

impl std::fmt::Debug for WrappedUnitOfWork<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.work.debug("WrappedUnitOfWork", f)
    }
}

/// A unit of work's transaction could not be started or ended.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum TransactionError {
    /// A failed append or a failed statement of the caller's aborted or partly wrote the transaction, or a
    /// failed or dropped savepoint begin ended it; it was rolled back.
    #[error("the unit of work was aborted by a failed statement and rolled back")]
    Aborted,
    /// A database error.
    #[error(transparent)]
    Database(#[from] error::DbError),
}

impl From<sqlx::Error> for TransactionError {
    fn from(source: sqlx::Error) -> Self {
        Self::Database(source.into())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum State {
    Open,
    Failed,
    /// The transaction ended under the unit of work; statements on its connection would run in autocommit.
    Ended,
}

/// Whether the connection was inside a transaction when the unit of work began.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Outer {
    InTransaction,
    Autocommit,
}

impl Outer {
    fn of(conn: &PgConnection) -> Self {
        if conn.is_in_transaction() {
            Self::InTransaction
        } else {
            Self::Autocommit
        }
    }
}

/// Whether statements other than the unit of work's own appends may have run in the transaction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CallerStatements {
    None,
    Possible,
}

/// SQLSTATE 25P02: the transaction is aborted and ignores commands until its end.
const IN_FAILED_TRANSACTION: &str = "25P02";

/// What both kinds of unit of work share. `C` is the transaction [`UnitOfWork`] owns, or the caller's
/// connection a [`WrappedUnitOfWork`] borrows.
struct Work<'a, C> {
    store: &'a store::Store,
    conn: C,
    /// Whether `conn` is known to come from the store's pool.
    lease: append::PoolLease,
    state: State,
    caller: CallerStatements,
    outer: Outer,
}

impl<C: std::ops::DerefMut<Target = PgConnection>> Work<'_, C> {
    fn conn(&mut self) -> &mut PgConnection {
        self.caller = CallerStatements::Possible;

        self.parts().1
    }

    fn append<'b, E: event::Event>(
        &'b mut self,
        stream: stream::StreamRef,
        expected: version::ExpectedVersion,
        events: &'b [E],
    ) -> append::Append<'b, E> {
        let lease = self.lease;
        self.check_ended();
        let (store, conn, state) = self.parts();

        append::Append::new(
            append::Target::Connection {
                store,
                conn,
                state: Some(state),
                lease,
            },
            stream,
            expected,
            events,
        )
    }

    fn append_many<'b>(
        &'b mut self,
        entries: Vec<append::AppendEntry<'b>>,
    ) -> append::AppendMany<'b> {
        let lease = self.lease;
        self.check_ended();
        let (store, conn, state) = self.parts();

        append::AppendMany::new(
            append::Target::Connection {
                store,
                conn,
                state: Some(state),
                lease,
            },
            entries,
        )
    }

    /// Fails with [`TransactionError::Aborted`] if a statement aborted the transaction or it ended.
    async fn check(&mut self) -> Result<(), TransactionError> {
        self.check_ended();
        if self.state == State::Open && self.caller == CallerStatements::Possible {
            self.check_usable().await?;
        }
        if self.state != State::Open {
            return Err(TransactionError::Aborted);
        }

        Ok(())
    }

    /// Marks the unit of work ended if its transaction is gone. sqlx ends the whole transaction when a
    /// savepoint begin fails or is dropped; the depth it tracks shows it without a round trip.
    fn check_ended(&mut self) {
        if self.outer == Outer::InTransaction && !self.conn.is_in_transaction() {
            self.state = State::Ended;
        }
    }

    /// Marks the unit of work failed if a statement aborted its transaction. sqlx does not expose the
    /// transaction status the server reports, so this costs one round trip; it runs only when the caller may
    /// have used the connection.
    async fn check_usable(&mut self) -> Result<(), TransactionError> {
        let (_, conn, _) = self.parts();

        match conn.execute(sql::statement::CHECK_TRANSACTION.sql()).await {
            Ok(_) => Ok(()),
            Err(error) if error::sqlstate(&error).as_deref() == Some(IN_FAILED_TRANSACTION) => {
                self.state = State::Failed;
                Ok(())
            }
            Err(error) => Err(error.into()),
        }
    }

    fn parts(&mut self) -> (&store::Store, &mut PgConnection, &mut State) {
        (self.store, &mut self.conn, &mut self.state)
    }

    fn debug(&self, name: &str, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct(name)
            .field("store", &self.store.name())
            .field("state", &self.state)
            .finish_non_exhaustive()
    }
}
