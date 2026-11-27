//! The database error shared by every operation's error enum.

use std::{error, fmt};

use sqlx::postgres::PgDatabaseError;

/// A database or connection error. The driver error is available through [`error::Error::source`], without
/// naming its type.
#[derive(Debug)]
pub struct DbError {
    source: sqlx::Error,
}

impl DbError {
    /// Whether running the operation again may succeed: a serialization failure, a deadlock, a lost
    /// connection or a server shutting down.
    pub fn is_retryable(&self) -> bool {
        if is_connection_loss(&self.source) {
            return true;
        }

        match sqlstate(&self.source).as_deref() {
            Some(code) => {
                matches!(code, "40001" | "40P01" | "57P01" | "57P02" | "57P03")
                    || code.starts_with("08")
            }
            None => false,
        }
    }

    /// The SQLSTATE code the server reported, if the error came from the server.
    pub fn code(&self) -> Option<String> {
        sqlstate(&self.source)
    }

    pub(crate) fn is_connection_loss(&self) -> bool {
        is_connection_loss(&self.source)
    }

    /// The partitioned table of `schema` that found no partition for a row, if that is the error: PostgreSQL
    /// reports it as a check violation (23514) that names the table and no constraint.
    pub(crate) fn unrouted_table(&self, schema: &str) -> Option<&str> {
        let error = self
            .source
            .as_database_error()?
            .try_downcast_ref::<PgDatabaseError>()?;

        (error.code() == CHECK_VIOLATION
            && error.constraint().is_none()
            && error.schema() == Some(schema))
        .then(|| error.table())
        .flatten()
    }

    /// The value of the one-column partition key `column` that no partition took, from the detail of the
    /// routing error: `Partition key of the failing row contains (category_id) = (3).` The sentence is
    /// translated with `lc_messages` and the key text is not, so only `(<column>) = (<value>)` is read.
    /// PostgreSQL leaves the detail out when the role may not read the key column or the table has row-level
    /// security.
    pub(crate) fn unrouted_key(&self, column: &str) -> Option<&str> {
        let detail = self
            .source
            .as_database_error()?
            .try_downcast_ref::<PgDatabaseError>()?
            .detail()?;
        let marker = format!("({column}) = (");
        let start = detail.find(&marker)? + marker.len();
        let value = &detail[start..];

        value.find(')').map(|end| &value[..end])
    }
}

/// SQLSTATE 23514: a row fails a CHECK constraint, or no partition takes it.
const CHECK_VIOLATION: &str = "23514";

impl fmt::Display for DbError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "database error: {}", self.source)
    }
}

impl error::Error for DbError {
    fn source(&self) -> Option<&(dyn error::Error + 'static)> {
        Some(&self.source)
    }
}

impl From<sqlx::Error> for DbError {
    fn from(source: sqlx::Error) -> Self {
        Self { source }
    }
}

pub(crate) fn sqlstate(error: &sqlx::Error) -> Option<String> {
    error
        .as_database_error()
        .and_then(|e| e.code())
        .map(|code| code.into_owned())
}

pub(crate) fn is_connection_loss(error: &sqlx::Error) -> bool {
    matches!(
        error,
        sqlx::Error::Io(_)
            | sqlx::Error::Tls(_)
            | sqlx::Error::Protocol(_)
            | sqlx::Error::WorkerCrashed
    )
}

/// The name of the constraint a database error names, if any.
pub(crate) fn constraint(error: &sqlx::Error) -> Option<&str> {
    error.as_database_error().and_then(|e| e.constraint())
}
