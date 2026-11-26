//! The database error shared by every operation's error enum.

use std::{error, fmt};

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
}

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
