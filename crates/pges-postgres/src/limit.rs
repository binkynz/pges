//! A store's writer limits: how far a commit id may be from the database clock (`max_clock_skew`) and how
//! long a writer transaction may stay open (`max_txn_age`).
//!
//! Each store records both in `pges.stores` when it is created ([`store::StoreDefinition::max_clock_skew`],
//! [`store::StoreDefinition::max_txn_age`]; defaults [`pges_core::store::DEFAULT_MAX_CLOCK_SKEW`] and
//! [`pges_core::store::DEFAULT_MAX_TXN_AGE`]). Retention and the sequencer's `resequence` read them from
//! there, so they cannot disagree with each other; [`store::Store::open`] refuses a writer whose own skew is
//! larger than the store's. [`check_writer_roles`] checks that the writer roles really cap their
//! transactions at `max_txn_age`.

use std::time::Duration;

use sqlx::PgConnection;

use pges_core::{interval, non_empty, store as core_store};

use crate::{
    admin, error,
    sql::{statement, typed::Query as _},
    store,
};

impl admin::Admin {
    /// Raises the store's writer limits.
    ///
    /// Raise `max_clock_skew` before opening writers with a larger [`store::StoreConfig::max_clock_skew`],
    /// and `max_txn_age` before lengthening the writer roles' `transaction_timeout`. Retention uses the new
    /// values from its next run; it holds the same maintenance lock, so a run in progress finishes with the
    /// old ones first.
    ///
    /// Neither limit can be lowered: commits already written under the larger value stay in the log, out of
    /// order by up to that much, and retention and `resequence` would assume less. Fails with
    /// [`SetWriterLimitsError::Lowering`] if either value is below the recorded one, and with
    /// [`SetWriterLimitsError::StoreNotFound`] if the store does not exist. Nothing changes when it fails.
    pub async fn set_writer_limits(
        &self,
        limits: WriterLimits,
    ) -> Result<(), SetWriterLimitsError> {
        let mut conn = self.session().await?;
        let result = raise_writer_limits(&mut conn, self.store_name(), limits).await;

        self.end_session(conn).await;

        result
    }
}

/// A store's writer limits, as [`admin::Admin::set_writer_limits`] records them.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub struct WriterLimits {
    /// The furthest a commit id's timestamp may be from the database clock.
    pub max_clock_skew: Duration,
    /// The longest a writer transaction stays open.
    pub max_txn_age: interval::Interval,
}

impl WriterLimits {
    /// The limits `max_clock_skew` and `max_txn_age`, such as `pges_core::interval!(1 h)`.
    pub fn new(max_clock_skew: Duration, max_txn_age: interval::Interval) -> Self {
        Self {
            max_clock_skew,
            max_txn_age,
        }
    }
}

impl Default for WriterLimits {
    /// [`core_store::DEFAULT_MAX_CLOCK_SKEW`] and [`core_store::DEFAULT_MAX_TXN_AGE`].
    fn default() -> Self {
        Self::new(
            core_store::DEFAULT_MAX_CLOCK_SKEW,
            pges_core::interval!(core_store::DEFAULT_MAX_TXN_AGE),
        )
    }
}

/// The roles that append: at least one.
///
/// Whether each role exists, and caps its transactions, is checked against the database by
/// [`check_writer_roles`].
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct WriterRoles(non_empty::NonEmpty<String>);

impl WriterRoles {
    /// The roles `first` and `rest`.
    pub fn new(first: impl Into<String>, rest: impl IntoIterator<Item: Into<String>>) -> Self {
        Self(non_empty::NonEmpty::new(
            first.into(),
            rest.into_iter().map(Into::into),
        ))
    }

    /// The one role `role`.
    pub fn one(role: impl Into<String>) -> Self {
        Self(non_empty::NonEmpty::one(role.into()))
    }

    /// The roles, in the order given.
    pub fn iter(&self) -> impl Iterator<Item = &str> {
        self.0.iter().map(String::as_str)
    }
}

impl From<non_empty::NonEmpty<String>> for WriterRoles {
    fn from(roles: non_empty::NonEmpty<String>) -> Self {
        Self(roles)
    }
}

impl TryFrom<Vec<String>> for WriterRoles {
    type Error = non_empty::EmptyError;

    /// The roles of a list read at run time, such as a command line; fails if it is empty.
    fn try_from(roles: Vec<String>) -> Result<Self, Self::Error> {
        non_empty::NonEmpty::try_from(roles).map(Self)
    }
}

/// The store's writer limits could not be recorded.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum SetWriterLimitsError {
    /// No store with this name exists.
    #[error("store {0} does not exist")]
    StoreNotFound(core_store::StoreName),
    /// A limit is below the store's recorded one; limits are only ever raised.
    #[error(
        "writer limits can only be raised; the store records max_clock_skew {max_clock_skew:?} and \
         max_txn_age {max_txn_age:?}"
    )]
    Lowering {
        /// The store's recorded `max_clock_skew`, unchanged.
        max_clock_skew: Duration,
        /// The store's recorded `max_txn_age`, unchanged.
        max_txn_age: Duration,
    },
    /// A database error.
    #[error(transparent)]
    Database(#[from] error::DbError),
}

impl From<sqlx::Error> for SetWriterLimitsError {
    fn from(source: sqlx::Error) -> Self {
        Self::Database(source.into())
    }
}

/// Checks that every role in `writer_roles` caps its transactions at `max_txn_age`: it has a
/// `transaction_timeout` other than 0 and at most `max_txn_age`, set with `ALTER ROLE … SET` (optionally
/// `IN DATABASE`) or `ALTER DATABASE … SET`. Settings are read in PostgreSQL's order of precedence: role in
/// database, role, database, every role.
///
/// Retention and the sequencer's `resequence` run it first. A value set only in `postgresql.conf`, or by a
/// session with `SET`, is not seen. `max_txn_age` is the store's, as recorded in the database.
pub async fn check_writer_roles(
    conn: &mut PgConnection,
    writer_roles: &WriterRoles,
    max_txn_age: Duration,
) -> Result<(), WriterRoleError> {
    for role in writer_roles.iter() {
        let statement::writer_timeout::Row { found, setting } =
            statement::writer_timeout::Query::fixed()
                .bind(statement::writer_timeout::Params { role })
                .fetch_one(&mut *conn)
                .await?;
        if !found {
            return Err(WriterRoleError::UnknownRole(role.to_owned()));
        }
        let Some(setting) = setting else {
            return Err(WriterRoleError::TransactionTimeoutNotSet {
                role: role.to_owned(),
            });
        };
        let Some(timeout) = parse_milliseconds_setting(&setting) else {
            return Err(WriterRoleError::InvalidTransactionTimeout {
                role: role.to_owned(),
                value: setting,
            });
        };
        if timeout.is_zero() {
            return Err(WriterRoleError::TransactionTimeoutNotSet {
                role: role.to_owned(),
            });
        }
        if timeout > max_txn_age {
            return Err(WriterRoleError::TransactionTimeoutTooLong {
                role: role.to_owned(),
                transaction_timeout: timeout,
                max_txn_age,
            });
        }
    }

    Ok(())
}

/// A writer role does not cap its transactions at the store's `max_txn_age`.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum WriterRoleError {
    /// A writer role does not exist.
    #[error("role {0:?} does not exist")]
    UnknownRole(String),
    /// A writer role has no `transaction_timeout`, or it is 0 (no limit).
    #[error("role {role:?} has no transaction_timeout; set one of at most max_txn_age")]
    TransactionTimeoutNotSet {
        /// The role.
        role: String,
    },
    /// A writer role's `transaction_timeout` is longer than the store's `max_txn_age`.
    #[error(
        "role {role:?} has transaction_timeout {transaction_timeout:?}, longer than max_txn_age {max_txn_age:?}"
    )]
    TransactionTimeoutTooLong {
        /// The role.
        role: String,
        /// Its `transaction_timeout`.
        transaction_timeout: Duration,
        /// The store's `max_txn_age`.
        max_txn_age: Duration,
    },
    /// A role's `transaction_timeout` setting could not be read.
    #[error("role {role:?} has a transaction_timeout this crate cannot read: {value:?}")]
    InvalidTransactionTimeout {
        /// The role.
        role: String,
        /// The setting as stored.
        value: String,
    },
    /// A database error.
    #[error(transparent)]
    Database(#[from] error::DbError),
}

impl From<sqlx::Error> for WriterRoleError {
    fn from(source: sqlx::Error) -> Self {
        Self::Database(source.into())
    }
}

/// Records `limits` unless either is below the recorded value. Runs under the maintenance lock, the only
/// other writer of these columns being store creation.
async fn raise_writer_limits(
    conn: &mut PgConnection,
    store: &core_store::StoreName,
    limits: WriterLimits,
) -> Result<(), SetWriterLimitsError> {
    let skew = store::interval(limits.max_clock_skew);
    let txn_age = store::interval(limits.max_txn_age.get());
    let raised = statement::raise_writer_limits::Query::fixed()
        .bind(statement::raise_writer_limits::Params {
            store: store.as_str(),
            max_clock_skew: skew,
            max_txn_age: txn_age,
        })
        .execute(&mut *conn)
        .await?;
    if raised.rows_affected() > 0 {
        return Ok(());
    }

    let recorded = statement::writer_limits::Query::fixed()
        .bind(statement::writer_limits::Params {
            store: store.as_str(),
        })
        .fetch_optional(&mut *conn)
        .await?;
    match recorded {
        None => Err(SetWriterLimitsError::StoreNotFound(store.clone())),
        Some(recorded) => Err(SetWriterLimitsError::Lowering {
            max_clock_skew: store::duration(recorded.max_clock_skew),
            max_txn_age: store::duration(recorded.max_txn_age),
        }),
    }
}

/// A time setting whose unit defaults to milliseconds, as PostgreSQL parses it: a number, optionally
/// fractional, then optionally `us`, `ms`, `s`, `min`, `h` or `d`. Quotes are stripped.
fn parse_milliseconds_setting(value: &str) -> Option<Duration> {
    let value = value.trim().trim_matches(|c| c == '\'' || c == '"').trim();
    let split = value
        .find(|c: char| !(c.is_ascii_digit() || c == '.' || c == '+'))
        .unwrap_or(value.len());
    let (number, unit) = value.split_at(split);
    let number: f64 = number.parse().ok()?;
    let millis_per_unit = match unit.trim() {
        "us" => 0.001,
        "" | "ms" => 1.0,
        "s" => 1_000.0,
        "min" => 60_000.0,
        "h" => 3_600_000.0,
        "d" => 86_400_000.0,
        _ => return None,
    };

    Duration::try_from_secs_f64(number * millis_per_unit / 1_000.0).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn writer_roles_are_never_empty() {
        assert_eq!(
            WriterRoles::try_from(Vec::new()),
            Err(non_empty::EmptyError)
        );

        let roles = WriterRoles::try_from(vec!["app".to_owned(), "jobs".to_owned()]).unwrap();
        assert_eq!(roles.iter().collect::<Vec<_>>(), ["app", "jobs"]);
        assert_eq!(roles, WriterRoles::new("app", ["jobs"]));
    }

    #[test]
    fn parses_time_settings_like_postgresql() {
        let ms = Duration::from_millis;
        assert_eq!(parse_milliseconds_setting("60000"), Some(ms(60_000)));
        assert_eq!(parse_milliseconds_setting("500ms"), Some(ms(500)));
        assert_eq!(parse_milliseconds_setting("30s"), Some(ms(30_000)));
        assert_eq!(parse_milliseconds_setting("5min"), Some(ms(300_000)));
        assert_eq!(parse_milliseconds_setting("'5 min'"), Some(ms(300_000)));
        assert_eq!(parse_milliseconds_setting("1.5h"), Some(ms(5_400_000)));
        assert_eq!(parse_milliseconds_setting("1d"), Some(ms(86_400_000)));
        assert_eq!(parse_milliseconds_setting("1000us"), Some(ms(1)));
        assert_eq!(parse_milliseconds_setting("0"), Some(Duration::ZERO));
        assert_eq!(parse_milliseconds_setting("5 fortnights"), None);
        assert_eq!(parse_milliseconds_setting("-1"), None);
        assert_eq!(parse_milliseconds_setting(""), None);
    }
}
