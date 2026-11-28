//! Store names and ids, and the defaults of the time limits every store records.

use std::{borrow::Cow, fmt, str::FromStr, time::Duration};

use crate::category;

/// The default of a store's `max_clock_skew`: how far a commit id's timestamp may be from the database
/// clock when it is appended. 5 seconds.
///
/// A store records its own value when it is created. Appends check commit ids against it, retention keeps
/// this margin before dropping a time leaf, and the logical source's `resequence` widens its search by it.
pub const DEFAULT_MAX_CLOCK_SKEW: Duration = Duration::from_secs(5);

/// The default of a store's `max_txn_age`: the longest a writer transaction stays open. 1 hour.
///
/// A store records its own value when it is created. Writer roles must cap their transactions at it
/// (`transaction_timeout`); retention and `resequence` refuse to run otherwise.
pub const DEFAULT_MAX_TXN_AGE: Duration = Duration::from_secs(3600);

/// The name of a store: the PostgreSQL schema that holds its tables, such as `ops`.
///
/// Names match `^[a-z][a-z0-9_]{0,39}$`, because they are interpolated into SQL unquoted, and are not a
/// schema PostgreSQL or pges reserves: `pges`, `public`, `information_schema`, or one starting with `pg_`.
/// For the same reason they are not an SQL keyword that cannot name a schema, such as `select` or `user`.
///
/// Write a literal name with [`store_name!`](crate::store_name), which checks it at compile time. Parse a
/// name read at run time with [`StoreName::parse`].
///
/// ```
/// use pges_core::store;
///
/// const OPS: store::StoreName = pges_core::store_name!("ops");
/// let configured = store::StoreName::parse("billing")?;
///
/// assert_eq!(OPS.as_str(), "ops");
/// assert_eq!(configured.as_str(), "billing");
/// assert_eq!(store::StoreName::parse("pg_ops"), Err(store::StoreNameError::Reserved));
/// # Ok::<(), store::StoreNameError>(())
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct StoreName(Cow<'static, str>);

impl StoreName {
    /// Longest allowed name, in bytes.
    pub const MAX_LEN: usize = 40;

    /// Validates a static name. It is a `const fn`; for a literal, [`store_name!`](crate::store_name) turns an
    /// invalid name into a compile error instead.
    pub const fn new(name: &'static str) -> Result<Self, StoreNameError> {
        match validate(name) {
            Ok(()) => Ok(Self(Cow::Borrowed(name))),
            Err(error) => Err(error),
        }
    }

    /// A name `validate` accepted.
    pub(crate) const fn from_validated(name: &'static str) -> Self {
        Self(Cow::Borrowed(name))
    }

    /// Validates a name read at run time, such as from configuration or the catalog.
    pub fn parse(name: impl Into<String>) -> Result<Self, StoreNameError> {
        let name = name.into();
        validate(&name)?;

        Ok(Self(Cow::Owned(name)))
    }

    /// The name.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for StoreName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl AsRef<str> for StoreName {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl FromStr for StoreName {
    type Err = StoreNameError;

    fn from_str(name: &str) -> Result<Self, Self::Err> {
        Self::parse(name)
    }
}

/// Why a store name is invalid.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum StoreNameError {
    /// The name is empty.
    #[error("store name is empty")]
    Empty,
    /// The name is longer than [`StoreName::MAX_LEN`] bytes.
    #[error("store name is longer than 40 bytes")]
    TooLong,
    /// The name does not start with `a`–`z`.
    #[error("store name must start with a lowercase ASCII letter")]
    InvalidStart,
    /// The name contains a byte other than `a`–`z`, `0`–`9` or `_`.
    #[error("store name may only contain a-z, 0-9 and _")]
    InvalidCharacter,
    /// The name is a schema PostgreSQL or pges reserves: `pges`, `public`, `information_schema`, or one
    /// starting with `pg_`.
    #[error("store name is reserved: pges, public, information_schema and pg_* are other schemas")]
    Reserved,
    /// The name is an SQL keyword that cannot name a schema without quotes, such as `select` or `user`.
    #[error("store name is a reserved SQL keyword")]
    Keyword,
}

/// A [`StoreName`](crate::store::StoreName) from a literal, checked at compile time.
///
/// The macro expands to a block with a `const` item that checks the name, so an invalid name is a compile
/// error (`cargo check` reports it too, also inside a generic function) and never a run-time panic.
///
/// ```
/// use pges_core::store;
///
/// const OPS: store::StoreName = pges_core::store_name!("ops");
/// let billing = pges_core::store_name!("billing");
///
/// assert_eq!(OPS.as_str(), "ops");
/// assert_eq!(billing.as_str(), "billing");
/// ```
///
/// ```compile_fail
/// let reserved = pges_core::store_name!("pg_catalog");
/// ```
#[macro_export]
macro_rules! store_name {
    ($name:expr $(,)?) => {{
        const NAME: $crate::store::StoreName = match $crate::__private::is_valid_store_name($name) {
            true => $crate::__private::store_name($name),
            #[allow(clippy::panic)]
            false => ::core::panic!(
                "invalid store name: it must match ^[a-z][a-z0-9_]{{0,39}}$ and not be pges, public, \
                 information_schema, a reserved SQL keyword, or start with pg_"
            ),
        };

        NAME
    }};
}

/// A store's id in `pges.stores`. Each store has its own log and its own positions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct StoreId(i32);

impl StoreId {
    /// Wraps an id read from the database.
    pub const fn new(id: i32) -> Self {
        Self(id)
    }

    /// The number.
    pub const fn get(self) -> i32 {
        self.0
    }
}

impl fmt::Display for StoreId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

const RESERVED: [&str; 3] = ["pges", "public", "information_schema"];

/// PostgreSQL 18's reserved keywords and those that cannot name a type or function (`pg_get_keywords()`
/// with `catcode` `R` or `T`): neither can name a schema unquoted.
const KEYWORDS: [&str; 101] = [
    "all",
    "analyse",
    "analyze",
    "and",
    "any",
    "array",
    "as",
    "asc",
    "asymmetric",
    "authorization",
    "binary",
    "both",
    "case",
    "cast",
    "check",
    "collate",
    "collation",
    "column",
    "concurrently",
    "constraint",
    "create",
    "cross",
    "current_catalog",
    "current_date",
    "current_role",
    "current_schema",
    "current_time",
    "current_timestamp",
    "current_user",
    "default",
    "deferrable",
    "desc",
    "distinct",
    "do",
    "else",
    "end",
    "except",
    "false",
    "fetch",
    "for",
    "foreign",
    "freeze",
    "from",
    "full",
    "grant",
    "group",
    "having",
    "ilike",
    "in",
    "initially",
    "inner",
    "intersect",
    "into",
    "is",
    "isnull",
    "join",
    "lateral",
    "leading",
    "left",
    "like",
    "limit",
    "localtime",
    "localtimestamp",
    "natural",
    "not",
    "notnull",
    "null",
    "offset",
    "on",
    "only",
    "or",
    "order",
    "outer",
    "overlaps",
    "placing",
    "primary",
    "references",
    "returning",
    "right",
    "select",
    "session_user",
    "similar",
    "some",
    "symmetric",
    "system_user",
    "table",
    "tablesample",
    "then",
    "to",
    "trailing",
    "true",
    "union",
    "unique",
    "user",
    "using",
    "variadic",
    "verbose",
    "when",
    "where",
    "window",
    "with",
];

pub(crate) const fn validate(name: &str) -> Result<(), StoreNameError> {
    match category::validate_identifier(name) {
        Ok(()) => {}
        Err(category::CategoryError::Empty) => return Err(StoreNameError::Empty),
        Err(category::CategoryError::TooLong) => return Err(StoreNameError::TooLong),
        Err(category::CategoryError::InvalidStart) => return Err(StoreNameError::InvalidStart),
        // Not returned: a store name may contain __, since it never becomes part of a partition's name.
        Err(
            category::CategoryError::InvalidCharacter | category::CategoryError::DoubleUnderscore,
        ) => {
            return Err(StoreNameError::InvalidCharacter);
        }
    }

    let bytes = name.as_bytes();
    if starts_with(bytes, b"pg_") {
        return Err(StoreNameError::Reserved);
    }

    let mut i = 0;
    while i < RESERVED.len() {
        let reserved = RESERVED[i].as_bytes();
        if bytes.len() == reserved.len() && starts_with(bytes, reserved) {
            return Err(StoreNameError::Reserved);
        }
        i += 1;
    }

    let mut i = 0;
    while i < KEYWORDS.len() {
        let keyword = KEYWORDS[i].as_bytes();
        if bytes.len() == keyword.len() && starts_with(bytes, keyword) {
            return Err(StoreNameError::Keyword);
        }
        i += 1;
    }

    Ok(())
}

const fn starts_with(bytes: &[u8], prefix: &[u8]) -> bool {
    if bytes.len() < prefix.len() {
        return false;
    }

    let mut i = 0;
    while i < prefix.len() {
        if bytes[i] != prefix[i] {
            return false;
        }
        i += 1;
    }

    true
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validates_static_names() {
        assert!(StoreName::new("ops_es").is_ok());
        assert!(StoreName::new("pgx").is_ok());
        assert!(StoreName::new("pges2").is_ok());
        assert!(StoreName::new("publication").is_ok());
        assert_eq!(StoreName::new(""), Err(StoreNameError::Empty));
        assert_eq!(StoreName::new("Ops"), Err(StoreNameError::InvalidStart));
        assert_eq!(StoreName::new("1ops"), Err(StoreNameError::InvalidStart));
        assert_eq!(
            StoreName::new("ops;drop"),
            Err(StoreNameError::InvalidCharacter)
        );
        assert_eq!(
            StoreName::new("a".repeat(41).leak()),
            Err(StoreNameError::TooLong)
        );
        assert!(StoreName::new("a".repeat(40).leak()).is_ok());
    }

    #[test]
    fn rejects_reserved_schemas() {
        for name in ["pges", "public", "information_schema", "pg_temp", "pg_"] {
            assert_eq!(
                StoreName::new(name),
                Err(StoreNameError::Reserved),
                "{name}"
            );
            assert_eq!(
                StoreName::parse(name),
                Err(StoreNameError::Reserved),
                "{name}"
            );
        }
    }

    /// Store names go into SQL unquoted, so a reserved keyword fails every statement: `CREATE SCHEMA select`
    /// is a syntax error.
    #[test]
    fn rejects_reserved_keywords() {
        for name in [
            "select",
            "user",
            "table",
            "order",
            "analyse",
            "authorization",
            "verbose",
        ] {
            assert_eq!(StoreName::new(name), Err(StoreNameError::Keyword), "{name}");
            assert_eq!(
                StoreName::parse(name),
                Err(StoreNameError::Keyword),
                "{name}"
            );
        }
        // Unreserved and column-name keywords are valid schema names.
        for name in ["action", "data", "name", "between", "values"] {
            assert!(StoreName::new(name).is_ok(), "{name}");
        }
    }

    #[test]
    fn parses_runtime_names() {
        let name = StoreName::parse(String::from("billing")).unwrap();

        assert_eq!(name.as_str(), "billing");
        assert_eq!(name, StoreName::new("billing").unwrap());
        assert_eq!("billing".parse::<StoreName>(), Ok(name));
        assert_eq!(
            StoreName::parse("Billing"),
            Err(StoreNameError::InvalidStart)
        );
    }

    #[test]
    fn macro_checks_literals_in_const() {
        const OPS: StoreName = crate::store_name!("ops");

        assert_eq!(OPS.as_str(), "ops");
        assert_eq!(crate::store_name!("ops").to_string(), "ops");
    }
}
