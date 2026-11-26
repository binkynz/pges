//! Names interpolated into replication commands.
//!
//! The replication protocol takes slot and publication names inside command text, not as bind parameters, so
//! both are validated on construction and never quoted.

use std::{borrow::Cow, fmt};

/// A replication slot name: 1 to 63 characters from `a-z`, `0-9` and `_`, the rule PostgreSQL itself applies.
///
/// Slot names are global to the server, not per database. A literal is checked at compile time with
/// [`slot_name!`](crate::slot_name).
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct SlotName(Cow<'static, str>);

impl SlotName {
    /// Validates `name`.
    pub fn new(name: impl Into<String>) -> Result<Self, NameError> {
        let name = name.into();
        if !Self::is_valid(&name) {
            return Err(NameError::Slot(name));
        }

        Ok(Self(Cow::Owned(name)))
    }

    /// Validates a `'static` name in `const` context: `None` when it is not a valid slot name.
    pub const fn from_static(name: &'static str) -> Option<Self> {
        match Self::is_valid(name) {
            true => Some(Self(Cow::Borrowed(name))),
            false => None,
        }
    }

    /// Whether `name` is a valid slot name.
    pub const fn is_valid(name: &str) -> bool {
        let bytes = name.as_bytes();
        if bytes.is_empty() || bytes.len() > MAX_LEN {
            return false;
        }

        let mut index = 0;
        while index < bytes.len() {
            if !matches!(bytes[index], b'a'..=b'z' | b'0'..=b'9' | b'_') {
                return false;
            }
            index += 1;
        }

        true
    }

    /// The name.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for SlotName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// A publication name: an unquoted lower-case identifier, matching `^[a-z_][a-z0-9_]{0,62}$`.
///
/// A literal is checked at compile time with [`publication_name!`](crate::publication_name).
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct PublicationName(Cow<'static, str>);

impl PublicationName {
    /// Validates `name`.
    pub fn new(name: impl Into<String>) -> Result<Self, NameError> {
        let name = name.into();
        if !Self::is_valid(&name) {
            return Err(NameError::Publication(name));
        }

        Ok(Self(Cow::Owned(name)))
    }

    /// Validates a `'static` name in `const` context: `None` when it is not a valid publication name.
    pub const fn from_static(name: &'static str) -> Option<Self> {
        match Self::is_valid(name) {
            true => Some(Self(Cow::Borrowed(name))),
            false => None,
        }
    }

    /// Whether `name` is a valid publication name.
    pub const fn is_valid(name: &str) -> bool {
        let bytes = name.as_bytes();
        if bytes.is_empty() || bytes.len() > MAX_LEN || !matches!(bytes[0], b'a'..=b'z' | b'_') {
            return false;
        }

        let mut index = 1;
        while index < bytes.len() {
            if !matches!(bytes[index], b'a'..=b'z' | b'0'..=b'9' | b'_') {
                return false;
            }
            index += 1;
        }

        true
    }

    /// The name.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for PublicationName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// The name of a snapshot exported by `CREATE_REPLICATION_SLOT … (SNAPSHOT 'export')`, such as
/// `00000003-0000001B-1`: 1 to 63 characters from `0-9`, `A-F`, `a-f` and `-`. Import it on a regular
/// connection with `SET TRANSACTION SNAPSHOT '<name>'`, as the first statement of a `REPEATABLE READ`
/// transaction, while the exporting replication connection runs no other command.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct SnapshotName(String);

impl SnapshotName {
    /// Validates `name`.
    pub fn new(name: impl Into<String>) -> Result<Self, NameError> {
        let name = name.into();
        let valid = (1..=MAX_LEN).contains(&name.len())
            && name.bytes().all(|b| b.is_ascii_hexdigit() || b == b'-');
        if !valid {
            return Err(NameError::Snapshot(name));
        }

        Ok(Self(name))
    }

    /// The name.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for SnapshotName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::str::FromStr for SnapshotName {
    type Err = NameError;

    fn from_str(name: &str) -> Result<Self, Self::Err> {
        Self::new(name)
    }
}

/// A name was rejected.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum NameError {
    /// Not a valid slot name.
    #[error("slot name {0:?} must be 1 to 63 characters from a-z, 0-9 and _")]
    Slot(String),
    /// Not a valid publication name.
    #[error("publication name {0:?} must match ^[a-z_][a-z0-9_]{{0,62}}$")]
    Publication(String),
    /// Not a valid exported snapshot name.
    #[error("snapshot name {0:?} must be 1 to 63 characters from 0-9, A-F, a-f and -")]
    Snapshot(String),
}

/// A [`SlotName`](crate::name::SlotName) from a literal, checked at compile time: an invalid name does
/// not compile, so nothing can fail at run time.
///
/// ```
/// const SLOT: pges_replication::name::SlotName = pges_replication::slot_name!("pges_16384");
///
/// assert_eq!(SLOT.as_str(), "pges_16384");
/// ```
///
/// ```compile_fail
/// let slot = pges_replication::slot_name!("Not-A-Slot");
/// ```
#[macro_export]
macro_rules! slot_name {
    ($name:expr $(,)?) => {{
        const NAME: $crate::name::SlotName = {
            #[allow(clippy::expect_used)]
            let name = $crate::name::SlotName::from_static($name)
                .expect("invalid slot name: it must be 1 to 63 characters from a-z, 0-9 and _");
            name
        };

        NAME
    }};
}

/// A [`PublicationName`](crate::name::PublicationName) from a literal, checked at compile time: an invalid
/// name does not compile, so nothing can fail at run time.
///
/// ```
/// let publication = pges_replication::publication_name!("pges");
///
/// assert_eq!(publication.as_str(), "pges");
/// ```
///
/// ```compile_fail
/// let publication = pges_replication::publication_name!("1pges");
/// ```
#[macro_export]
macro_rules! publication_name {
    ($name:expr $(,)?) => {{
        const NAME: $crate::name::PublicationName = {
            #[allow(clippy::expect_used)]
            let name = $crate::name::PublicationName::from_static($name)
                .expect("invalid publication name: it must match ^[a-z_][a-z0-9_]{0,62}$");
            name
        };

        NAME
    }};
}

/// `NAMEDATALEN - 1`.
const MAX_LEN: usize = 63;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn slot_names_follow_postgres() {
        assert!(SlotName::new("pges_16384").is_ok());
        assert!(SlotName::new("0slot").is_ok());
        assert!(SlotName::new("a".repeat(63)).is_ok());
        for bad in ["", "Slot", "a-b", "a b", "a;drop", "é"] {
            assert!(SlotName::new(bad).is_err(), "{bad:?}");
        }
        assert!(SlotName::new("a".repeat(64)).is_err());
    }

    #[test]
    fn publication_names_are_plain_identifiers() {
        assert!(PublicationName::new("pges").is_ok());
        assert!(PublicationName::new("_p1").is_ok());
        for bad in ["", "1p", "P", "a,b", "a'b", "a\"b"] {
            assert!(PublicationName::new(bad).is_err(), "{bad:?}");
        }
    }

    #[test]
    fn literals_follow_the_same_rules() {
        const SLOT: SlotName = crate::slot_name!("pges_16384");
        const PUBLICATION: PublicationName = crate::publication_name!("_p1");

        assert_eq!(SLOT, SlotName::new("pges_16384").unwrap());
        assert_eq!(PUBLICATION, PublicationName::new("_p1").unwrap());
        assert_eq!(SlotName::from_static("0slot"), SlotName::new("0slot").ok());
        for bad in ["", "Slot", "a-b", "é"] {
            assert_eq!(SlotName::from_static(bad), None, "{bad:?}");
            assert_eq!(PublicationName::from_static(bad), None, "{bad:?}");
        }
        assert_eq!(PublicationName::from_static("1p"), None);
        assert!(SlotName::is_valid(&"a".repeat(63)));
        assert!(!SlotName::is_valid(&"a".repeat(64)));
        assert!(PublicationName::is_valid(&"a".repeat(63)));
        assert!(!PublicationName::is_valid(&"a".repeat(64)));
    }

    #[test]
    fn snapshot_names_are_hex_and_dashes() {
        assert!(SnapshotName::new("00000003-0000001B-1").is_ok());
        for bad in ["", "0000-'x", "00000003 1", "G1"] {
            assert!(SnapshotName::new(bad).is_err(), "{bad:?}");
        }
    }
}
