//! Stream versions.

use std::{fmt, num::NonZeroU64};

/// The position of an event within its stream. The first event is version 1; there is no version 0.
///
/// Versions are stored as `int8`, so the largest is [`Version::MAX`] (`i64::MAX`); the constructors refuse
/// anything above it rather than clamping. Write a literal with [`version!`](crate::version!), which checks
/// it at compile time.
///
/// ```
/// use pges_core::version;
///
/// const THIRD: version::Version = version!(3);
///
/// assert_eq!(THIRD.get(), 3);
/// assert_eq!(version::Version::new(0), None);
/// assert_eq!(version::Version::new(u64::MAX), None);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Version(NonZeroU64);

impl Version {
    /// Version 1, the first event of a stream.
    pub const FIRST: Self = Self(NonZeroU64::MIN);

    /// The largest version: `i64::MAX`.
    pub const MAX: Self = match NonZeroU64::new(i64::MAX.unsigned_abs()) {
        Some(max) => Self(max),
        None => Self::FIRST,
    };

    /// `version`, or `None` for 0 or above [`Version::MAX`].
    pub const fn new(version: u64) -> Option<Self> {
        if version > Self::MAX.get() {
            return None;
        }

        match NonZeroU64::new(version) {
            Some(version) => Some(Self(version)),
            None => None,
        }
    }

    /// `version`, or `None` below 1.
    pub const fn from_i64(version: i64) -> Option<Self> {
        if version < 1 {
            return None;
        }

        Self::new(version.unsigned_abs())
    }

    /// The number.
    pub const fn get(self) -> u64 {
        self.0.get()
    }

    /// The number as an `i64`, which always holds it.
    pub const fn to_i64(self) -> i64 {
        self.0.get().cast_signed()
    }

    /// The version `n` events later, or `None` above [`Version::MAX`].
    pub const fn checked_add(self, n: u64) -> Option<Self> {
        match self.0.get().checked_add(n) {
            Some(version) => Self::new(version),
            None => None,
        }
    }
}

/// A [`Version`](crate::version::Version) from a literal, checked at compile time.
///
/// The macro expands to a block with a `const` item, so 0 or a value above
/// [`Version::MAX`](crate::version::Version::MAX) is a compile error and never a run-time panic. The argument
/// must be a constant. `use pges_core::version;` imports both this macro and the module.
///
/// ```compile_fail
/// let zero = pges_core::version!(0);
/// ```
#[macro_export]
macro_rules! version {
    ($version:expr $(,)?) => {{
        const VERSION: $crate::version::Version = match $crate::version::Version::new($version) {
            ::core::option::Option::Some(version) => version,
            #[allow(clippy::panic)]
            ::core::option::Option::None => {
                ::core::panic!("a version is between 1 and i64::MAX")
            }
        };

        VERSION
    }};
}

impl fmt::Display for Version {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

/// What an append expects the stream's current version to be.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum ExpectedVersion {
    /// No check: append after whatever is there, creating the stream if needed.
    Any,
    /// The stream must not exist yet.
    NoStream,
    /// The stream's last event must be this version.
    Exact(Version),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn zero_is_not_a_version() {
        assert_eq!(Version::new(0), None);
        assert_eq!(Version::new(1), Some(Version::FIRST));
    }

    #[test]
    fn adds_without_overflowing() {
        assert_eq!(Version::FIRST.checked_add(2).map(Version::get), Some(3));
        assert_eq!(Version::MAX.checked_add(1), None);
        assert_eq!(Version::FIRST.checked_add(u64::MAX), None);
    }

    #[test]
    fn versions_fit_an_i64() {
        assert_eq!(Version::new(i64::MAX.unsigned_abs()), Some(Version::MAX));
        assert_eq!(Version::new(i64::MAX.unsigned_abs() + 1), None);
        assert_eq!(Version::from_i64(0), None);
        assert_eq!(Version::from_i64(-3), None);
        assert_eq!(Version::from_i64(2).map(Version::get), Some(2));
        assert_eq!(Version::MAX.to_i64(), i64::MAX);
        assert_eq!(crate::version!(4).get(), 4);
    }
}
