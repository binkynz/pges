//! Store positions.

use std::{fmt, ops};

use crate::stream;

/// The position of an event in its store's log. Positions are gap-free across the store and follow commit
/// order; within one category they increase but have gaps (other categories took them).
///
/// A subscription's cursor is the position of the last event it handled, or [`Position::BEGINNING`].
///
/// Positions are stored as `int8`, so the largest is [`Position::MAX`] (`i64::MAX`); the constructors refuse
/// anything above it rather than clamping. Write a literal with [`position!`](crate::position!), which checks
/// it at compile time.
///
/// ```
/// use pges_core::position;
///
/// const AT: position::Position = position!(42);
///
/// assert_eq!(AT.get(), 42);
/// assert_eq!(position::Position::new(u64::MAX), None);
/// assert_eq!(position::Position::MAX.to_i64(), i64::MAX);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Default)]
pub struct Position(u64);

impl Position {
    /// Before the first event.
    pub const BEGINNING: Self = Self(0);

    /// The largest position: `i64::MAX`.
    pub const MAX: Self = Self(i64::MAX.unsigned_abs());

    /// `position`, or `None` above [`Position::MAX`].
    pub const fn new(position: u64) -> Option<Self> {
        if position > Self::MAX.0 {
            return None;
        }

        Some(Self(position))
    }

    /// `position`, or `None` if it is negative.
    pub const fn from_i64(position: i64) -> Option<Self> {
        if position < 0 {
            return None;
        }

        Some(Self(position.unsigned_abs()))
    }

    /// The number.
    pub const fn get(self) -> u64 {
        self.0
    }

    /// The number as an `i64`, which always holds it.
    pub const fn to_i64(self) -> i64 {
        self.0.cast_signed()
    }

    /// The position `n` events later, or `None` above [`Position::MAX`].
    pub const fn checked_add(self, n: u64) -> Option<Self> {
        match self.0.checked_add(n) {
            Some(position) => Self::new(position),
            None => None,
        }
    }
}

/// A [`Position`](crate::position::Position) from a literal, checked at compile time.
///
/// The macro expands to a block with a `const` item, so a value above
/// [`Position::MAX`](crate::position::Position::MAX) is a compile error and never a run-time panic. The
/// argument must be a constant. `use pges_core::position;` imports both this macro and the module.
///
/// ```compile_fail
/// let too_far = pges_core::position!(u64::MAX);
/// ```
#[macro_export]
macro_rules! position {
    ($position:expr $(,)?) => {{
        const POSITION: $crate::position::Position =
            match $crate::position::Position::new($position) {
                ::core::option::Option::Some(position) => position,
                #[allow(clippy::panic)]
                ::core::option::Option::None => {
                    ::core::panic!("a position is at most i64::MAX")
                }
            };

        POSITION
    }};
}

impl fmt::Display for Position {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

/// An event read from the store's log: a [`stream::StreamEvent`] plus its position.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct LogEvent<E> {
    /// Its position in the store's log.
    pub position: Position,
    /// The event and its stream details.
    pub inner: stream::StreamEvent<E>,
}

impl<E> LogEvent<E> {
    /// Pairs an event with its position. Used by the subscription crate.
    pub fn new(position: Position, inner: stream::StreamEvent<E>) -> Self {
        Self { position, inner }
    }
}

impl<E> ops::Deref for LogEvent<E> {
    type Target = stream::StreamEvent<E>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn positions_fit_an_i64() {
        assert_eq!(Position::new(i64::MAX.unsigned_abs()), Some(Position::MAX));
        assert_eq!(Position::new(i64::MAX.unsigned_abs() + 1), None);
        assert_eq!(Position::from_i64(-1), None);
        assert_eq!(Position::from_i64(7).map(Position::get), Some(7));
        assert_eq!(Position::MAX.to_i64(), i64::MAX);
        assert_eq!(Position::MAX.checked_add(1), None);
        assert_eq!(crate::position!(3).checked_add(2), Position::new(5));
    }
}
