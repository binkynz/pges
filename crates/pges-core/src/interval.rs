//! Bounded, non-zero durations for timers, leases and retry delays.

use std::time::Duration;

/// A duration between [`Interval::MIN`] (1 ms) and [`Interval::MAX`] (30 days).
///
/// Timers, leases and retry delays take an `Interval`, so a zero delay (a busy loop) or a huge one (an
/// overflowing `Instant + Duration`, or a PostgreSQL `interval` out of range) cannot be configured. 30 days
/// is far above any timer the crates run, and small enough that adding it to an `Instant` or converting it
/// to microseconds as an `i64` cannot overflow.
///
/// Write a literal with [`interval!`](crate::interval!), which checks it at compile time, or call the
/// `const fn` constructors in a `const` item. A duration known only at run time goes through
/// [`Interval::new`] or `TryFrom<Duration>`.
///
/// ```
/// use std::time::Duration;
///
/// use pges_core::interval;
///
/// const LEASE: interval::Interval = interval!(15 s);
/// const RETRY: interval::Interval = match interval::Interval::from_millis(500) {
///     Some(retry) => retry,
///     None => panic!("out of range"),
/// };
///
/// assert_eq!(LEASE.get(), Duration::from_secs(15));
/// assert_eq!(RETRY.get(), Duration::from_millis(500));
/// assert_eq!(interval::Interval::new(Duration::ZERO), None);
/// assert_eq!(
///     interval::Interval::try_from(Duration::from_secs(31 * 24 * 3600)),
///     Err(interval::IntervalError::TooLong)
/// );
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Interval(Duration);

impl Interval {
    /// The shortest interval: 1 millisecond.
    pub const MIN: Self = Self(Duration::from_millis(1));

    /// The longest interval: 30 days.
    pub const MAX: Self = Self(Duration::from_secs(30 * 24 * 60 * 60));

    /// `duration`, or `None` if it is shorter than [`Interval::MIN`] or longer than [`Interval::MAX`].
    pub const fn new(duration: Duration) -> Option<Self> {
        match check(duration) {
            Ok(()) => Some(Self(duration)),
            Err(_) => None,
        }
    }

    /// `millis` milliseconds, or `None` if out of range.
    pub const fn from_millis(millis: u64) -> Option<Self> {
        Self::new(Duration::from_millis(millis))
    }

    /// `secs` seconds, or `None` if out of range.
    pub const fn from_secs(secs: u64) -> Option<Self> {
        Self::new(Duration::from_secs(secs))
    }

    /// The duration.
    pub const fn get(self) -> Duration {
        self.0
    }
}

impl From<Interval> for Duration {
    fn from(interval: Interval) -> Self {
        interval.0
    }
}

impl TryFrom<Duration> for Interval {
    type Error = IntervalError;

    fn try_from(duration: Duration) -> Result<Self, Self::Error> {
        check(duration)?;

        Ok(Self(duration))
    }
}

/// Why a duration is not an [`Interval`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum IntervalError {
    /// The duration is shorter than [`Interval::MIN`], 1 millisecond.
    #[error("interval is shorter than 1 ms")]
    TooShort,
    /// The duration is longer than [`Interval::MAX`], 30 days.
    #[error("interval is longer than 30 days")]
    TooLong,
}

/// An [`Interval`](crate::interval::Interval) from a literal, checked at compile time.
///
/// Write the number followed by a unit: `ms`, `s`, `min`, `h` or `d`. A single expression is read as a
/// [`Duration`](std::time::Duration) that must be a constant. The macro expands to a block with a `const` item
/// that checks the value, so a value out of range is a compile error (`cargo check` reports it too, also
/// inside a generic function) and never a run-time panic. `use pges_core::interval;` imports both this macro
/// and the module.
///
/// ```
/// use std::time::Duration;
///
/// use pges_core::interval;
///
/// const RECHECK: interval::Interval = interval!(500 ms);
///
/// assert_eq!(RECHECK.get(), Duration::from_millis(500));
/// assert_eq!(interval!(15 s).get(), Duration::from_secs(15));
/// assert_eq!(interval!(2 min).get(), Duration::from_secs(120));
/// assert_eq!(interval!(1 h).get(), Duration::from_secs(3600));
/// assert_eq!(interval!(30 d), interval::Interval::MAX);
/// assert_eq!(interval!(Duration::from_secs(5)).get(), Duration::from_secs(5));
/// ```
///
/// ```compile_fail
/// let zero = pges_core::interval!(0 ms);
/// ```
///
/// ```compile_fail
/// let too_long = pges_core::interval!(31 d);
/// ```
#[macro_export]
macro_rules! interval {
    ($millis:literal ms) => {
        $crate::interval!(::core::time::Duration::from_millis($millis))
    };
    ($secs:literal s) => {
        $crate::interval!(::core::time::Duration::from_secs($secs))
    };
    ($mins:literal min) => {
        $crate::interval!(::core::time::Duration::from_secs($mins * 60))
    };
    ($hours:literal h) => {
        $crate::interval!(::core::time::Duration::from_secs($hours * 60 * 60))
    };
    ($days:literal d) => {
        $crate::interval!(::core::time::Duration::from_secs($days * 24 * 60 * 60))
    };
    ($duration:expr $(,)?) => {{
        const INTERVAL: $crate::interval::Interval =
            match $crate::interval::Interval::new($duration) {
                ::core::option::Option::Some(interval) => interval,
                #[allow(clippy::panic)]
                ::core::option::Option::None => {
                    ::core::panic!("invalid interval: it must be between 1 ms and 30 days")
                }
            };

        INTERVAL
    }};
}

const fn check(duration: Duration) -> Result<(), IntervalError> {
    if duration.as_nanos() < Interval::MIN.0.as_nanos() {
        return Err(IntervalError::TooShort);
    }
    if duration.as_nanos() > Interval::MAX.0.as_nanos() {
        return Err(IntervalError::TooLong);
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_one_millisecond_to_thirty_days() {
        assert_eq!(Interval::new(Duration::ZERO), None);
        assert_eq!(Interval::new(Duration::from_micros(999)), None);
        assert_eq!(Interval::new(Duration::from_millis(1)), Some(Interval::MIN));
        assert_eq!(Interval::from_secs(30 * 24 * 3600), Some(Interval::MAX));
        assert_eq!(
            Interval::new(Interval::MAX.get() + Duration::from_nanos(1)),
            None
        );
        assert_eq!(Interval::new(Duration::MAX), None);
        assert_eq!(Interval::from_millis(u64::MAX), None);
    }

    #[test]
    fn reports_why_a_duration_is_out_of_range() {
        assert_eq!(
            Interval::try_from(Duration::ZERO),
            Err(IntervalError::TooShort)
        );
        assert_eq!(
            Interval::try_from(Duration::MAX),
            Err(IntervalError::TooLong)
        );
        assert_eq!(
            Interval::try_from(Duration::from_secs(1)).map(Duration::from),
            Ok(Duration::from_secs(1))
        );
    }

    #[test]
    fn max_fits_instants_and_postgres_microseconds() {
        let micros = i64::try_from(Interval::MAX.get().as_micros());

        assert!(micros.is_ok());
        assert!(
            std::time::Instant::now()
                .checked_add(Interval::MAX.get())
                .is_some()
        );
    }

    #[test]
    fn macro_checks_literals_in_const() {
        const RECHECK: Interval = crate::interval!(500 ms);

        assert_eq!(RECHECK.get(), Duration::from_millis(500));
        assert_eq!(crate::interval!(1 ms), Interval::MIN);
        assert_eq!(crate::interval!(15 s).get(), Duration::from_secs(15));
        assert_eq!(crate::interval!(3 min).get(), Duration::from_secs(180));
        assert_eq!(crate::interval!(2 h).get(), Duration::from_secs(7200));
        assert_eq!(crate::interval!(30 d), Interval::MAX);
        assert_eq!(
            crate::interval!(crate::store::DEFAULT_MAX_TXN_AGE).get(),
            crate::store::DEFAULT_MAX_TXN_AGE
        );
    }
}
