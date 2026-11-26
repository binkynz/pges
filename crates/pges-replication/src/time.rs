//! PostgreSQL timestamps on the wire: microseconds since 2000-01-01 UTC.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// Seconds from 1970-01-01 to 2000-01-01.
const POSTGRES_EPOCH_SECS: u64 = 946_684_800;

/// Saturates at the PostgreSQL epoch when the platform's `SystemTime` cannot hold the value, so a corrupt
/// timestamp from the server never panics.
pub(crate) fn from_postgres(micros: i64) -> SystemTime {
    let epoch = UNIX_EPOCH + Duration::from_secs(POSTGRES_EPOCH_SECS);
    let offset = Duration::from_micros(micros.unsigned_abs());
    let time = if micros >= 0 {
        epoch.checked_add(offset)
    } else {
        epoch.checked_sub(offset)
    };
    time.unwrap_or(epoch)
}

pub(crate) fn to_postgres(time: SystemTime) -> i64 {
    let epoch = UNIX_EPOCH + Duration::from_secs(POSTGRES_EPOCH_SECS);
    let micros = |d: Duration| i64::try_from(d.as_micros()).unwrap_or(i64::MAX);
    match time.duration_since(epoch) {
        Ok(after) => micros(after),
        Err(before) => -micros(before.duration()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trips() {
        for micros in [0, 1, -1, 1_000_000, 789_000_000_123_456] {
            assert_eq!(to_postgres(from_postgres(micros)), micros);
        }
    }

    #[test]
    fn extreme_timestamps_do_not_panic() {
        from_postgres(i64::MAX);
        from_postgres(i64::MIN);
    }
}
