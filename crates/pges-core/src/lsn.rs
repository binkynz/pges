//! WAL positions.

use std::{fmt, str};

/// A position in the write-ahead log (`pg_lsn`).
///
/// Formats as PostgreSQL 18 does, `16/B374D848` with the low half zero-padded to eight digits, and parses
/// with or without the padding.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Default)]
pub struct Lsn(u64);

impl Lsn {
    /// `0/00000000`, the invalid LSN. As the start of `START_REPLICATION`, it means "from the slot's confirmed
    /// position".
    pub const ZERO: Self = Self(0);

    /// The LSN with this byte offset.
    pub const fn new(lsn: u64) -> Self {
        Self(lsn)
    }

    /// The byte offset.
    pub const fn get(self) -> u64 {
        self.0
    }
}

impl From<u64> for Lsn {
    fn from(lsn: u64) -> Self {
        Self(lsn)
    }
}

impl From<Lsn> for u64 {
    fn from(lsn: Lsn) -> Self {
        lsn.0
    }
}

impl fmt::Display for Lsn {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:X}/{:08X}", self.0 >> 32, self.0 & 0xFFFF_FFFF)
    }
}

impl str::FromStr for Lsn {
    type Err = ParseLsnError;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        let invalid = || ParseLsnError(text.to_owned());
        let (high, low) = text.split_once('/').ok_or_else(invalid)?;
        let half = |part: &str| {
            let valid =
                (1..=8).contains(&part.len()) && part.bytes().all(|b| b.is_ascii_hexdigit());
            if !valid {
                return Err(invalid());
            }

            u32::from_str_radix(part, 16).map_err(|_| invalid())
        };

        Ok(Self((u64::from(half(high)?) << 32) | u64::from(half(low)?)))
    }
}

/// The text is not an LSN of the form `X/Y`, with one to eight hex digits on each side.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("not an LSN: {0:?}")]
pub struct ParseLsnError(String);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn formats_like_postgres_18() {
        assert_eq!(Lsn::new(0x16_B374_D848).to_string(), "16/B374D848");
        assert_eq!(Lsn::new(0x1_0000_00AB).to_string(), "1/000000AB");
        assert_eq!(Lsn::ZERO.to_string(), "0/00000000");
    }

    #[test]
    fn parses_padded_and_unpadded() {
        assert_eq!("16/B374D848".parse(), Ok(Lsn::new(0x16_B374_D848)));
        assert_eq!("1/AB".parse(), Ok(Lsn::new(0x1_0000_00AB)));
        assert_eq!("0/0".parse(), Ok(Lsn::ZERO));
        assert_eq!("FFFFFFFF/ffffffff".parse(), Ok(Lsn::new(u64::MAX)));
    }

    #[test]
    fn rejects_malformed() {
        for text in [
            "",
            "1",
            "1/",
            "/1",
            "1/2/3",
            "123456789/0",
            "g/0",
            "+1/0",
            " 1/0",
        ] {
            assert!(text.parse::<Lsn>().is_err(), "{text:?}");
        }
    }

    #[test]
    fn round_trips() {
        for lsn in [0, 1, 0xFFFF_FFFF, 0x1_0000_0000, u64::MAX] {
            assert_eq!(Lsn::new(lsn).to_string().parse(), Ok(Lsn::new(lsn)));
        }
    }
}
