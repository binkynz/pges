//! High-water marks: how far a store's log has been sequenced, and the [`Notice`] the sequencer sends with
//! `NOTIFY`.

use std::fmt::Write as _;

use crate::{category, lsn, position};

/// The newest position of a store's log, overall and per category, as the sequencer announces it.
///
/// A high-water mark is a hint for waking subscribers, never the truth: the log table is. Every position at
/// or below [`HighWater::store`] is already visible in the log on the primary. On a replica it is visible
/// once `pg_last_wal_replay_lsn()` has reached [`HighWater::lsn`].
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct HighWater {
    store: position::Position,
    categories: Vec<(category::CategoryId, position::Position)>,
    lsn: Option<lsn::Lsn>,
}

impl HighWater {
    /// A high-water mark. `categories` is sorted by id; a category listed twice keeps its highest position.
    pub fn new(
        store: position::Position,
        mut categories: Vec<(category::CategoryId, position::Position)>,
        lsn: Option<lsn::Lsn>,
    ) -> Self {
        categories.sort_unstable_by(|a, b| a.0.cmp(&b.0).then(b.1.cmp(&a.1)));
        categories.dedup_by_key(|(id, _)| *id);

        Self {
            store,
            categories,
            lsn,
        }
    }

    /// The store's newest position.
    pub fn store(&self) -> position::Position {
        self.store
    }

    /// The newest position of each category that has events, sorted by category id, each id once.
    pub fn categories(&self) -> &[(category::CategoryId, position::Position)] {
        &self.categories
    }

    /// The WAL position the sequencer read after the log rows committed, if it read one.
    pub fn lsn(&self) -> Option<lsn::Lsn> {
        self.lsn
    }

    /// The category's newest position, or [`position::Position::BEGINNING`] if it has no events.
    pub fn category(&self, id: category::CategoryId) -> position::Position {
        self.categories
            .binary_search_by_key(&id, |(category, _)| *category)
            .map(|index| self.categories[index].1)
            .unwrap_or(position::Position::BEGINNING)
    }
}

/// What the sequencer's `NOTIFY pges_<store>` carries after a batch wrote to the store's log: the store's
/// high-water mark, the WAL position read after the batch committed, and the new positions of the
/// categories the batch touched.
///
/// Like every high-water mark it is a hint. A notification can be lost or merged with the next one, so a
/// subscriber treats it as "read the log again", never as a list of everything that changed.
///
/// # Format
///
/// A short ASCII text, versioned by its first field. It never reaches the `NOTIFY` payload limit (8000
/// bytes): the categories are replaced by `*` when they would not fit in [`Notice::MAX_LEN`] bytes.
///
/// ```text
/// payload    := "1" ";" store ";" lsn ";" categories
/// store      := decimal u64
/// lsn        := "" | PostgreSQL LSN text, e.g. "16/B374D848"
/// categories := "" | "*" | id ":" position ("," id ":" position)*
/// ```
///
/// `1;42;0/16B3D848;3:40,5:42`: the store is at 42, and the batch touched categories 3 (now at 40) and 5
/// (now at 42). `*` means any category may have moved. A decoder rejects a version it does not know; the
/// subscriber then reads the log again, as for any wake-up.
///
/// ```
/// use pges_core::{category, high_water, position};
///
/// let high = high_water::HighWater::new(
///     position!(42),
///     vec![(category::CategoryId::new(3), position!(40))],
///     None,
/// );
/// let notice = high_water::Notice::new(&high, [category::CategoryId::new(3)]);
/// let payload = notice.encode();
/// assert_eq!(payload, "1;42;;3:40");
/// assert_eq!(high_water::Notice::decode(&payload), Ok(notice));
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Notice {
    store: position::Position,
    lsn: Option<lsn::Lsn>,
    categories: NoticeCategories,
}

/// The categories a [`Notice`] names.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum NoticeCategories {
    /// Every category the batch touched, with its new position, sorted by id. Categories not listed did not
    /// move in that batch.
    Listed(Vec<(category::CategoryId, position::Position)>),
    /// Too many to list: any category may have moved.
    Omitted,
}

impl Notice {
    /// The longest payload [`Notice::encode`] returns, in bytes. PostgreSQL refuses payloads of 8000 bytes or
    /// more.
    pub const MAX_LEN: usize = 7999;

    /// The notice for a batch that touched `touched`, with their positions taken from `high_water`. A
    /// touched category that `high_water` does not list is left out.
    pub fn new(
        high_water: &HighWater,
        touched: impl IntoIterator<Item = category::CategoryId>,
    ) -> Self {
        let mut ids: Vec<category::CategoryId> = touched.into_iter().collect();
        ids.sort_unstable();
        ids.dedup();
        let categories = ids
            .into_iter()
            .filter_map(|id| {
                high_water
                    .categories
                    .binary_search_by_key(&id, |(category, _)| *category)
                    .ok()
                    .map(|index| high_water.categories[index])
            })
            .collect();

        Self {
            store: high_water.store,
            lsn: high_water.lsn,
            categories: NoticeCategories::Listed(categories),
        }
    }

    /// The store's newest position.
    pub fn store(&self) -> position::Position {
        self.store
    }

    /// The WAL position the sequencer read after the log rows committed, if it read one.
    pub fn lsn(&self) -> Option<lsn::Lsn> {
        self.lsn
    }

    /// The categories the batch touched.
    pub fn categories(&self) -> &NoticeCategories {
        &self.categories
    }

    /// The payload text, at most [`Notice::MAX_LEN`] bytes. Categories that would not fit are replaced by
    /// `*`, which decodes as [`NoticeCategories::Omitted`].
    pub fn encode(&self) -> String {
        let mut text = format!("{NOTICE_VERSION};{};", self.store);
        if let Some(lsn) = self.lsn {
            let _ = write!(text, "{lsn}");
        }
        text.push(';');

        let header = text.len();
        match &self.categories {
            NoticeCategories::Omitted => text.push('*'),
            NoticeCategories::Listed(categories) => {
                for (index, (id, at)) in categories.iter().enumerate() {
                    if index > 0 {
                        text.push(',');
                    }
                    let _ = write!(text, "{id}:{at}");
                    if text.len() > Self::MAX_LEN {
                        text.truncate(header);
                        text.push('*');
                        break;
                    }
                }
            }
        }

        text
    }

    /// Parses a payload written by [`Notice::encode`].
    ///
    /// Fails with [`NoticeError::UnsupportedVersion`] for a format version this crate does not know, and
    /// with [`NoticeError::Malformed`] for anything else that is not a notice.
    pub fn decode(payload: &str) -> Result<Self, NoticeError> {
        let malformed = || NoticeError::Malformed(payload.to_owned());
        let mut fields = payload.split(';');
        let version = fields.next().unwrap_or_default();
        if version != NOTICE_VERSION {
            return Err(NoticeError::UnsupportedVersion(version.to_owned()));
        }

        let (Some(store), Some(lsn), Some(categories), None) =
            (fields.next(), fields.next(), fields.next(), fields.next())
        else {
            return Err(malformed());
        };
        let store = parse_number(store)
            .and_then(position::Position::new)
            .ok_or_else(malformed)?;
        let lsn = match lsn {
            "" => None,
            text => Some(text.parse::<lsn::Lsn>().map_err(|_| malformed())?),
        };
        let categories = match categories {
            "*" => NoticeCategories::Omitted,
            "" => NoticeCategories::Listed(Vec::new()),
            list => NoticeCategories::Listed(
                list.split(',')
                    .map(parse_category)
                    .collect::<Option<Vec<_>>>()
                    .ok_or_else(malformed)?,
            ),
        };

        Ok(Self {
            store,
            lsn,
            categories,
        })
    }
}

/// A `NOTIFY` payload could not be decoded as a [`Notice`].
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum NoticeError {
    /// The payload's format version (its first field) is not one this crate knows.
    #[error("unsupported notice format {0:?}")]
    UnsupportedVersion(String),
    /// The payload is not a valid notice.
    #[error("malformed notice {0:?}")]
    Malformed(String),
}

const NOTICE_VERSION: &str = "1";

/// `id:position`.
fn parse_category(entry: &str) -> Option<(category::CategoryId, position::Position)> {
    let (id, at) = entry.split_once(':')?;
    let id = i32::try_from(parse_number(id)?).ok()?;

    Some((
        category::CategoryId::new(id),
        position::Position::new(parse_number(at)?)?,
    ))
}

/// A decimal number without sign or leading zeros, so every value has one spelling.
fn parse_number(text: &str) -> Option<u64> {
    let canonical = !text.is_empty()
        && text.bytes().all(|b| b.is_ascii_digit())
        && (text == "0" || !text.starts_with('0'));

    canonical.then(|| text.parse().ok()).flatten()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn id(n: i32) -> category::CategoryId {
        category::CategoryId::new(n)
    }

    fn at(n: u64) -> position::Position {
        position::Position::new(n).unwrap()
    }

    fn wide() -> HighWater {
        let max = position::Position::MAX.get();
        let categories = (1..=2000).map(|n| (id(n), at(max - n as u64))).collect();
        HighWater::new(at(max), categories, Some(lsn::Lsn::new(u64::MAX)))
    }

    #[test]
    fn sorts_categories_and_keeps_the_highest_duplicate() {
        let high = HighWater::new(
            at(9),
            vec![(id(3), at(4)), (id(1), at(2)), (id(3), at(9))],
            None,
        );

        assert_eq!(high.categories, vec![(id(1), at(2)), (id(3), at(9))]);
        assert_eq!(high.category(id(3)), at(9));
        assert_eq!(high.category(id(2)), position::Position::BEGINNING);
    }

    #[test]
    fn a_notice_lists_only_the_touched_categories() {
        let high = HighWater::new(
            at(42),
            vec![(id(3), at(40)), (id(5), at(42)), (id(7), at(1))],
            Some(lsn::Lsn::new(0x16B3_D848)),
        );

        let notice = Notice::new(&high, [id(5), id(3), id(5), id(9)]);

        assert_eq!(
            notice.categories,
            NoticeCategories::Listed(vec![(id(3), at(40)), (id(5), at(42))])
        );
        assert_eq!(notice.encode(), "1;42;0/16B3D848;3:40,5:42");
    }

    #[test]
    fn notices_round_trip() {
        let high = HighWater::new(at(9), vec![(id(1), at(2)), (id(3), at(9))], None);
        let cases = [
            Notice::new(&high, []),
            Notice::new(&high, [id(1), id(3)]),
            Notice::new(&wide(), [id(1), id(2000)]),
            Notice {
                store: at(0),
                lsn: Some(lsn::Lsn::ZERO),
                categories: NoticeCategories::Omitted,
            },
        ];

        for notice in cases {
            assert_eq!(Notice::decode(&notice.encode()), Ok(notice));
        }
    }

    #[test]
    fn too_many_categories_are_omitted_and_the_payload_stays_under_the_limit() {
        let high = wide();
        let notice = Notice::new(&high, high.categories.iter().map(|(id, _)| *id));
        let payload = notice.encode();

        assert!(payload.len() <= Notice::MAX_LEN, "{} bytes", payload.len());
        assert!(payload.ends_with(";*"), "{payload}");
        let decoded = Notice::decode(&payload).unwrap();
        assert_eq!(decoded.categories, NoticeCategories::Omitted);
        assert_eq!((decoded.store, decoded.lsn), (high.store, high.lsn));

        // Everything that fits is listed in full, and the largest such payload is still under the limit.
        let mut fitting = 0;
        for n in 1..=high.categories.len() {
            let payload =
                Notice::new(&high, high.categories[..n].iter().map(|(id, _)| *id)).encode();
            assert!(payload.len() <= Notice::MAX_LEN, "{} bytes", payload.len());
            if payload.ends_with('*') {
                break;
            }
            fitting = n;
        }
        assert!(fitting > 150, "only {fitting} categories fit");
    }

    #[test]
    fn malformed_notices_and_unknown_versions_are_rejected() {
        assert_eq!(
            Notice::decode("2;1;;"),
            Err(NoticeError::UnsupportedVersion("2".to_owned()))
        );
        assert_eq!(
            Notice::decode(""),
            Err(NoticeError::UnsupportedVersion(String::new()))
        );
        for payload in [
            "1",
            "1;",
            "1;1;",
            "1;1;;;",
            "1;x;;",
            "1;-1;;",
            "1;01;;",
            "1;1;zz;",
            "1;1;;3",
            "1;1;;3:",
            "1;1;;:3",
            "1;1;;3:4,",
            "1;1;;99999999999:1",
            "1;1;;3:4;",
            "1;18446744073709551616;;",
            "1;9223372036854775808;;",
            "1;1;;3:9223372036854775808",
        ] {
            assert!(
                matches!(Notice::decode(payload), Err(NoticeError::Malformed(_))),
                "{payload:?}"
            );
        }
    }
}
