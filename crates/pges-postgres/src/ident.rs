//! Store and category names are interpolated into SQL, so they are validated instead of quoted. The names of
//! the partitions, indexes and constraints the crate creates are built here, so every one stays within
//! PostgreSQL's identifier limit: 63 bytes, beyond which it silently truncates.

/// The longest identifier PostgreSQL keeps (`NAMEDATALEN - 1`); longer ones are silently truncated.
pub(crate) const MAX_IDENTIFIER: usize = 63;

/// The CHECK constraint a new partition carries while it is attached, so the attach skips its validation
/// scan. Constraint names are per table.
pub(crate) const BOUND: &str = "bound";

/// Whether a name read from the catalog can be interpolated into DDL unquoted: `^[a-z][a-z0-9_]*$` and at most
/// [`MAX_IDENTIFIER`] bytes.
pub(crate) fn is_plain_identifier(name: &str) -> bool {
    let mut chars = name.chars();

    matches!(chars.next(), Some('a'..='z'))
        && name.len() <= MAX_IDENTIFIER
        && chars.all(|c| matches!(c, 'a'..='z' | '0'..='9' | '_'))
}

/// Separates a category's name from the rest of the names built from it. Category names cannot contain it, so
/// no name built from a category equals another category's partition `commits_<category>`, and no two of
/// them are equal: `commits_a__202611` is category `a`'s November leaf, and category `a_202611`'s partition is
/// `commits_a_202611`.
pub(crate) const SEPARATOR: &str = "__";

/// The LIST partition of `commits` holding one category: `commits_<category>`.
pub(crate) fn category_partition(category: &str) -> String {
    format!("commits_{category}")
}

/// The primary key of a category partition: `commits_<category>__pkey`. It is named, not derived: the name
/// PostgreSQL derives, `commits_<category>_pkey`, is category `<category>_pkey`'s partition.
pub(crate) fn category_partition_key(category: &str) -> String {
    format!("commits_{category}{SEPARATOR}pkey")
}

/// A RANGE leaf of a category partition: `commits_<category>__<suffix>`, where the suffix is the leaf's lower
/// bound as `YYYYMM` (month buckets) or `YYYYMMDD` (day buckets, and the one leaf bridging a bucket change).
pub(crate) fn leaf(category: &str, suffix: &str) -> String {
    format!("commits_{category}{SEPARATOR}{suffix}")
}

/// A regular expression matching the names of a category's leaves.
pub(crate) fn leaf_pattern(category: &str) -> String {
    format!("^commits_{category}{SEPARATOR}[0-9]{{6}}([0-9]{{2}})?$")
}

/// The leaf's unique `(stream_id, last_version)` index: `<leaf>_s`.
pub(crate) fn leaf_stream_index(leaf: &str) -> String {
    format!("{leaf}_s")
}

/// Hash partition `remainder` of `table` (`streams` or `snapshots`) in a store with `partitions` of them, as
/// store migration 0001 names it: `<table>_p<remainder>`, padded with zeros to 3 digits, or to as many as
/// the highest remainder has.
#[cfg(test)]
pub(crate) fn hash_partition(table: &str, remainder: u16, partitions: u16) -> String {
    let digits = partitions.saturating_sub(1).to_string().len().max(3);
    format!("{table}_p{remainder:0digits$}")
}

/// Log partition `n`, holding positions `[n * 10^8, (n + 1) * 10^8)`: `log_<n, at least 4 digits>`.
pub(crate) fn log_partition(n: i64) -> String {
    format!("log_{n:04}")
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The name PostgreSQL gives a partition's copy of the parent's primary key. It truncates the table part
    /// of longer names, which could make two leaves' names collide.
    fn primary_key(table: &str) -> String {
        format!("{table}_pkey")
    }

    #[test]
    fn plain_identifiers_fit_and_need_no_quoting() {
        assert!(is_plain_identifier("commits_jobs__202611"));
        assert!(!is_plain_identifier("Commits"));
        assert!(!is_plain_identifier("commits\"; drop"));
        assert!(!is_plain_identifier(&"a".repeat(MAX_IDENTIFIER + 1)));
    }

    /// Every name the crate creates, or PostgreSQL derives from one, fits for the longest valid category with
    /// the longest leaf suffix. Store names only qualify these names, so their length does not matter.
    #[test]
    fn generated_names_fit_for_the_longest_category() {
        let category = "c".repeat(pges_core::category::CategoryName::MAX_LEN);

        let partition = category_partition(&category);
        let day_leaf = leaf(&category, "20261124");
        let month_leaf = leaf(&category, "202611");
        let names = [
            category_partition_key(&category),
            leaf_stream_index(&day_leaf),
            leaf_stream_index(&month_leaf),
            primary_key(&day_leaf),
            primary_key(&month_leaf),
            BOUND.to_owned(),
            log_partition(i64::MAX / 100_000_000),
            primary_key(&log_partition(i64::MAX / 100_000_000)),
            partition,
            day_leaf,
            month_leaf,
        ];

        for name in &names {
            assert!(
                name.len() <= MAX_IDENTIFIER,
                "{name} is {} bytes",
                name.len()
            );
            assert!(is_plain_identifier(name), "{name}");
        }
        assert_eq!(names[1].len(), 60, "the longest name the crate chooses");
        assert_eq!(names[3].len(), 63, "the longest name PostgreSQL derives");
    }

    /// Hash partitions keep the 3-digit names up to 1000 partitions, widen above, never collide, and fit.
    #[test]
    fn hash_partition_names_widen_with_the_partitions() {
        assert_eq!(hash_partition("streams", 7, 64), "streams_p007");
        assert_eq!(hash_partition("streams", 999, 1000), "streams_p999");
        assert_eq!(hash_partition("streams", 7, 1001), "streams_p0007");
        assert_eq!(hash_partition("streams", 1000, 1001), "streams_p1000");
        assert_eq!(
            hash_partition("snapshots", 65534, u16::MAX),
            "snapshots_p65534"
        );

        for partitions in [1, 999, 1000, 1001, 10_000, 10_001, u16::MAX] {
            let names: std::collections::HashSet<String> = (0..partitions)
                .map(|remainder| hash_partition("snapshots", remainder, partitions))
                .collect();
            assert_eq!(names.len(), usize::from(partitions));
            assert!(names.iter().all(|name| is_plain_identifier(name)));
        }
    }

    /// No name built for one category equals one built for another, for categories named like each other's
    /// leaves, leaf indexes and keys, and none takes a name reserved for objects on `commits` itself
    /// (`commits__*`).
    #[test]
    fn names_of_different_categories_never_collide() {
        let categories = [
            "a",
            "a_",
            "a_202611",
            "a_20261124",
            "a_202611_s",
            "a_202611_pkey",
            "a_pkey",
            "a_s",
            "pkey",
        ];
        let mut seen = std::collections::HashMap::new();

        for category in categories {
            assert!(
                pges_core::category::CategoryName::leak(category.to_owned()).is_ok(),
                "{category}"
            );
            let mut names = vec![
                category_partition(category),
                category_partition_key(category),
            ];
            for suffix in ["202611", "20261124"] {
                let leaf = leaf(category, suffix);
                names.push(leaf_stream_index(&leaf));
                names.push(primary_key(&leaf));
                names.push(leaf);
            }
            for name in names {
                assert!(!name.starts_with("commits__"), "{name}");
                if let Some(other) = seen.insert(name.clone(), category) {
                    panic!("{name} is built for {other} and {category}");
                }
            }
        }
    }
}
