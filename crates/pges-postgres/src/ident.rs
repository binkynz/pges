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

/// The LIST partition of `commits` holding one category: `commits_<category>`.
pub(crate) fn category_partition(category: &str) -> String {
    format!("commits_{category}")
}

/// A RANGE leaf of a category partition: `commits_<category>_<suffix>`, where the suffix is the leaf's lower
/// bound as `YYYYMM` (month buckets) or `YYYYMMDD` (day buckets, and the one leaf bridging a bucket change).
pub(crate) fn leaf(category: &str, suffix: &str) -> String {
    format!("commits_{category}_{suffix}")
}

/// The leaf's unique `(stream_id, last_version)` index: `<leaf>_s`.
pub(crate) fn leaf_stream_index(leaf: &str) -> String {
    format!("{leaf}_s")
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
        assert!(is_plain_identifier("commits_jobs_202611"));
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
            primary_key(&partition),
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
        assert_eq!(names[1].len(), 59, "the longest name the crate chooses");
        assert_eq!(names[3].len(), 62, "the longest name PostgreSQL derives");
    }
}
