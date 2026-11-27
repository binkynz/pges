//! Category names.

use std::fmt;

/// The name of a category: a group of streams of one aggregate type inside a store, such as `jobs`.
///
/// Names match `^[a-z][a-z0-9_]{0,39}$` and do not contain `__`, because they become part of partition names,
/// where `__` separates the category from the rest of the name. Write a literal name
/// with [`category!`](crate::category!), which checks it at compile time. Names read at run time go through
/// [`CategoryName::leak`].
///
/// ```
/// use pges_core::category;
///
/// const JOBS: category::CategoryName = category!("jobs");
///
/// assert_eq!(JOBS.as_str(), "jobs");
/// assert_eq!(category!("orders").as_str(), "orders");
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct CategoryName(&'static str);

impl CategoryName {
    /// Longest allowed name, in bytes.
    pub const MAX_LEN: usize = 40;

    /// Validates a static name.
    pub const fn new(name: &'static str) -> Result<Self, CategoryError> {
        match validate(name) {
            Ok(()) => Ok(Self(name)),
            Err(error) => Err(error),
        }
    }

    /// Validates a name read at run time and leaks it, so it can be used as a `CategoryName`.
    ///
    /// Call it once per category (the store caches the result); every call leaks a new allocation.
    pub fn leak(name: String) -> Result<Self, CategoryError> {
        validate(&name)?;

        Ok(Self(Box::leak(name.into_boxed_str())))
    }

    /// The name.
    pub const fn as_str(&self) -> &'static str {
        self.0
    }
}

impl fmt::Display for CategoryName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.0)
    }
}

/// A category's id within its store (`categories.category_id`).
///
/// Ids are assigned by the store when the category is registered and never reused.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct CategoryId(i32);

impl CategoryId {
    /// Wraps an id read from the store.
    pub const fn new(id: i32) -> Self {
        Self(id)
    }

    /// The number.
    pub const fn get(self) -> i32 {
        self.0
    }
}

impl fmt::Display for CategoryId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

/// Why a category name is invalid.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum CategoryError {
    /// The name is empty.
    #[error("category name is empty")]
    Empty,
    /// The name is longer than [`CategoryName::MAX_LEN`] bytes.
    #[error("category name is longer than 40 bytes")]
    TooLong,
    /// The name does not start with `a`–`z`.
    #[error("category name must start with a lowercase ASCII letter")]
    InvalidStart,
    /// The name contains a byte other than `a`–`z`, `0`–`9` or `_`.
    #[error("category name may only contain a-z, 0-9 and _")]
    InvalidCharacter,
    /// The name contains `__`, which separates a category's name from the rest of its partitions' names.
    #[error("category name may not contain __")]
    DoubleUnderscore,
}

/// A [`CategoryName`](crate::category::CategoryName) from a literal, checked at compile time.
///
/// The macro expands to a block with a `const` item that checks the name, so an invalid name is a compile
/// error (`cargo check` reports it too, also inside a generic function) and never a run-time panic.
/// `use pges_core::category;` imports both this macro and the module.
///
/// ```
/// use pges_core::category;
///
/// const JOBS: category::CategoryName = category!("jobs");
/// let orders = category!("orders");
///
/// assert_eq!(JOBS.as_str(), "jobs");
/// assert_eq!(orders.as_str(), "orders");
/// ```
///
/// ```compile_fail
/// let invalid = pges_core::category!("Jobs");
/// ```
#[macro_export]
macro_rules! category {
    ($name:expr $(,)?) => {{
        const NAME: $crate::category::CategoryName =
            match $crate::category::CategoryName::new($name) {
                ::core::result::Result::Ok(name) => name,
                #[allow(clippy::panic)]
                ::core::result::Result::Err(_) => {
                    ::core::panic!(
                        "invalid category name: it must match ^[a-z][a-z0-9_]{{0,39}}$ and not contain __"
                    )
                }
            };

        NAME
    }};
}

/// `^[a-z][a-z0-9_]{0,39}$`: the rule category and store names share.
pub(crate) const fn validate_identifier(name: &str) -> Result<(), CategoryError> {
    let bytes = name.as_bytes();
    if bytes.is_empty() {
        return Err(CategoryError::Empty);
    }
    if bytes.len() > CategoryName::MAX_LEN {
        return Err(CategoryError::TooLong);
    }
    if !bytes[0].is_ascii_lowercase() {
        return Err(CategoryError::InvalidStart);
    }

    let mut i = 1;
    while i < bytes.len() {
        let byte = bytes[i];
        if !(byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_') {
            return Err(CategoryError::InvalidCharacter);
        }
        i += 1;
    }

    Ok(())
}

pub(crate) const fn validate(name: &str) -> Result<(), CategoryError> {
    if let Err(error) = validate_identifier(name) {
        return Err(error);
    }

    let bytes = name.as_bytes();
    let mut i = 1;
    while i < bytes.len() {
        if bytes[i] == b'_' && bytes[i - 1] == b'_' {
            return Err(CategoryError::DoubleUnderscore);
        }
        i += 1;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validates_names() {
        assert!(CategoryName::new("jobs").is_ok());
        assert!(CategoryName::new("job_items_2").is_ok());
        assert_eq!(CategoryName::new(""), Err(CategoryError::Empty));
        assert_eq!(CategoryName::new("Jobs"), Err(CategoryError::InvalidStart));
        assert_eq!(CategoryName::new("2jobs"), Err(CategoryError::InvalidStart));
        assert_eq!(
            CategoryName::new("jobs-x"),
            Err(CategoryError::InvalidCharacter)
        );
        assert_eq!(
            CategoryName::new("a".repeat(41).leak()),
            Err(CategoryError::TooLong)
        );
        assert!(CategoryName::new("a".repeat(40).leak()).is_ok());
        assert!(CategoryName::new("a_202611").is_ok());
        assert!(CategoryName::new("jobs_").is_ok());
        assert_eq!(
            CategoryName::new("jobs__202611"),
            Err(CategoryError::DoubleUnderscore)
        );
        assert_eq!(
            CategoryName::new("jobs__"),
            Err(CategoryError::DoubleUnderscore)
        );
    }

    #[test]
    fn macro_checks_literals_in_const() {
        const JOBS: CategoryName = crate::category!("jobs");

        assert_eq!(JOBS, CategoryName::new("jobs").unwrap());
        assert_eq!(crate::category!("job_items_2").as_str(), "job_items_2");
    }

    #[test]
    fn leaks_runtime_names() {
        let name = CategoryName::leak("orders".to_string()).unwrap();

        assert_eq!(name.as_str(), "orders");
        assert!(CategoryName::leak("Orders".to_string()).is_err());
    }
}
