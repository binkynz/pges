//! Category names read from the database.

use std::{
    collections::BTreeMap,
    sync::{Mutex, PoisonError},
};

use pges_core::category;

/// A category name read from the database, leaked once per process however many subscriptions use it.
/// `None` if it does not pass the category name rule, which the `categories` CHECK makes impossible.
pub(crate) fn intern(name: &str) -> Option<category::CategoryName> {
    static INTERNED: Mutex<BTreeMap<String, category::CategoryName>> = Mutex::new(BTreeMap::new());

    let mut interned = INTERNED.lock().unwrap_or_else(PoisonError::into_inner);
    if let Some(existing) = interned.get(name) {
        return Some(*existing);
    }

    let leaked = category::CategoryName::leak(name.to_owned()).ok()?;
    interned.insert(name.to_owned(), leaked);

    Some(leaked)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn interns_each_name_once() {
        let a = intern("interned_jobs").unwrap();
        let b = intern("interned_jobs").unwrap();

        assert!(std::ptr::eq(a.as_str(), b.as_str()));
        assert!(intern("Not Valid").is_none());
    }
}
