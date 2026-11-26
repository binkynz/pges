//! The client-side cache of a store's categories and event type ids.

use std::{
    any::TypeId,
    collections::{BTreeMap, HashMap, HashSet},
    sync::{Arc, Mutex, PoisonError},
};

use pges_core::{category, frame, store};

use crate::sql;

#[derive(Default)]
pub(crate) struct Catalog {
    ids_by_name: HashMap<&'static str, category::CategoryId>,
    categories: HashMap<category::CategoryId, Category>,
    registered: HashSet<TypeId>,
}

#[derive(Clone)]
pub(crate) struct Category {
    pub(crate) id: category::CategoryId,
    pub(crate) name: category::CategoryName,
    pub(crate) read_stream: Arc<sql::typed::Rendered<sql::statement::read_stream::Query>>,
    pub(crate) types: Arc<TypeTable>,
}

/// Event type ids of one category, in both directions. Replaced as a whole when it changes.
#[derive(Default)]
pub(crate) struct TypeTable {
    by_name: HashMap<String, frame::EventTypeId>,
    by_id: HashMap<u32, String>,
}

impl TypeTable {
    pub(crate) fn id(&self, name: &str) -> Option<frame::EventTypeId> {
        self.by_name.get(name).copied()
    }

    pub(crate) fn name(&self, id: frame::EventTypeId) -> Option<&str> {
        self.by_id.get(&id.get()).map(String::as_str)
    }

    fn contains_all(&self, names: &[&str]) -> bool {
        names.iter().all(|name| self.by_name.contains_key(*name))
    }

    fn with(&self, rows: &[(i32, String)]) -> Self {
        let mut table = Self {
            by_name: self.by_name.clone(),
            by_id: self.by_id.clone(),
        };
        for (id, name) in rows {
            let Ok(id) = u32::try_from(*id) else { continue };
            table
                .by_name
                .insert(name.clone(), frame::EventTypeId::new(id));
            table.by_id.insert(id, name.clone());
        }

        table
    }
}

impl Catalog {
    pub(crate) fn category(&self, name: category::CategoryName) -> Option<Category> {
        let id = self.ids_by_name.get(name.as_str())?;

        self.categories.get(id).cloned()
    }

    pub(crate) fn category_by_id(&self, id: category::CategoryId) -> Option<Category> {
        self.categories.get(&id).cloned()
    }

    /// The category, if `E` was registered and its types are cached.
    pub(crate) fn registered(&self, key: TypeId, name: category::CategoryName) -> Option<Category> {
        if !self.registered.contains(&key) {
            return None;
        }

        self.category(name)
    }

    /// Marks `key` registered when every one of `types` has an id.
    pub(crate) fn mark_registered(
        &mut self,
        key: TypeId,
        category_id: category::CategoryId,
        types: &[&str],
    ) -> bool {
        let complete = self
            .categories
            .get(&category_id)
            .is_some_and(|c| c.types.contains_all(types));
        if complete {
            self.registered.insert(key);
        }

        complete
    }

    /// Adds categories read from the database, keeping the type ids already cached.
    pub(crate) fn add_categories(
        &mut self,
        store: &store::StoreName,
        rows: &[(category::CategoryId, String)],
    ) {
        for (id, name) in rows {
            if self.categories.contains_key(id) {
                continue;
            }
            let Some(name) = intern(name) else { continue };

            self.ids_by_name.insert(name.as_str(), *id);
            self.categories.insert(
                *id,
                Category {
                    id: *id,
                    name,
                    read_stream: Arc::new(sql::read_stream(store, name)),
                    types: Arc::default(),
                },
            );
        }
    }

    /// Adds event types read from the database to a category's table.
    pub(crate) fn add_types(
        &mut self,
        category_id: category::CategoryId,
        rows: &[(i32, String)],
    ) -> Arc<TypeTable> {
        let Some(category) = self.categories.get_mut(&category_id) else {
            return Arc::default();
        };
        category.types = Arc::new(category.types.with(rows));

        category.types.clone()
    }
}

/// Category names read from the database are leaked once per process, however many stores are opened.
/// Names that fail validation are skipped; the `categories` table's CHECK makes that impossible.
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

    #[test]
    fn registers_only_when_every_type_has_an_id() {
        struct Marker;
        let mut catalog = Catalog::default();
        let jobs = category::CategoryId::new(3);
        let store = pges_core::store_name!("s1");
        catalog.add_categories(&store, &[(jobs, "jobs".into())]);
        catalog.add_types(jobs, &[(10, "job.created".into())]);

        let key = TypeId::of::<Marker>();
        assert!(!catalog.mark_registered(key, jobs, &["job.created", "job.assigned"]));

        let types = catalog.add_types(jobs, &[(11, "job.assigned".into())]);
        assert!(catalog.mark_registered(key, jobs, &["job.created", "job.assigned"]));
        assert_eq!(types.id("job.assigned"), Some(frame::EventTypeId::new(11)));
        assert_eq!(types.name(frame::EventTypeId::new(10)), Some("job.created"));
    }
}
