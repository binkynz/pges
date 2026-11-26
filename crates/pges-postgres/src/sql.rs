//! SQL templates and statement registries. This module supports the pges crates and is not a stable API: it
//! can change in any release.
//!
//! Each pges crate keeps its SQL in `.sql` files under its `sql/` directory, declares every statement in one
//! registry ([`template::Statement`] constants), and renders them with [`template::Values`] once per store.
//! [`statement`] is this crate's registry. A query's parameters and columns have Rust types ([`typed`]), so a
//! call site that binds or reads the wrong type does not compile.

pub mod statement;
pub mod template;
pub mod typed;

use pges_core::{category, store as core_store};

use crate::{sql::typed::Query as _, store};

/// The validated name of an opened store, for rendering another crate's statements.
pub fn store_name(store: &store::Store) -> &core_store::StoreName {
    &store.inner.name
}

/// The statements a store runs on its hot paths, rendered once when it is opened.
pub(crate) struct StoreSql {
    pub(crate) append: typed::Rendered<statement::append::Query>,
    pub(crate) append_many: typed::Rendered<statement::append_many::Query>,
    pub(crate) classify: typed::Rendered<statement::classify::Query>,
    pub(crate) lock_stream: typed::Rendered<statement::lock_stream::Query>,
    pub(crate) try_lock_stream: typed::Rendered<statement::lock_stream::Query>,
    pub(crate) stream_head: typed::Rendered<statement::stream_head::Query>,
    pub(crate) snapshot_read: typed::Rendered<statement::snapshot_read::Query>,
    pub(crate) snapshot_upsert: typed::Rendered<statement::snapshot_upsert::Query>,
    pub(crate) register_types: typed::Rendered<statement::register_types::Query>,
    pub(crate) select_types: typed::Rendered<statement::select_types::Query>,
    pub(crate) category_types: typed::Rendered<statement::category_types::Query>,
    pub(crate) all_types: typed::Rendered<statement::all_types::Query>,
    pub(crate) categories: typed::Rendered<statement::categories::Query>,
    pub(crate) mark_deleted: typed::Rendered<statement::mark_deleted::Query>,
    pub(crate) delete_commits: typed::Rendered<statement::delete_commits::Query>,
    pub(crate) delete_snapshot: typed::Rendered<statement::delete_snapshot::Query>,
}

impl StoreSql {
    pub(crate) fn new(store: &core_store::StoreName, outbox: store::Outbox) -> Self {
        let values = template::Values::new().store(store);
        let (append, append_many) = match outbox {
            store::Outbox::Disabled => (
                statement::append::Query::render(&values),
                statement::append_many::Query::render(&values),
            ),
            store::Outbox::Enabled => (
                typed::Rendered::variant::<statement::append_outbox::Query>(&values),
                typed::Rendered::variant::<statement::append_many_outbox::Query>(&values),
            ),
        };

        Self {
            append,
            append_many,
            classify: statement::classify::Query::render(&values),
            lock_stream: statement::lock_stream::Query::render(&values),
            try_lock_stream: typed::Rendered::variant::<statement::try_lock_stream::Query>(&values),
            stream_head: statement::stream_head::Query::render(&values),
            snapshot_read: statement::snapshot_read::Query::render(&values),
            snapshot_upsert: statement::snapshot_upsert::Query::render(&values),
            register_types: statement::register_types::Query::render(&values),
            select_types: statement::select_types::Query::render(&values),
            category_types: statement::category_types::Query::render(&values),
            all_types: statement::all_types::Query::render(&values),
            categories: statement::categories::Query::render(&values),
            mark_deleted: statement::mark_deleted::Query::render(&values),
            delete_commits: statement::delete_commits::Query::render(&values),
            delete_snapshot: statement::delete_snapshot::Query::render(&values),
        }
    }
}

/// Reads one stream's commits from its category's partition, one page at a time. Rendered once per category
/// a store learns about.
pub(crate) fn read_stream(
    store: &core_store::StoreName,
    category: category::CategoryName,
) -> typed::Rendered<statement::read_stream::Query> {
    statement::read_stream::Query::render(&template::Values::new().store(store).category(category))
}

/// The administration statements that name only the store, rendered once per store. Statements that name a
/// leaf or a log partition are rendered when that partition is handled.
#[derive(Debug)]
pub(crate) struct AdminSql {
    pub(crate) insert_category: typed::Rendered<statement::insert_category::Query>,
    pub(crate) select_category: typed::Rendered<statement::select_category::Query>,
    pub(crate) set_bucket: typed::Rendered<statement::set_bucket::Query>,
    pub(crate) category_buckets: typed::Rendered<statement::category_buckets::Query>,
    pub(crate) last_position: typed::Rendered<statement::last_position::Query>,
    pub(crate) set_category_retention: typed::Rendered<statement::set_category_retention::Query>,
    pub(crate) retention_categories: typed::Rendered<statement::retention_categories::Query>,
    pub(crate) resequenced: typed::Rendered<statement::resequenced::Query>,
    pub(crate) scan_stop: typed::Rendered<statement::scan_stop::Query>,
    pub(crate) raise_horizon: typed::Rendered<statement::raise_horizon::Query>,
    pub(crate) forget_deleted: typed::Rendered<statement::forget_deleted::Query>,
    pub(crate) horizon: typed::Rendered<statement::horizon::Query>,
    pub(crate) raise_scan_from: typed::Rendered<statement::raise_scan_from::Query>,
    pub(crate) prune_category: typed::Rendered<statement::prune_category::Query>,
}

impl AdminSql {
    pub(crate) fn new(store: &core_store::StoreName) -> Self {
        let values = template::Values::new().store(store);

        Self {
            insert_category: statement::insert_category::Query::render(&values),
            select_category: statement::select_category::Query::render(&values),
            set_bucket: statement::set_bucket::Query::render(&values),
            category_buckets: statement::category_buckets::Query::render(&values),
            last_position: statement::last_position::Query::render(&values),
            set_category_retention: statement::set_category_retention::Query::render(&values),
            retention_categories: statement::retention_categories::Query::render(&values),
            resequenced: statement::resequenced::Query::render(&values),
            scan_stop: statement::scan_stop::Query::render(&values),
            raise_horizon: statement::raise_horizon::Query::render(&values),
            forget_deleted: statement::forget_deleted::Query::render(&values),
            horizon: statement::horizon::Query::render(&values),
            raise_scan_from: statement::raise_scan_from::Query::render(&values),
            prune_category: statement::prune_category::Query::render(&values),
        }
    }
}
