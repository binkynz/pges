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

use uuid::Uuid;

use pges_core::{category, store as core_store};

use crate::{route, sql::typed::Query as _, store};

/// The validated name of an opened store, for rendering another crate's statements.
pub fn store_name(store: &store::Store) -> &core_store::StoreName {
    &store.inner.name
}

/// The statements a store runs on its hot paths, rendered once when it is opened.
pub(crate) struct StoreSql {
    append: Append,
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
    /// The statements of `store`. With `leaves`, a single append names its stream's partition of `streams`
    /// (`route`), one prepared statement per partition, when they fit in `cache_capacity` (the pool's
    /// statement cache per connection) with [`STATEMENT_CACHE_RESERVE`] to spare. Otherwise it goes through
    /// `streams` itself, one prepared statement: more partition statements than the cache holds would evict
    /// each other and every other statement.
    pub(crate) fn new(
        store: &core_store::StoreName,
        outbox: store::Outbox,
        leaves: Option<route::Leaves>,
        cache_capacity: usize,
    ) -> Self {
        let values = template::Values::new().store(store);
        let append_many = match outbox {
            store::Outbox::Disabled => statement::append_many::Query::render(&values),
            store::Outbox::Enabled => {
                typed::Rendered::variant::<statement::append_many_outbox::Query>(&values)
            }
        };
        let append = match leaves.filter(|leaves| fits(leaves, cache_capacity)) {
            Some(leaves) => Append::Routed {
                appends: leaves
                    .names()
                    .iter()
                    .map(|leaf| render_append(&values.clone().stream_table(Some(leaf)), outbox))
                    .collect(),
                leaves,
            },
            None => Append::Parent(render_append(&values.clone().stream_table(None), outbox)),
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

impl StoreSql {
    /// The single append for `stream_id`: the one naming its partition when the store routes appends.
    pub(crate) fn append(&self, stream_id: Uuid) -> &typed::Rendered<statement::append::Query> {
        match &self.append {
            Append::Parent(append) => append,
            Append::Routed { leaves, appends } => &appends[leaves.index(stream_id)],
        }
    }
}

/// Whether one prepared append per partition of `leaves` fits a statement cache of `capacity` per
/// connection, with [`STATEMENT_CACHE_RESERVE`] to spare.
fn fits(leaves: &route::Leaves, capacity: usize) -> bool {
    leaves.names().len() + STATEMENT_CACHE_RESERVE <= capacity
}

/// Statement cache entries per connection left to the other statements when the per-partition appends are
/// kept prepared: the store's own (about 20, one stream read per category) and the application's. sqlx's
/// default capacity of 100 keeps the default 64 partitions prepared.
pub(crate) const STATEMENT_CACHE_RESERVE: usize = 36;

/// Lists the hash partitions of a store's `streams`, for [`route::Leaves::from_catalog`].
pub(crate) fn stream_leaves(
    store: &core_store::StoreName,
) -> typed::Rendered<statement::stream_leaves::Query> {
    statement::stream_leaves::Query::render(&template::Values::new().store(store))
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

/// How a store's single appends reach `streams`.
enum Append {
    /// Through the partitioned table. The plan cache decides whether a plan prunes it to one partition: a
    /// generic plan locks every partition.
    Parent(typed::Rendered<statement::append::Query>),
    /// To the partition holding the stream: one statement per partition, by remainder.
    Routed {
        leaves: route::Leaves,
        appends: Vec<typed::Rendered<statement::append::Query>>,
    },
}

fn render_append(
    values: &template::Values,
    outbox: store::Outbox,
) -> typed::Rendered<statement::append::Query> {
    match outbox {
        store::Outbox::Disabled => statement::append::Query::render(values),
        store::Outbox::Enabled => {
            typed::Rendered::variant::<statement::append_outbox::Query>(values)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The table the append of a store with `partitions` updates, when its pool's connections cache
    /// `capacity` statements.
    fn append_target(partitions: u16, capacity: usize) -> String {
        let probes = route::probes();
        let rows: Vec<statement::stream_leaves::Row> = (0..partitions)
            .map(|remainder| statement::stream_leaves::Row {
                leaf: format!("streams_p{remainder:03}"),
                modulus: Some(i32::from(partitions)),
                remainder: Some(i32::from(remainder)),
                probes: Some(
                    (1..)
                        .zip(&probes)
                        .filter(|(_, probe)| route::remainder(**probe, partitions) == remainder)
                        .map(|(position, _)| position)
                        .collect(),
                ),
            })
            .collect();
        let leaves = route::Leaves::from_catalog(&rows, &probes);
        assert!(leaves.is_some());
        let sql = StoreSql::new(
            &pges_core::store_name!("es"),
            store::Outbox::Disabled,
            leaves,
            capacity,
        );
        let append = sql.append(probes[0]).as_str();
        let start = append.find("UPDATE es.").expect("an update of streams") + "UPDATE es.".len();

        append[start..]
            .split_whitespace()
            .next()
            .unwrap()
            .to_owned()
    }

    #[test]
    fn appends_name_partitions_only_while_their_statements_fit_the_statement_cache() {
        let partition_64 = format!("streams_p{:03}", route::remainder(route::probes()[0], 64));
        let partition_256 = format!("streams_p{:03}", route::remainder(route::probes()[0], 256));

        assert_eq!(append_target(64, 100), partition_64);
        assert_eq!(append_target(64, 99), "streams");
        assert_eq!(append_target(64, 0), "streams");
        assert_eq!(append_target(256, 100), "streams");
        assert_eq!(append_target(256, 291), "streams");
        assert_eq!(append_target(256, 292), partition_256);
    }

    #[test]
    fn without_partitions_the_append_goes_through_streams() {
        let sql = StoreSql::new(
            &pges_core::store_name!("es"),
            store::Outbox::Enabled,
            None,
            100,
        );
        let append = sql.append(uuid::Uuid::nil());

        assert!(append.as_str().contains("UPDATE es.streams s"));
        assert!(append.as_str().contains("INSERT INTO es.pending"));
    }
}
