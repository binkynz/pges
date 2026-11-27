//! The registry of pges-sequencer: every statement the crate runs.
//!
//! Each statement is a constant, listed in [`ALL`]. A query is declared with
//! [`typed_query!`](pges_postgres::typed_query), which adds a module of the same name in lower case with its
//! typed parameters and row, and is listed in [`TYPED`] too. `tests/statement.rs` checks every one against
//! PostgreSQL 18.

use sqlx::postgres::types::PgInterval;
use uuid::Uuid;

use pges_postgres::sql::{template, typed};

/// A file below `sql/`, embedded.
macro_rules! sql_file {
    ($path:literal) => {
        template::File {
            path: $path,
            text: include_str!(concat!("../../sql/", $path)),
        }
    };
}

pges_postgres::typed_query! {
    /// Takes the lease if it is free or expired.
    pub LEASE_ACQUIRE, mod lease_acquire {
        name: "lease/acquire",
        files: [sql_file!("lease/acquire.sql")],
        params<'q> {
            ttl: PgInterval,
            leader: Option<&'q str>,
        },
        row {
            fence: i64,
            source: Option<String>,
            source_token: Option<Vec<u8>>,
            halted: Option<String>,
        },
    }
}

pges_postgres::typed_query! {
    /// Takes the lease for a resequence, only while sequencing is halted.
    pub LEASE_ACQUIRE_HALTED, mod lease_acquire_halted {
        name: "lease/acquire_halted",
        files: [sql_file!("lease/acquire_halted.sql")],
        params {
            ttl: PgInterval,
        },
        row {
            fence: i64,
        },
    }
}

pges_postgres::typed_query! {
    /// Whether sequencing is halted.
    pub LEASE_HALTED, mod lease_halted {
        name: "lease/halted",
        files: [sql_file!("lease/halted.sql")],
        params {},
        row {
            halted: bool as "?column?",
        },
    }
}

pges_postgres::typed_query! {
    /// Extends the lease.
    pub LEASE_RENEW, mod lease_renew {
        name: "lease/renew",
        files: [sql_file!("lease/renew.sql")],
        params {
            ttl: PgInterval,
            fence: i64,
            shared_version: i32,
            store_version: i32,
        },
        row {},
    }
}

pges_postgres::typed_query! {
    /// Gives the lease up.
    pub LEASE_RELEASE, mod lease_release {
        name: "lease/release",
        files: [sql_file!("lease/release.sql")],
        params {
            fence: i64,
        },
        row {},
    }
}

pges_postgres::typed_query! {
    /// Records why sequencing must not resume by itself.
    pub LEASE_HALT, mod lease_halt {
        name: "lease/halt",
        files: [sql_file!("lease/halt.sql")],
        params<'q> {
            fence: i64,
            reason: &'q str,
        },
        row {},
    }
}

pges_postgres::typed_query! {
    /// Records why sequencing must not resume by itself, outside a term, while no process holds the lease.
    pub LEASE_HALT_UNLEASED, mod lease_halt_unleased {
        name: "lease/halt_unleased",
        files: [sql_file!("lease/halt_unleased.sql")],
        params<'q> {
            reason: &'q str,
        },
        row {},
    }
}

pges_postgres::typed_query! {
    /// The durable token under a fence.
    pub LEASE_STORED_TOKEN, mod lease_stored_token {
        name: "lease/stored_token",
        files: [sql_file!("lease/stored_token.sql")],
        params {
            fence: i64,
        },
        row {
            source: Option<String>,
            source_token: Option<Vec<u8>>,
        },
    }
}

pges_postgres::typed_query! {
    /// The fence check that opens every batch.
    pub LEASE_HOLD_FENCE, mod lease_hold_fence {
        name: "lease/hold_fence",
        files: [sql_file!("lease/hold_fence.sql")],
        params {
            fence: i64,
        },
        row {
            held: i32 as "?column?",
        },
    }
}

pges_postgres::typed_query! {
    /// Stores the resume token under the fence.
    pub LEASE_STORE_TOKEN, mod lease_store_token {
        name: "lease/store_token",
        files: [sql_file!("lease/store_token.sql")],
        params<'q> {
            fence: i64,
            source: &'q str,
            token: &'q [u8],
        },
        row {},
    }
}

/// Makes the current transaction commit synchronously.
pub const SYNCHRONOUS_COMMIT: template::Statement = template::Statement {
    name: "transaction/synchronous_commit",
    files: &[sql_file!("transaction/synchronous_commit.sql")],
    kind: template::Kind::Ddl,
    params: &[],
    columns: &[],
};

/// Opens a transaction in REPEATABLE READ, to import a snapshot.
pub const REPEATABLE_READ: template::Statement = template::Statement {
    name: "transaction/repeatable_read",
    files: &[sql_file!("transaction/repeatable_read.sql")],
    kind: template::Kind::Ddl,
    params: &[],
    columns: &[],
};

/// Imports a snapshot a new replication slot exported.
pub const SET_SNAPSHOT: template::Statement = template::Statement {
    name: "transaction/snapshot",
    files: &[sql_file!("transaction/snapshot.sql")],
    kind: template::Kind::Ddl,
    params: &[],
    columns: &[],
};

pges_postgres::typed_query! {
    /// Every store with its coarse high-water mark.
    pub LOG_STORES, mod log_stores {
        name: "log/stores",
        files: [sql_file!("log/stores.sql")],
        params {},
        row {
            store_id: i32,
            schema_name: String,
            last_position: i64,
        },
    }
}

pges_postgres::typed_query! {
    /// One store, by id.
    pub LOG_STORE, mod log_store {
        name: "log/store",
        files: [sql_file!("log/store.sql")],
        params {
            store_id: i32,
        },
        row {
            schema_name: String,
            last_position: i64,
        },
    }
}

pges_postgres::typed_query! {
    /// Writes a store's log rows.
    pub LOG_INSERT, mod log_insert {
        name: "log/insert",
        files: [sql_file!("log/insert.sql")],
        params<'q> {
            last_positions: &'q [i64],
            commit_ids: &'q [Uuid],
            category_ids: &'q [i32],
            event_counts: &'q [i16],
        },
        row {},
    }
}

pges_postgres::typed_query! {
    /// Writes the coarse high-water marks of a store's categories.
    pub LOG_SET_CATEGORY_POSITIONS, mod log_set_category_positions {
        name: "log/set_category_positions",
        files: [sql_file!("log/set_category_positions.sql")],
        params<'q> {
            category_ids: &'q [i32],
            positions: &'q [i64],
        },
        row {},
    }
}

pges_postgres::typed_query! {
    /// Writes a store's coarse high-water mark.
    pub LOG_SET_STORE_POSITION, mod log_set_store_position {
        name: "log/set_store_position",
        files: [sql_file!("log/set_store_position.sql")],
        params {
            last_position: i64,
            store_id: i32,
        },
        row {},
    }
}

pges_postgres::typed_query! {
    /// A store's newest position.
    pub LOG_NEWEST, mod log_newest {
        name: "log/newest",
        files: [sql_file!("log/newest.sql")],
        params {},
        row {
            last_position: i64,
        },
    }
}

pges_postgres::typed_query! {
    /// The newest position of each category of a store.
    pub LOG_CATEGORY_NEWEST, mod log_category_newest {
        name: "log/category_newest",
        files: [sql_file!("log/category_newest.sql")],
        params {},
        row {
            category_id: i32,
            last_position: i64,
        },
    }
}

pges_postgres::typed_query! {
    /// The ids of a store's newest commits.
    pub LOG_TAIL, mod log_tail {
        name: "log/tail",
        files: [sql_file!("log/tail.sql")],
        params {
            limit: i64,
        },
        row {
            commit_id: Uuid,
        },
    }
}

pges_postgres::typed_query! {
    /// The ids of a store's commits above a position.
    pub LOG_ABOVE, mod log_above {
        name: "log/above",
        files: [sql_file!("log/above.sql")],
        params {
            after: i64,
        },
        row {
            commit_id: Uuid,
        },
    }
}

pges_postgres::typed_query! {
    /// The WAL insert position.
    pub WAL_INSERT_LSN, mod wal_insert_lsn {
        name: "log/wal_insert_lsn",
        files: [sql_file!("log/wal_insert_lsn.sql")],
        params {},
        row {
            lsn: i64 as "int8",
        },
    }
}

pges_postgres::typed_query! {
    /// Sends one notification per store.
    pub NOTIFY, mod notify {
        name: "notify/send",
        files: [sql_file!("notify/send.sql")],
        params {
            channels: Vec<String>,
            payloads: Vec<String>,
        },
        row {
            pg_notify: (),
        },
    }
}

pges_postgres::typed_query! {
    /// The outbox stores.
    pub OUTBOX_STORES, mod outbox_stores {
        name: "outbox/stores",
        files: [sql_file!("outbox/stores.sql")],
        params {},
        row {
            store_id: i32,
            schema_name: String,
        },
    }
}

pges_postgres::typed_query! {
    /// Takes the oldest queued commits of one outbox store.
    pub OUTBOX_DRAIN, mod outbox_drain {
        name: "outbox/drain",
        files: [sql_file!("outbox/drain.sql")],
        params {
            limit: i64,
        },
        row {
            commit_id: Uuid,
            category_id: i32,
            event_count: i16,
        },
    }
}

/// The start of the outbox recheck.
pub const OUTBOX_PROBE_HEAD: template::File = sql_file!("outbox/probe_head.sql");

/// One outbox store's part of the recheck.
pub const OUTBOX_PROBE_STORE: template::File = sql_file!("outbox/probe_store.sql");

/// What joins two stores' parts of the recheck.
pub const OUTBOX_PROBE_UNION: template::File = sql_file!("outbox/probe_union.sql");

/// The end of the outbox recheck.
pub const OUTBOX_PROBE_TAIL: template::File = sql_file!("outbox/probe_tail.sql");

pges_postgres::typed_query! {
    /// The outbox recheck, as composed for two stores (here the same one twice). The source composes it for
    /// its outbox stores from the same files, with [`typed::Rendered::compose`].
    pub OUTBOX_PROBE, mod outbox_probe {
        name: "outbox/probe",
        files: [
            OUTBOX_PROBE_HEAD,
            OUTBOX_PROBE_STORE,
            OUTBOX_PROBE_UNION,
            OUTBOX_PROBE_STORE,
            OUTBOX_PROBE_TAIL,
        ],
        params {},
        row {
            pending: i32 as "?column?",
        },
    }
}

pges_postgres::typed_query! {
    /// The outbox recheck without outbox stores.
    pub OUTBOX_PROBE_NONE, mod outbox_probe_none {
        name: "outbox/probe_none",
        files: [sql_file!("outbox/probe_none.sql")],
        like outbox_probe,
    }
}

pges_postgres::typed_query! {
    /// The current database's oid and name.
    pub LOGICAL_DATABASE, mod logical_database {
        name: "logical/database",
        files: [sql_file!("logical/database.sql")],
        params {},
        row {
            oid: i64,
            name: String,
        },
    }
}

pges_postgres::typed_query! {
    /// Every store, with whether it uses the outbox.
    pub LOGICAL_STORES, mod logical_stores {
        name: "logical/stores",
        files: [sql_file!("logical/stores.sql")],
        params {},
        row {
            store_id: i32,
            schema_name: String,
            outbox: bool,
        },
    }
}

pges_postgres::typed_query! {
    /// Whether the publication exists.
    pub PUBLICATION_EXISTS, mod publication_exists {
        name: "logical/publication_exists",
        files: [sql_file!("logical/publication_exists.sql")],
        params<'q> {
            publication: &'q str,
        },
        row {
            exists: i32 as "?column?",
        },
    }
}

/// Creates the publication.
pub const CREATE_PUBLICATION: template::Statement = template::Statement {
    name: "logical/create_publication",
    files: &[sql_file!("logical/create_publication.sql")],
    kind: template::Kind::Ddl,
    params: &[],
    columns: &[],
};

pges_postgres::typed_query! {
    /// The schemas whose table the publication sends.
    pub PUBLISHED, mod published {
        name: "logical/published",
        files: [sql_file!("logical/published.sql")],
        params<'q> {
            publication: &'q str,
            table: &'q str,
        },
        row {
            schemaname: String,
        },
    }
}

/// Adds a store's commits to the publication.
pub const PUBLISH_STORE: template::Statement = template::Statement {
    name: "logical/publish_store",
    files: &[sql_file!("logical/publish_store.sql")],
    kind: template::Kind::Ddl,
    params: &[],
    columns: &[],
};

pges_postgres::typed_query! {
    /// The replication slot's state.
    pub SLOT, mod slot {
        name: "logical/slot",
        files: [sql_file!("logical/slot.sql")],
        params<'q> {
            slot: &'q str,
        },
        row {
            database: Option<String>,
            plugin: Option<String>,
            invalidation_reason: Option<String>,
            failover: Option<bool>,
            active: Option<bool>,
            confirmed_flush: Option<i64>,
        },
    }
}

pges_postgres::typed_query! {
    /// Whether a store holds a commit.
    pub STORE_HAS_COMMITS, mod store_has_commits {
        name: "logical/store_has_commits",
        files: [sql_file!("logical/store_has_commits.sql")],
        params {},
        row {
            found: i32 as "?column?",
        },
    }
}

pges_postgres::typed_query! {
    /// The stored token, without a fence.
    pub LOGICAL_STORED_TOKEN, mod logical_stored_token {
        name: "logical/stored_token",
        files: [sql_file!("logical/stored_token.sql")],
        params {},
        row {
            source: Option<String>,
            source_token: Option<Vec<u8>>,
        },
    }
}

pges_postgres::typed_query! {
    /// Stores a new slot's consistent point as the token while none is stored.
    pub STORE_INITIAL_TOKEN, mod store_initial_token {
        name: "logical/store_initial_token",
        files: [sql_file!("logical/store_initial_token.sql")],
        params<'q> {
            source: &'q str,
            token: &'q [u8],
        },
        row {},
    }
}

pges_postgres::typed_query! {
    /// Ends the walsender holding the slot.
    pub TERMINATE_HOLDER, mod terminate_holder {
        name: "logical/terminate_holder",
        files: [sql_file!("logical/terminate_holder.sql")],
        params<'q> {
            slot: &'q str,
        },
        row {
            pg_terminate_backend: bool,
        },
    }
}

pges_postgres::typed_query! {
    /// The shortest `max_txn_age` of the logical stores.
    pub RESEQUENCE_SHORTEST_MAX_TXN_AGE, mod resequence_shortest_max_txn_age {
        name: "resequence/shortest_max_txn_age",
        files: [sql_file!("resequence/shortest_max_txn_age.sql")],
        params {},
        row {
            micros: Option<i64> as "int8",
        },
    }
}

pges_postgres::typed_query! {
    /// A store's search window.
    pub RESEQUENCE_WINDOW, mod resequence_window {
        name: "resequence/window",
        files: [sql_file!("resequence/window.sql")],
        params {
            store_id: i32,
        },
        row {
            window: PgInterval as "?column?",
        },
    }
}

pges_postgres::typed_query! {
    /// Where a store's search starts.
    pub RESEQUENCE_BOUNDS, mod resequence_bounds {
        name: "resequence/bounds",
        files: [sql_file!("resequence/bounds.sql")],
        params {
            window: PgInterval,
        },
        row {
            floor: Uuid,
            stop: Uuid,
        },
    }
}

pges_postgres::typed_query! {
    /// A store's categories with their resequenced ranges.
    pub RESEQUENCE_CATEGORIES, mod resequence_categories {
        name: "resequence/categories",
        files: [sql_file!("resequence/categories.sql")],
        params {},
        row {
            category_id: i32,
            resequenced_from: i64,
            resequenced_through: i64,
        },
    }
}

pges_postgres::typed_query! {
    /// The position after which a category's search looks for log rows.
    pub RESEQUENCE_AFTER, mod resequence_after {
        name: "resequence/after",
        files: [sql_file!("resequence/after.sql")],
        params {
            category_id: i32,
            stop: Uuid,
            resequenced_from: i64,
            resequenced_through: i64,
        },
        row {
            last_position: i64,
        },
    }
}

pges_postgres::typed_query! {
    /// A category's commits without a log row.
    pub RESEQUENCE_ORPHANS, mod resequence_orphans {
        name: "resequence/orphans",
        files: [sql_file!("resequence/orphans.sql")],
        params {
            category_id: i32,
            floor: Uuid,
            after: i64,
        },
        row {
            commit_id: Uuid,
            event_count: i16,
        },
    }
}

pges_postgres::typed_query! {
    /// The fence check that opens a resequence's write.
    pub RESEQUENCE_HOLD_FENCE, mod resequence_hold_fence {
        name: "resequence/hold_fence",
        files: [sql_file!("resequence/hold_fence.sql")],
        params {
            fence: i64,
        },
        row {
            held: i32 as "?column?",
        },
    }
}

pges_postgres::typed_query! {
    /// Records the positions a resequence gave each category.
    pub RESEQUENCE_RECORD_RANGES, mod resequence_record_ranges {
        name: "resequence/record_ranges",
        files: [sql_file!("resequence/record_ranges.sql")],
        params<'q> {
            category_ids: &'q [i32],
            firsts: &'q [i64],
            lasts: &'q [i64],
        },
        row {},
    }
}

pges_postgres::typed_query! {
    /// Stores the token, clears `halted` and releases the lease.
    pub RESEQUENCE_FINISH, mod resequence_finish {
        name: "resequence/finish",
        files: [sql_file!("resequence/finish.sql")],
        params<'q> {
            fence: i64,
            source: &'q str,
            token: &'q [u8],
        },
        row {},
    }
}

/// Every statement of the crate.
pub const ALL: &[&template::Statement] = &[
    &LEASE_ACQUIRE,
    &LEASE_ACQUIRE_HALTED,
    &LEASE_HALTED,
    &LEASE_RENEW,
    &LEASE_RELEASE,
    &LEASE_HALT,
    &LEASE_HALT_UNLEASED,
    &LEASE_STORED_TOKEN,
    &LEASE_HOLD_FENCE,
    &LEASE_STORE_TOKEN,
    &SYNCHRONOUS_COMMIT,
    &REPEATABLE_READ,
    &SET_SNAPSHOT,
    &LOG_STORES,
    &LOG_STORE,
    &LOG_INSERT,
    &LOG_SET_CATEGORY_POSITIONS,
    &LOG_SET_STORE_POSITION,
    &LOG_NEWEST,
    &LOG_CATEGORY_NEWEST,
    &LOG_TAIL,
    &LOG_ABOVE,
    &WAL_INSERT_LSN,
    &NOTIFY,
    &OUTBOX_STORES,
    &OUTBOX_DRAIN,
    &OUTBOX_PROBE,
    &OUTBOX_PROBE_NONE,
    &LOGICAL_DATABASE,
    &LOGICAL_STORES,
    &PUBLICATION_EXISTS,
    &CREATE_PUBLICATION,
    &PUBLISHED,
    &PUBLISH_STORE,
    &SLOT,
    &STORE_HAS_COMMITS,
    &LOGICAL_STORED_TOKEN,
    &STORE_INITIAL_TOKEN,
    &TERMINATE_HOLDER,
    &RESEQUENCE_SHORTEST_MAX_TXN_AGE,
    &RESEQUENCE_WINDOW,
    &RESEQUENCE_BOUNDS,
    &RESEQUENCE_CATEGORIES,
    &RESEQUENCE_AFTER,
    &RESEQUENCE_ORPHANS,
    &RESEQUENCE_HOLD_FENCE,
    &RESEQUENCE_RECORD_RANGES,
    &RESEQUENCE_FINISH,
];

/// Every query of the crate with its Rust types, for the registry test.
pub const TYPED: &[typed::Shape] = &[
    typed::Shape::of::<lease_acquire::Query>(),
    typed::Shape::of::<lease_acquire_halted::Query>(),
    typed::Shape::of::<lease_halted::Query>(),
    typed::Shape::of::<lease_renew::Query>(),
    typed::Shape::of::<lease_release::Query>(),
    typed::Shape::of::<lease_halt::Query>(),
    typed::Shape::of::<lease_halt_unleased::Query>(),
    typed::Shape::of::<lease_stored_token::Query>(),
    typed::Shape::of::<lease_hold_fence::Query>(),
    typed::Shape::of::<lease_store_token::Query>(),
    typed::Shape::of::<log_stores::Query>(),
    typed::Shape::of::<log_store::Query>(),
    typed::Shape::of::<log_insert::Query>(),
    typed::Shape::of::<log_set_category_positions::Query>(),
    typed::Shape::of::<log_set_store_position::Query>(),
    typed::Shape::of::<log_newest::Query>(),
    typed::Shape::of::<log_category_newest::Query>(),
    typed::Shape::of::<log_tail::Query>(),
    typed::Shape::of::<log_above::Query>(),
    typed::Shape::of::<wal_insert_lsn::Query>(),
    typed::Shape::of::<notify::Query>(),
    typed::Shape::of::<outbox_stores::Query>(),
    typed::Shape::of::<outbox_drain::Query>(),
    typed::Shape::of::<outbox_probe::Query>(),
    typed::Shape::of::<outbox_probe_none::Query>(),
    typed::Shape::of::<logical_database::Query>(),
    typed::Shape::of::<logical_stores::Query>(),
    typed::Shape::of::<publication_exists::Query>(),
    typed::Shape::of::<published::Query>(),
    typed::Shape::of::<slot::Query>(),
    typed::Shape::of::<store_has_commits::Query>(),
    typed::Shape::of::<logical_stored_token::Query>(),
    typed::Shape::of::<store_initial_token::Query>(),
    typed::Shape::of::<terminate_holder::Query>(),
    typed::Shape::of::<resequence_shortest_max_txn_age::Query>(),
    typed::Shape::of::<resequence_window::Query>(),
    typed::Shape::of::<resequence_bounds::Query>(),
    typed::Shape::of::<resequence_categories::Query>(),
    typed::Shape::of::<resequence_after::Query>(),
    typed::Shape::of::<resequence_orphans::Query>(),
    typed::Shape::of::<resequence_hold_fence::Query>(),
    typed::Shape::of::<resequence_record_ranges::Query>(),
    typed::Shape::of::<resequence_finish::Query>(),
];
