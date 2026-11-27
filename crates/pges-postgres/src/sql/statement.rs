//! The registry of pges-postgres: every statement the crate runs.
//!
//! Each statement is a constant, listed in [`ALL`]. A query is declared with [`typed_query!`](crate::typed_query),
//! which adds a module of the same name in lower case with its typed parameters and row, and is listed in
//! [`TYPED`] too. `tests/statement.rs` checks every one against PostgreSQL 18.

use sqlx::postgres::types::PgInterval;
use uuid::Uuid;

use crate::sql::{template, typed};

/// A file below `sql/`, embedded.
macro_rules! sql_file {
    ($path:literal) => {
        template::File {
            path: $path,
            text: include_str!(concat!("../../sql/", $path)),
        }
    };
}

crate::typed_query! {
    /// Appends one commit to one stream, in a store without the outbox.
    pub APPEND, mod append {
        name: "append/one",
        files: [sql_file!("append/head.sql"), sql_file!("append/commit.sql")],
        params<'q> {
            stream_id: Uuid,
            category_id: i32,
            expected: i64,
            event_count: i16,
            format: i16,
            payload: &'q [u8],
            key: Option<&'q str>,
            commit_id: Uuid,
            max_clock_skew: PgInterval,
        },
        row {
            commit_id: Uuid,
            last_version: i64,
        },
    }
}

crate::typed_query! {
    /// Appends one commit to one stream and enqueues it, in a store with the outbox.
    pub APPEND_OUTBOX, mod append_outbox {
        name: "append/one_outbox",
        files: [sql_file!("append/head.sql"), sql_file!("append/commit_outbox.sql")],
        like append,
    }
}

crate::typed_query! {
    /// Appends one commit to each of several streams, in a store without the outbox.
    pub APPEND_MANY, mod append_many {
        name: "append/many",
        files: [sql_file!("append/many_head.sql"), sql_file!("append/many_commit.sql")],
        params<'q> {
            stream_ids: Vec<Uuid>,
            category_ids: Vec<i32>,
            expected: Vec<i64>,
            event_counts: Vec<i16>,
            formats: Vec<i16>,
            payloads: Vec<&'q [u8]>,
            keys: Vec<Option<&'q str>>,
            commit_ids: Vec<Uuid>,
            max_clock_skew: PgInterval,
        },
        row {
            stream_id: Uuid,
            commit_id: Uuid,
            last_version: i64,
        },
    }
}

crate::typed_query! {
    /// Appends one commit to each of several streams and enqueues them, in a store with the outbox.
    pub APPEND_MANY_OUTBOX, mod append_many_outbox {
        name: "append/many_outbox",
        files: [sql_file!("append/many_head.sql"), sql_file!("append/many_commit_outbox.sql")],
        like append_many,
    }
}

/// Begins an `append_many` transaction whose statements are planned for their parameters.
pub const BEGIN_MANY: template::Statement = template::Statement {
    name: "append/begin_many",
    files: &[sql_file!("append/begin_many.sql")],
    kind: template::Kind::Ddl,
    params: &[],
    columns: &[],
};

crate::typed_query! {
    /// Sets `plan_cache_mode` for the rest of the transaction and returns its previous value.
    pub PLAN_MODE, mod plan_mode {
        name: "append/plan_mode",
        files: [sql_file!("append/plan_mode.sql")],
        params<'q> {
            mode: &'q str,
        },
        row {
            previous: String,
            current: String,
        },
    }
}

crate::typed_query! {
    /// Finds out why an append returned no row.
    pub CLASSIFY, mod classify {
        name: "append/classify",
        files: [sql_file!("append/classify.sql")],
        params {
            stream_id: Uuid,
            category_id: i32,
            commit_id: Uuid,
        },
        row {
            version: Option<i64>,
            state: Option<i16>,
            category_id: Option<i32>,
            last_commit_id: Option<Uuid>,
            done: Option<i64>,
        },
    }
}

crate::typed_query! {
    /// Locks a stream's row.
    pub LOCK_STREAM, mod lock_stream {
        name: "append/lock_stream",
        files: [sql_file!("append/lock_stream.sql")],
        params {
            stream_id: Uuid,
        },
        row {
            last_commit_id: Uuid,
        },
    }
}

crate::typed_query! {
    /// Locks a stream's row without waiting.
    pub TRY_LOCK_STREAM, mod try_lock_stream {
        name: "append/try_lock_stream",
        files: [sql_file!("append/try_lock_stream.sql")],
        like lock_stream,
    }
}

crate::typed_query! {
    /// Lists the hash partitions of `streams` and the probes each holds, to route appends.
    pub STREAM_LEAVES, mod stream_leaves {
        name: "append/stream_leaves",
        files: [sql_file!("append/stream_leaves.sql")],
        params<'q> {
            probes: &'q [Uuid],
        },
        row {
            leaf: String,
            modulus: Option<i32>,
            remainder: Option<i32>,
            probes: Option<Vec<i32>>,
        },
    }
}

crate::typed_query! {
    /// Reads a stream's bounds.
    pub STREAM_HEAD, mod stream_head {
        name: "read/stream_head",
        files: [sql_file!("read/stream_head.sql")],
        params {
            stream_id: Uuid,
        },
        row {
            version: i64,
            floor_version: i64,
            first_commit_id: Uuid,
            last_commit_id: Uuid,
            category_id: i32,
            state: i16,
        },
    }
}

crate::typed_query! {
    /// Reads one page of a stream's commits from its category's partition.
    pub READ_STREAM, mod read_stream {
        name: "read/stream",
        files: [sql_file!("read/stream.sql")],
        params {
            stream_id: Uuid,
            from_version: i64,
            first_commit: Uuid,
            last_commit: Uuid,
            page: i64,
        },
        row<'r> {
            last_version: i64,
            event_count: i16,
            commit_id: Uuid,
            format: i16,
            payload: &'r [u8],
        },
    }
}

crate::typed_query! {
    /// Reads a stream's snapshot.
    pub SNAPSHOT_READ, mod snapshot_read {
        name: "snapshot/read",
        files: [sql_file!("snapshot/read.sql")],
        params {
            stream_id: Uuid,
        },
        row<'r> {
            version: i64,
            commit_id: Uuid,
            category_id: i32,
            state_version: i32,
            format: i16,
            state: &'r [u8],
        },
    }
}

crate::typed_query! {
    /// Saves a stream's snapshot.
    pub SNAPSHOT_UPSERT, mod snapshot_upsert {
        name: "snapshot/upsert",
        files: [sql_file!("snapshot/upsert.sql")],
        params<'q> {
            version: i64,
            stream_id: Uuid,
            commit_id: Uuid,
            category_id: i32,
            state_version: i32,
            format: i16,
            state: &'q [u8],
            min_distance: i64,
        },
        row {},
    }
}

crate::typed_query! {
    /// Registers event type names of a category.
    pub REGISTER_TYPES, mod register_types {
        name: "event_type/register",
        files: [sql_file!("event_type/register.sql")],
        params<'q> {
            category_id: i32,
            names: &'q [&'q str],
        },
        row {},
    }
}

crate::typed_query! {
    /// Reads the ids of some event types of a category.
    pub SELECT_TYPES, mod select_types {
        name: "event_type/select",
        files: [sql_file!("event_type/select.sql")],
        params<'q> {
            category_id: i32,
            names: &'q [&'q str],
        },
        row {
            event_type_id: i32,
            name: String,
        },
    }
}

crate::typed_query! {
    /// Reads every event type of a category.
    pub CATEGORY_TYPES, mod category_types {
        name: "event_type/by_category",
        files: [sql_file!("event_type/by_category.sql")],
        params {
            category_id: i32,
        },
        row {
            event_type_id: i32,
            name: String,
        },
    }
}

crate::typed_query! {
    /// Reads every event type of the store.
    pub ALL_TYPES, mod all_types {
        name: "event_type/all",
        files: [sql_file!("event_type/all.sql")],
        params {},
        row {
            category_id: i32,
            event_type_id: i32,
            name: String,
        },
    }
}

crate::typed_query! {
    /// Reads the store's categories.
    pub CATEGORIES, mod categories {
        name: "category/list",
        files: [sql_file!("category/list.sql")],
        params {},
        row {
            category_id: i32,
            name: String,
        },
    }
}

crate::typed_query! {
    /// Marks a stream deleted.
    pub MARK_DELETED, mod mark_deleted {
        name: "delete/mark_deleted",
        files: [sql_file!("delete/mark_deleted.sql")],
        params {
            stream_id: Uuid,
        },
        row {
            first_commit_id: Uuid,
        },
    }
}

crate::typed_query! {
    /// Deletes a stream's commits and records their ids.
    pub DELETE_COMMITS, mod delete_commits {
        name: "delete/commits",
        files: [sql_file!("delete/commits.sql")],
        params {
            stream_id: Uuid,
            category_id: i32,
            first_commit_id: Uuid,
            deleted_commit_id: Uuid,
        },
        row {},
    }
}

crate::typed_query! {
    /// Deletes a stream's snapshot.
    pub DELETE_SNAPSHOT, mod delete_snapshot {
        name: "delete/snapshot",
        files: [sql_file!("delete/snapshot.sql")],
        params {
            stream_id: Uuid,
        },
        row {},
    }
}

crate::typed_query! {
    /// Whether the shared schema exists.
    pub SHARED_SCHEMA_EXISTS, mod shared_schema_exists {
        name: "open/shared_schema_exists",
        files: [sql_file!("open/shared_schema_exists.sql")],
        params {},
        row {
            exists: bool as "?column?",
        },
    }
}

crate::typed_query! {
    /// Reads a store's row when it is opened.
    pub OPEN_STORE, mod open_store {
        name: "open/store",
        files: [sql_file!("open/store.sql")],
        params<'q> {
            store: &'q str,
            max_clock_skew: Option<PgInterval>,
        },
        row {
            store_id: i32,
            outbox: bool,
            dcb: bool,
            max_clock_skew: PgInterval,
            skew_fits: bool as "?column?",
        },
    }
}

crate::typed_query! {
    /// Takes the migration lock.
    pub MIGRATE_LOCK, mod migrate_lock {
        name: "migrate/lock",
        files: [sql_file!("migrate/lock.sql")],
        params {},
        row {
            pg_advisory_xact_lock: (),
        },
    }
}

crate::typed_query! {
    /// Reads a scope's schema version and compatibility window.
    pub SCOPE, mod scope {
        name: "migrate/scope",
        files: [sql_file!("migrate/scope.sql")],
        params<'q> {
            scope: &'q str,
        },
        row {
            schema_version: i32,
            min_compatible: i32,
        },
    }
}

crate::typed_query! {
    /// Reads a scope's oldest supported schema version.
    pub MIN_COMPATIBLE, mod min_compatible {
        name: "migrate/min_compatible",
        files: [sql_file!("migrate/min_compatible.sql")],
        params<'q> {
            scope: &'q str,
        },
        row {
            min_compatible: i32,
        },
    }
}

crate::typed_query! {
    /// Finds a scope whose oldest supported schema version is above this crate's newest one.
    pub TOO_OLD, mod too_old {
        name: "migrate/too_old",
        files: [sql_file!("migrate/too_old.sql")],
        params<'q> {
            shared_version: i32,
            store_version: i32,
            store: Option<&'q str>,
            outbox_version: i32,
        },
        row {
            scope: String,
            min_compatible: i32,
        },
    }
}

crate::typed_query! {
    /// Whether `pges.migrations` exists.
    pub MIGRATIONS_TABLE_EXISTS, mod migrations_table_exists {
        name: "migrate/migrations_table_exists",
        files: [sql_file!("migrate/migrations_table_exists.sql")],
        params {},
        row {
            exists: bool as "?column?",
        },
    }
}

crate::typed_query! {
    /// Reads the migrations applied to a scope.
    pub APPLIED_MIGRATIONS, mod applied_migrations {
        name: "migrate/applied",
        files: [sql_file!("migrate/applied.sql")],
        params<'q> {
            scope: &'q str,
        },
        row {
            version: i32,
            checksum: Vec<u8>,
        },
    }
}

crate::typed_query! {
    /// Whether `pges.stores` exists.
    pub STORES_TABLE_EXISTS, mod stores_table_exists {
        name: "migrate/stores_table_exists",
        files: [sql_file!("migrate/stores_table_exists.sql")],
        params {},
        row {
            exists: bool as "?column?",
        },
    }
}

crate::typed_query! {
    /// Whether a store exists.
    pub STORE_EXISTS, mod store_exists {
        name: "migrate/store_exists",
        files: [sql_file!("migrate/store_exists.sql")],
        params<'q> {
            store: &'q str,
        },
        row {
            exists: bool,
        },
    }
}

/// Creates a store's schema.
pub const CREATE_SCHEMA: template::Statement = template::Statement {
    name: "migrate/create_schema",
    files: &[sql_file!("migrate/create_schema.sql")],
    kind: template::Kind::Ddl,
    params: &[],
    columns: &[],
};

crate::typed_query! {
    /// Records a new store.
    pub INSERT_STORE, mod insert_store {
        name: "migrate/insert_store",
        files: [sql_file!("migrate/insert_store.sql")],
        params<'q> {
            store: &'q str,
            outbox: bool,
            max_clock_skew: PgInterval,
            max_txn_age: PgInterval,
        },
        row {},
    }
}

crate::typed_query! {
    /// Sets the number of hash partitions the first store migration creates.
    pub SET_STREAM_PARTITIONS, mod set_stream_partitions {
        name: "migrate/set_stream_partitions",
        files: [sql_file!("migrate/set_stream_partitions.sql")],
        params<'q> {
            partitions: &'q str,
        },
        row {
            set_config: String,
        },
    }
}

/// Sets the search path to the store for the transaction.
pub const SET_SEARCH_PATH: template::Statement = template::Statement {
    name: "migrate/set_search_path",
    files: &[sql_file!("migrate/set_search_path.sql")],
    kind: template::Kind::Ddl,
    params: &[],
    columns: &[],
};

crate::typed_query! {
    /// Records an applied migration.
    pub RECORD_MIGRATION, mod record_migration {
        name: "migrate/record",
        files: [sql_file!("migrate/record.sql")],
        params<'q> {
            version: i32,
            scope: &'q str,
            name: &'q str,
            checksum: &'q [u8],
        },
        row {},
    }
}

crate::typed_query! {
    /// Moves a scope's schema version.
    pub SET_SCOPE_VERSION, mod set_scope_version {
        name: "migrate/set_scope_version",
        files: [sql_file!("migrate/set_scope_version.sql")],
        params<'q> {
            scope: &'q str,
            schema_version: i32,
            min_compatible: i32,
        },
        row {},
    }
}

crate::typed_query! {
    /// Takes a store's maintenance lock.
    pub MAINTAIN_LOCK, mod maintain_lock {
        name: "maintain/lock",
        files: [sql_file!("maintain/lock.sql")],
        params<'q> {
            store: &'q str,
        },
        row {
            pg_advisory_lock: (),
        },
    }
}

crate::typed_query! {
    /// Releases a store's maintenance lock.
    pub MAINTAIN_UNLOCK, mod maintain_unlock {
        name: "maintain/unlock",
        files: [sql_file!("maintain/unlock.sql")],
        params<'q> {
            store: &'q str,
        },
        row {
            pg_advisory_unlock: bool,
        },
    }
}

/// Sets the session up for maintenance.
pub const MAINTAIN_SESSION: template::Statement = template::Statement {
    name: "maintain/session",
    files: &[sql_file!("maintain/session.sql")],
    kind: template::Kind::Ddl,
    params: &[],
    columns: &[],
};

crate::typed_query! {
    /// Registers a category.
    pub INSERT_CATEGORY, mod insert_category {
        name: "category/insert",
        files: [sql_file!("category/insert.sql")],
        params<'q> {
            name: &'q str,
            bucket: PgInterval,
            retention: i16,
            retain_for: Option<PgInterval>,
        },
        row {},
    }
}

crate::typed_query! {
    /// Reads a category's id and bucket.
    pub SELECT_CATEGORY, mod select_category {
        name: "category/select",
        files: [sql_file!("category/select.sql")],
        params<'q> {
            name: &'q str,
        },
        row {
            category_id: i32,
            bucket: PgInterval,
        },
    }
}

crate::typed_query! {
    /// Changes a category's bucket.
    pub SET_BUCKET, mod set_bucket {
        name: "category/set_bucket",
        files: [sql_file!("category/set_bucket.sql")],
        params<'q> {
            name: &'q str,
            bucket: PgInterval,
        },
        row {
            category_id: i32,
        },
    }
}

crate::typed_query! {
    /// Reads every category's bucket.
    pub CATEGORY_BUCKETS, mod category_buckets {
        name: "category/buckets",
        files: [sql_file!("category/buckets.sql")],
        params {},
        row {
            category_id: i32,
            name: String,
            bucket: PgInterval,
        },
    }
}

/// Creates a category's partition of `commits`.
pub const CREATE_CATEGORY_PARTITION: template::Statement = template::Statement {
    name: "category/create_partition",
    files: &[sql_file!("category/create_partition.sql")],
    kind: template::Kind::Ddl,
    params: &[],
    columns: &[],
};

/// Attaches a category's partition to `commits`.
pub const ATTACH_CATEGORY_PARTITION: template::Statement = template::Statement {
    name: "category/attach_partition",
    files: &[sql_file!("category/attach_partition.sql")],
    kind: template::Kind::Ddl,
    params: &[],
    columns: &[],
};

crate::typed_query! {
    /// Finds attached leaves that still have their attach constraint.
    pub UNFINISHED_LEAVES, mod unfinished_leaves {
        name: "leaf/unfinished",
        files: [sql_file!("leaf/unfinished.sql")],
        params<'q> {
            parent: &'q str,
            bound: &'q str,
        },
        row {
            relname: String,
        },
    }
}

crate::typed_query! {
    /// Lists the time leaves a category needs.
    pub LEAVES_TO_CREATE, mod leaves_to_create {
        name: "leaf/to_create",
        files: [sql_file!("leaf/to_create.sql")],
        params<'q> {
            parent: &'q str,
            unit: &'q str,
            bucket: PgInterval,
            suffix_format: &'q str,
            buckets: f64,
        },
        row {
            to_char: String,
            lower: String,
            upper: String,
        },
    }
}

/// Creates a time leaf, detached.
pub const CREATE_LEAF: template::Statement = template::Statement {
    name: "leaf/create",
    files: &[sql_file!("leaf/create.sql")],
    kind: template::Kind::Ddl,
    params: &[],
    columns: &[],
};

/// Compresses a new leaf's payloads with lz4.
pub const COMPRESS_LEAF: template::Statement = template::Statement {
    name: "leaf/compress",
    files: &[sql_file!("leaf/compress.sql")],
    kind: template::Kind::Ddl,
    params: &[],
    columns: &[],
};

/// Creates a new leaf's unique index.
pub const CREATE_LEAF_INDEX: template::Statement = template::Statement {
    name: "leaf/create_index",
    files: &[sql_file!("leaf/create_index.sql")],
    kind: template::Kind::Ddl,
    params: &[],
    columns: &[],
};

/// Adds a new leaf's attach constraint.
pub const ADD_LEAF_BOUND: template::Statement = template::Statement {
    name: "leaf/add_bound",
    files: &[sql_file!("leaf/add_bound.sql")],
    kind: template::Kind::Ddl,
    params: &[],
    columns: &[],
};

/// Attaches a new leaf to its category's partition.
pub const ATTACH_LEAF: template::Statement = template::Statement {
    name: "leaf/attach",
    files: &[sql_file!("leaf/attach.sql")],
    kind: template::Kind::Ddl,
    params: &[],
    columns: &[],
};

crate::typed_query! {
    /// What a name in the store's schema is: a table or not, a partition or not, and its parent.
    pub PARTITION_STATE, mod partition_state {
        name: "partition/state",
        files: [sql_file!("partition/state.sql")],
        params<'q> {
            store: &'q str,
            table: &'q str,
        },
        row {
            is_table: Option<bool>,
            relispartition: bool,
            parent: Option<String>,
        },
    }
}

crate::typed_query! {
    /// Reads, for every category, the upper bound of its newest attached leaf and how far ahead of now it is.
    pub LEAF_HEADROOM, mod leaf_headroom {
        name: "partition/headroom",
        files: [sql_file!("partition/headroom.sql")],
        params {},
        row {
            category_id: i32,
            name: String,
            bucket: PgInterval,
            upper: Option<i64>,
            ahead: Option<i64>,
        },
    }
}

/// Drops a partition's attach constraint.
pub const DROP_BOUND: template::Statement = template::Statement {
    name: "partition/drop_bound",
    files: &[sql_file!("partition/drop_bound.sql")],
    kind: template::Kind::Ddl,
    params: &[],
    columns: &[],
};

/// Drops a leaf or log partition.
pub const DROP_PARTITION: template::Statement = template::Statement {
    name: "partition/drop",
    files: &[sql_file!("partition/drop.sql")],
    kind: template::Kind::Ddl,
    params: &[],
    columns: &[],
};

crate::typed_query! {
    /// Whether a partition is being detached concurrently.
    pub DETACH_PENDING, mod detach_pending {
        name: "partition/detach_pending",
        files: [sql_file!("partition/detach_pending.sql")],
        params<'q> {
            store: &'q str,
            table: &'q str,
        },
        row {
            inhdetachpending: bool,
        },
    }
}

/// Detaches a partition without blocking writers.
pub const DETACH_CONCURRENTLY: template::Statement = template::Statement {
    name: "partition/detach_concurrently",
    files: &[sql_file!("partition/detach_concurrently.sql")],
    kind: template::Kind::Ddl,
    params: &[],
    columns: &[],
};

/// Finishes an interrupted concurrent detach.
pub const DETACH_FINALIZE: template::Statement = template::Statement {
    name: "partition/detach_finalize",
    files: &[sql_file!("partition/detach_finalize.sql")],
    kind: template::Kind::Ddl,
    params: &[],
    columns: &[],
};

crate::typed_query! {
    /// Reads the number of the newest log partition.
    pub NEWEST_LOG_PARTITION, mod newest_log_partition {
        name: "log/newest_partition",
        files: [sql_file!("log/newest_partition.sql")],
        params<'q> {
            log: &'q str,
        },
        row {
            max: Option<i64>,
        },
    }
}

crate::typed_query! {
    /// Reads the store's last position.
    pub LAST_POSITION, mod last_position {
        name: "log/last_position",
        files: [sql_file!("log/last_position.sql")],
        params {},
        row {
            coalesce: i64,
        },
    }
}

/// Creates a log partition, detached.
pub const CREATE_LOG_PARTITION: template::Statement = template::Statement {
    name: "log/create_partition",
    files: &[sql_file!("log/create_partition.sql")],
    kind: template::Kind::Ddl,
    params: &[],
    columns: &[],
};

/// Adds a new log partition's attach constraint.
pub const ADD_LOG_BOUND: template::Statement = template::Statement {
    name: "log/add_bound",
    files: &[sql_file!("log/add_bound.sql")],
    kind: template::Kind::Ddl,
    params: &[],
    columns: &[],
};

/// Attaches a new log partition.
pub const ATTACH_LOG_PARTITION: template::Statement = template::Statement {
    name: "log/attach_partition",
    files: &[sql_file!("log/attach_partition.sql")],
    kind: template::Kind::Ddl,
    params: &[],
    columns: &[],
};

crate::typed_query! {
    /// Reads the store's log partitions.
    pub LOG_PARTITIONS, mod log_partitions {
        name: "log/partitions",
        files: [sql_file!("log/partitions.sql")],
        params<'q> {
            log: &'q str,
        },
        row {
            relname: String,
            n: i64,
            inhdetachpending: bool,
        },
    }
}

crate::typed_query! {
    /// Whether a log partition holds rows retention has not passed.
    pub LOG_PARTITION_PINNED, mod log_partition_pinned {
        name: "log/pinned",
        files: [sql_file!("log/pinned.sql")],
        params {},
        row {
            exists: bool,
        },
    }
}

crate::typed_query! {
    /// Deletes one batch of a category's log rows from one log partition.
    pub PRUNE_LOG, mod prune_log {
        name: "log/prune",
        files: [sql_file!("log/prune.sql")],
        params {
            category_id: i32,
            scan_from: i64,
            batch: i64,
        },
        row {},
    }
}

/// Vacuums a pruned log partition.
pub const VACUUM_LOG_PARTITION: template::Statement = template::Statement {
    name: "log/vacuum",
    files: &[sql_file!("log/vacuum.sql")],
    kind: template::Kind::Ddl,
    params: &[],
    columns: &[],
};

crate::typed_query! {
    /// Reads a store's default retention.
    pub STORE_DEFAULT_RETENTION, mod store_default_retention {
        name: "retention/store_default",
        files: [sql_file!("retention/store_default.sql")],
        params<'q> {
            store: &'q str,
        },
        row {
            retention: i16,
            retain_for: Option<PgInterval>,
        },
    }
}

crate::typed_query! {
    /// Sets a store's default retention.
    pub SET_DEFAULT_RETENTION, mod set_default_retention {
        name: "retention/set_default",
        files: [sql_file!("retention/set_default.sql")],
        params<'q> {
            store: &'q str,
            retention: i16,
            retain_for: Option<PgInterval>,
        },
        row {},
    }
}

crate::typed_query! {
    /// Sets a category's retention.
    pub SET_CATEGORY_RETENTION, mod set_category_retention {
        name: "retention/set_category",
        files: [sql_file!("retention/set_category.sql")],
        params<'q> {
            name: &'q str,
            retention: i16,
            retain_for: Option<PgInterval>,
        },
        row {},
    }
}

crate::typed_query! {
    /// Reads every category's retention.
    pub RETENTION_CATEGORIES, mod retention_categories {
        name: "retention/categories",
        files: [sql_file!("retention/categories.sql")],
        params {},
        row {
            category_id: i32,
            name: String,
            retention: i16,
            retain_for: Option<PgInterval>,
            retention_scan_from: i64,
        },
    }
}

crate::typed_query! {
    /// Reads why the sequencer is halted.
    pub SEQUENCER_HALTED, mod sequencer_halted {
        name: "retention/halted",
        files: [sql_file!("retention/halted.sql")],
        params {},
        row {
            halted: Option<String>,
        },
    }
}

crate::typed_query! {
    /// Finds leaves an interrupted retention run left detached.
    pub DETACHED_LEAVES, mod detached_leaves {
        name: "retention/detached_leaves",
        files: [sql_file!("retention/detached_leaves.sql")],
        params<'q> {
            store: &'q str,
            pattern: &'q str,
            parent: &'q str,
        },
        row {
            relname: String,
        },
    }
}

crate::typed_query! {
    /// Lists the leaves a category's retention no longer keeps.
    pub LEAVES_PAST_RETENTION, mod leaves_past_retention {
        name: "retention/leaves_past",
        files: [sql_file!("retention/leaves_past.sql")],
        params<'q> {
            parent: &'q str,
            keep: PgInterval,
            max_clock_skew: PgInterval,
            max_txn_age: PgInterval,
        },
        row {
            leaf: String,
            detaching: bool,
            upper: Uuid,
        },
    }
}

/// Makes the scan's transaction read one snapshot.
pub const SCAN_SNAPSHOT: template::Statement = template::Statement {
    name: "retention/snapshot",
    files: &[sql_file!("retention/snapshot.sql")],
    kind: template::Kind::Ddl,
    params: &[],
    columns: &[],
};

crate::typed_query! {
    /// Reads the positions resequence gave a category.
    pub RESEQUENCED, mod resequenced {
        name: "retention/resequenced",
        files: [sql_file!("retention/resequenced.sql")],
        params {
            category_id: i32,
        },
        row {
            resequenced_from: i64,
            resequenced_through: i64,
        },
    }
}

crate::typed_query! {
    /// Finds where the scan of a category's log rows stops.
    pub SCAN_STOP, mod scan_stop {
        name: "retention/scan_stop",
        files: [sql_file!("retention/scan_stop.sql")],
        params {
            category_id: i32,
            scan_from: i64,
            upper: Uuid,
            max_txn_age: PgInterval,
            max_clock_skew: PgInterval,
        },
        row {
            last_position: i64,
        },
    }
}

crate::typed_query! {
    /// Scans a category's log rows for one leaf.
    pub SCAN, mod scan {
        name: "retention/scan",
        files: [sql_file!("retention/scan.sql")],
        params {
            category_id: i32,
            scan_from: i64,
            stop: i64,
            upper: Uuid,
            max_txn_age: PgInterval,
            max_clock_skew: PgInterval,
            resequenced_from: i64,
            resequenced_through: i64,
        },
        row {
            horizon: Option<i64>,
            sequenced: bool,
            next_scan_from: Option<i64>,
        },
    }
}

crate::typed_query! {
    /// Finds long-lived streams with commits in a leaf.
    pub LIVE_STREAMS, mod live_streams {
        name: "retention/live_streams",
        files: [sql_file!("retention/live_streams.sql")],
        params {
            keep: PgInterval,
            limit: i64,
        },
        row {
            stream_id: Uuid,
        },
    }
}

crate::typed_query! {
    /// Finds streams with commits in a leaf and no recent enough snapshot.
    pub MISSING_SNAPSHOTS, mod missing_snapshots {
        name: "retention/missing_snapshots",
        files: [sql_file!("retention/missing_snapshots.sql")],
        params {
            category_id: i32,
            limit: i64,
        },
        row {
            stream_id: Uuid,
        },
    }
}

/// Leaves the planner only nested loops for the transaction's next joins.
pub const PROBE_JOINS: template::Statement = template::Statement {
    name: "retention/probe_joins",
    files: &[sql_file!("retention/probe_joins.sql")],
    kind: template::Kind::Ddl,
    params: &[],
    columns: &[],
};

/// Gives the planner back its choice of joins.
pub const PLAN_JOINS: template::Statement = template::Statement {
    name: "retention/plan_joins",
    files: &[sql_file!("retention/plan_joins.sql")],
    kind: template::Kind::Ddl,
    params: &[],
    columns: &[],
};

crate::typed_query! {
    /// Raises the floors of a leaf's streams.
    pub RAISE_FLOORS, mod raise_floors {
        name: "retention/raise_floors",
        files: [sql_file!("retention/raise_floors.sql")],
        params {},
        row {},
    }
}

crate::typed_query! {
    /// Raises a category's retention horizon.
    pub RAISE_HORIZON, mod raise_horizon {
        name: "retention/raise_horizon",
        files: [sql_file!("retention/raise_horizon.sql")],
        params {
            category_id: i32,
            horizon: Option<i64>,
        },
        row {
            horizon_position: i64,
        },
    }
}

crate::typed_query! {
    /// Forgets a category's deleted commits whose log rows the retention horizon covers.
    pub FORGET_DELETED, mod forget_deleted {
        name: "retention/forget_deleted",
        files: [sql_file!("retention/forget_deleted.sql")],
        params {
            category_id: i32,
            upper: Uuid,
            scan_from: i64,
            horizon: i64,
        },
        row {},
    }
}

crate::typed_query! {
    /// Reads a category's retention horizon.
    pub HORIZON, mod horizon {
        name: "retention/horizon",
        files: [sql_file!("retention/horizon.sql")],
        params {
            category_id: i32,
        },
        row {
            horizon_position: i64,
        },
    }
}

crate::typed_query! {
    /// Moves the start of a category's next log scan.
    pub RAISE_SCAN_FROM, mod raise_scan_from {
        name: "retention/raise_scan_from",
        files: [sql_file!("retention/raise_scan_from.sql")],
        params {
            category_id: i32,
            scan_from: i64,
        },
        row {},
    }
}

crate::typed_query! {
    /// Reads a category's id and scan start before pruning.
    pub PRUNE_CATEGORY, mod prune_category {
        name: "retention/prune_category",
        files: [sql_file!("retention/prune_category.sql")],
        params<'q> {
            name: &'q str,
        },
        row {
            category_id: i32,
            retention_scan_from: i64,
        },
    }
}

crate::typed_query! {
    /// Lists a store's time leaves that no append can still write to.
    pub CLOSED_LEAVES, mod closed_leaves {
        name: "freeze/closed_leaves",
        files: [sql_file!("freeze/closed_leaves.sql")],
        params<'q> {
            commits: &'q str,
            max_txn_age: PgInterval,
            max_clock_skew: PgInterval,
        },
        row {
            leaf: String,
        },
    }
}

crate::typed_query! {
    /// Reads how frozen each partition of a store is.
    pub PARTITION_AGES, mod partition_ages {
        name: "freeze/partitions",
        files: [sql_file!("freeze/partitions.sql")],
        params<'q> {
            store: &'q str,
        },
        row {
            relname: String,
            pages: Option<i64>,
            frozen_pages: Option<i64>,
            dead_rows: Option<i64>,
            xid_age: Option<i32>,
            mxid_age: Option<i32>,
        },
    }
}

/// Freezes and analyzes a closed partition.
pub const FREEZE_PARTITION: template::Statement = template::Statement {
    name: "freeze/vacuum",
    files: &[sql_file!("freeze/vacuum.sql")],
    kind: template::Kind::Ddl,
    params: &[],
    columns: &[],
};

crate::typed_query! {
    /// Reads a store's writer limits.
    pub WRITER_LIMITS, mod writer_limits {
        name: "limit/recorded",
        files: [sql_file!("limit/recorded.sql")],
        params<'q> {
            store: &'q str,
        },
        row {
            max_clock_skew: PgInterval,
            max_txn_age: PgInterval,
        },
    }
}

crate::typed_query! {
    /// Raises a store's writer limits.
    pub RAISE_WRITER_LIMITS, mod raise_writer_limits {
        name: "limit/raise",
        files: [sql_file!("limit/raise.sql")],
        params<'q> {
            store: &'q str,
            max_clock_skew: PgInterval,
            max_txn_age: PgInterval,
        },
        row {},
    }
}

crate::typed_query! {
    /// Reads a role's transaction_timeout.
    pub WRITER_TIMEOUT, mod writer_timeout {
        name: "limit/writer_timeout",
        files: [sql_file!("limit/writer_timeout.sql")],
        params<'q> {
            role: &'q str,
        },
        row {
            found: bool,
            setting: Option<String>,
        },
    }
}

crate::typed_query! {
    /// Checks whether a transaction is still usable.
    pub CHECK_TRANSACTION, mod check_transaction {
        name: "unit_of_work/check",
        files: [sql_file!("unit_of_work/check.sql")],
        params {},
        row {
            one: i32 as "?column?",
        },
    }
}

/// Every statement of the crate.
pub const ALL: &[&template::Statement] = &[
    &APPEND,
    &APPEND_OUTBOX,
    &APPEND_MANY,
    &APPEND_MANY_OUTBOX,
    &BEGIN_MANY,
    &PLAN_MODE,
    &CLASSIFY,
    &LOCK_STREAM,
    &TRY_LOCK_STREAM,
    &STREAM_LEAVES,
    &STREAM_HEAD,
    &READ_STREAM,
    &SNAPSHOT_READ,
    &SNAPSHOT_UPSERT,
    &REGISTER_TYPES,
    &SELECT_TYPES,
    &CATEGORY_TYPES,
    &ALL_TYPES,
    &CATEGORIES,
    &MARK_DELETED,
    &DELETE_COMMITS,
    &DELETE_SNAPSHOT,
    &SHARED_SCHEMA_EXISTS,
    &OPEN_STORE,
    &MIGRATE_LOCK,
    &SCOPE,
    &MIN_COMPATIBLE,
    &TOO_OLD,
    &MIGRATIONS_TABLE_EXISTS,
    &APPLIED_MIGRATIONS,
    &STORES_TABLE_EXISTS,
    &STORE_EXISTS,
    &CREATE_SCHEMA,
    &INSERT_STORE,
    &SET_STREAM_PARTITIONS,
    &SET_SEARCH_PATH,
    &RECORD_MIGRATION,
    &SET_SCOPE_VERSION,
    &MAINTAIN_LOCK,
    &MAINTAIN_UNLOCK,
    &MAINTAIN_SESSION,
    &INSERT_CATEGORY,
    &SELECT_CATEGORY,
    &SET_BUCKET,
    &CATEGORY_BUCKETS,
    &CREATE_CATEGORY_PARTITION,
    &ATTACH_CATEGORY_PARTITION,
    &UNFINISHED_LEAVES,
    &LEAVES_TO_CREATE,
    &CREATE_LEAF,
    &COMPRESS_LEAF,
    &CREATE_LEAF_INDEX,
    &ADD_LEAF_BOUND,
    &ATTACH_LEAF,
    &PARTITION_STATE,
    &LEAF_HEADROOM,
    &DROP_BOUND,
    &DROP_PARTITION,
    &DETACH_PENDING,
    &DETACH_CONCURRENTLY,
    &DETACH_FINALIZE,
    &NEWEST_LOG_PARTITION,
    &LAST_POSITION,
    &CREATE_LOG_PARTITION,
    &ADD_LOG_BOUND,
    &ATTACH_LOG_PARTITION,
    &LOG_PARTITIONS,
    &LOG_PARTITION_PINNED,
    &PRUNE_LOG,
    &VACUUM_LOG_PARTITION,
    &STORE_DEFAULT_RETENTION,
    &SET_DEFAULT_RETENTION,
    &SET_CATEGORY_RETENTION,
    &RETENTION_CATEGORIES,
    &SEQUENCER_HALTED,
    &DETACHED_LEAVES,
    &LEAVES_PAST_RETENTION,
    &SCAN_SNAPSHOT,
    &RESEQUENCED,
    &SCAN_STOP,
    &SCAN,
    &LIVE_STREAMS,
    &MISSING_SNAPSHOTS,
    &PROBE_JOINS,
    &RAISE_FLOORS,
    &PLAN_JOINS,
    &RAISE_HORIZON,
    &FORGET_DELETED,
    &HORIZON,
    &RAISE_SCAN_FROM,
    &PRUNE_CATEGORY,
    &CLOSED_LEAVES,
    &PARTITION_AGES,
    &FREEZE_PARTITION,
    &WRITER_LIMITS,
    &RAISE_WRITER_LIMITS,
    &WRITER_TIMEOUT,
    &CHECK_TRANSACTION,
];

/// Every query of the crate with its Rust types, for the registry test.
pub const TYPED: &[typed::Shape] = &[
    typed::Shape::of::<append::Query>(),
    typed::Shape::of::<append_outbox::Query>(),
    typed::Shape::of::<append_many::Query>(),
    typed::Shape::of::<append_many_outbox::Query>(),
    typed::Shape::of::<plan_mode::Query>(),
    typed::Shape::of::<classify::Query>(),
    typed::Shape::of::<lock_stream::Query>(),
    typed::Shape::of::<try_lock_stream::Query>(),
    typed::Shape::of::<stream_leaves::Query>(),
    typed::Shape::of::<stream_head::Query>(),
    typed::Shape::of::<read_stream::Query>(),
    typed::Shape::of::<snapshot_read::Query>(),
    typed::Shape::of::<snapshot_upsert::Query>(),
    typed::Shape::of::<register_types::Query>(),
    typed::Shape::of::<select_types::Query>(),
    typed::Shape::of::<category_types::Query>(),
    typed::Shape::of::<all_types::Query>(),
    typed::Shape::of::<categories::Query>(),
    typed::Shape::of::<mark_deleted::Query>(),
    typed::Shape::of::<delete_commits::Query>(),
    typed::Shape::of::<delete_snapshot::Query>(),
    typed::Shape::of::<shared_schema_exists::Query>(),
    typed::Shape::of::<open_store::Query>(),
    typed::Shape::of::<migrate_lock::Query>(),
    typed::Shape::of::<scope::Query>(),
    typed::Shape::of::<min_compatible::Query>(),
    typed::Shape::of::<too_old::Query>(),
    typed::Shape::of::<migrations_table_exists::Query>(),
    typed::Shape::of::<applied_migrations::Query>(),
    typed::Shape::of::<stores_table_exists::Query>(),
    typed::Shape::of::<store_exists::Query>(),
    typed::Shape::of::<insert_store::Query>(),
    typed::Shape::of::<set_stream_partitions::Query>(),
    typed::Shape::of::<record_migration::Query>(),
    typed::Shape::of::<set_scope_version::Query>(),
    typed::Shape::of::<maintain_lock::Query>(),
    typed::Shape::of::<maintain_unlock::Query>(),
    typed::Shape::of::<insert_category::Query>(),
    typed::Shape::of::<select_category::Query>(),
    typed::Shape::of::<set_bucket::Query>(),
    typed::Shape::of::<category_buckets::Query>(),
    typed::Shape::of::<unfinished_leaves::Query>(),
    typed::Shape::of::<leaves_to_create::Query>(),
    typed::Shape::of::<partition_state::Query>(),
    typed::Shape::of::<leaf_headroom::Query>(),
    typed::Shape::of::<detach_pending::Query>(),
    typed::Shape::of::<newest_log_partition::Query>(),
    typed::Shape::of::<last_position::Query>(),
    typed::Shape::of::<log_partitions::Query>(),
    typed::Shape::of::<log_partition_pinned::Query>(),
    typed::Shape::of::<prune_log::Query>(),
    typed::Shape::of::<store_default_retention::Query>(),
    typed::Shape::of::<set_default_retention::Query>(),
    typed::Shape::of::<set_category_retention::Query>(),
    typed::Shape::of::<retention_categories::Query>(),
    typed::Shape::of::<sequencer_halted::Query>(),
    typed::Shape::of::<detached_leaves::Query>(),
    typed::Shape::of::<leaves_past_retention::Query>(),
    typed::Shape::of::<resequenced::Query>(),
    typed::Shape::of::<scan_stop::Query>(),
    typed::Shape::of::<scan::Query>(),
    typed::Shape::of::<live_streams::Query>(),
    typed::Shape::of::<missing_snapshots::Query>(),
    typed::Shape::of::<raise_floors::Query>(),
    typed::Shape::of::<raise_horizon::Query>(),
    typed::Shape::of::<forget_deleted::Query>(),
    typed::Shape::of::<horizon::Query>(),
    typed::Shape::of::<raise_scan_from::Query>(),
    typed::Shape::of::<prune_category::Query>(),
    typed::Shape::of::<closed_leaves::Query>(),
    typed::Shape::of::<partition_ages::Query>(),
    typed::Shape::of::<writer_limits::Query>(),
    typed::Shape::of::<raise_writer_limits::Query>(),
    typed::Shape::of::<writer_timeout::Query>(),
    typed::Shape::of::<check_transaction::Query>(),
];
