//! The registry of pges-bench: every statement the benchmarks run besides those of the pges crates.
//!
//! Each statement is a constant, listed in [`ALL`]. A query is declared with
//! [`typed_query!`](pges::postgres::typed_query), which adds a module of the same name in lower case with its
//! typed parameters and row, and is listed in [`TYPED`] too. `tests/statement.rs` checks every one against
//! PostgreSQL 18.

use uuid::Uuid;

use pges::postgres::sql::{template, typed};

/// A file below `sql/`, embedded.
macro_rules! sql_file {
    ($p:literal) => {
        template::File {
            path: $p,
            text: include_str!(concat!("../../sql/", $p)),
        }
    };
}

/// Gathers planner statistics for the whole database.
pub const ANALYZE: template::Statement = template::Statement {
    name: "database/analyze",
    files: &[sql_file!("database/analyze.sql")],
    kind: template::Kind::Ddl,
    params: &[],
    columns: &[],
};

/// Vacuums and analyzes the whole database.
pub const VACUUM_ANALYZE: template::Statement = template::Statement {
    name: "database/vacuum_analyze",
    files: &[sql_file!("database/vacuum_analyze.sql")],
    kind: template::Kind::Ddl,
    params: &[],
    columns: &[],
};

pges::postgres::typed_query! {
    /// A server setting, or NULL if there is none of that name.
    pub SETTING, mod setting {
        name: "database/setting",
        files: [sql_file!("database/setting.sql")],
        params<'q> { name: &'q str },
        row { value: Option<String> as "current_setting" },
    }
}

/// Creates `pg_walinspect`, to count full-page image bytes.
pub const WALINSPECT: template::Statement = template::Statement {
    name: "wal/walinspect",
    files: &[sql_file!("wal/walinspect.sql")],
    kind: template::Kind::Ddl,
    params: &[],
    columns: &[],
};

/// Creates `pg_stat_statements`.
pub const STAT_STATEMENTS: template::Statement = template::Statement {
    name: "wal/stat_statements",
    files: &[sql_file!("wal/stat_statements.sql")],
    kind: template::Kind::Ddl,
    params: &[],
    columns: &[],
};

pges::postgres::typed_query! {
    /// Makes the backend report its pending statistics now.
    pub FLUSH_STATS, mod flush_stats {
        name: "wal/flush_stats",
        files: [sql_file!("wal/flush_stats.sql")],
        params {},
        row { flushed: () as "pg_stat_force_next_flush" },
    }
}

/// Runs a checkpoint.
pub const CHECKPOINT: template::Statement = template::Statement {
    name: "wal/checkpoint",
    files: &[sql_file!("wal/checkpoint.sql")],
    kind: template::Kind::Ddl,
    params: &[],
    columns: &[],
};

pges::postgres::typed_query! {
    /// The WAL insert position and the server's WAL counters.
    pub WAL_MARK, mod wal_mark {
        name: "wal/mark",
        files: [sql_file!("wal/mark.sql")],
        params {},
        row {
            lsn: String as "pg_current_wal_insert_lsn",
            wal_records: Option<i64>,
            wal_fpi: Option<i64>,
        },
    }
}

pges::postgres::typed_query! {
    /// The WAL insert position, and the bytes written since a start position.
    pub WAL_END, mod wal_end {
        name: "wal/end",
        files: [sql_file!("wal/end.sql")],
        params { start: typed::PgLsn },
        row { lsn: String as "pg_current_wal_insert_lsn", bytes: i64 as "pg_wal_lsn_diff" },
    }
}

pges::postgres::typed_query! {
    /// The server's WAL record and full-page image counters.
    pub WAL_COUNTERS, mod wal_counters {
        name: "wal/counters",
        files: [sql_file!("wal/counters.sql")],
        params {},
        row { wal_records: Option<i64>, wal_fpi: Option<i64> },
    }
}

pges::postgres::typed_query! {
    /// Whether the WAL is flushed up to a position.
    pub WAL_FLUSHED, mod wal_flushed {
        name: "wal/flushed",
        files: [sql_file!("wal/flushed.sql")],
        params { lsn: typed::PgLsn },
        row { flushed: bool as "?column?" },
    }
}

pges::postgres::typed_query! {
    /// The bytes of full-page images between two WAL positions.
    pub FPI_BYTES, mod fpi_bytes {
        name: "wal/fpi_bytes",
        files: [sql_file!("wal/fpi_bytes.sql")],
        params { start: typed::PgLsn, end: typed::PgLsn },
        row { bytes: i64 as "coalesce" },
    }
}

pges::postgres::typed_query! {
    /// Whether the server is a standby.
    pub IN_RECOVERY, mod in_recovery {
        name: "server/in_recovery",
        files: [sql_file!("server/in_recovery.sql")],
        params {},
        row { in_recovery: bool as "pg_is_in_recovery" },
    }
}

/// Makes the replica a synchronous standby.
pub const SYNCHRONOUS_STANDBY: template::Statement = template::Statement {
    name: "server/synchronous_standby",
    files: &[sql_file!("server/synchronous_standby.sql")],
    kind: template::Kind::Ddl,
    params: &[],
    columns: &[],
};

pges::postgres::typed_query! {
    /// Makes the server reload its configuration.
    pub RELOAD, mod reload {
        name: "server/reload",
        files: [sql_file!("server/reload.sql")],
        params {},
        row { reloaded: bool as "pg_reload_conf" },
    }
}

pges::postgres::typed_query! {
    /// The log position of some commits.
    pub POSITIONS, mod positions {
        name: "sequencer/positions",
        files: [sql_file!("sequencer/positions.sql")],
        params<'q> { commit_ids: &'q [Uuid] },
        row { commit_id: Uuid, last_position: i64 },
    }
}

pges::postgres::typed_query! {
    /// The id of the store's only category.
    pub CATEGORY_ID, mod category_id {
        name: "partitions/category_id",
        files: [sql_file!("partitions/category_id.sql")],
        params {},
        row { category_id: i32 },
    }
}

pges::postgres::typed_query! {
    /// The commit ids of a page of the log.
    pub LOG_PAGE, mod log_page {
        name: "partitions/log_page",
        files: [sql_file!("partitions/log_page.sql")],
        params { after: i64, limit: i64 },
        row { commit_id: Uuid },
    }
}

pges::postgres::typed_query! {
    /// Registers a category with daily leaves.
    pub INSERT_CATEGORY, mod insert_category {
        name: "partitions/insert_category",
        files: [sql_file!("partitions/insert_category.sql")],
        params<'q> { name: &'q str },
        row { category_id: i32 },
    }
}

/// Sets the transaction's time zone to UTC.
pub const UTC: template::Statement = template::Statement {
    name: "partitions/utc",
    files: &[sql_file!("partitions/utc.sql")],
    kind: template::Kind::Ddl,
    params: &[],
    columns: &[],
};

pges::postgres::typed_query! {
    /// The daily leaves from some days back to tomorrow.
    pub DAYS, mod days {
        name: "partitions/days",
        files: [sql_file!("partitions/days.sql")],
        params { back: i32 },
        row { suffix: String as "to_char", from: String as "day", to: String as "text" },
    }
}

pges::postgres::typed_query! {
    /// Fills a category's leaves with generated commits.
    pub FILL, mod fill {
        name: "partitions/fill",
        files: [sql_file!("partitions/fill.sql")],
        params { events: i32, streams: i32, back: i32 },
        row {},
    }
}

pges::postgres::typed_query! {
    /// Writes a log row for every commit, in commit id order.
    pub WRITE_LOG, mod write_log {
        name: "partitions/log",
        files: [sql_file!("partitions/log.sql")],
        params {},
        row {},
    }
}

pges::postgres::typed_query! {
    /// The number of leaves of a category's partition.
    pub LIVE_LEAVES, mod live_leaves {
        name: "partitions/live_leaves",
        files: [sql_file!("partitions/live_leaves.sql")],
        params {},
        row { leaves: i64 as "count" },
    }
}

pges::postgres::typed_query! {
    /// The category read in one statement with a `LATERAL` lookup per log row, as pges-subscribe read
    /// before it fetched a page's commits by id.
    pub CATEGORY_READ_LATERAL, mod category_read_lateral {
        name: "partitions/category_read_lateral",
        files: [sql_file!("partitions/category_read_lateral.sql")],
        params { category_id: i32, after: i64, head: i64, limit: i64 },
        row {
            last_position: i64,
            event_count: i16,
            stream_id: Uuid,
            last_version: i64,
            commit_id: Uuid,
            format: i16,
            payload: Vec<u8>,
        },
    }
}

pges::postgres::typed_query! {
    /// The backend memory held by cached plans.
    pub PLAN_MEMORY, mod plan_memory {
        name: "partitions/plan_memory",
        files: [sql_file!("partitions/plan_memory.sql")],
        params {},
        row { bytes: i64 as "coalesce" },
    }
}

/// Makes prepared statements use custom plans.
pub const CUSTOM_PLAN: template::Statement = template::Statement {
    name: "partitions/custom_plan",
    files: &[sql_file!("partitions/custom_plan.sql")],
    kind: template::Kind::Ddl,
    params: &[],
    columns: &[],
};

/// Makes prepared statements use a generic plan.
pub const GENERIC_PLAN: template::Statement = template::Statement {
    name: "partitions/generic_plan",
    files: &[sql_file!("partitions/generic_plan.sql")],
    kind: template::Kind::Ddl,
    params: &[],
    columns: &[],
};

/// Restores the default plan cache mode.
pub const RESET_PLAN_MODE: template::Statement = template::Statement {
    name: "partitions/reset_plan_mode",
    files: &[sql_file!("partitions/reset_plan_mode.sql")],
    kind: template::Kind::Ddl,
    params: &[],
    columns: &[],
};

/// Prepares a category read as `bench_read`; the caller appends the statement.
pub const PREPARE_READ: template::Statement = template::Statement {
    name: "partitions/prepare_read",
    files: &[sql_file!("partitions/prepare_read.sql")],
    kind: template::Kind::Ddl,
    params: &[],
    columns: &[],
};

/// Prepares a store-wide commit fetch as `bench_read`; the caller appends the statement.
pub const PREPARE_FETCH: template::Statement = template::Statement {
    name: "partitions/prepare_fetch",
    files: &[sql_file!("partitions/prepare_fetch.sql")],
    kind: template::Kind::Ddl,
    params: &[],
    columns: &[],
};

/// Runs `bench_read` under `EXPLAIN ANALYZE`; the caller appends the parameter values.
pub const EXPLAIN: template::Statement = template::Statement {
    name: "partitions/explain",
    files: &[sql_file!("partitions/explain.sql")],
    kind: template::Kind::Ddl,
    params: &[],
    columns: &[],
};

/// Runs `bench_read`; the caller appends the parameter values.
pub const EXECUTE: template::Statement = template::Statement {
    name: "partitions/execute",
    files: &[sql_file!("partitions/execute.sql")],
    kind: template::Kind::Ddl,
    params: &[],
    columns: &[],
};

/// Drops the prepared statement `bench_read`.
pub const DEALLOCATE: template::Statement = template::Statement {
    name: "partitions/deallocate",
    files: &[sql_file!("partitions/deallocate.sql")],
    kind: template::Kind::Ddl,
    params: &[],
    columns: &[],
};

/// Starts a transaction.
pub const BEGIN: template::Statement = template::Statement {
    name: "partitions/begin",
    files: &[sql_file!("partitions/begin.sql")],
    kind: template::Kind::Ddl,
    params: &[],
    columns: &[],
};

/// Ends the transaction, releasing its locks.
pub const ROLLBACK: template::Statement = template::Statement {
    name: "partitions/rollback",
    files: &[sql_file!("partitions/rollback.sql")],
    kind: template::Kind::Ddl,
    params: &[],
    columns: &[],
};

pges::postgres::typed_query! {
    /// The relation locks this backend holds, and how many are fast-path.
    pub LOCKS, mod locks {
        name: "partitions/locks",
        files: [sql_file!("partitions/locks.sql")],
        params {},
        row { relation: i64 as "count" at 0, fast_path: i64 as "count" at 1 },
    }
}

pges::postgres::typed_query! {
    /// The active client backends of the database, and how many wait on the lock manager.
    pub ACTIVITY, mod activity {
        name: "partitions/activity",
        files: [sql_file!("partitions/activity.sql")],
        params {},
        row { active: i64 as "count" at 0, lock_manager: i64 as "count" at 1 },
    }
}

/// Creates the function that gives a generated commit its id.
pub const COMMIT_ID_FUNCTION: template::Statement = template::Statement {
    name: "scale/commit_id_function",
    files: &[sql_file!("scale/commit_id_function.sql")],
    kind: template::Kind::Ddl,
    params: &[],
    columns: &[],
};

/// Creates the function that gives a generated stream its id.
pub const STREAM_ID_FUNCTION: template::Statement = template::Statement {
    name: "scale/stream_id_function",
    files: &[sql_file!("scale/stream_id_function.sql")],
    kind: template::Kind::Ddl,
    params: &[],
    columns: &[],
};

/// Creates the table recording what each fill generated.
pub const DATASET_CREATE: template::Statement = template::Statement {
    name: "scale/dataset_create",
    files: &[sql_file!("scale/dataset_create.sql")],
    kind: template::Kind::Ddl,
    params: &[],
    columns: &[],
};

pges::postgres::typed_query! {
    /// Records one generated category.
    pub DATASET_INSERT, mod dataset_insert {
        name: "scale/dataset_insert",
        files: [sql_file!("scale/dataset_insert.sql")],
        params<'q> {
            store: &'q str,
            category: &'q str,
            category_id: i32,
            streams: i64,
            commits: i64,
            events: i16,
            t0: i64,
            span: i64,
        },
        row {},
    }
}

pges::postgres::typed_query! {
    /// The generated categories of a store.
    pub DATASET, mod dataset {
        name: "scale/dataset",
        files: [sql_file!("scale/dataset.sql")],
        params<'q> { store: &'q str },
        row {
            category: String,
            category_id: i32,
            streams: i64,
            commits: i64,
            events: i16,
            t0: i64,
            span: i64,
        },
    }
}

pges::postgres::typed_query! {
    /// Whether a database exists.
    pub DATABASE_EXISTS, mod database_exists {
        name: "scale/database_exists",
        files: [sql_file!("scale/database_exists.sql")],
        params<'q> { name: &'q str },
        row { exists: bool },
    }
}

pges::postgres::typed_query! {
    /// The id of a category by name.
    pub CATEGORY_NAMED, mod category_named {
        name: "scale/category_named",
        files: [sql_file!("scale/category_named.sql")],
        params<'q> { name: &'q str },
        row { category_id: i32 },
    }
}

pges::postgres::typed_query! {
    /// When a category's generated commits start and end, in milliseconds since the epoch.
    pub SPAN, mod span {
        name: "scale/span",
        files: [sql_file!("scale/span.sql")],
        params<'q> { unit: &'q str, step: &'q str, back: i32 },
        row { t0: i64, t1: i64 },
    }
}

pges::postgres::typed_query! {
    /// The past leaves of a category, before those maintenance creates.
    pub PAST_BUCKETS, mod past_buckets {
        name: "scale/past_buckets",
        files: [sql_file!("scale/past_buckets.sql")],
        params<'q> { unit: &'q str, step: &'q str, format: &'q str, back: i32 },
        row { suffix: String, lower: String, upper: String },
    }
}

pges::postgres::typed_query! {
    /// The payload frame of a commit.
    pub PAYLOAD_FRAME, mod payload_frame {
        name: "scale/payload_frame",
        files: [sql_file!("scale/payload_frame.sql")],
        params { commit_id: Uuid },
        row { format: i16, payload: Vec<u8> },
    }
}

pges::postgres::typed_query! {
    /// Deletes a template commit and its stream.
    pub FORGET_TEMPLATE, mod forget_template {
        name: "scale/forget_template",
        files: [sql_file!("scale/forget_template.sql")],
        params { commit_id: Uuid, stream_id: Uuid },
        row {},
    }
}

pges::postgres::typed_query! {
    /// Inserts a chunk of a category's generated commits.
    pub FILL_COMMITS, mod fill_commits {
        name: "scale/fill_commits",
        files: [sql_file!("scale/fill_commits.sql")],
        params<'q> {
            category_id: i32,
            from: i64,
            to: i64,
            streams: i64,
            commits: i64,
            t0: i64,
            span: i64,
            events: i16,
            format: i16,
            payload: &'q [u8],
        },
        row {},
    }
}

pges::postgres::typed_query! {
    /// Inserts a chunk of a category's generated streams.
    pub FILL_STREAMS, mod fill_streams {
        name: "scale/fill_streams",
        files: [sql_file!("scale/fill_streams.sql")],
        params {
            category_id: i32,
            from: i64,
            to: i64,
            streams: i64,
            commits: i64,
            t0: i64,
            span: i64,
            events: i16,
        },
        row {},
    }
}

pges::postgres::typed_query! {
    /// Writes the log rows of one time window of generated commits.
    pub FILL_LOG, mod fill_log {
        name: "scale/fill_log",
        files: [sql_file!("scale/fill_log.sql")],
        params<'q> {
            category_ids: &'q [i32],
            froms: &'q [i64],
            tos: &'q [i64],
            t0s: &'q [i64],
            spans: &'q [i64],
            commits: &'q [i64],
            events: &'q [i16],
            after: i64,
        },
        row { last_position: i64 },
    }
}

pges::postgres::typed_query! {
    /// Writes the coarse high-water marks after a fill.
    pub SET_POSITIONS, mod set_positions {
        name: "scale/set_positions",
        files: [sql_file!("scale/set_positions.sql")],
        params<'q> { store: &'q str },
        row {},
    }
}

pges::postgres::typed_query! {
    /// Random generated streams of a category.
    pub SAMPLE_STREAMS, mod sample_streams {
        name: "scale/sample_streams",
        files: [sql_file!("scale/sample_streams.sql")],
        params { category_id: i32, streams: i64, count: i32 },
        row { stream_id: Uuid },
    }
}

pges::postgres::typed_query! {
    /// The store's newest log position.
    pub NEWEST_POSITION, mod newest_position {
        name: "scale/newest_position",
        files: [sql_file!("scale/newest_position.sql")],
        params {},
        row { last_position: i64 },
    }
}

/// Every statement of this crate.
pub const ALL: &[&template::Statement] = &[
    &ANALYZE,
    &VACUUM_ANALYZE,
    &SETTING,
    &WALINSPECT,
    &STAT_STATEMENTS,
    &FLUSH_STATS,
    &CHECKPOINT,
    &WAL_MARK,
    &WAL_END,
    &WAL_COUNTERS,
    &WAL_FLUSHED,
    &FPI_BYTES,
    &IN_RECOVERY,
    &SYNCHRONOUS_STANDBY,
    &RELOAD,
    &POSITIONS,
    &CATEGORY_ID,
    &LOG_PAGE,
    &INSERT_CATEGORY,
    &UTC,
    &DAYS,
    &FILL,
    &WRITE_LOG,
    &LIVE_LEAVES,
    &CATEGORY_READ_LATERAL,
    &PLAN_MEMORY,
    &CUSTOM_PLAN,
    &GENERIC_PLAN,
    &RESET_PLAN_MODE,
    &PREPARE_READ,
    &PREPARE_FETCH,
    &EXPLAIN,
    &EXECUTE,
    &DEALLOCATE,
    &BEGIN,
    &ROLLBACK,
    &LOCKS,
    &ACTIVITY,
    &COMMIT_ID_FUNCTION,
    &STREAM_ID_FUNCTION,
    &DATASET_CREATE,
    &DATASET_INSERT,
    &DATASET,
    &DATABASE_EXISTS,
    &CATEGORY_NAMED,
    &SPAN,
    &PAST_BUCKETS,
    &PAYLOAD_FRAME,
    &FORGET_TEMPLATE,
    &FILL_COMMITS,
    &FILL_STREAMS,
    &FILL_LOG,
    &SET_POSITIONS,
    &SAMPLE_STREAMS,
    &NEWEST_POSITION,
];

/// Every query of this crate with its Rust types, for the registry test.
pub const TYPED: &[typed::Shape] = &[
    typed::Shape::of::<setting::Query>(),
    typed::Shape::of::<flush_stats::Query>(),
    typed::Shape::of::<wal_mark::Query>(),
    typed::Shape::of::<wal_end::Query>(),
    typed::Shape::of::<wal_counters::Query>(),
    typed::Shape::of::<wal_flushed::Query>(),
    typed::Shape::of::<fpi_bytes::Query>(),
    typed::Shape::of::<in_recovery::Query>(),
    typed::Shape::of::<reload::Query>(),
    typed::Shape::of::<positions::Query>(),
    typed::Shape::of::<category_id::Query>(),
    typed::Shape::of::<log_page::Query>(),
    typed::Shape::of::<insert_category::Query>(),
    typed::Shape::of::<days::Query>(),
    typed::Shape::of::<fill::Query>(),
    typed::Shape::of::<write_log::Query>(),
    typed::Shape::of::<live_leaves::Query>(),
    typed::Shape::of::<category_read_lateral::Query>(),
    typed::Shape::of::<plan_memory::Query>(),
    typed::Shape::of::<locks::Query>(),
    typed::Shape::of::<activity::Query>(),
    typed::Shape::of::<dataset_insert::Query>(),
    typed::Shape::of::<dataset::Query>(),
    typed::Shape::of::<database_exists::Query>(),
    typed::Shape::of::<category_named::Query>(),
    typed::Shape::of::<span::Query>(),
    typed::Shape::of::<past_buckets::Query>(),
    typed::Shape::of::<payload_frame::Query>(),
    typed::Shape::of::<forget_template::Query>(),
    typed::Shape::of::<fill_commits::Query>(),
    typed::Shape::of::<fill_streams::Query>(),
    typed::Shape::of::<fill_log::Query>(),
    typed::Shape::of::<set_positions::Query>(),
    typed::Shape::of::<sample_streams::Query>(),
    typed::Shape::of::<newest_position::Query>(),
];
