//! The registry of pges-subscribe: every statement the crate runs.
//!
//! Each statement is a query declared with [`typed_query!`](pges_postgres::typed_query), which adds a module of
//! the same name in lower case with its typed parameters and row, and is listed in [`ALL`] and in [`TYPED`],
//! except the `BEGIN` statements of a handler's transaction, which are listed in [`ALL`] only.
//! `tests/statement.rs` checks every one against PostgreSQL 18.

use sqlx::postgres::types::PgInterval;
use uuid::Uuid;

use pges_postgres::sql::{template, typed};

/// A file below `sql/`, embedded.
macro_rules! sql_file {
    ($p:literal) => {
        template::File {
            path: $p,
            text: include_str!(concat!("../../sql/", $p)),
        }
    };
}

pges_postgres::typed_query! {
    /// `H`, the store's newest position, 0 for an empty log.
    pub HEAD, mod head {
        name: "read/head",
        files: [sql_file!("read/head.sql")],
        params {},
        row { head: i64 as "coalesce" },
    }
}

pges_postgres::typed_query! {
    /// Category ids by name.
    pub CATEGORIES, mod categories {
        name: "read/categories",
        files: [sql_file!("read/categories.sql")],
        params<'q> { names: &'q [&'q str] },
        row { category_id: i32, name: String },
    }
}

pges_postgres::typed_query! {
    /// Category names by id.
    pub CATEGORIES_BY_ID, mod categories_by_id {
        name: "read/categories_by_id",
        files: [sql_file!("read/categories_by_id.sql")],
        params<'q> { category_ids: &'q [i32] },
        row { category_id: i32, name: String },
    }
}

pges_postgres::typed_query! {
    /// Every event type of some categories.
    pub EVENT_TYPES, mod event_types {
        name: "read/event_types",
        files: [sql_file!("read/event_types.sql")],
        params<'q> { category_ids: &'q [i32] },
        row { category_id: i32, event_type_id: i32, name: String },
    }
}

pges_postgres::typed_query! {
    /// The retention horizon of some categories.
    pub HORIZONS, mod horizons {
        name: "read/horizons",
        files: [sql_file!("read/horizons.sql")],
        params<'q> { category_ids: &'q [i32] },
        row { category_id: i32, horizon_position: i64 },
    }
}

pges_postgres::typed_query! {
    /// The category with the highest retention horizon.
    pub HIGHEST_HORIZON, mod highest_horizon {
        name: "read/highest_horizon",
        files: [sql_file!("read/highest_horizon.sql")],
        params {},
        row { category_id: i32, horizon_position: i64 },
    }
}

pges_postgres::typed_query! {
    /// A page of the log of every category, checked against the highest retention horizon.
    pub READ_LOG, mod read_log {
        name: "read/log",
        files: [sql_file!("read/log.sql")],
        params { after: i64, head: i64, limit: i64 },
        row { last_position: i64, commit_id: Uuid, category_id: i32, event_count: i16 },
    }
}

pges_postgres::typed_query! {
    /// Which of some commits `delete_stream` deleted.
    pub DELETED_COMMITS, mod deleted_commits {
        name: "read/deleted_commits",
        files: [sql_file!("read/deleted_commits.sql")],
        params<'q> { category_ids: &'q [i32], commit_ids: &'q [Uuid] },
        row { category_id: i32, commit_id: Uuid },
    }
}

pges_postgres::typed_query! {
    /// A page of one category's log, checked against its retention horizon.
    pub READ_CATEGORY_LOG, mod read_category_log {
        name: "read/category_log",
        files: [sql_file!("read/category_log.sql")],
        params { category_id: i32, after: i64, head: i64, limit: i64 },
        row { last_position: i64, commit_id: Uuid, event_count: i16 },
    }
}

pges_postgres::typed_query! {
    /// The commits of one category named by a page of the log.
    pub FETCH_COMMITS, mod fetch_commits {
        name: "read/commits",
        files: [sql_file!("read/commits.sql")],
        params<'q> { commit_ids: &'q [Uuid] },
        row {
            commit_id: Uuid,
            stream_id: Uuid,
            last_version: i64,
            format: i16,
            payload: Vec<u8>,
        },
    }
}

pges_postgres::typed_query! {
    /// The commits of one category named by a page of the log, with the payloads of one shard.
    pub FETCH_COMMITS_SHARDED, mod fetch_commits_sharded {
        name: "read/commits_sharded",
        files: [sql_file!("read/commits_sharded.sql")],
        params<'q> { commit_ids: &'q [Uuid], shards: i64, shard: i64 },
        row {
            commit_id: Uuid,
            stream_id: Uuid,
            last_version: i64,
            format: i16,
            payload: Option<Vec<u8>>,
        },
    }
}

pges_postgres::typed_query! {
    /// The newest log entry of each of some categories.
    pub NEWEST_BY_CATEGORY, mod newest_by_category {
        name: "read/newest_by_category",
        files: [sql_file!("read/newest_by_category.sql")],
        params<'q> { category_ids: &'q [i32] },
        row { category_id: i32, last_position: i64 },
    }
}

pges_postgres::typed_query! {
    /// Creates every shard row of a subscription unless the subscription exists.
    pub CREATE, mod create {
        name: "subscription/create",
        files: [sql_file!("subscription/create.sql")],
        params<'q> { position: i64, name: &'q str, shards: i16, category_ids: &'q [i32] },
        row {},
    }
}

pges_postgres::typed_query! {
    /// Takes over the lapsed lease of the lowest free shard and bumps its fence, while the database still
    /// supports this crate.
    pub ACQUIRE, mod acquire {
        name: "subscription/acquire",
        files: [sql_file!("subscription/acquire.sql")],
        params<'q> {
            name: &'q str,
            owner: Uuid,
            lease: PgInterval,
            shards: i16,
            shared_version: i32,
            store: &'q str,
            store_version: i32,
            outbox_version: i32,
        },
        row {
            too_old: Option<bool>,
            shard: Option<i16>,
            fence: Option<i64>,
            position: Option<i64>,
            shards: Option<i16>,
            category_ids: Option<Vec<i32>>,
        },
    }
}

pges_postgres::typed_query! {
    /// Replaces the stored definition and cursor of every shard.
    pub REDEFINE, mod redefine {
        name: "subscription/redefine",
        files: [sql_file!("subscription/redefine.sql")],
        params<'q> { name: &'q str, position: i64, shards: i16, category_ids: &'q [i32] },
        row {},
    }
}

pges_postgres::typed_query! {
    /// Moves a shard's cursor, fenced, if it is still at `from`.
    pub CHECKPOINT, mod checkpoint {
        name: "subscription/checkpoint",
        files: [sql_file!("subscription/checkpoint.sql")],
        params<'q> { name: &'q str, shard: i16, position: i64, fence: i64, from: i64 },
        row {},
    }
}

pges_postgres::typed_query! {
    /// Locks a shard's row for a `run_pg` batch, fenced.
    pub HOLD, mod hold {
        name: "subscription/hold",
        files: [sql_file!("subscription/hold.sql")],
        params<'q> {
            name: &'q str,
            shard: i16,
            fence: i64,
            shared_version: i32,
            store: &'q str,
            store_version: i32,
            outbox_version: i32,
            keepalives_idle: i32,
            keepalives_interval: i32,
            keepalives_count: i32,
            user_timeout: i32,
        },
        row { xact: Option<i64> },
    }
}

pges_postgres::typed_query! {
    /// Moves a shard's cursor at the end of a `run_pg` batch and renews its lease, fenced, if the cursor is
    /// still at `from` and it runs in the batch's transaction.
    pub CHECKPOINT_HELD, mod checkpoint_held {
        name: "subscription/checkpoint_held",
        files: [sql_file!("subscription/checkpoint_held.sql")],
        params<'q> {
            name: &'q str,
            shard: i16,
            position: i64,
            fence: i64,
            xact: i64,
            lease: PgInterval,
            from: i64,
        },
        row { in_batch: Option<bool>, moved: Option<bool> },
    }
}

pges_postgres::typed_query! {
    /// Moves a shard's cursor and renews its lease, fenced, if the cursor is still at `from`.
    pub CHECKPOINT_RENEW, mod checkpoint_renew {
        name: "subscription/checkpoint_renew",
        files: [sql_file!("subscription/checkpoint_renew.sql")],
        params<'q> {
            name: &'q str,
            shard: i16,
            position: i64,
            fence: i64,
            lease: PgInterval,
            shared_version: i32,
            store: &'q str,
            store_version: i32,
            outbox_version: i32,
            from: i64,
        },
        row {},
    }
}

pges_postgres::typed_query! {
    /// Renews a shard's lease, fenced.
    pub RENEW, mod renew {
        name: "subscription/renew",
        files: [sql_file!("subscription/renew.sql")],
        params<'q> {
            name: &'q str,
            shard: i16,
            fence: i64,
            lease: PgInterval,
            shared_version: i32,
            store: &'q str,
            store_version: i32,
            outbox_version: i32,
        },
        row {},
    }
}

pges_postgres::typed_query! {
    /// Gives a shard's lease up, fenced.
    pub RELEASE, mod release {
        name: "subscription/release",
        files: [sql_file!("subscription/release.sql")],
        params<'q> { name: &'q str, shard: i16, fence: i64 },
        row {},
    }
}

pges_postgres::typed_query! {
    /// A shard's stored cursor, fenced.
    pub CURSOR, mod cursor {
        name: "subscription/cursor",
        files: [sql_file!("subscription/cursor.sql")],
        params<'q> { name: &'q str, shard: i16, fence: i64 },
        row { position: i64 },
    }
}

pges_postgres::typed_query! {
    /// Records an instance of a sharded subscription as alive and counts the live instances.
    pub HEARTBEAT, mod heartbeat {
        name: "subscription/heartbeat",
        files: [sql_file!("subscription/heartbeat.sql")],
        params<'q> { name: &'q str, owner: Uuid, lease: PgInterval },
        row { members: i64 as "?column?" },
    }
}

pges_postgres::typed_query! {
    /// Removes an instance of a sharded subscription.
    pub LEAVE, mod leave {
        name: "subscription/leave",
        files: [sql_file!("subscription/leave.sql")],
        params<'q> { name: &'q str, owner: Uuid },
        row {},
    }
}

pges_postgres::typed_query! {
    /// Creates a backfill's lanes unless they exist.
    pub BACKFILL_CREATE, mod backfill_create {
        name: "backfill/create",
        files: [sql_file!("backfill/create.sql")],
        params<'q> { name: &'q str, lanes: i32, category_ids: &'q [i32], until: i64 },
        row {},
    }
}

pges_postgres::typed_query! {
    /// A backfill's lanes, with their definition and cursors.
    pub BACKFILL_LANES, mod backfill_lanes {
        name: "backfill/lanes",
        files: [sql_file!("backfill/lanes.sql")],
        params<'q> { name: &'q str },
        row { lane: i16, lanes: i16, until: i64, position: i64, category_ids: Vec<i32> },
    }
}

pges_postgres::typed_query! {
    /// Locks one lane's row for a batch, if its cursor is still where the run left it.
    pub BACKFILL_HOLD, mod backfill_hold {
        name: "backfill/hold",
        files: [sql_file!("backfill/hold.sql")],
        params<'q> { name: &'q str, lane: i16, from: i64 },
        row { xact: Option<i64> },
    }
}

pges_postgres::typed_query! {
    /// Moves one lane's cursor, if it is still where the run left it and this is the batch's transaction.
    pub BACKFILL_CHECKPOINT, mod backfill_checkpoint {
        name: "backfill/checkpoint",
        files: [sql_file!("backfill/checkpoint.sql")],
        params<'q> { name: &'q str, lane: i16, position: i64, from: i64, xact: i64 },
        row { in_batch: Option<bool>, moved: Option<bool> },
    }
}

pges_postgres::typed_query! {
    /// Moves the cursors of lanes that had no events in a page.
    pub BACKFILL_ADVANCE, mod backfill_advance {
        name: "backfill/advance",
        files: [sql_file!("backfill/advance.sql")],
        params<'q> { name: &'q str, lanes: &'q [i16], position: i64 },
        row {},
    }
}

pges_postgres::typed_query! {
    /// A subscription's cursor, for a backfill that ends there.
    pub BACKFILL_SUBSCRIPTION, mod backfill_subscription {
        name: "backfill/subscription",
        files: [sql_file!("backfill/subscription.sql")],
        params<'q> { name: &'q str },
        row { position: i64 },
    }
}

pges_postgres::typed_query! {
    /// The primary's WAL insert position, as a byte offset.
    pub INSERT_LSN, mod insert_lsn {
        name: "replica/insert_lsn",
        files: [sql_file!("replica/insert_lsn.sql")],
        params {},
        row { offset: i64 as "int8" },
    }
}

pges_postgres::typed_query! {
    /// Whether the server is a standby, and the WAL position it has replayed.
    pub REPLAY_LSN, mod replay_lsn {
        name: "replica/replay_lsn",
        files: [sql_file!("replica/replay_lsn.sql")],
        params {},
        row { in_recovery: bool as "pg_is_in_recovery", offset: Option<i64> as "int8" },
    }
}

pges_postgres::typed_query! {
    /// The WAL page and segment sizes.
    pub WAL_LAYOUT, mod wal_layout {
        name: "replica/wal_layout",
        files: [sql_file!("replica/wal_layout.sql")],
        params {},
        row { block: i64 as "current_setting", segment: i64 as "setting" },
    }
}

/// Begins a batch's transaction at the session's default level, with the batch's savepoint.
pub const BEGIN: template::Statement = template::Statement {
    name: "transaction/begin",
    files: &[sql_file!("transaction/begin.sql")],
    kind: template::Kind::Ddl,
    params: &[],
    columns: &[],
};

/// Begins a batch's transaction in READ COMMITTED, with the batch's savepoint.
pub const BEGIN_READ_COMMITTED: template::Statement = template::Statement {
    name: "transaction/begin_read_committed",
    files: &[sql_file!("transaction/begin_read_committed.sql")],
    kind: template::Kind::Ddl,
    params: &[],
    columns: &[],
};

/// Begins a batch's transaction in REPEATABLE READ, with the batch's savepoint.
pub const BEGIN_REPEATABLE_READ: template::Statement = template::Statement {
    name: "transaction/begin_repeatable_read",
    files: &[sql_file!("transaction/begin_repeatable_read.sql")],
    kind: template::Kind::Ddl,
    params: &[],
    columns: &[],
};

/// Begins a batch's transaction in SERIALIZABLE, with the batch's savepoint.
pub const BEGIN_SERIALIZABLE: template::Statement = template::Statement {
    name: "transaction/begin_serializable",
    files: &[sql_file!("transaction/begin_serializable.sql")],
    kind: template::Kind::Ddl,
    params: &[],
    columns: &[],
};

/// Rolls back to the batch's savepoint: fails if the handler ended the batch's transaction.
pub const ROLLBACK_TO_BATCH: template::Statement = template::Statement {
    name: "transaction/rollback_to_batch",
    files: &[sql_file!("transaction/rollback_to_batch.sql")],
    kind: template::Kind::Ddl,
    params: &[],
    columns: &[],
};

/// Every statement of this crate.
pub const ALL: &[&template::Statement] = &[
    &HEAD,
    &CATEGORIES,
    &CATEGORIES_BY_ID,
    &EVENT_TYPES,
    &HORIZONS,
    &HIGHEST_HORIZON,
    &READ_LOG,
    &DELETED_COMMITS,
    &READ_CATEGORY_LOG,
    &FETCH_COMMITS,
    &FETCH_COMMITS_SHARDED,
    &NEWEST_BY_CATEGORY,
    &CREATE,
    &ACQUIRE,
    &REDEFINE,
    &HOLD,
    &CHECKPOINT_HELD,
    &CHECKPOINT,
    &CHECKPOINT_RENEW,
    &RENEW,
    &RELEASE,
    &CURSOR,
    &HEARTBEAT,
    &LEAVE,
    &BACKFILL_CREATE,
    &BACKFILL_LANES,
    &BACKFILL_HOLD,
    &BACKFILL_CHECKPOINT,
    &BACKFILL_ADVANCE,
    &BACKFILL_SUBSCRIPTION,
    &INSERT_LSN,
    &REPLAY_LSN,
    &WAL_LAYOUT,
    &BEGIN,
    &BEGIN_READ_COMMITTED,
    &BEGIN_REPEATABLE_READ,
    &BEGIN_SERIALIZABLE,
    &ROLLBACK_TO_BATCH,
];

/// Every statement of this crate with its Rust types, for the registry test.
pub const TYPED: &[typed::Shape] = &[
    typed::Shape::of::<head::Query>(),
    typed::Shape::of::<categories::Query>(),
    typed::Shape::of::<categories_by_id::Query>(),
    typed::Shape::of::<event_types::Query>(),
    typed::Shape::of::<horizons::Query>(),
    typed::Shape::of::<highest_horizon::Query>(),
    typed::Shape::of::<read_log::Query>(),
    typed::Shape::of::<deleted_commits::Query>(),
    typed::Shape::of::<read_category_log::Query>(),
    typed::Shape::of::<fetch_commits::Query>(),
    typed::Shape::of::<fetch_commits_sharded::Query>(),
    typed::Shape::of::<newest_by_category::Query>(),
    typed::Shape::of::<create::Query>(),
    typed::Shape::of::<acquire::Query>(),
    typed::Shape::of::<redefine::Query>(),
    typed::Shape::of::<hold::Query>(),
    typed::Shape::of::<checkpoint_held::Query>(),
    typed::Shape::of::<checkpoint::Query>(),
    typed::Shape::of::<checkpoint_renew::Query>(),
    typed::Shape::of::<renew::Query>(),
    typed::Shape::of::<release::Query>(),
    typed::Shape::of::<cursor::Query>(),
    typed::Shape::of::<heartbeat::Query>(),
    typed::Shape::of::<leave::Query>(),
    typed::Shape::of::<backfill_create::Query>(),
    typed::Shape::of::<backfill_lanes::Query>(),
    typed::Shape::of::<backfill_hold::Query>(),
    typed::Shape::of::<backfill_checkpoint::Query>(),
    typed::Shape::of::<backfill_advance::Query>(),
    typed::Shape::of::<backfill_subscription::Query>(),
    typed::Shape::of::<insert_lsn::Query>(),
    typed::Shape::of::<replay_lsn::Query>(),
    typed::Shape::of::<wal_layout::Query>(),
];
