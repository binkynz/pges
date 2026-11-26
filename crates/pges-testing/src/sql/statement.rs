//! The registry of pges-testing: every statement the crate runs, besides those of `pges-postgres` it reuses
//! for its fixtures.
//!
//! Each statement is a constant, listed in [`ALL`]. A query is declared with
//! [`typed_query!`](pges_postgres::typed_query), which adds a module of the same name in lower case with its
//! typed parameters and row, and is listed in [`TYPED`] too. `tests/statement.rs` checks every one against
//! PostgreSQL 18.

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

/// Creates an empty database.
pub const CREATE_DATABASE: template::Statement = template::Statement {
    name: "database/create",
    files: &[sql_file!("database/create.sql")],
    kind: template::Kind::Ddl,
    params: &[],
    columns: &[],
};

/// Creates a test database from the template.
pub const CREATE_FROM_TEMPLATE: template::Statement = template::Statement {
    name: "database/create_from_template",
    files: &[sql_file!("database/create_from_template.sql")],
    kind: template::Kind::Ddl,
    params: &[],
    columns: &[],
};

/// Drops a test database, ending its connections.
pub const DROP_DATABASE: template::Statement = template::Statement {
    name: "database/drop",
    files: &[sql_file!("database/drop.sql")],
    kind: template::Kind::Ddl,
    params: &[],
    columns: &[],
};

/// Drops a database if it exists, ending its connections.
pub const DROP_DATABASE_IF_EXISTS: template::Statement = template::Statement {
    name: "database/drop_if_exists",
    files: &[sql_file!("database/drop_if_exists.sql")],
    kind: template::Kind::Ddl,
    params: &[],
    columns: &[],
};

pges_postgres::typed_query! {
    /// The replication slots of a database, with the process streaming from each.
    pub SLOTS, mod slots {
        name: "database/slots",
        files: [sql_file!("database/slots.sql")],
        params<'q> { database: &'q str },
        row { slot_name: String, active_pid: Option<i32> },
    }
}

pges_postgres::typed_query! {
    /// Ends a server process.
    pub TERMINATE, mod terminate {
        name: "database/terminate",
        files: [sql_file!("database/terminate.sql")],
        params { pid: i32 },
        row { terminated: bool as "pg_terminate_backend" },
    }
}

pges_postgres::typed_query! {
    /// Drops a replication slot.
    pub DROP_SLOT, mod drop_slot {
        name: "database/drop_slot",
        files: [sql_file!("database/drop_slot.sql")],
        params<'q> { slot: &'q str },
        row { dropped: () as "pg_drop_replication_slot" },
    }
}

pges_postgres::typed_query! {
    /// The databases named `t_*`.
    pub TEST_DATABASES, mod test_databases {
        name: "database/test_databases",
        files: [sql_file!("database/test_databases.sql")],
        params {},
        row { datname: String },
    }
}

pges_postgres::typed_query! {
    /// Serialises building the template across test processes.
    pub TEMPLATE_LOCK, mod template_lock {
        name: "template/lock",
        files: [sql_file!("template/lock.sql")],
        params {},
        row { locked: () as "pg_advisory_lock" },
    }
}

pges_postgres::typed_query! {
    /// Releases [`TEMPLATE_LOCK`].
    pub TEMPLATE_UNLOCK, mod template_unlock {
        name: "template/unlock",
        files: [sql_file!("template/unlock.sql")],
        params {},
        row { unlocked: bool as "pg_advisory_unlock" },
    }
}

pges_postgres::typed_query! {
    /// Whether a database exists.
    pub DATABASE_EXISTS, mod database_exists {
        name: "template/exists",
        files: [sql_file!("template/exists.sql")],
        params<'q> { name: &'q str },
        row { exists: bool },
    }
}

/// Gives the template, built under a temporary name, its final name.
pub const RENAME_TO_TEMPLATE: template::Statement = template::Statement {
    name: "template/rename",
    files: &[sql_file!("template/rename.sql")],
    kind: template::Kind::Ddl,
    params: &[],
    columns: &[],
};

/// Marks a database as a template.
pub const MARK_TEMPLATE: template::Statement = template::Statement {
    name: "template/mark",
    files: &[sql_file!("template/mark.sql")],
    kind: template::Kind::Ddl,
    params: &[],
    columns: &[],
};

/// Sets the session's time zone to UTC.
pub const UTC: template::Statement = template::Statement {
    name: "fixture/utc",
    files: &[sql_file!("fixture/utc.sql")],
    kind: template::Kind::Ddl,
    params: &[],
    columns: &[],
};

pges_postgres::typed_query! {
    /// Registers a category with the defaults, returning its id.
    pub INSERT_CATEGORY, mod insert_category {
        name: "fixture/insert_category",
        files: [sql_file!("fixture/insert_category.sql")],
        params<'q> { name: &'q str },
        row { category_id: i32 },
    }
}

pges_postgres::typed_query! {
    /// A month leaf's name suffix and bounds.
    pub MONTH, mod month {
        name: "fixture/month",
        files: [sql_file!("fixture/month.sql")],
        params { offset: i32 },
        row { suffix: String as "to_char", from: String as "b", to: String as "text" },
    }
}

pges_postgres::typed_query! {
    /// The primary's WAL insert position, as text.
    pub INSERT_LSN, mod insert_lsn {
        name: "replica/insert_lsn",
        files: [sql_file!("replica/insert_lsn.sql")],
        params {},
        row { lsn: String as "pg_current_wal_insert_lsn" },
    }
}

pges_postgres::typed_query! {
    /// Whether the replica has replayed WAL up to a position.
    pub REPLAYED, mod replayed {
        name: "replica/replayed",
        files: [sql_file!("replica/replayed.sql")],
        params { lsn: typed::PgLsn },
        row { reached: Option<bool> as "?column?" },
    }
}

pges_postgres::typed_query! {
    /// Pauses WAL replay on the replica.
    pub PAUSE_REPLAY, mod pause_replay {
        name: "replica/pause",
        files: [sql_file!("replica/pause.sql")],
        params {},
        row { paused: () as "pg_wal_replay_pause" },
    }
}

pges_postgres::typed_query! {
    /// Resumes WAL replay on the replica.
    pub RESUME_REPLAY, mod resume_replay {
        name: "replica/resume",
        files: [sql_file!("replica/resume.sql")],
        params {},
        row { resumed: () as "pg_wal_replay_resume" },
    }
}

pges_postgres::typed_query! {
    /// What the other connections of this connection's application hold: open transactions and locks.
    pub CONNECTIONS, mod connections {
        name: "cancel/connections",
        files: [sql_file!("cancel/connections.sql")],
        params {},
        row { started_micros: Option<i64>, in_transaction: Option<i64>, locks: Option<i64> },
    }
}

pges_postgres::typed_query! {
    /// Whether another connection to this database ran a statement and waits inside its transaction.
    pub WAITING, mod waiting {
        name: "cancel/waiting",
        files: [sql_file!("cancel/waiting.sql")],
        params<'q> { statement: &'q str },
        row { waiting: Option<bool> },
    }
}

/// Every statement of this crate.
pub const ALL: &[&template::Statement] = &[
    &CREATE_DATABASE,
    &CREATE_FROM_TEMPLATE,
    &DROP_DATABASE,
    &DROP_DATABASE_IF_EXISTS,
    &SLOTS,
    &TERMINATE,
    &DROP_SLOT,
    &TEST_DATABASES,
    &TEMPLATE_LOCK,
    &TEMPLATE_UNLOCK,
    &DATABASE_EXISTS,
    &RENAME_TO_TEMPLATE,
    &MARK_TEMPLATE,
    &UTC,
    &INSERT_CATEGORY,
    &MONTH,
    &INSERT_LSN,
    &REPLAYED,
    &PAUSE_REPLAY,
    &RESUME_REPLAY,
    &CONNECTIONS,
    &WAITING,
];

/// Every query of this crate with its Rust types, for the registry test.
pub const TYPED: &[typed::Shape] = &[
    typed::Shape::of::<slots::Query>(),
    typed::Shape::of::<terminate::Query>(),
    typed::Shape::of::<drop_slot::Query>(),
    typed::Shape::of::<test_databases::Query>(),
    typed::Shape::of::<template_lock::Query>(),
    typed::Shape::of::<template_unlock::Query>(),
    typed::Shape::of::<database_exists::Query>(),
    typed::Shape::of::<insert_category::Query>(),
    typed::Shape::of::<month::Query>(),
    typed::Shape::of::<insert_lsn::Query>(),
    typed::Shape::of::<replayed::Query>(),
    typed::Shape::of::<pause_replay::Query>(),
    typed::Shape::of::<resume_replay::Query>(),
    typed::Shape::of::<connections::Query>(),
    typed::Shape::of::<waiting::Query>(),
];
