//! Test support for pges.
//!
//! - [`server`] finds a PostgreSQL 18 server with logical replication enabled: `PGES_TEST_DATABASE_URL`, the
//!   local cluster from `pg-up` in the Nix dev shell, or a reusable Docker container started through
//!   testcontainers, in that order. `PGES_TEST_BACKEND=docker|local` forces one.
//! - [`TestDb::new`] gives each test its own database, cloned from a template that already has the shared
//!   `pges` migration applied. Cloning a template is a file copy, so it takes milliseconds. A failed test
//!   leaves nothing behind: dropping a [`TestDb`] that was not cleaned up drops its database and slots.
//! - [`create_store`] creates a store with the migration runner of `pges-postgres`. [`register_category`] and
//!   [`create_log_partition`] create partitions with the statements `Admin` runs, but also in the past, so
//!   tests can have data in leaves retention would drop.
//! - [`proxy::Proxy`] sits between a test and the server and can drop every connection, hold traffic one
//!   way, or refuse new connections, to test what happens when a connection or a response is lost.
//! - [`replica::Pair`] starts a private primary with a streaming hot standby, for replica reads.
//! - [`cancel`] drops a future at each of its await points and checks that it left no connection inside a
//!   transaction or holding a lock.
//! - `tls::Pki` (feature `tls`) generates a CA, server and client certificates for TLS tests.
//! - [`statement`] checks a crate's statement registry: files, parameter and column types, DDL, and that no
//!   SQL bypasses it.
//!
//! The container is started with `ReuseDirective::Always` under a fixed name, so it survives the test process
//! and the next run reuses it. Remove it with `docker rm -f pges-test-pg18`.
#![cfg_attr(docsrs, feature(doc_cfg))]
#![warn(missing_docs)]

pub mod cancel;
pub mod proxy;
pub mod replica;
#[doc(hidden)]
pub mod sql;
pub mod statement;
#[cfg(feature = "tls")]
pub mod tls;

#[cfg(doctest)]
#[doc = include_str!("../README.md")]
struct ReadmeDoctests;

use std::num::NonZeroU16;

use anyhow::{Context, Result, bail};
use sqlx::{Connection, Executor, PgConnection, PgPool};
use testcontainers::{ImageExt, ReuseDirective, runners::AsyncRunner};
use testcontainers_modules::postgres::Postgres;
use tokio::sync::OnceCell;

use pges_core::{category, position, store as core_store, version};
use pges_postgres::{
    migrate,
    sql::{self as postgres_sql, template, typed::Query as _},
    store,
};

/// Names a server to use instead of the local cluster or Docker.
pub const ENV_URL: &str = "PGES_TEST_DATABASE_URL";
const CONTAINER_NAME: &str = "pges-test-pg18";
const CONTAINER_TAG: &str = "18";

/// Server settings the tests rely on. `pg-up` in flake.nix passes the same ones.
const SERVER_ARGS: &[&str] = &[
    "-c",
    "wal_level=logical",
    "-c",
    "max_replication_slots=64",
    "-c",
    "max_wal_senders=64",
    "-c",
    "max_connections=300",
    "-c",
    "fsync=off",
    "-c",
    "synchronous_commit=off",
    "-c",
    "full_page_writes=off",
];

/// Forces a backend: `docker` or `local`.
pub const ENV_BACKEND: &str = "PGES_TEST_BACKEND";
/// Directory of the local cluster started by `pg-up`; defaults to `.pg` at the workspace root.
pub const ENV_PG_DIR: &str = "PGES_PG_DIR";
const LOCAL_PORT: u16 = 54318;

/// Where the test server comes from. Chosen once per process:
///
/// 1. `PGES_TEST_DATABASE_URL`, if set.
/// 2. `PGES_TEST_BACKEND=docker` or `=local` forces that backend.
/// 3. Otherwise the local cluster started by `pg-up` (Nix dev shell), if its socket exists.
/// 4. Otherwise a reusable Docker container.
enum Backend {
    Url(String),
    Local { dir: std::path::PathBuf },
    Docker,
}

impl Backend {
    fn select() -> Result<Self> {
        if let Ok(url) = std::env::var(ENV_URL) {
            return Ok(Self::Url(url));
        }

        let dir = std::env::var_os(ENV_PG_DIR)
            .map(std::path::PathBuf::from)
            .unwrap_or_else(|| std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../.pg"));
        let local_running = dir.join(format!(".s.PGSQL.{LOCAL_PORT}")).exists();

        match std::env::var(ENV_BACKEND).as_deref() {
            Ok("docker") => Ok(Self::Docker),
            Ok("local") if local_running => Ok(Self::Local { dir }),
            Ok("local") => bail!(
                "{ENV_BACKEND}=local but no server at {}; run pg-up",
                dir.display()
            ),
            Ok(other) => bail!("{ENV_BACKEND} must be docker or local, not {other:?}"),
            Err(_) if local_running => Ok(Self::Local { dir }),
            Err(_) => Ok(Self::Docker),
        }
    }
}

impl std::fmt::Display for Backend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Url(_) => write!(f, "{ENV_URL}"),
            Self::Local { dir } => write!(f, "local cluster in {}", dir.display()),
            Self::Docker => write!(f, "Docker container {CONTAINER_NAME}"),
        }
    }
}

/// The test server shared by every test in the process.
pub struct Server {
    /// URL of the server's `postgres` database.
    admin_url: String,
}

static SERVER: OnceCell<Server> = OnceCell::const_new();

/// The shared test server, started on first use.
pub async fn server() -> Result<&'static Server> {
    SERVER
        .get_or_try_init(|| async {
            let backend = Backend::select()?;
            eprintln!("pges-testing: using {backend}");
            let container = match backend {
                Backend::Url(url) => return Ok(Server { admin_url: url }),
                Backend::Local { dir } => {
                    return Ok(Server {
                        admin_url: format!(
                            "postgres://postgres@localhost/postgres?host={}&port={LOCAL_PORT}",
                            dir.display()
                        ),
                    });
                }
                Backend::Docker => Postgres::default(),
            };
            let container = container
                .with_tag(CONTAINER_TAG)
                .with_cmd(SERVER_ARGS.iter().copied())
                .with_container_name(CONTAINER_NAME)
                .with_reuse(ReuseDirective::Always)
                .start()
                .await
                .context("starting the PostgreSQL 18 container (is Docker running? or set PGES_TEST_DATABASE_URL)")?;
            let host = container.get_host().await?;
            let port = container.get_host_port_ipv4(5432).await?;
            // Reused containers are never stopped by testcontainers; leaking the handle keeps it that way.
            std::mem::forget(container);
            Ok(Server { admin_url: format!("postgres://postgres:postgres@{host}:{port}/postgres") })
        })
        .await
}

impl Server {
    /// URL of the server's `postgres` database.
    pub fn admin_url(&self) -> &str {
        &self.admin_url
    }

    /// URL of another database on the same server.
    pub fn url_for(&self, database: &str) -> Result<String> {
        let mut url = url_parts(&self.admin_url)?;
        url.path = format!("/{database}");
        Ok(url.join())
    }

    /// A connection to the server's `postgres` database.
    pub async fn admin(&self) -> Result<PgConnection> {
        PgConnection::connect(&self.admin_url)
            .await
            .context("connecting to the test server")
    }
}

/// A fresh database for one test. Call [`TestDb::cleanup`] at the end of the test.
///
/// A test that fails never reaches its cleanup, so dropping a `TestDb` that was not cleaned up drops the
/// database itself: it ends the processes streaming from the database's replication slots, drops the
/// slots, then the database ([`drop_database`]). `Drop` cannot await, so it does this on a thread of its
/// own with its own runtime and blocks until it is done; that works inside an async test, during a panic,
/// and with no runtime at all. Errors are printed, never raised.
///
/// A test process that is killed runs no `Drop`. So the first [`TestDb::new`] of each process also
/// drops `t_*` databases created more than an hour ago (the name carries its creation time), with their
/// slots: those belong to processes that are gone.
pub struct TestDb {
    name: String,
    url: String,
    ownership: Ownership,
}

impl TestDb {
    /// The database's name: `t_<uuidv7>` for one made by [`TestDb::new`].
    pub fn name(&self) -> &str {
        &self.name
    }

    /// The connection URL for the database.
    pub fn url(&self) -> &str {
        &self.url
    }

    /// Clones the migrated template into a new database.
    pub async fn new() -> Result<Self> {
        let server = server().await?;
        SWEPT
            .get_or_init(|| sweep(server.admin_url(), STALE_AFTER))
            .await;
        let template = ensure_template(server).await?;
        let name = format!("t_{}", uuid::Uuid::now_v7().simple());
        let values = template::Values::new()
            .ident(template::IdentPlaceholder::Database, &ident(&name)?)
            .ident(template::IdentPlaceholder::Template, &ident(&template)?);
        let mut admin = server.admin().await?;
        admin
            .execute(sql::statement::CREATE_FROM_TEMPLATE.render_text(&values))
            .await
            .with_context(|| format!("cloning {template}"))?;
        Ok(Self {
            url: server.url_for(&name)?,
            name,
            ownership: Ownership::Owned(Cleanup::Pending),
        })
    }

    /// A database this crate did not create, such as one on a server of the test's own. Neither
    /// [`TestDb::cleanup`] nor dropping it touches it.
    pub fn existing(name: impl Into<String>, url: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            url: url.into(),
            ownership: Ownership::Existing,
        }
    }

    /// A small connection pool for this database.
    pub async fn pool(&self) -> Result<PgPool> {
        sqlx::postgres::PgPoolOptions::new()
            .max_connections(8)
            .connect(&self.url)
            .await
            .with_context(|| format!("connecting to {}", self.name))
    }

    /// A new connection to this database.
    pub async fn connect(&self) -> Result<PgConnection> {
        PgConnection::connect(&self.url)
            .await
            .with_context(|| format!("connecting to {}", self.name))
    }

    /// Drops the database, terminating any connections still open to it. Replication slots in the database
    /// must be dropped first: a slot left behind fails the cleanup, so a test notices it.
    pub async fn cleanup(mut self) -> Result<()> {
        if self.ownership == Ownership::Existing {
            return Ok(());
        }

        let drop = sql::statement::DROP_DATABASE.render(&database(&self.name)?);
        let mut admin = server().await?.admin().await?;
        admin.execute(template::text(drop.as_str())).await?;
        self.ownership = Ownership::Owned(Cleanup::Done);
        Ok(())
    }

    /// Like [`TestDb::cleanup`], but first ends the processes streaming from the database's replication
    /// slots and drops the slots ([`drop_database`]).
    pub async fn cleanup_with_slots(mut self) -> Result<()> {
        if self.ownership == Ownership::Existing {
            return Ok(());
        }

        drop_database(server().await?.admin_url(), &self.name).await?;
        self.ownership = Ownership::Owned(Cleanup::Done);
        Ok(())
    }
}

impl Drop for TestDb {
    fn drop(&mut self) {
        if self.ownership != Ownership::Owned(Cleanup::Pending) {
            return;
        }
        let Some(server) = SERVER.get() else {
            return;
        };

        let admin_url = server.admin_url.clone();
        let name = self.name.clone();
        let dropped = std::thread::spawn(move || -> Result<()> {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()?;
            runtime.block_on(drop_database(&admin_url, &name))
        })
        .join();
        match dropped {
            Ok(Ok(())) => {}
            Ok(Err(error)) => eprintln!("pges-testing: dropping {}: {error:#}", self.name),
            Err(_) => eprintln!("pges-testing: dropping {} panicked", self.name),
        }
    }
}

impl std::fmt::Debug for Server {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Server").finish_non_exhaustive()
    }
}

impl std::fmt::Debug for TestDb {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TestDb")
            .field("name", &self.name)
            .field("ownership", &self.ownership)
            .finish_non_exhaustive()
    }
}

/// Drops database `name` on the server of `admin_url`, a URL of another database on that server: ends the
/// processes streaming from the database's replication slots, drops the slots, then drops the database with
/// `FORCE`, which ends its other connections. A missing database is not an error. Slots whose process is
/// slow to end are retried for up to 20 s. Fails without dropping anything if `name` is not a plain
/// identifier (`^[a-z][a-z0-9_]*$`, at most 63 bytes), as every database this crate and the benchmarks
/// create is.
pub async fn drop_database(admin_url: &str, name: &str) -> Result<()> {
    let drop = sql::statement::DROP_DATABASE_IF_EXISTS.render(&database(name)?);
    let mut admin = PgConnection::connect(admin_url)
        .await
        .context("connecting to the server")?;
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(20);

    loop {
        let slots = sql::statement::slots::Query::fixed()
            .bind(sql::statement::slots::Params { database: name })
            .fetch_all(&mut admin)
            .await?;
        if slots.is_empty() {
            break;
        }

        let mut failed = None;
        for slot in slots {
            if let Some(pid) = slot.active_pid {
                sql::statement::terminate::Query::fixed()
                    .bind(sql::statement::terminate::Params { pid })
                    .execute(&mut admin)
                    .await?;
            }
            if let Err(error) = sql::statement::drop_slot::Query::fixed()
                .bind(sql::statement::drop_slot::Params {
                    slot: &slot.slot_name,
                })
                .execute(&mut admin)
                .await
            {
                failed = Some((slot.slot_name, error));
            }
        }
        match failed {
            None => break,
            Some((slot, error)) if tokio::time::Instant::now() >= deadline => {
                return Err(error).with_context(|| format!("dropping replication slot {slot}"));
            }
            Some(_) => tokio::time::sleep(std::time::Duration::from_millis(50)).await,
        }
    }

    admin
        .execute(template::text(drop.as_str()))
        .await
        .with_context(|| format!("dropping {name}"))?;
    Ok(())
}

/// Whether [`TestDb`] drops its database.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Ownership {
    Owned(Cleanup),
    Existing,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Cleanup {
    Pending,
    Done,
}

/// How old a `t_*` database must be before [`sweep`] drops it. Far longer than any test runs, because other
/// test processes may share the server.
const STALE_AFTER: std::time::Duration = std::time::Duration::from_secs(3600);

static SWEPT: OnceCell<()> = OnceCell::const_new();

/// Drops the `t_*` databases created more than `stale_after` ago, with their replication slots. Failures
/// are printed: a sweep is housekeeping and must not fail a test.
async fn sweep(admin_url: &str, stale_after: std::time::Duration) {
    let Some(cutoff) = std::time::SystemTime::now().checked_sub(stale_after) else {
        return;
    };
    let names: Vec<String> = match async {
        let mut admin = PgConnection::connect(admin_url).await?;
        sql::statement::test_databases::Query::fixed()
            .bind(sql::statement::test_databases::Params {})
            .fetch_all(&mut admin)
            .await
    }
    .await
    {
        Ok(rows) => rows.into_iter().map(|row| row.datname).collect(),
        Err(error) => {
            eprintln!("pges-testing: listing stale test databases: {error}");
            return;
        }
    };

    for name in names {
        let Some(created) = created_at(&name) else {
            continue;
        };
        if created >= cutoff {
            continue;
        }
        if let Err(error) = drop_database(admin_url, &name).await {
            eprintln!("pges-testing: dropping stale test database {name}: {error:#}");
        }
    }
}

/// When the `t_<uuidv7>` database `name` was created, from the timestamp in its UUID.
fn created_at(name: &str) -> Option<std::time::SystemTime> {
    let id = uuid::Uuid::try_parse(name.strip_prefix("t_")?).ok()?;
    let (seconds, nanos) = id.get_timestamp()?.to_unix();

    std::time::UNIX_EPOCH.checked_add(std::time::Duration::new(seconds, nanos))
}

/// The template's name carries a hash of the shared migrations, so editing one builds a new template.
async fn ensure_template(server: &Server) -> Result<String> {
    let checksums: Vec<u8> = migrate::SHARED.iter().flat_map(|m| m.checksum()).collect();
    let name = format!("pges_tpl_{:016x}", fnv1a(&checksums));
    // Built under a temporary name and renamed at the end, so a crash never leaves a half-built template.
    let building = format!("{name}_building");
    let values = template::Values::new()
        .ident(template::IdentPlaceholder::Database, &ident(&building)?)
        .ident(template::IdentPlaceholder::Template, &ident(&name)?);
    let mut admin = server.admin().await?;
    admin.execute(sql::statement::TEMPLATE_LOCK.sql()).await?;
    let result = async {
        let exists = sql::statement::database_exists::Query::fixed()
            .bind(sql::statement::database_exists::Params { name: &name })
            .fetch_one(&mut admin)
            .await?
            .exists;
        if exists {
            return Ok(());
        }
        admin
            .execute(sql::statement::DROP_DATABASE_IF_EXISTS.render_text(&values))
            .await?;
        admin
            .execute(sql::statement::CREATE_DATABASE.render_text(&values))
            .await?;
        {
            let mut conn = PgConnection::connect(&server.url_for(&building)?).await?;
            apply_pges(&mut conn).await?;
            conn.close().await?;
        }
        admin
            .execute(sql::statement::RENAME_TO_TEMPLATE.render_text(&values))
            .await?;
        admin
            .execute(sql::statement::MARK_TEMPLATE.render_text(&values))
            .await?;
        Ok::<_, anyhow::Error>(())
    }
    .await;
    admin.execute(sql::statement::TEMPLATE_UNLOCK.sql()).await?;
    result.map(|()| name)
}

async fn apply_pges(conn: &mut PgConnection) -> Result<()> {
    migrate::run_shared(conn)
        .await
        .context("shared pges migrations")
}

/// A store definition for tests: the defaults with `outbox`, and 4 hash partitions of `streams` and
/// `snapshots` instead of 64, so tests stay fast.
pub fn definition(outbox: store::Outbox) -> store::StoreDefinition {
    let mut definition = store::StoreDefinition::default();
    definition.outbox = outbox;
    definition.stream_partitions = TEST_STREAM_PARTITIONS;

    definition
}

/// Creates a store with the migration runner: one transaction, `search_path` set to the store.
///
/// A database holds stores of one outbox kind: a store whose `outbox` differs from the existing stores'
/// fails with `pges_postgres::migrate::MigrateError::OutboxMismatch`, which `anyhow::Error::downcast_ref`
/// returns.
pub async fn create_store(
    conn: &mut PgConnection,
    store: &core_store::StoreName,
    definition: &store::StoreDefinition,
) -> Result<()> {
    migrate::run_store(conn, store, definition)
        .await
        .with_context(|| format!("creating store {store}"))
}

/// Registers a category and attaches its partitions with the statements `Admin` runs: the LIST partition
/// `commits_<category>`, and monthly leaves from `months_back` before the current month to `months_ahead`
/// after it, each created detached, constrained, then attached. Returns the category's id.
pub async fn register_category(
    conn: &mut PgConnection,
    store: &core_store::StoreName,
    category: category::CategoryName,
    months_back: u16,
    months_ahead: u16,
) -> Result<category::CategoryId> {
    conn.execute(sql::statement::UTC.sql()).await?;
    let insert =
        sql::statement::insert_category::Query::render(&template::Values::new().store(store));
    let id = category::CategoryId::new(
        insert
            .bind(sql::statement::insert_category::Params {
                name: category.as_str(),
            })
            .fetch_one(&mut *conn)
            .await?
            .category_id,
    );
    let values = template::Values::new()
        .store(store)
        .category(category)
        .category_id(id);
    let key = ident(&format!("commits_{category}__pkey"))?;
    conn.execute(
        postgres_sql::statement::CREATE_CATEGORY_PARTITION.render_text(
            &values
                .clone()
                .ident(template::IdentPlaceholder::Index, &key),
        ),
    )
    .await
    .context("creating the category partition")?;
    conn.execute(postgres_sql::statement::ATTACH_CATEGORY_PARTITION.render_text(&values))
        .await
        .context("attaching the category partition")?;

    for offset in -i32::from(months_back)..=i32::from(months_ahead) {
        let sql::statement::month::Row { suffix, from, to } = sql::statement::month::Query::fixed()
            .bind(sql::statement::month::Params { offset })
            .fetch_one(&mut *conn)
            .await?;
        let leaf = ident(&format!("commits_{category}__{suffix}"))?;
        let leaf_values = values
            .clone()
            .ident(template::IdentPlaceholder::Leaf, &leaf)
            .ident(template::IdentPlaceholder::Table, &leaf)
            .ident(
                template::IdentPlaceholder::Index,
                &ident(&format!("{leaf}_s"))?,
            )
            .ident(template::IdentPlaceholder::Bound, &ident(BOUND)?)
            .range(&timestamptz(&from)?, &timestamptz(&to)?);
        for statement in [
            &postgres_sql::statement::CREATE_LEAF,
            &postgres_sql::statement::COMPRESS_LEAF,
            &postgres_sql::statement::CREATE_LEAF_INDEX,
            &postgres_sql::statement::ADD_LEAF_BOUND,
            &postgres_sql::statement::ATTACH_LEAF,
            &postgres_sql::statement::DROP_BOUND,
        ] {
            let ddl = statement.render(&leaf_values);
            conn.execute(template::text(ddl.as_str()))
                .await
                .with_context(|| format!("leaf DDL: {ddl}"))?;
        }
    }
    Ok(id)
}

/// Creates and attaches log partition `n`, covering positions [n * 10^8, (n + 1) * 10^8), with the statements
/// `Admin` runs.
pub async fn create_log_partition(
    conn: &mut PgConnection,
    store: &core_store::StoreName,
    n: u32,
) -> Result<()> {
    let (from, to) = (i64::from(n) * 100_000_000, (i64::from(n) + 1) * 100_000_000);
    let partition = ident(&format!("log_{n:04}"))?;
    let values = template::Values::new()
        .store(store)
        .ident(template::IdentPlaceholder::LogPartition, &partition)
        .ident(template::IdentPlaceholder::Table, &partition)
        .ident(template::IdentPlaceholder::Bound, &ident(BOUND)?)
        .range(&template::Literal::int(from), &template::Literal::int(to));
    for statement in [
        &postgres_sql::statement::CREATE_LOG_PARTITION,
        &postgres_sql::statement::ADD_LOG_BOUND,
        &postgres_sql::statement::ATTACH_LOG_PARTITION,
        &postgres_sql::statement::DROP_BOUND,
    ] {
        let ddl = statement.render(&values);
        conn.execute(template::text(ddl.as_str()))
            .await
            .with_context(|| format!("log DDL: {ddl}"))?;
    }
    Ok(())
}

/// `n` as a position, for tests. Panics above [`position::Position::MAX`]; a literal can use
/// `pges_core::position!` instead.
#[allow(clippy::expect_used)]
pub fn position(n: u64) -> position::Position {
    position::Position::new(n).expect("a position is at most i64::MAX")
}

/// `n` as a version, for tests. Panics for 0 or above [`version::Version::MAX`]; a literal can use
/// `pges_core::version!` instead.
#[allow(clippy::expect_used)]
pub fn version(n: u64) -> version::Version {
    version::Version::new(n).expect("a version is between 1 and i64::MAX")
}

/// The CHECK constraint a new partition carries while it is attached, as `Admin` names it.
const BOUND: &str = "bound";

/// Hash partitions of `streams` and `snapshots` in [`definition`]: small, so tests stay fast.
const TEST_STREAM_PARTITIONS: NonZeroU16 = match NonZeroU16::new(4) {
    Some(partitions) => partitions,
    None => NonZeroU16::MIN,
};

/// A name interpolated into SQL: database, partition, index and constraint names.
fn ident(name: &str) -> Result<template::Ident> {
    template::Ident::new(name)
        .with_context(|| format!("{name:?} must match ^[a-z][a-z0-9_]*$ and fit in 63 bytes"))
}

/// The values of a statement that names one database.
fn database(name: &str) -> Result<template::Values> {
    Ok(template::Values::new().ident(template::IdentPlaceholder::Database, &ident(name)?))
}

fn timestamptz(text: &str) -> Result<template::Literal> {
    template::Literal::timestamptz(text).with_context(|| format!("timestamptz text {text:?}"))
}

fn fnv1a(bytes: &[u8]) -> u64 {
    bytes.iter().fold(0xcbf2_9ce4_8422_2325, |h, b| {
        (h ^ u64::from(*b)).wrapping_mul(0x0100_0000_01b3)
    })
}

/// Just enough URL handling to swap the database name while keeping query parameters (unix socket `host`).
struct UrlParts {
    prefix: String,
    path: String,
    query: String,
}

impl UrlParts {
    fn join(&self) -> String {
        format!("{}{}{}", self.prefix, self.path, self.query)
    }
}

fn url_parts(url: &str) -> Result<UrlParts> {
    let Some(scheme_end) = url.find("://") else {
        bail!("not a postgres URL: {url}")
    };
    let after = scheme_end + 3;
    let (body, query) = match url[after..].find('?') {
        Some(q) => (&url[..after + q], url[after + q..].to_string()),
        None => (url, String::new()),
    };
    let (prefix, path) = match body[after..].find('/') {
        Some(p) => (body[..after + p].to_string(), body[after + p..].to_string()),
        None => (body.to_string(), String::new()),
    };
    Ok(UrlParts {
        prefix,
        path,
        query,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reads_the_creation_time_from_the_name() {
        let before = std::time::SystemTime::now() - std::time::Duration::from_millis(2);
        let name = format!("t_{}", uuid::Uuid::now_v7().simple());

        let created = created_at(&name).unwrap();
        assert!(created >= before && created <= std::time::SystemTime::now());
        assert_eq!(created_at("t_not_a_uuid"), None);
        assert_eq!(created_at("pges_tpl_0123"), None);
        // A UUID without a timestamp.
        assert_eq!(
            created_at(&format!("t_{}", uuid::Uuid::nil().simple())),
            None
        );
    }

    #[tokio::test]
    async fn the_sweep_drops_only_stale_test_databases() {
        let server = server().await.unwrap();
        let hours_ago = |hours: u64| {
            let at = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                - std::time::Duration::from_secs(hours * 3600);
            let timestamp =
                uuid::Timestamp::from_unix(uuid::NoContext, at.as_secs(), at.subsec_nanos());
            format!("t_{}", uuid::Uuid::new_v7(timestamp).simple())
        };
        let stale = hours_ago(2);
        let fresh = hours_ago(0);
        let mut admin = server.admin().await.unwrap();
        for name in [&stale, &fresh] {
            admin
                .execute(sqlx::AssertSqlSafe(format!(r#"CREATE DATABASE "{name}""#)))
                .await
                .unwrap();
        }

        sweep(server.admin_url(), STALE_AFTER).await;

        let left: Vec<String> = sqlx::query_scalar(
            "SELECT datname::text FROM pg_database WHERE datname = ANY($1) ORDER BY 1",
        )
        .bind(vec![stale.clone(), fresh.clone()])
        .fetch_all(&mut admin)
        .await
        .unwrap();
        drop_database(server.admin_url(), &fresh).await.unwrap();
        drop_database(server.admin_url(), &stale).await.unwrap();
        assert_eq!(left, [fresh]);
    }

    #[test]
    fn swaps_database_and_keeps_socket_params() {
        let u = url_parts("postgres://postgres@/postgres?host=/tmp/x/.pg&port=54318").unwrap();
        assert_eq!(
            UrlParts {
                path: "/t_1".into(),
                ..u
            }
            .join(),
            "postgres://postgres@/t_1?host=/tmp/x/.pg&port=54318"
        );
        let u = url_parts("postgres://p:p@localhost:5432/postgres").unwrap();
        assert_eq!(
            UrlParts {
                path: "/t_1".into(),
                ..u
            }
            .join(),
            "postgres://p:p@localhost:5432/t_1"
        );
    }

    #[test]
    fn rejects_unsafe_identifiers() {
        assert!(ident("t_0193abc").is_ok());
        assert!(ident("T_x\"").is_err());
    }
}
