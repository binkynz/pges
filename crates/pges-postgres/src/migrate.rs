//! Embedded, checksummed migrations.
//!
//! Three kinds of scope are versioned separately, each with its own rows in `pges.migrations` and
//! `pges.scopes`:
//!
//! - the shared `pges` schema, once per database ([`SHARED`], applied by [`run_shared`]);
//! - each store schema ([`STORE`], applied by [`run_store`], which also creates the store);
//! - the outbox of a store created with [`store::Outbox::Enabled`], scope `<store>.outbox` ([`OUTBOX`],
//!   applied by [`run_store`] when it creates the store).
//!
//! Every migration runs in its own transaction holding `pg_advisory_xact_lock(hashtext('pges.migrate'))`,
//! so concurrent runners apply each migration once. Store migrations run with `SET LOCAL search_path` set to
//! the store, so their SQL uses unqualified names. A migration that was already applied is compared with the
//! embedded text by its SHA-256 checksum; an edited migration fails with [`MigrateError::ChecksumMismatch`].
//!
//! Migrations are expand/contract: a database migrated by a newer release stays usable by older releases
//! down to the scope's `min_compatible` version. A runner whose newest migration is below `min_compatible`
//! refuses to run with [`MigrateError::CrateTooOld`].

use std::{collections::BTreeMap, fmt};

use futures_util::future::BoxFuture;
use sha2::{Digest, Sha256};
use sqlx::{Connection, Executor, PgConnection};

use pges_core::store as core_store;

use crate::{
    error,
    sql::{statement, template, typed::Query as _},
    store,
};

/// The scope name of the shared `pges` schema.
pub const SHARED_SCOPE: &str = "pges";

/// One embedded migration.
#[derive(Debug, Clone, Copy)]
pub struct Migration {
    version: SchemaVersion,
    name: &'static str,
    sql: &'static str,
    min_compatible: SchemaVersion,
}

impl Migration {
    /// The migration's version within its scope, starting at 1.
    pub const fn version(&self) -> SchemaVersion {
        self.version
    }

    /// The migration's name, such as `0001_init`.
    pub const fn name(&self) -> &'static str {
        self.name
    }

    /// The SQL text.
    pub const fn sql(&self) -> &'static str {
        self.sql
    }

    /// The oldest schema version of the scope that crates must support once this migration is applied.
    pub const fn min_compatible(&self) -> SchemaVersion {
        self.min_compatible
    }

    /// The SHA-256 of the SQL text, as stored in `pges.migrations.checksum`.
    pub fn checksum(&self) -> [u8; 32] {
        Sha256::digest(self.sql.as_bytes()).into()
    }
}

/// A scope's schema version: the version of a migration, or of the newest one applied to a scope.
/// Migrations of a scope are numbered from 1.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct SchemaVersion(i32);

impl SchemaVersion {
    /// No migration: the version of a scope that does not exist yet.
    pub const NONE: Self = Self(0);

    /// The number.
    pub const fn get(self) -> i32 {
        self.0
    }
}

impl fmt::Display for SchemaVersion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

/// Migrations of the shared `pges` schema, in order.
pub const SHARED: &[Migration] = &[Migration {
    version: SchemaVersion(1),
    name: "0001_init",
    sql: include_str!("../migrations/pges/0001_init.sql"),
    min_compatible: SchemaVersion(1),
}];

/// Migrations of every store schema, in order.
pub const STORE: &[Migration] = &[Migration {
    version: SchemaVersion(1),
    name: "0001_init",
    sql: include_str!("../migrations/store/0001_init.sql"),
    min_compatible: SchemaVersion(1),
}];

/// Migrations of the outbox of a store created with [`store::Outbox::Enabled`], scope `<store>.outbox`
/// ([`outbox_scope`]), in order. The first is applied with the store, in the transaction that creates it; a
/// store created without the outbox never gets them.
pub const OUTBOX: &[Migration] = &[Migration {
    version: SchemaVersion(1),
    name: "opt_outbox",
    sql: include_str!("../migrations/store/opt_outbox.sql"),
    min_compatible: SchemaVersion(1),
}];

/// The scope of a store's outbox migrations: `<store>.outbox`. Store names cannot contain a `.`, so it is
/// never a store's own scope.
pub fn outbox_scope(store: &core_store::StoreName) -> String {
    format!("{store}.{OUTBOX_SCOPE_SUFFIX}")
}

/// Applies every [`SHARED`] migration the database does not have yet.
///
/// Fails if an applied migration was edited, or if the database requires a newer crate.
///
/// Run it on a connection outside a transaction. Each migration then runs in a transaction of its own, which a
/// dropped future rolls back. On a connection already inside a transaction, each is a savepoint instead, and a
/// future dropped while one begins rolls back the caller's whole transaction without its `Transaction`
/// knowing, so its commit does nothing.
pub fn run_shared(conn: &mut PgConnection) -> BoxFuture<'_, Result<(), MigrateError>> {
    Box::pin(run_shared_inner(conn))
}

async fn run_shared_inner(conn: &mut PgConnection) -> Result<(), MigrateError> {
    let latest = latest_version(SHARED);
    let applied = applied_migrations(conn, SHARED_SCOPE)
        .await
        .map_err(MigrateError::database)?;
    if is_current(SHARED_SCOPE, SHARED, &applied)? {
        return ensure_supported(conn, SHARED_SCOPE, latest).await;
    }

    for migration in SHARED {
        let mut tx = conn.begin().await.map_err(MigrateError::database)?;
        lock(&mut tx).await?;

        let applied = applied_migrations(&mut tx, SHARED_SCOPE)
            .await
            .map_err(MigrateError::database)?;
        if let Some(checksum) = applied.get(&migration.version) {
            verify_checksum(SHARED_SCOPE, migration, checksum)?;
            continue;
        }
        if !applied.is_empty() {
            ensure_supported(&mut tx, SHARED_SCOPE, latest).await?;
        }

        apply(&mut tx, SHARED_SCOPE, migration).await?;
        tx.commit().await.map_err(MigrateError::database)?;
    }

    Ok(())
}

/// Creates the store `name` from `definition` if it does not exist, then applies every [`STORE`] migration it
/// does not have yet, and every [`OUTBOX`] migration of a store with the outbox. [`run_shared`] must have run
/// first.
///
/// Store creation is one transaction: the schema, its `pges.stores` row, the first migration and the
/// options in `definition` (with the outbox, the first [`OUTBOX`] migration). The definition of an existing store is never changed, and `definition` is then
/// not read. A database holds stores of one outbox kind; a store whose [`store::StoreDefinition::outbox`]
/// differs from the existing stores' fails with [`MigrateError::OutboxMismatch`]. Creating a store without the
/// outbox fails with [`MigrateError::Failed`] (SQLSTATE 42501) when the role lacks the privileges of the
/// `pges` publication's owner, which adding the store's `commits` to it needs.
///
/// Run it on a connection outside a transaction. Each migration then runs in a transaction of its own, which a
/// dropped future rolls back. On a connection already inside a transaction, each is a savepoint instead, and a
/// future dropped while one begins rolls back the caller's whole transaction without its `Transaction`
/// knowing, so its commit does nothing.
pub fn run_store<'a>(
    conn: &'a mut PgConnection,
    name: &'a core_store::StoreName,
    definition: &'a store::StoreDefinition,
) -> BoxFuture<'a, Result<(), MigrateError>> {
    Box::pin(run_store_inner(conn, name, definition))
}

async fn run_store_inner(
    conn: &mut PgConnection,
    store_name: &core_store::StoreName,
    definition: &store::StoreDefinition,
) -> Result<(), MigrateError> {
    let scope = store_name.as_str();

    let latest = latest_version(STORE);
    if store_exists(conn, scope).await? {
        let applied = applied_migrations(conn, scope)
            .await
            .map_err(MigrateError::database)?;
        if is_current(scope, STORE, &applied)? {
            ensure_supported(conn, scope, latest).await?;
            return run_outbox(conn, store_name).await;
        }
    }

    for migration in STORE {
        let mut tx = conn.begin().await.map_err(MigrateError::database)?;
        lock(&mut tx).await?;

        if !store_exists(&mut tx, scope).await? {
            create_store(&mut tx, store_name, definition, migration).await?;
            tx.commit().await.map_err(MigrateError::database)?;
            continue;
        }

        let applied = applied_migrations(&mut tx, scope)
            .await
            .map_err(MigrateError::database)?;
        if let Some(checksum) = applied.get(&migration.version) {
            verify_checksum(scope, migration, checksum)?;
            continue;
        }
        ensure_supported(&mut tx, scope, latest).await?;

        set_search_path(&mut tx, store_name).await?;
        apply(&mut tx, scope, migration).await?;
        tx.commit().await.map_err(MigrateError::database)?;
    }

    run_outbox(conn, store_name).await
}

/// Checks the applied [`OUTBOX`] migrations of a store with the outbox and applies the missing ones. A store
/// without the outbox has none applied, and gets none.
async fn run_outbox(
    conn: &mut PgConnection,
    store_name: &core_store::StoreName,
) -> Result<(), MigrateError> {
    let scope = outbox_scope(store_name);
    let latest = latest_version(OUTBOX);
    let applied = applied_migrations(conn, &scope)
        .await
        .map_err(MigrateError::database)?;
    if applied.is_empty() {
        return Ok(());
    }
    if is_current(&scope, OUTBOX, &applied)? {
        return ensure_supported(conn, &scope, latest).await;
    }

    for migration in OUTBOX {
        let mut tx = conn.begin().await.map_err(MigrateError::database)?;
        lock(&mut tx).await?;

        let applied = applied_migrations(&mut tx, &scope)
            .await
            .map_err(MigrateError::database)?;
        if let Some(checksum) = applied.get(&migration.version) {
            verify_checksum(&scope, migration, checksum)?;
            continue;
        }
        ensure_supported(&mut tx, &scope, latest).await?;

        set_search_path(&mut tx, store_name).await?;
        apply(&mut tx, &scope, migration).await?;
        tx.commit().await.map_err(MigrateError::database)?;
    }

    Ok(())
}

/// Runs [`run_shared`], then [`run_store`].
///
/// Run it on a connection outside a transaction. Each migration then runs in a transaction of its own, which a
/// dropped future rolls back. On a connection already inside a transaction, each is a savepoint instead, and a
/// future dropped while one begins rolls back the caller's whole transaction without its `Transaction`
/// knowing, so its commit does nothing.
pub fn run<'a>(
    conn: &'a mut PgConnection,
    name: &'a core_store::StoreName,
    definition: &'a store::StoreDefinition,
) -> BoxFuture<'a, Result<(), MigrateError>> {
    Box::pin(async move {
        run_shared(conn).await?;
        run_store(conn, name, definition).await
    })
}

/// The migrations could not be applied.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum MigrateError {
    /// The shared `pges` schema is missing; run [`run_shared`] first.
    #[error("the shared pges schema is missing; run the shared migrations first")]
    SharedSchemaMissing,
    /// An applied migration differs from the embedded one.
    #[error("migration {version} ({name}) of scope {scope} was applied with a different text")]
    ChecksumMismatch {
        /// The scope.
        scope: String,
        /// The migration's version.
        version: SchemaVersion,
        /// The migration's name.
        name: &'static str,
    },
    /// The database was migrated by a release that no longer supports this crate's schema version.
    #[error(
        "scope {scope} requires schema version {min_compatible} or newer; this crate knows {crate_version}"
    )]
    CrateTooOld {
        /// The scope.
        scope: String,
        /// The oldest schema version the database still supports.
        min_compatible: SchemaVersion,
        /// The newest schema version this crate knows.
        crate_version: SchemaVersion,
    },
    /// The store would be created with [`store::StoreDefinition::outbox`] set differently from the database's
    /// other stores. A database is sequenced by one kind of source, so either every store has the outbox or
    /// none has. Nothing was created.
    #[error(
        "store {store} cannot be created with outbox {outbox:?}: the database's other stores differ"
    )]
    OutboxMismatch {
        /// The store that was not created.
        store: core_store::StoreName,
        /// Its outbox setting.
        outbox: store::Outbox,
    },
    /// A migration's SQL failed.
    #[error("migration {version} ({name}) of scope {scope} failed: {source}")]
    Failed {
        /// The scope.
        scope: String,
        /// The migration's version.
        version: SchemaVersion,
        /// The migration's name.
        name: &'static str,
        /// The database error.
        source: error::DbError,
    },
    /// The runner's own queries failed.
    #[error(transparent)]
    Database(error::DbError),
}

impl MigrateError {
    fn database(source: sqlx::Error) -> Self {
        Self::Database(source.into())
    }
}

/// The newest version of `migrations`: the schema version this crate needs a scope at, and the one it
/// compares the scope's `min_compatible` with. [`SchemaVersion::NONE`] for no migrations.
pub fn latest_version(migrations: &[Migration]) -> SchemaVersion {
    migrations.last().map_or(SchemaVersion::NONE, |m| m.version)
}

/// The scopes [`crate_too_old`] reads.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum Scopes<'a> {
    /// The shared `pges` scope and one store's.
    OneStore(&'a core_store::StoreName),
    /// The shared `pges` scope and every store's.
    AllStores,
}

/// A scope that no longer supports this crate: a newer release migrated it and raised its `min_compatible`
/// above this crate's newest migration.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct CrateTooOld {
    /// `pges`, a store's name, or a store's outbox scope (`<store>.outbox`).
    pub scope: String,
    /// The oldest schema version the scope still supports.
    pub min_compatible: SchemaVersion,
    /// The newest schema version this crate knows for the scope.
    pub crate_version: SchemaVersion,
}

/// Finds a scope that no longer supports this crate, the check [`store::Store::open`] makes at start: the
/// shared scope first, then stores by name, each store's outbox scope right after the store. `None` when every
/// scope read supports it.
///
/// Subscriptions and the sequencer run it after a lease renewal or acquire was refused. Both carry the same
/// check, so a process this crate's version cannot serve any more loses its lease within one renewal, a
/// standby takes none, and either stops instead of standing by.
pub async fn crate_too_old(
    conn: &mut PgConnection,
    scopes: Scopes<'_>,
) -> Result<Option<CrateTooOld>, error::DbError> {
    let (shared_version, store_version, outbox_version) = (
        latest_version(SHARED),
        latest_version(STORE),
        latest_version(OUTBOX),
    );
    let store = match scopes {
        Scopes::OneStore(store) => Some(store.as_str()),
        Scopes::AllStores => None,
    };
    let row = statement::too_old::Query::fixed()
        .bind(statement::too_old::Params {
            shared_version: shared_version.get(),
            store_version: store_version.get(),
            store,
            outbox_version: outbox_version.get(),
        })
        .fetch_optional(conn)
        .await?;

    Ok(row.map(|row| CrateTooOld {
        crate_version: if row.scope == SHARED_SCOPE {
            shared_version
        } else if row.scope.ends_with(&format!(".{OUTBOX_SCOPE_SUFFIX}")) {
            outbox_version
        } else {
            store_version
        },
        scope: row.scope,
        min_compatible: SchemaVersion(row.min_compatible),
    }))
}

/// How a scope's schema compares with what this crate needs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Compatibility {
    Compatible,
    Missing,
    Behind {
        database: SchemaVersion,
        required: SchemaVersion,
    },
    CrateTooOld {
        min_compatible: SchemaVersion,
        crate_version: SchemaVersion,
    },
    ChecksumMismatch {
        version: SchemaVersion,
    },
}

/// Compares a scope in the database with `migrations`: its schema version, its compatibility window and the
/// checksums of the migrations both know.
pub(crate) async fn compatibility(
    conn: &mut PgConnection,
    scope: &str,
    migrations: &[Migration],
) -> Result<Compatibility, sqlx::Error> {
    let crate_version = latest_version(migrations);
    let scope_row = statement::scope::Query::fixed()
        .bind(statement::scope::Params { scope })
        .fetch_optional(&mut *conn)
        .await?;
    let Some(scope_row) = scope_row else {
        return Ok(Compatibility::Missing);
    };
    let (schema_version, min_compatible) = (
        SchemaVersion(scope_row.schema_version),
        SchemaVersion(scope_row.min_compatible),
    );

    if min_compatible > crate_version {
        return Ok(Compatibility::CrateTooOld {
            min_compatible,
            crate_version,
        });
    }
    if schema_version < crate_version {
        return Ok(Compatibility::Behind {
            database: schema_version,
            required: crate_version,
        });
    }

    let applied = applied_migrations(conn, scope).await?;
    for migration in migrations {
        match applied.get(&migration.version) {
            Some(checksum) if checksum.as_slice() != migration.checksum() => {
                return Ok(Compatibility::ChecksumMismatch {
                    version: migration.version,
                });
            }
            _ => {}
        }
    }

    Ok(Compatibility::Compatible)
}

/// SQLSTATE raised by `pges.check_one_source_kind` when a store's outbox flag differs from the other
/// stores'.
const OUTBOX_MISMATCH: &str = "PGE02";

const OUTBOX_SCOPE_SUFFIX: &str = "outbox";

/// Whether every migration is applied, failing on an edited one.
fn is_current(
    scope: &str,
    migrations: &[Migration],
    applied: &BTreeMap<SchemaVersion, Vec<u8>>,
) -> Result<bool, MigrateError> {
    let mut current = true;
    for migration in migrations {
        match applied.get(&migration.version) {
            Some(checksum) => verify_checksum(scope, migration, checksum)?,
            None => current = false,
        }
    }

    Ok(current)
}

fn verify_checksum(
    scope: &str,
    migration: &Migration,
    checksum: &[u8],
) -> Result<(), MigrateError> {
    if checksum == migration.checksum() {
        return Ok(());
    }

    Err(MigrateError::ChecksumMismatch {
        scope: scope.to_owned(),
        version: migration.version,
        name: migration.name,
    })
}

async fn ensure_supported(
    conn: &mut PgConnection,
    scope: &str,
    crate_version: SchemaVersion,
) -> Result<(), MigrateError> {
    let row = statement::min_compatible::Query::fixed()
        .bind(statement::min_compatible::Params { scope })
        .fetch_optional(conn)
        .await
        .map_err(MigrateError::database)?;

    match row.map(|row| SchemaVersion(row.min_compatible)) {
        Some(min_compatible) if min_compatible > crate_version => Err(MigrateError::CrateTooOld {
            scope: scope.to_owned(),
            min_compatible,
            crate_version,
        }),
        _ => Ok(()),
    }
}

async fn lock(conn: &mut PgConnection) -> Result<(), MigrateError> {
    statement::migrate_lock::Query::fixed()
        .bind(statement::migrate_lock::Params {})
        .execute(conn)
        .await
        .map_err(MigrateError::database)?;

    Ok(())
}

/// Applied migrations of a scope by version, with their checksums. Empty before the shared schema exists.
async fn applied_migrations(
    conn: &mut PgConnection,
    scope: &str,
) -> Result<BTreeMap<SchemaVersion, Vec<u8>>, sqlx::Error> {
    let table_exists = statement::migrations_table_exists::Query::fixed()
        .bind(statement::migrations_table_exists::Params {})
        .fetch_one(&mut *conn)
        .await?
        .exists;
    if !table_exists {
        return Ok(BTreeMap::new());
    }

    let rows = statement::applied_migrations::Query::fixed()
        .bind(statement::applied_migrations::Params { scope })
        .fetch_all(conn)
        .await?;

    Ok(rows
        .into_iter()
        .map(|row| (SchemaVersion(row.version), row.checksum))
        .collect())
}

async fn store_exists(conn: &mut PgConnection, store: &str) -> Result<bool, MigrateError> {
    let shared_exists = statement::stores_table_exists::Query::fixed()
        .bind(statement::stores_table_exists::Params {})
        .fetch_one(&mut *conn)
        .await
        .map_err(MigrateError::database)?
        .exists;
    if !shared_exists {
        return Err(MigrateError::SharedSchemaMissing);
    }

    let row = statement::store_exists::Query::fixed()
        .bind(statement::store_exists::Params { store })
        .fetch_one(conn)
        .await
        .map_err(MigrateError::database)?;

    Ok(row.exists)
}

async fn create_store(
    conn: &mut PgConnection,
    store_name: &core_store::StoreName,
    definition: &store::StoreDefinition,
    first: &Migration,
) -> Result<(), MigrateError> {
    let store = store_name.as_str();
    let values = template::Values::new().store(store_name);

    conn.execute(statement::CREATE_SCHEMA.render_text(&values))
        .await
        .map_err(|e| failed(store, first, e))?;
    statement::insert_store::Query::fixed()
        .bind(statement::insert_store::Params {
            store,
            outbox: definition.outbox == store::Outbox::Enabled,
            max_clock_skew: store::interval(definition.max_clock_skew),
            max_txn_age: store::interval(definition.max_txn_age.get()),
        })
        .execute(&mut *conn)
        .await
        .map_err(|e| match error::sqlstate(&e).as_deref() {
            Some(OUTBOX_MISMATCH) => MigrateError::OutboxMismatch {
                store: store_name.clone(),
                outbox: definition.outbox,
            },
            _ => failed(store, first, e),
        })?;
    set_search_path(conn, store_name).await?;
    statement::set_stream_partitions::Query::fixed()
        .bind(statement::set_stream_partitions::Params {
            partitions: &definition.stream_partitions.get().to_string(),
        })
        .execute(&mut *conn)
        .await
        .map_err(|e| failed(store, first, e))?;

    apply(conn, store, first).await?;

    if definition.outbox == store::Outbox::Enabled
        && let Some(outbox) = OUTBOX.first()
    {
        apply(conn, &outbox_scope(store_name), outbox).await?;
    }

    Ok(())
}

async fn set_search_path(
    conn: &mut PgConnection,
    store: &core_store::StoreName,
) -> Result<(), MigrateError> {
    let set = statement::SET_SEARCH_PATH.render_text(&template::Values::new().store(store));
    conn.execute(set).await.map_err(MigrateError::database)?;

    Ok(())
}

/// Runs a migration's SQL, records it and moves the scope's compatibility window. The caller holds the
/// transaction and the lock.
async fn apply(
    conn: &mut PgConnection,
    scope: &str,
    migration: &Migration,
) -> Result<(), MigrateError> {
    conn.execute(migration.sql)
        .await
        .map_err(|e| failed(scope, migration, e))?;
    statement::record_migration::Query::fixed()
        .bind(statement::record_migration::Params {
            version: migration.version.0,
            scope,
            name: migration.name,
            checksum: &migration.checksum(),
        })
        .execute(&mut *conn)
        .await
        .map_err(|e| failed(scope, migration, e))?;
    statement::set_scope_version::Query::fixed()
        .bind(statement::set_scope_version::Params {
            scope,
            schema_version: migration.version.0,
            min_compatible: migration.min_compatible.0,
        })
        .execute(&mut *conn)
        .await
        .map_err(|e| failed(scope, migration, e))?;

    Ok(())
}

fn failed(scope: &str, migration: &Migration, source: sqlx::Error) -> MigrateError {
    MigrateError::Failed {
        scope: scope.to_owned(),
        version: migration.version,
        name: migration.name,
        source: source.into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn migrations_are_numbered_from_one_without_gaps() {
        for migrations in [SHARED, STORE, OUTBOX] {
            for (i, migration) in migrations.iter().enumerate() {
                assert_eq!(
                    migration.version.get() as usize,
                    i + 1,
                    "{}",
                    migration.name
                );
                assert!(migration.min_compatible <= migration.version);
            }
        }
    }

    #[test]
    fn checksums_are_sha256_of_the_text() {
        let checksum = SHARED[0].checksum();

        assert_eq!(checksum.len(), 32);
        assert_ne!(checksum, STORE[0].checksum());
    }

    #[test]
    fn the_outbox_scope_is_never_a_store_scope() {
        let scope = outbox_scope(&pges_core::store_name!("es"));

        assert_eq!(scope, "es.outbox");
        assert!(core_store::StoreName::parse(scope).is_err());
    }
}
