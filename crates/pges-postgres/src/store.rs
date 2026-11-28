//! Opening a store and registering event types.

use std::{
    any::TypeId,
    num::NonZeroU16,
    sync::{Arc, PoisonError, RwLock},
    time::Duration,
};

use sqlx::{ConnectOptions, PgConnection, PgPool, postgres::types::PgInterval};

use pges_core::{category, event, frame, interval, store};

use crate::{admin, catalog, error, migrate, route, sql, sql::typed::Query as _, unit_of_work};

/// A store: one PostgreSQL schema holding streams, commits and the log of one bounded context.
///
/// Cheap to clone; clones share the pool and the cached category and event type ids.
#[derive(Clone)]
pub struct Store {
    pub(crate) inner: Arc<Inner>,
}

impl Store {
    /// Opens the store named in `config`.
    ///
    /// With [`AutoMigrate::CreateOrMigrate`] it first applies the migrations, creating the store from the
    /// [`StoreDefinition`] if it does not exist. Then it checks that the shared `pges` schema and the store
    /// schema (and, for a store with the outbox, its outbox scope `<store>.outbox`) are at the version this
    /// crate needs and that their applied migrations match the embedded ones, reads the store's `outbox` and
    /// `dcb` flags, lists the partitions of `streams` so each append can name its stream's partition (with
    /// [`AppendRouting::Auto`], the default), and loads the ids of its categories and event types.
    ///
    /// Fails with [`OpenError::StoreNotFound`] for a store that does not exist (and is not created), with
    /// [`OpenError::StoreRequiresFeature`] for a store that needs a feature this build lacks, and with
    /// [`OpenError::ClockSkewAboveStore`] if [`StoreConfig::max_clock_skew`] is set larger than the store's
    /// own.
    pub async fn open(pool: PgPool, config: StoreConfig) -> Result<Self, OpenError> {
        let store_name = config.name.clone();

        let mut conn = pool.acquire().await.map_err(OpenError::database)?;
        if let Some(definition) = config.definition() {
            migrate::run(&mut conn, &store_name, &definition).await?;
        }

        let shared_exists = sql::statement::shared_schema_exists::Query::fixed()
            .bind(sql::statement::shared_schema_exists::Params {})
            .fetch_one(&mut *conn)
            .await
            .map_err(OpenError::database)?
            .exists;
        if !shared_exists {
            return Err(OpenError::SchemaBehind {
                scope: migrate::SHARED_SCOPE.to_owned(),
                database: migrate::SchemaVersion::NONE,
                required: migrate::latest_version(migrate::SHARED),
            });
        }
        check_compatibility(&mut conn, migrate::SHARED_SCOPE, migrate::SHARED).await?;

        let row = sql::statement::open_store::Query::fixed()
            .bind(sql::statement::open_store::Params {
                store: store_name.as_str(),
                max_clock_skew: config.max_clock_skew.map(interval),
            })
            .fetch_optional(&mut *conn)
            .await
            .map_err(OpenError::database)?;
        let Some(sql::statement::open_store::Row {
            store_id,
            outbox,
            dcb,
            max_clock_skew: recorded_skew,
            skew_fits,
        }) = row
        else {
            return Err(OpenError::StoreNotFound(store_name));
        };
        let store_id = store::StoreId::new(store_id);
        check_compatibility(&mut conn, store_name.as_str(), migrate::STORE).await?;
        if let Some(configured) = config.max_clock_skew
            && !skew_fits
        {
            // Retention and resequence rely on the store's value; a writer with a wider tolerance could write
            // a commit into a leaf retention is dropping, or one resequence does not search.
            return Err(OpenError::ClockSkewAboveStore {
                configured,
                store: duration(recorded_skew),
            });
        }
        if dcb {
            // DCB appends (tag_heads, commit_tags) are not implemented; a writer that skipped them would
            // break the store's consistency boundaries.
            return Err(OpenError::StoreRequiresFeature("dcb"));
        }

        if outbox {
            check_compatibility(
                &mut conn,
                &migrate::outbox_scope(&store_name),
                migrate::OUTBOX,
            )
            .await?;
        }

        let outbox = if outbox {
            Outbox::Enabled
        } else {
            Outbox::Disabled
        };
        let leaves = match config.append_routing {
            AppendRouting::Auto => {
                let probes = route::probes();
                let partitions = sql::stream_leaves(&store_name)
                    .bind(sql::statement::stream_leaves::Params { probes: &probes })
                    .fetch_all(&mut *conn)
                    .await
                    .map_err(OpenError::database)?;

                route::Leaves::from_catalog(&partitions, &probes)
            }
            AppendRouting::Streams => None,
        };
        let statements =
            sql::StoreSql::new(&store_name, outbox, leaves, statement_cache_capacity(&pool));
        let mut catalog = catalog::Catalog::default();
        let categories: Vec<(category::CategoryId, String)> = statements
            .categories
            .bind(sql::statement::categories::Params {})
            .fetch_all(&mut *conn)
            .await
            .map_err(OpenError::database)?
            .into_iter()
            .map(|row| (category::CategoryId::new(row.category_id), row.name))
            .collect();
        catalog.add_categories(&store_name, &categories);
        let types = statements
            .all_types
            .bind(sql::statement::all_types::Params {})
            .fetch_all(&mut *conn)
            .await
            .map_err(OpenError::database)?;
        for (category_id, _) in &categories {
            let rows: Vec<(i32, String)> = types
                .iter()
                .filter(|row| row.category_id == category_id.get())
                .map(|row| (row.event_type_id, row.name.clone()))
                .collect();
            catalog.add_types(*category_id, &rows);
        }

        Ok(Self {
            inner: Arc::new(Inner {
                pool,
                admin_sql: Arc::new(sql::AdminSql::new(&store_name)),
                name: store_name,
                store_id,
                max_clock_skew: config.max_clock_skew.map_or(recorded_skew, interval),
                write_format: config.write_format,
                sql: statements,
                catalog: RwLock::new(catalog),
            }),
        })
    }

    /// The store's name, which is also its schema.
    pub fn name(&self) -> &store::StoreName {
        &self.inner.name
    }

    /// The store's id in `pges.stores`, which keys its high-water mark in the sequencer's watch.
    pub fn store_id(&self) -> store::StoreId {
        self.inner.store_id
    }

    /// The pool the store runs on.
    pub fn pool(&self) -> &PgPool {
        &self.inner.pool
    }

    /// Administration of this store: categories and partitions.
    pub fn admin(&self) -> admin::Admin {
        admin::Admin::for_store(
            self.inner.pool.clone(),
            self.inner.name.clone(),
            self.inner.admin_sql.clone(),
        )
    }

    /// Registers `E`'s event types, so their ids are cached.
    ///
    /// Optional: the store registers `E` before the first append or read that uses it. Registration runs on
    /// a pooled connection in its own transaction, never on a caller's connection, so the ids are committed
    /// before any payload refers to them. Fails with [`RegisterError::UnknownCategory`] if `E`'s category is
    /// not registered in the store.
    ///
    /// An append inside a [`unit_of_work::UnitOfWork`] or through [`Store::append_in`] that registers
    /// implicitly therefore needs a second pooled connection while it holds the first. With a pool of size 1
    /// a unit of work from [`Store::begin`] fails with [`crate::append::AppendError::NoSpareConnection`]; a
    /// caller's connection taken from the same exhausted pool waits for the pool's acquire timeout. Call
    /// `register` for every event type at startup to avoid both.
    pub async fn register<E: event::Event>(&self) -> Result<(), RegisterError> {
        let () = E::__CHECK;
        self.ensure_registered(TypeId::of::<E>(), E::CATEGORY, E::TYPES)
            .await?;

        Ok(())
    }

    /// Starts a unit of work: one transaction for appends to several streams and the caller's own writes.
    pub async fn begin(
        &self,
    ) -> Result<unit_of_work::UnitOfWork<'_>, unit_of_work::TransactionError> {
        unit_of_work::UnitOfWork::begin(self).await
    }

    /// Whether the event type `key` can be used without a round trip: it is registered, or its category and
    /// every one of `types` are cached (loaded when the store was opened), in which case it is marked
    /// registered now.
    pub(crate) fn is_registered_locally(
        &self,
        key: TypeId,
        name: category::CategoryName,
        types: &'static [&'static str],
    ) -> bool {
        let mut catalog = self.catalog_mut();
        if catalog.registered(key, name).is_some() {
            return true;
        }

        catalog
            .category(name)
            .is_some_and(|category| catalog.mark_registered(key, category.id, types))
    }

    /// The category and its type ids, registering `types` first if needed.
    pub(crate) async fn ensure_registered(
        &self,
        key: TypeId,
        name: category::CategoryName,
        types: &'static [&'static str],
    ) -> Result<catalog::Category, RegisterError> {
        if let Some(category) = self.catalog().registered(key, name) {
            return Ok(category);
        }

        let category = self.category(name).await?;
        if self.catalog_mut().mark_registered(key, category.id, types) {
            return self.category(name).await;
        }

        let names: Vec<&str> = types.to_vec();
        self.inner
            .sql
            .register_types
            .bind(sql::statement::register_types::Params {
                category_id: category.id.get(),
                names: &names,
            })
            .execute(&self.inner.pool)
            .await
            .map_err(RegisterError::database)?;
        let rows: Vec<(i32, String)> = self
            .inner
            .sql
            .select_types
            .bind(sql::statement::select_types::Params {
                category_id: category.id.get(),
                names: &names,
            })
            .fetch_all(&self.inner.pool)
            .await
            .map_err(RegisterError::database)?
            .into_iter()
            .map(|row| (row.event_type_id, row.name))
            .collect();

        let mut catalog = self.catalog_mut();
        catalog.add_types(category.id, &rows);
        catalog.mark_registered(key, category.id, types);

        catalog
            .category_by_id(category.id)
            .ok_or(RegisterError::UnknownCategory(name))
    }

    /// The category, reloading the store's categories once if it is not cached.
    pub(crate) async fn category(
        &self,
        name: category::CategoryName,
    ) -> Result<catalog::Category, RegisterError> {
        if let Some(category) = self.catalog().category(name) {
            return Ok(category);
        }

        self.reload_categories().await?;

        self.catalog()
            .category(name)
            .ok_or(RegisterError::UnknownCategory(name))
    }

    /// The category with this id, reloading the store's categories once if it is not cached.
    pub(crate) async fn category_by_id(
        &self,
        id: category::CategoryId,
    ) -> Result<Option<catalog::Category>, sqlx::Error> {
        if let Some(category) = self.catalog().category_by_id(id) {
            return Ok(Some(category));
        }

        self.reload_categories().await?;

        Ok(self.catalog().category_by_id(id))
    }

    /// Reloads a category's event type ids, for a stored id the cache does not know yet.
    pub(crate) async fn reload_types(
        &self,
        category_id: category::CategoryId,
    ) -> Result<Arc<catalog::TypeTable>, sqlx::Error> {
        let rows: Vec<(i32, String)> = self
            .inner
            .sql
            .category_types
            .bind(sql::statement::category_types::Params {
                category_id: category_id.get(),
            })
            .fetch_all(&self.inner.pool)
            .await?
            .into_iter()
            .map(|row| (row.event_type_id, row.name))
            .collect();

        Ok(self.catalog_mut().add_types(category_id, &rows))
    }

    async fn reload_categories(&self) -> Result<(), sqlx::Error> {
        let rows: Vec<(category::CategoryId, String)> = self
            .inner
            .sql
            .categories
            .bind(sql::statement::categories::Params {})
            .fetch_all(&self.inner.pool)
            .await?
            .into_iter()
            .map(|row| (category::CategoryId::new(row.category_id), row.name))
            .collect();
        self.catalog_mut().add_categories(&self.inner.name, &rows);

        Ok(())
    }

    fn catalog(&self) -> std::sync::RwLockReadGuard<'_, catalog::Catalog> {
        self.inner
            .catalog
            .read()
            .unwrap_or_else(PoisonError::into_inner)
    }

    fn catalog_mut(&self) -> std::sync::RwLockWriteGuard<'_, catalog::Catalog> {
        self.inner
            .catalog
            .write()
            .unwrap_or_else(PoisonError::into_inner)
    }
}

impl std::fmt::Debug for Store {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Store")
            .field("name", &self.inner.name)
            .field("store_id", &self.inner.store_id)
            .finish_non_exhaustive()
    }
}

/// How to open a store, and whether to create or migrate it first.
///
/// Only [`StoreConfig::migrate`] creates a store: the options a store is created with are in its
/// [`StoreDefinition`], and [`Store::open`] reads them back from the database.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct StoreConfig {
    /// The store's name and schema.
    pub name: store::StoreName,
    /// How far a commit id's timestamp may be from the database's clock when this writer appends. Default
    /// `None`: the store's recorded `max_clock_skew` ([`StoreDefinition::max_clock_skew`], or what
    /// [`admin::Admin::set_writer_limits`] raised it to).
    ///
    /// A writer that wants a tighter tolerance sets a value. [`Store::open`] refuses one larger than the
    /// store's recorded value ([`OpenError::ClockSkewAboveStore`]): retention and the sequencer's
    /// `resequence` rely on the recorded value. Raise the store's with [`admin::Admin::set_writer_limits`]
    /// first.
    pub max_clock_skew: Option<Duration>,
    /// The payload frame format appends write. Switch only after every reader supports it.
    pub write_format: frame::Format,
    /// Whether [`Store::open`] applies migrations first, and what it creates a missing store with.
    pub migrate: AutoMigrate,
    /// Whether a single append names its stream's partition of `streams` or goes through `streams`. Default
    /// [`AppendRouting::Auto`].
    pub append_routing: AppendRouting,
}

impl StoreConfig {
    /// The defaults for opening the store named `name`, such as `pges_core::store_name!("ops_es")`, without
    /// migrating.
    pub fn new(name: store::StoreName) -> Self {
        Self {
            name,
            max_clock_skew: None,
            write_format: frame::Format::LATEST,
            migrate: AutoMigrate::Disabled,
            append_routing: AppendRouting::Auto,
        }
    }

    /// The definition [`Store::open`] migrates with, if any.
    fn definition(&self) -> Option<StoreDefinition> {
        match self.migrate {
            AutoMigrate::Disabled => None,
            AutoMigrate::CreateOrMigrate(definition) => Some(definition),
        }
    }
}

/// What a store is created with. None of it changes afterwards, except the writer limits, which
/// [`admin::Admin::set_writer_limits`] can raise.
///
/// [`migrate::run`] and [`AutoMigrate::CreateOrMigrate`] use it only when the store does not exist yet; an
/// existing store keeps its own.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub struct StoreDefinition {
    /// Hash partitions of `streams` and `snapshots`. Default 64, enough for about 1B streams. More partitions
    /// cost [`Store::append_many`] batches that touch many of them, which lock each one they touch.
    ///
    /// A single append names its stream's partition ([`AppendRouting::Auto`]) only while one prepared
    /// statement per partition fits each connection's statement cache with 36 entries to spare: sqlx's
    /// `statement_cache_capacity` (default 100) at least `stream_partitions + 36`. The default 64 fit; with
    /// more, appends go through `streams`, whose plans may lock every partition. Each partition statement
    /// holds about 130 KB of plan cache per connection (8.6 MB at 64 partitions).
    ///
    /// The partitions are `streams_p<remainder>` and `snapshots_p<remainder>`, the remainder padded with
    /// zeros to 3 digits, or to as many as the highest remainder has (`streams_p0000` to `streams_p1000` for
    /// 1001). The store is created in one transaction that locks every partition and its primary key, so
    /// thousands of partitions need a larger `max_locks_per_transaction`.
    pub stream_partitions: NonZeroU16,
    /// Whether appends also enqueue into `pending`, for the outbox sequencer source. All stores of a database
    /// must agree: creating a store that differs fails with [`migrate::MigrateError::OutboxMismatch`].
    pub outbox: Outbox,
    /// The store's `max_clock_skew`: the furthest any writer's commit ids may be from the database clock.
    /// Default [`store::DEFAULT_MAX_CLOCK_SKEW`] (5 s). A writer's own [`StoreConfig::max_clock_skew`], if
    /// set, must not be larger.
    pub max_clock_skew: Duration,
    /// The longest a writer transaction stays open. Default [`store::DEFAULT_MAX_TXN_AGE`] (1 hour). Cap
    /// the writer roles' transactions at it (`transaction_timeout`): retention and `resequence` refuse to
    /// run otherwise.
    pub max_txn_age: interval::Interval,
}

impl Default for StoreDefinition {
    fn default() -> Self {
        Self {
            stream_partitions: DEFAULT_STREAM_PARTITIONS,
            outbox: Outbox::Disabled,
            max_clock_skew: store::DEFAULT_MAX_CLOCK_SKEW,
            max_txn_age: pges_core::interval!(store::DEFAULT_MAX_TXN_AGE),
        }
    }
}

/// Whether appends enqueue into the `pending` table read by the outbox sequencer source.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[non_exhaustive]
pub enum Outbox {
    /// Appends also insert into `pending`.
    Enabled,
    /// No outbox queue.
    #[default]
    Disabled,
}

/// How a single append reaches its stream's row in `streams`. Batches ([`Store::append_many`]) always go
/// through `streams`.
///
/// [`Store::save_snapshot`] locks the stream's row the same way, but runs unprepared: with `Auto` it names
/// the partition even when the appends fall back because of the statement cache.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[non_exhaustive]
pub enum AppendRouting {
    /// Name the stream's hash partition, so an append locks only that partition whatever the planner's
    /// statistics and `plan_cache_mode`, and its plan is reused. Each connection prepares one statement per
    /// partition, about 130 KB of plan cache each.
    ///
    /// [`Store::open`] falls back to [`AppendRouting::Streams`] when the statements do not fit the pool's
    /// statement cache (`statement_cache_capacity` below [`StoreDefinition::stream_partitions`] + 36), when
    /// the role may not select, insert and update every partition directly, or when the partitions are not
    /// the plain hash partitions the store was created with. Routing is fixed when the store is opened.
    #[default]
    Auto,
    /// Through the partitioned `streams`, with one prepared statement. A custom plan locks the stream's
    /// partition; a generic plan (statistics of a nearly empty store, or `plan_cache_mode =
    /// force_generic_plan`) locks every partition.
    Streams,
}

/// Whether [`Store::open`] applies migrations.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[non_exhaustive]
pub enum AutoMigrate {
    /// Only check; a database that is behind fails with [`OpenError::SchemaBehind`].
    #[default]
    Disabled,
    /// Apply the shared and store migrations, creating the store with this definition if it does not
    /// exist.
    CreateOrMigrate(StoreDefinition),
}

/// The store could not be opened.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum OpenError {
    /// No store with this name exists.
    #[error("store {0} does not exist")]
    StoreNotFound(store::StoreName),
    /// The database's schema for `scope` is older than this crate needs; run the migrations.
    #[error("scope {scope} is at schema version {database}; this crate needs {required}")]
    SchemaBehind {
        /// `pges`, the store's name, or its outbox scope (`<store>.outbox`).
        scope: String,
        /// The database's schema version, [`migrate::SchemaVersion::NONE`] if the scope does not exist.
        database: migrate::SchemaVersion,
        /// The version this crate needs.
        required: migrate::SchemaVersion,
    },
    /// The database was migrated by a release that no longer supports this crate.
    #[error(
        "scope {scope} requires schema version {min_compatible} or newer; this crate knows {crate_version}"
    )]
    CrateTooOld {
        /// `pges`, the store's name, or its outbox scope (`<store>.outbox`).
        scope: String,
        /// The oldest schema version the database still supports.
        min_compatible: migrate::SchemaVersion,
        /// The newest schema version this crate knows.
        crate_version: migrate::SchemaVersion,
    },
    /// An applied migration differs from the one embedded in this crate.
    #[error("migration {version} of scope {scope} was applied with a different text")]
    ChecksumMismatch {
        /// `pges`, the store's name, or its outbox scope (`<store>.outbox`).
        scope: String,
        /// The migration's version.
        version: migrate::SchemaVersion,
    },
    /// [`StoreConfig::max_clock_skew`] is set larger than the store's recorded `max_clock_skew`. Raise the
    /// store's with [`admin::Admin::set_writer_limits`], or open with a smaller skew or `None` (the store's).
    #[error("max_clock_skew {configured:?} is larger than the store's {store:?}")]
    ClockSkewAboveStore {
        /// [`StoreConfig::max_clock_skew`].
        configured: Duration,
        /// The store's recorded value.
        store: Duration,
    },
    /// The store uses a feature this build of the crate lacks, such as `dcb`.
    #[error("store requires the {0} feature, which this build lacks")]
    StoreRequiresFeature(&'static str),
    /// Applying migrations failed.
    #[error(transparent)]
    Migrate(#[from] migrate::MigrateError),
    /// A database error.
    #[error(transparent)]
    Database(error::DbError),
}

impl OpenError {
    fn database(source: sqlx::Error) -> Self {
        Self::Database(source.into())
    }
}

/// An event type could not be registered.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum RegisterError {
    /// The event's category is not registered in the store.
    #[error("category {0} is not registered in the store")]
    UnknownCategory(category::CategoryName),
    /// A database error.
    #[error(transparent)]
    Database(error::DbError),
}

impl RegisterError {
    pub(crate) fn database(source: sqlx::Error) -> Self {
        Self::Database(source.into())
    }
}

impl From<sqlx::Error> for RegisterError {
    fn from(source: sqlx::Error) -> Self {
        Self::database(source)
    }
}

/// Hash partitions of `streams` and `snapshots` unless the definition says otherwise.
const DEFAULT_STREAM_PARTITIONS: NonZeroU16 = match NonZeroU16::new(64) {
    Some(partitions) => partitions,
    None => NonZeroU16::MIN,
};

pub(crate) struct Inner {
    pub(crate) pool: PgPool,
    pub(crate) name: store::StoreName,
    pub(crate) store_id: store::StoreId,
    pub(crate) max_clock_skew: PgInterval,
    pub(crate) write_format: frame::Format,
    pub(crate) sql: sql::StoreSql,
    admin_sql: Arc<sql::AdminSql>,
    catalog: RwLock<catalog::Catalog>,
}

async fn check_compatibility(
    conn: &mut PgConnection,
    scope: &str,
    migrations: &[migrate::Migration],
) -> Result<(), OpenError> {
    let compatibility = migrate::compatibility(conn, scope, migrations)
        .await
        .map_err(OpenError::database)?;

    match compatibility {
        migrate::Compatibility::Compatible => Ok(()),
        migrate::Compatibility::Missing => Err(OpenError::SchemaBehind {
            scope: scope.to_owned(),
            database: migrate::SchemaVersion::NONE,
            required: migrate::latest_version(migrations),
        }),
        migrate::Compatibility::Behind { database, required } => Err(OpenError::SchemaBehind {
            scope: scope.to_owned(),
            database,
            required,
        }),
        migrate::Compatibility::CrateTooOld {
            min_compatible,
            crate_version,
        } => Err(OpenError::CrateTooOld {
            scope: scope.to_owned(),
            min_compatible,
            crate_version,
        }),
        migrate::Compatibility::ChecksumMismatch { version } => Err(OpenError::ChecksumMismatch {
            scope: scope.to_owned(),
            version,
        }),
    }
}

/// The statement cache capacity of the pool's connections. sqlx exposes it only in the options' URL form; the
/// default (100) when it is missing.
fn statement_cache_capacity(pool: &PgPool) -> usize {
    pool.connect_options()
        .to_url_lossy()
        .query_pairs()
        .find(|(key, _)| key == "statement-cache-capacity")
        .and_then(|(_, value)| value.parse().ok())
        .unwrap_or(DEFAULT_STATEMENT_CACHE_CAPACITY)
}

/// sqlx's default statement cache capacity per connection.
const DEFAULT_STATEMENT_CACHE_CAPACITY: usize = 100;

/// An interval as a duration, a month counting 30 days and a day 24 hours, as PostgreSQL compares them.
/// Negative intervals are zero.
pub(crate) fn duration(interval: PgInterval) -> Duration {
    let days = i64::from(interval.months) * 30 + i64::from(interval.days);
    let micros = days
        .saturating_mul(86_400_000_000)
        .saturating_add(interval.microseconds);

    Duration::from_micros(u64::try_from(micros).unwrap_or(0))
}

pub(crate) fn interval(duration: Duration) -> PgInterval {
    PgInterval {
        months: 0,
        days: 0,
        microseconds: i64::try_from(duration.as_micros()).unwrap_or(i64::MAX),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn the_statement_cache_capacity_is_read_from_the_pool() -> anyhow::Result<()> {
        let options: sqlx::postgres::PgConnectOptions = "postgres://user@localhost/db".parse()?;
        let default = PgPool::connect_lazy_with(options.clone());
        let small = PgPool::connect_lazy_with(options.statement_cache_capacity(7));

        assert_eq!(statement_cache_capacity(&default), 100);
        assert_eq!(statement_cache_capacity(&small), 7);
        Ok(())
    }
}
