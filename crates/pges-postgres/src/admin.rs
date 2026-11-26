//! Administration: categories and partition maintenance.
//!
//! Every operation takes `pg_advisory_lock(hashtext('pges.maintain.' || store))` on its own connection, runs
//! with `TimeZone = 'UTC'` and `lock_timeout = '1s'`, and retries DDL that times out waiting for a lock.
//! New partitions are created detached, given a CHECK constraint matching their bounds, and then attached, so
//! attaching takes only a SHARE UPDATE EXCLUSIVE lock on the parent and skips the validation scan.
//!
//! A category's time leaves never overlap: new leaves start at the upper bound of the newest existing one, so
//! a bucket change applies from there.

use std::{sync::Arc, time::Duration};

use sqlx::{
    Connection, Executor, PgConnection, PgPool, SqlStr, pool::PoolConnection,
    postgres::types::PgInterval,
};

use pges_core::{category, store};

use crate::{
    catalog, error, ident, retention,
    sql::{self, statement, template, typed::Query as _},
};

/// Creates categories, keeps their partitions ahead of time and applies their retention.
#[derive(Debug, Clone)]
pub struct Admin {
    pool: PgPool,
    store: store::StoreName,
    sql: Arc<sql::AdminSql>,
}

impl Admin {
    /// Administration of the store named `store`, such as `pges_core::store_name!("ops_es")`.
    pub fn new(pool: PgPool, store: store::StoreName) -> Self {
        let sql = Arc::new(sql::AdminSql::new(&store));

        Self::for_store(pool, store, sql)
    }

    /// Registers a category, creates its partition of `commits` and the time leaves from the current
    /// bucket to [`LEAVES_AHEAD`] buckets ahead.
    ///
    /// The category, its partition and its leaves are created in one transaction: no store sees the category
    /// before appends to it can succeed. A new category takes [`CategoryConfig::retention`], or the store's
    /// default retention ([`Admin::set_default_retention`]) when it is `None`. Idempotent: for a registered
    /// category it only creates what is missing, and neither its bucket nor its retention is changed;
    /// [`Admin::change_bucket`] and [`Admin::set_retention`] do that. Stores opened before the
    /// call learn about the category the first time they need it.
    pub async fn register_category(
        &self,
        name: category::CategoryName,
        config: CategoryConfig,
    ) -> Result<(), RegisterCategoryError> {
        let mut conn = self.session().await?;
        let result = register_category(&mut conn, self, name, config).await;

        self.end_session(conn).await;

        result
    }

    /// Changes the bucket of a registered category, then creates the leaves it needs for the next
    /// [`LEAVES_AHEAD`] buckets.
    ///
    /// Existing leaves keep their span. The new bucket applies from the upper bound of the category's newest
    /// leaf, so with leaves kept ahead it takes effect a few buckets later. When that bound is not a bound of
    /// the new bucket (from [`Bucket::Day`] to [`Bucket::Month`] in mid-month), one leaf spans from it to the
    /// next bound. Leaves never overlap and leave no gap.
    ///
    /// Fails with [`ChangeBucketError::UnknownCategory`] if the category is not registered.
    pub async fn change_bucket(
        &self,
        name: category::CategoryName,
        bucket: Bucket,
    ) -> Result<(), ChangeBucketError> {
        let mut conn = self.session().await?;
        let result = change_bucket(&mut conn, self, name, bucket).await;

        self.end_session(conn).await;

        result
    }

    /// Creates the time leaves every category needs for the next [`LEAVES_AHEAD`] buckets, and the next log
    /// partition once the store has used half of the newest one. Run it on a schedule; it is idempotent.
    pub async fn maintain(&self) -> Result<(), MaintainError> {
        let mut conn = self.session().await?;
        let result = maintain(&mut conn, self).await;

        self.end_session(conn).await;

        result
    }

    pub(crate) fn for_store(
        pool: PgPool,
        store: store::StoreName,
        sql: Arc<sql::AdminSql>,
    ) -> Self {
        Self { pool, store, sql }
    }

    pub(crate) fn store(&self) -> &str {
        self.store.as_str()
    }

    pub(crate) fn store_name(&self) -> &store::StoreName {
        &self.store
    }

    pub(crate) fn sql(&self) -> &sql::AdminSql {
        &self.sql
    }

    pub(crate) fn pool(&self) -> &PgPool {
        &self.pool
    }

    /// A connection holding the store's maintenance lock, with the session settings maintenance needs.
    pub(crate) async fn session(&self) -> Result<PoolConnection<sqlx::Postgres>, error::DbError> {
        let mut conn = self.pool.acquire().await?;
        // The session settings must not reach other users of the pool.
        conn.close_on_drop();

        // Locked before lock_timeout is set: the timeout also applies to advisory locks, and a second
        // maintainer must wait for the first, not fail after a second.
        statement::maintain_lock::Query::fixed()
            .bind(statement::maintain_lock::Params {
                store: self.store(),
            })
            .execute(&mut *conn)
            .await?;
        conn.execute(statement::MAINTAIN_SESSION.sql()).await?;

        Ok(conn)
    }

    pub(crate) async fn end_session(&self, mut conn: PoolConnection<sqlx::Postgres>) {
        // Closing the connection releases the lock too; unlocking first just releases it sooner.
        let _unlocked = statement::maintain_unlock::Query::fixed()
            .bind(statement::maintain_unlock::Params {
                store: self.store(),
            })
            .execute(&mut *conn)
            .await;
    }
}

/// How many buckets ahead of the current one time leaves are kept, so maintenance has slack to retry.
pub const LEAVES_AHEAD: u16 = 3;

/// How a category is set up.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[non_exhaustive]
pub struct CategoryConfig {
    /// The time span of one partition of the category's commits.
    pub bucket: Bucket,
    /// What retention removes from the category; `None` takes the store's default
    /// ([`Admin::set_default_retention`]), which is [`retention::Retention::Forever`] unless set.
    pub retention: Option<retention::Retention>,
}

/// The time span of one partition of a category's commits. Aim for at most about 100-200 live leaves per
/// category.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[non_exhaustive]
pub enum Bucket {
    /// One partition per UTC day.
    Day,
    /// One partition per UTC month.
    #[default]
    Month,
}

/// A category could not be registered. Nothing was registered.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum RegisterCategoryError {
    /// The category exists with a bucket this crate does not support.
    #[error("category {category} has an unsupported bucket")]
    UnsupportedBucket {
        /// The category.
        category: category::CategoryName,
    },
    /// A database error, including DDL that kept timing out waiting for a lock.
    #[error(transparent)]
    Database(#[from] error::DbError),
}

/// A category's bucket could not be changed.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum ChangeBucketError {
    /// The category is not registered in the store.
    #[error("category {0} is not registered in the store")]
    UnknownCategory(category::CategoryName),
    /// A database error, including DDL that kept timing out waiting for a lock. The new bucket may already
    /// be recorded; [`Admin::maintain`] creates the leaves it needs.
    #[error(transparent)]
    Database(#[from] error::DbError),
}

/// Partition maintenance failed. What was created before the failure stays; running it again continues.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum MaintainError {
    /// A category has a bucket this crate does not support.
    #[error("category {category} has an unsupported bucket")]
    UnsupportedBucket {
        /// The category.
        category: category::CategoryName,
    },
    /// A database error, including DDL that kept timing out waiting for a lock.
    #[error(transparent)]
    Database(#[from] error::DbError),
}

impl From<sqlx::Error> for RegisterCategoryError {
    fn from(source: sqlx::Error) -> Self {
        Self::Database(source.into())
    }
}

impl From<sqlx::Error> for ChangeBucketError {
    fn from(source: sqlx::Error) -> Self {
        Self::Database(source.into())
    }
}

impl From<sqlx::Error> for MaintainError {
    fn from(source: sqlx::Error) -> Self {
        Self::Database(source.into())
    }
}

/// Log partitions hold this many positions each.
pub(crate) const LOG_PARTITION_SPAN: i64 = 100_000_000;

/// Attempts for DDL, or a registration transaction, that times out waiting for a lock.
pub(crate) const DDL_ATTEMPTS: u32 = 8;

/// SQLSTATE 55P03: `lock_timeout` expired.
pub(crate) const LOCK_NOT_AVAILABLE: &str = "55P03";

/// What DDL does when it times out waiting for a lock.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum OnLockTimeout {
    /// In autocommit: retry the statement with backoff.
    RetryStatement,
    /// Inside a transaction, which the timeout aborted: fail, and let the caller retry the transaction.
    Fail,
}

impl Bucket {
    fn interval(self) -> PgInterval {
        let (months, days) = match self {
            Self::Day => (0, 1),
            Self::Month => (1, 0),
        };

        PgInterval {
            months,
            days,
            microseconds: 0,
        }
    }

    fn unit(self) -> &'static str {
        match self {
            Self::Day => "day",
            Self::Month => "month",
        }
    }

    fn suffix_format(self) -> &'static str {
        match self {
            Self::Day => "YYYYMMDD",
            Self::Month => "YYYYMM",
        }
    }

    fn from_interval(interval: &PgInterval) -> Option<Self> {
        match (interval.months, interval.days, interval.microseconds) {
            (1, 0, 0) => Some(Self::Month),
            (0, 1, 0) => Some(Self::Day),
            _ => None,
        }
    }
}

/// Registers the category in one transaction, retried as a whole while its DDL times out waiting for a lock.
async fn register_category(
    conn: &mut PgConnection,
    admin: &Admin,
    name: category::CategoryName,
    config: CategoryConfig,
) -> Result<(), RegisterCategoryError> {
    let mut backoff = Duration::from_millis(50);
    let mut attempt = 1;

    loop {
        let mut tx = conn.begin().await?;
        match register_in_transaction(&mut tx, admin, name, config).await {
            Ok(()) => {
                tx.commit().await?;
                return Ok(());
            }
            Err(RegisterCategoryError::Database(error))
                if attempt < DDL_ATTEMPTS
                    && error.code().as_deref() == Some(LOCK_NOT_AVAILABLE) =>
            {
                // A failed rollback leaves the connection unusable: report why the attempt failed, not
                // why the rollback did. The session closes the connection when it is dropped.
                if tx.rollback().await.is_err() {
                    return Err(RegisterCategoryError::Database(error));
                }
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(Duration::from_secs(2));
                attempt += 1;
            }
            Err(error) => {
                let _rolled_back = tx.rollback().await;
                return Err(error);
            }
        }
    }
}

async fn register_in_transaction(
    conn: &mut PgConnection,
    admin: &Admin,
    name: category::CategoryName,
    config: CategoryConfig,
) -> Result<(), RegisterCategoryError> {
    let (retention, retain_for) = match config.retention {
        Some(policy) => retention::to_columns(policy),
        None => retention::store_default(conn, admin.store()).await?,
    };
    admin
        .sql()
        .insert_category
        .bind(statement::insert_category::Params {
            name: name.as_str(),
            bucket: config.bucket.interval(),
            retention,
            retain_for,
        })
        .execute(&mut *conn)
        .await?;
    let row = admin
        .sql()
        .select_category
        .bind(statement::select_category::Params {
            name: name.as_str(),
        })
        .fetch_one(&mut *conn)
        .await?;
    let category_id = category::CategoryId::new(row.category_id);
    let bucket = Bucket::from_interval(&row.bucket)
        .ok_or(RegisterCategoryError::UnsupportedBucket { category: name })?;

    ensure_category_partition(
        conn,
        admin.store_name(),
        name,
        category_id,
        OnLockTimeout::Fail,
    )
    .await?;
    ensure_leaves(
        conn,
        admin.store_name(),
        name,
        category_id,
        bucket,
        OnLockTimeout::Fail,
    )
    .await?;

    Ok(())
}

async fn change_bucket(
    conn: &mut PgConnection,
    admin: &Admin,
    name: category::CategoryName,
    bucket: Bucket,
) -> Result<(), ChangeBucketError> {
    let row = admin
        .sql()
        .set_bucket
        .bind(statement::set_bucket::Params {
            name: name.as_str(),
            bucket: bucket.interval(),
        })
        .fetch_optional(&mut *conn)
        .await?;
    let Some(category_id) = row.map(|row| category::CategoryId::new(row.category_id)) else {
        return Err(ChangeBucketError::UnknownCategory(name));
    };

    ensure_leaves(
        conn,
        admin.store_name(),
        name,
        category_id,
        bucket,
        OnLockTimeout::RetryStatement,
    )
    .await?;

    Ok(())
}

async fn maintain(conn: &mut PgConnection, admin: &Admin) -> Result<(), MaintainError> {
    let categories = admin
        .sql()
        .category_buckets
        .bind(statement::category_buckets::Params {})
        .fetch_all(&mut *conn)
        .await?;

    for row in categories {
        let category_id = category::CategoryId::new(row.category_id);
        // Names become part of DDL; the categories table's CHECK already enforces this.
        let Some(category) = catalog::intern(&row.name) else {
            continue;
        };
        let Some(bucket) = Bucket::from_interval(&row.bucket) else {
            return Err(MaintainError::UnsupportedBucket { category });
        };

        ensure_category_partition(
            conn,
            admin.store_name(),
            category,
            category_id,
            OnLockTimeout::RetryStatement,
        )
        .await?;
        ensure_leaves(
            conn,
            admin.store_name(),
            category,
            category_id,
            bucket,
            OnLockTimeout::RetryStatement,
        )
        .await?;
    }
    ensure_log_partitions(conn, admin).await?;

    Ok(())
}

/// Creates and attaches the category's partition of `commits`, finishing a creation that was interrupted.
async fn ensure_category_partition(
    conn: &mut PgConnection,
    store: &store::StoreName,
    category: category::CategoryName,
    category_id: category::CategoryId,
    on_timeout: OnLockTimeout,
) -> Result<(), sqlx::Error> {
    let name = ident::category_partition(category.as_str());
    let values = template::Values::new()
        .store(store)
        .category(category)
        .category_id(category_id);

    match partition_state(conn, store.as_str(), &name).await? {
        PartitionState::Attached => return Ok(()),
        PartitionState::Detached => {}
        PartitionState::Missing => {
            let create = statement::CREATE_CATEGORY_PARTITION.render_text(&values);
            ddl(conn, create, on_timeout).await?;
        }
    }

    let attach = statement::ATTACH_CATEGORY_PARTITION.render_text(&values);
    ddl(conn, attach, on_timeout).await
}

/// Creates the category's missing time leaves (`leaf/to_create.sql`), after finishing attaches that were
/// interrupted.
async fn ensure_leaves(
    conn: &mut PgConnection,
    store: &store::StoreName,
    category: category::CategoryName,
    category_id: category::CategoryId,
    bucket: Bucket,
    on_timeout: OnLockTimeout,
) -> Result<(), sqlx::Error> {
    let parent = format!("{store}.{}", ident::category_partition(category.as_str()));
    let bound = plain(ident::BOUND)?;

    let unfinished = statement::unfinished_leaves::Query::fixed()
        .bind(statement::unfinished_leaves::Params {
            parent: &parent,
            bound: ident::BOUND,
        })
        .fetch_all(&mut *conn)
        .await?;
    for row in unfinished {
        let Some(leaf) = template::Ident::new(row.relname) else {
            continue;
        };
        let values = template::Values::new()
            .store(store)
            .ident(template::IdentPlaceholder::Table, &leaf)
            .ident(template::IdentPlaceholder::Bound, &bound);
        ddl(conn, statement::DROP_BOUND.render_text(&values), on_timeout).await?;
    }

    let leaves = statement::leaves_to_create::Query::fixed()
        .bind(statement::leaves_to_create::Params {
            parent: &parent,
            unit: bucket.unit(),
            bucket: bucket.interval(),
            suffix_format: bucket.suffix_format(),
            buckets: f64::from(LEAVES_AHEAD + 1),
        })
        .fetch_all(&mut *conn)
        .await?;

    for row in leaves {
        let (suffix, from, to) = (row.to_char, row.lower, row.upper);
        let name = ident::leaf(category.as_str(), &suffix);
        let leaf = plain(&name)?;
        let index = plain(&ident::leaf_stream_index(&name))?;
        let values = template::Values::new()
            .store(store)
            .category(category)
            .category_id(category_id)
            .ident(template::IdentPlaceholder::Leaf, &leaf)
            .ident(template::IdentPlaceholder::Table, &leaf)
            .ident(template::IdentPlaceholder::Index, &index)
            .ident(template::IdentPlaceholder::Bound, &bound)
            .range(&timestamptz(&from)?, &timestamptz(&to)?);

        match partition_state(conn, store.as_str(), &name).await? {
            PartitionState::Attached => continue,
            // Never attached, so no row was ever routed into it.
            PartitionState::Detached => {
                ddl(
                    conn,
                    statement::DROP_PARTITION.render_text(&values),
                    on_timeout,
                )
                .await?;
            }
            PartitionState::Missing => {}
        }

        for statement in [
            &statement::CREATE_LEAF,
            &statement::COMPRESS_LEAF,
            &statement::CREATE_LEAF_INDEX,
            &statement::ADD_LEAF_BOUND,
            &statement::ATTACH_LEAF,
            &statement::DROP_BOUND,
        ] {
            ddl(conn, statement.render_text(&values), on_timeout).await?;
        }
    }

    Ok(())
}

/// Creates log partition 0, and the next one once the store has passed half of the newest.
async fn ensure_log_partitions(conn: &mut PgConnection, admin: &Admin) -> Result<(), sqlx::Error> {
    let store = admin.store_name();
    let newest = statement::newest_log_partition::Query::fixed()
        .bind(statement::newest_log_partition::Params {
            log: &format!("{store}.log"),
        })
        .fetch_one(&mut *conn)
        .await?
        .max;
    let last_position = admin
        .sql()
        .last_position
        .bind(statement::last_position::Params {})
        .fetch_one(&mut *conn)
        .await?
        .coalesce;

    let next = match newest {
        None => 0,
        Some(n) if last_position >= n * LOG_PARTITION_SPAN + LOG_PARTITION_SPAN / 2 => n + 1,
        Some(_) => return Ok(()),
    };
    let (from, to) = (next * LOG_PARTITION_SPAN, (next + 1) * LOG_PARTITION_SPAN);
    let name = ident::log_partition(next);
    let partition = plain(&name)?;
    let values = template::Values::new()
        .store(store)
        .ident(template::IdentPlaceholder::LogPartition, &partition)
        .ident(template::IdentPlaceholder::Table, &partition)
        .ident(template::IdentPlaceholder::Bound, &plain(ident::BOUND)?)
        .range(&template::Literal::int(from), &template::Literal::int(to));
    let retry = OnLockTimeout::RetryStatement;

    if partition_state(conn, store.as_str(), &name).await? == PartitionState::Detached {
        ddl(conn, statement::DROP_PARTITION.render_text(&values), retry).await?;
    }
    for statement in [
        &statement::CREATE_LOG_PARTITION,
        &statement::ADD_LOG_BOUND,
        &statement::ATTACH_LOG_PARTITION,
        &statement::DROP_BOUND,
    ] {
        ddl(conn, statement.render_text(&values), retry).await?;
    }

    Ok(())
}

/// A name the crate built, or read from the catalog, as an identifier for DDL.
pub(crate) fn plain(name: &str) -> Result<template::Ident, sqlx::Error> {
    template::Ident::new(name)
        .ok_or_else(|| sqlx::Error::Protocol(format!("{name:?} cannot be used unquoted in DDL")))
}

/// A partition bound read from the catalog, as a literal for DDL.
fn timestamptz(text: &str) -> Result<template::Literal, sqlx::Error> {
    template::Literal::timestamptz(text)
        .ok_or_else(|| sqlx::Error::Protocol(format!("{text:?} is not a timestamptz bound")))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PartitionState {
    Missing,
    Detached,
    Attached,
}

pub(crate) async fn partition_state(
    conn: &mut PgConnection,
    store: &str,
    table: &str,
) -> Result<PartitionState, sqlx::Error> {
    let attached = statement::partition_state::Query::fixed()
        .bind(statement::partition_state::Params { store, table })
        .fetch_optional(&mut *conn)
        .await?
        .map(|row| row.relispartition);

    Ok(match attached {
        None => PartitionState::Missing,
        Some(false) => PartitionState::Detached,
        Some(true) => PartitionState::Attached,
    })
}

/// Runs DDL. In autocommit it retries with backoff while the statement times out waiting for a lock.
pub(crate) async fn ddl(
    conn: &mut PgConnection,
    statement: SqlStr,
    on_timeout: OnLockTimeout,
) -> Result<(), sqlx::Error> {
    let mut backoff = Duration::from_millis(50);
    let mut attempt = 1;

    loop {
        match conn.execute(statement.clone()).await {
            Ok(_) => return Ok(()),
            Err(e)
                if on_timeout == OnLockTimeout::RetryStatement
                    && attempt < DDL_ATTEMPTS
                    && error::sqlstate(&e).as_deref() == Some(LOCK_NOT_AVAILABLE) =>
            {
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(Duration::from_secs(2));
                attempt += 1;
            }
            Err(e) => return Err(e),
        }
    }
}
