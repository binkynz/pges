//! Administration: categories and partition maintenance.
//!
//! Every operation takes `pg_advisory_lock(hashtext('pges.maintain.' || store))` on its own connection, runs
//! with `TimeZone = 'UTC'` and `lock_timeout = '1s'`, and retries DDL that times out waiting for a lock.
//! New partitions are created detached, given a CHECK constraint matching their bounds, and then attached, so
//! attaching takes only a SHARE UPDATE EXCLUSIVE lock on the parent and skips the validation scan.
//!
//! A category's time leaves never overlap: new leaves start at the upper bound of the newest existing one, so
//! a bucket change applies from there.

use std::{
    fmt,
    sync::Arc,
    time::{Duration, SystemTime},
};

use sqlx::{
    Connection, Executor, PgConnection, PgPool, SqlStr, pool::PoolConnection,
    postgres::types::PgInterval,
};

use pges_core::{category, position, store};

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

    /// Reports how far ahead of now each category's time leaves reach, and how many positions the store's
    /// newest log partition has left. This is what to alert on to catch a stopped [`Admin::maintain`].
    ///
    /// An append whose commit id is past the newest leaf of its category fails with
    /// [`AppendError::NoPartition`](crate::append::AppendError::NoPartition); alert when
    /// [`CategoryHeadroom::below_one_bucket`] holds for a category. When the log fills its newest partition,
    /// the sequencer stops assigning positions (it retries until a partition is added). `maintain` adds the
    /// next partition once half of the newest is used, so [`LogHeadroom::remaining`] dips just below
    /// [`LOG_PARTITION_POSITIONS`] / 2 until its next run; alert when it falls below
    /// [`LOG_PARTITION_POSITIONS`] / 4.
    ///
    /// It reads the catalog only and does not take the maintenance lock, so it answers while maintenance,
    /// retention or a freeze runs.
    pub async fn partition_headroom(&self) -> Result<PartitionHeadroom, PartitionHeadroomError> {
        let mut conn = self.pool.acquire().await?;

        partition_headroom(&mut conn, self).await
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

/// How many positions one log partition holds.
pub const LOG_PARTITION_POSITIONS: u64 = LOG_PARTITION_SPAN.unsigned_abs();

/// How far ahead of now a store's partitions reach ([`Admin::partition_headroom`]).
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct PartitionHeadroom {
    /// One entry per registered category, in id order.
    pub categories: Vec<CategoryHeadroom>,
    /// The log's partitions.
    pub log: LogHeadroom,
}

/// How far ahead of now a category's time leaves reach.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct CategoryHeadroom {
    /// The category.
    pub category: category::CategoryName,
    /// The category's bucket.
    pub bucket: Bucket,
    /// The upper bound of the category's newest time leaf: appends to the category with a later commit id
    /// fail with [`AppendError::NoPartition`](crate::append::AppendError::NoPartition). `None`: the category
    /// has no leaf.
    pub leaves_until: Option<SystemTime>,
    /// How long from now, by the database's clock, until [`CategoryHeadroom::leaves_until`]. Zero once it has
    /// passed, or when the category has no leaf.
    pub ahead: Duration,
}

/// How many positions the store's newest log partition has left.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub struct LogHeadroom {
    /// The first position the newest log partition does not hold. `None`: the store has no log partition,
    /// so the sequencer cannot write until [`Admin::maintain`] creates one.
    pub end: Option<position::Position>,
    /// How many more positions the sequencer can give before it needs another partition. Zero when the store
    /// has no log partition.
    pub remaining: u64,
}

impl CategoryHeadroom {
    /// Whether the category's leaves reach less than one bucket ahead of now: a day for [`Bucket::Day`], 28
    /// days for [`Bucket::Month`]. With [`Admin::maintain`] running they reach [`LEAVES_AHEAD`] buckets past
    /// the current one, so this holds only when it has not run for about that long.
    pub fn below_one_bucket(&self) -> bool {
        self.ahead < self.bucket.shortest()
    }
}

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
    /// The name of one of the category's partitions, or of an index the crate creates for one (the category
    /// partition's primary key `commits_<category>__pkey`, a leaf's `<leaf>_s`), is taken by a relation of
    /// the store's schema that is not that partition or index.
    #[error("{name} is taken by a relation the crate did not create under that name")]
    NameTaken {
        /// The taken name: the partition's, or its index's.
        name: RelationName,
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
    /// The name of one of the category's leaves, or of a leaf's index `<leaf>_s`, is taken by a relation of
    /// the store's schema that is not that leaf or index. The new bucket is recorded; [`Admin::maintain`]
    /// creates the leaves once the name is free.
    #[error("{name} is taken by a relation the crate did not create under that name")]
    NameTaken {
        /// The taken name: the leaf's, or its index's.
        name: RelationName,
    },
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
    /// The name of a partition, or of an index the crate creates for one (a category partition's primary key
    /// `commits_<category>__pkey`, a leaf's `<leaf>_s`), is taken by a relation of the store's schema that is
    /// not that partition or index.
    #[error("{name} is taken by a relation the crate did not create under that name")]
    NameTaken {
        /// The taken name: the partition's, or its index's.
        name: RelationName,
    },
    /// A database error, including DDL that kept timing out waiting for a lock.
    #[error(transparent)]
    Database(#[from] error::DbError),
}

/// The partitions' headroom could not be read.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum PartitionHeadroomError {
    /// A category has a bucket this crate does not support.
    #[error("category {category} has an unsupported bucket")]
    UnsupportedBucket {
        /// The category.
        category: category::CategoryName,
    },
    /// A database error.
    #[error(transparent)]
    Database(#[from] error::DbError),
}

impl From<sqlx::Error> for PartitionHeadroomError {
    fn from(source: sqlx::Error) -> Self {
        Self::Database(source.into())
    }
}

impl From<EnsureError> for RegisterCategoryError {
    fn from(error: EnsureError) -> Self {
        match error {
            EnsureError::NameTaken(name) => Self::NameTaken { name },
            EnsureError::Database(source) => source.into(),
        }
    }
}

impl From<EnsureError> for ChangeBucketError {
    fn from(error: EnsureError) -> Self {
        match error {
            EnsureError::NameTaken(name) => Self::NameTaken { name },
            EnsureError::Database(source) => source.into(),
        }
    }
}

impl From<EnsureError> for MaintainError {
    fn from(error: EnsureError) -> Self {
        match error {
            EnsureError::NameTaken(name) => Self::NameTaken { name },
            EnsureError::Database(source) => source.into(),
        }
    }
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

/// [`LOG_PARTITION_POSITIONS`], for arithmetic on positions read from the database.
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

    /// The bucket's shortest span: a month is at least 28 days.
    fn shortest(self) -> Duration {
        let days = match self {
            Self::Day => 1,
            Self::Month => 28,
        };

        Duration::from_secs(days * 24 * 60 * 60)
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
) -> Result<(), EnsureError> {
    let name = ident::category_partition(category.as_str());
    let key = plain(&ident::category_partition_key(category.as_str()))?;
    let values = template::Values::new()
        .store(store)
        .category(category)
        .category_id(category_id)
        .ident(template::IdentPlaceholder::Index, &key);

    match partition_state(conn, store.as_str(), &name, "commits").await? {
        PartitionState::Attached => return Ok(()),
        PartitionState::Detached => {}
        PartitionState::Missing => {
            // CREATE TABLE would fail on a taken key name with a bare 42P07, which names no relation.
            let key_name = ident::category_partition_key(category.as_str());
            if relation_exists(conn, store.as_str(), &key_name).await? {
                return Err(EnsureError::name_taken(key_name));
            }

            let create = statement::CREATE_CATEGORY_PARTITION.render_text(&values);
            ddl(conn, create, on_timeout).await?;
        }
        PartitionState::Taken => return Err(EnsureError::name_taken(name)),
    }

    let attach = statement::ATTACH_CATEGORY_PARTITION.render_text(&values);
    Ok(ddl(conn, attach, on_timeout).await?)
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
) -> Result<(), EnsureError> {
    let partition = ident::category_partition(category.as_str());
    let parent = format!("{store}.{partition}");
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
        let index_name = ident::leaf_stream_index(&name);
        let index = plain(&index_name)?;
        let values = template::Values::new()
            .store(store)
            .category(category)
            .category_id(category_id)
            .ident(template::IdentPlaceholder::Leaf, &leaf)
            .ident(template::IdentPlaceholder::Table, &leaf)
            .ident(template::IdentPlaceholder::Index, &index)
            .ident(template::IdentPlaceholder::Bound, &bound)
            .range(&timestamptz(&from)?, &timestamptz(&to)?);

        match partition_state(conn, store.as_str(), &name, &partition).await? {
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
            PartitionState::Taken => return Err(EnsureError::name_taken(name)),
        }
        // Checked after the drop above, which drops a detached leaf's own index. CREATE UNIQUE INDEX would
        // fail on a taken name with a bare 42P07 and leave the new leaf detached.
        if relation_exists(conn, store.as_str(), &index_name).await? {
            return Err(EnsureError::name_taken(index_name));
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
async fn ensure_log_partitions(conn: &mut PgConnection, admin: &Admin) -> Result<(), EnsureError> {
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

    match partition_state(conn, store.as_str(), &name, "log").await? {
        PartitionState::Detached => {
            ddl(conn, statement::DROP_PARTITION.render_text(&values), retry).await?;
        }
        PartitionState::Taken => return Err(EnsureError::name_taken(name)),
        PartitionState::Missing | PartitionState::Attached => {}
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

/// What a partition's name refers to in the store's schema.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PartitionState {
    Missing,
    /// A table that is no partition.
    Detached,
    /// A partition of the expected parent.
    Attached,
    /// Something else: a partition of another parent, or a relation that is not a table.
    Taken,
}

/// What `table` is, for a partition of `parent`; both are names in the store's schema.
pub(crate) async fn partition_state(
    conn: &mut PgConnection,
    store: &str,
    table: &str,
    parent: &str,
) -> Result<PartitionState, sqlx::Error> {
    let row = statement::partition_state::Query::fixed()
        .bind(statement::partition_state::Params { store, table })
        .fetch_optional(&mut *conn)
        .await?;

    Ok(match row {
        None => PartitionState::Missing,
        Some(row) if row.is_table != Some(true) => PartitionState::Taken,
        Some(row) if !row.relispartition => PartitionState::Detached,
        Some(row) if row.parent.as_deref() == Some(parent) => PartitionState::Attached,
        Some(_) => PartitionState::Taken,
    })
}

/// Whether the store's schema has a relation named `table`.
pub(crate) async fn relation_exists(
    conn: &mut PgConnection,
    store: &str,
    table: &str,
) -> Result<bool, sqlx::Error> {
    let row = statement::partition_state::Query::fixed()
        .bind(statement::partition_state::Params { store, table })
        .fetch_optional(&mut *conn)
        .await?;

    Ok(row.is_some())
}

/// The name of a relation (a table or an index) in a store's schema, without the schema. Its `Debug` is the
/// quoted name, as for a string.
#[derive(Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct RelationName(String);

impl RelationName {
    /// The name, without the schema.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for RelationName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl fmt::Display for RelationName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl AsRef<str> for RelationName {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl PartialEq<str> for RelationName {
    fn eq(&self, other: &str) -> bool {
        self.0 == other
    }
}

impl PartialEq<&str> for RelationName {
    fn eq(&self, other: &&str) -> bool {
        self.0 == *other
    }
}

/// Why a partition could not be created.
pub(crate) enum EnsureError {
    NameTaken(RelationName),
    Database(sqlx::Error),
}

impl EnsureError {
    fn name_taken(name: String) -> Self {
        Self::NameTaken(RelationName(name))
    }
}

impl From<sqlx::Error> for EnsureError {
    fn from(source: sqlx::Error) -> Self {
        Self::Database(source)
    }
}

async fn partition_headroom(
    conn: &mut PgConnection,
    admin: &Admin,
) -> Result<PartitionHeadroom, PartitionHeadroomError> {
    let rows = admin
        .sql()
        .leaf_headroom
        .bind(statement::leaf_headroom::Params {})
        .fetch_all(&mut *conn)
        .await?;
    let mut categories = Vec::with_capacity(rows.len());
    for row in rows {
        // Names become part of DDL; the categories table's CHECK already enforces this.
        let Some(category) = catalog::intern(&row.name) else {
            continue;
        };
        let Some(bucket) = Bucket::from_interval(&row.bucket) else {
            return Err(PartitionHeadroomError::UnsupportedBucket { category });
        };

        categories.push(CategoryHeadroom {
            category,
            bucket,
            leaves_until: row.upper.and_then(system_time),
            ahead: row.ahead.map_or(Duration::ZERO, micros_ahead),
        });
    }

    let newest = statement::newest_log_partition::Query::fixed()
        .bind(statement::newest_log_partition::Params {
            log: &format!("{}.log", admin.store()),
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
    let end = newest.map(|n| n.saturating_add(1).saturating_mul(LOG_PARTITION_SPAN));
    let log = LogHeadroom {
        end: end.and_then(position::Position::from_i64),
        remaining: end.map_or(0, |end| {
            u64::try_from(end.saturating_sub(last_position).saturating_sub(1)).unwrap_or(0)
        }),
    };

    Ok(PartitionHeadroom { categories, log })
}

/// A time in microseconds since the Unix epoch; `None` if the platform cannot represent it.
fn system_time(micros: i64) -> Option<SystemTime> {
    let offset = Duration::from_micros(micros.unsigned_abs());

    if micros >= 0 {
        SystemTime::UNIX_EPOCH.checked_add(offset)
    } else {
        SystemTime::UNIX_EPOCH.checked_sub(offset)
    }
}

/// A distance ahead of now in microseconds; zero when it is behind.
fn micros_ahead(micros: i64) -> Duration {
    Duration::from_micros(u64::try_from(micros).unwrap_or(0))
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
