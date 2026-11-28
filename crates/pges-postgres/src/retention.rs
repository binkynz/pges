//! Retention: what each category keeps, and the [`admin::Admin`] operations that drop the rest.
//!
//! A category's policy is a [`Retention`]. [`admin::Admin::apply_retention`] drops the time leaves a policy
//! no longer keeps, once they are fully sequenced, and records the category's retention horizon first, so a
//! subscription whose cursor is behind it fails with `CursorBeforeHorizon` instead of skipping events.
//! [`admin::Admin::prune_log`] removes a category's log rows below its horizon, for stores where another
//! category keeps the log partitions alive.
//!
//! The margins retention keeps come from the store's writer limits ([`limit`]), the same values appends
//! and the sequencer's `resequence` use.

use std::{fmt, time::Duration};

use sqlx::{Connection, Executor, PgConnection, postgres::types::PgInterval};
use uuid::Uuid;

use pges_core::{category, position, stream};

use crate::{
    admin, catalog, error, ident, limit, lock,
    sql::{statement, template, typed::Query as _},
    store,
};

impl admin::Admin {
    /// Sets the retention of a registered category. It applies from the next
    /// [`admin::Admin::apply_retention`]; nothing is dropped now.
    ///
    /// Fails with [`SetRetentionError::UnknownCategory`] if the category is not registered, and with
    /// [`SetRetentionError::KeepTooLong`] if `keep` is longer than [`Retention::MAX_KEEP`].
    pub async fn set_retention(
        &self,
        name: category::CategoryName,
        retention: Retention,
    ) -> Result<(), SetRetentionError> {
        let (code, retain_for) = to_columns(&retention)?;
        let mut conn = self.session().await?;
        let updated = self
            .sql()
            .set_category_retention
            .bind(statement::set_category_retention::Params {
                name: name.as_str(),
                retention: code,
                retain_for,
            })
            .execute(&mut *conn)
            .await;

        self.end_session(conn).await;

        match updated?.rows_affected() {
            0 => Err(SetRetentionError::UnknownCategory(name)),
            _ => Ok(()),
        }
    }

    /// Sets the store's default retention: the policy of categories registered from now on without one of
    /// their own ([`admin::CategoryConfig::retention`]). Registered categories keep theirs.
    ///
    /// Fails with [`SetDefaultRetentionError::StoreNotFound`] if the store does not exist, and with
    /// [`SetDefaultRetentionError::KeepTooLong`] if `keep` is longer than [`Retention::MAX_KEEP`].
    pub async fn set_default_retention(
        &self,
        retention: Retention,
    ) -> Result<(), SetDefaultRetentionError> {
        let (code, retain_for) = to_columns(&retention)?;
        let mut conn = self.session().await?;
        let updated = statement::set_default_retention::Query::fixed()
            .bind(statement::set_default_retention::Params {
                store: self.store(),
                retention: code,
                retain_for,
            })
            .execute(&mut *conn)
            .await;

        self.end_session(conn).await;

        match updated?.rows_affected() {
            0 => Err(SetDefaultRetentionError::StoreNotFound(
                self.store_name().clone(),
            )),
            _ => Ok(()),
        }
    }

    /// Drops the time leaves each category's [`Retention`] no longer keeps, oldest first, then the log
    /// partitions no category needs any more. Run it on a schedule; a leaf it refuses is tried again on the
    /// next run.
    ///
    /// A leaf is dropped only when all of these hold:
    ///
    /// - its upper bound is older than the policy's `keep` plus the store's `max_clock_skew`, and older than
    ///   the store's `max_txn_age` plus twice the skew, so no append can still write to it (both recorded in
    ///   `pges.stores`, [`limit`]);
    /// - every commit in it has been sequenced;
    /// - for [`Retention::Window`], no stream with commits in it had a commit within `keep`: such a stream
    ///   is long-lived and would lose its start ([`Refusal::reason`] is [`RefusalReason::LiveStreams`]);
    /// - for [`Retention::SnapshotAndTruncate`], every stream with commits in it that is not deleted has a
    ///   snapshot at or after its last version in the leaf ([`RefusalReason::MissingSnapshots`]).
    ///
    /// Before the leaf is detached, the category's retention horizon is raised to the newest position in it,
    /// its start position to the position before the oldest commit that stays (or the horizon, if that is
    /// lower), and each of its streams' retention floor to its newest version in it, in one transaction,
    /// which also forgets the commits `delete_stream` recorded whose log rows the horizon now covers. From
    /// then on a subscription whose cursor is behind the horizon fails with `CursorBeforeHorizon`, a new
    /// subscription created with `Start::Retained` and a backfill read the category after its start
    /// position ([`DroppedLeaf::start`]), and a read below a stream's floor fails with
    /// [`crate::read::ReadError::Truncated`]: load such streams with [`store::Store::load_snapshotted`]. A
    /// category's later leaves are not tried after a refusal.
    ///
    /// Fails without dropping anything while the sequencer is halted (`pges.sequencer.halted`, a lost
    /// replication slot awaiting `resequence`), and unless every role in
    /// [`RetentionConfig::writer_roles`] has a `transaction_timeout` of at most the store's `max_txn_age`,
    /// set on the role or the database ([`limit::check_writer_roles`]).
    pub async fn apply_retention(
        &self,
        config: &RetentionConfig,
    ) -> Result<RetentionReport, RetentionError> {
        let mut conn = self.session().await?;
        let result = apply_retention(&mut conn, self, config).await;

        self.end_session(conn).await;

        result
    }

    /// Deletes a category's log rows that only point at commits retention already dropped, then drops the
    /// log partitions no category needs any more. A row whose commit still exists is kept, so the
    /// sequencer's `resequence` never takes that commit for one without a log row. So is a row the sequencer
    /// wrote after it last stored the store's high-water mark (`pges.stores.last_position`), so a sequencer
    /// that stopped before storing it does not hand out that row's position again.
    ///
    /// Log partitions hold every category's positions, so a category that keeps its commits pins every log
    /// partition of the store. In a store that mixes such categories with short-lived ones, run this for
    /// the short-lived ones after [`admin::Admin::apply_retention`]. Rows are deleted in batches, and each
    /// partition that lost rows is vacuumed; it is no longer insert-only.
    ///
    /// Fails with [`PruneLogError::UnknownCategory`] if the category is not registered.
    pub async fn prune_log(
        &self,
        name: category::CategoryName,
    ) -> Result<PrunedLog, PruneLogError> {
        let mut conn = self.session().await?;
        let result = prune_log(&mut conn, self, name).await;

        self.end_session(conn).await;

        result
    }
}

/// What retention keeps of a category.
///
/// `keep` is at most [`Retention::MAX_KEEP`]; to keep everything, use [`Retention::Forever`].
#[derive(Debug, Clone, PartialEq, Eq, Default)]
#[non_exhaustive]
pub enum Retention {
    /// Nothing is dropped.
    #[default]
    Forever,
    /// For categories whose streams are short-lived: time leaves older than `keep` are dropped whole.
    Window {
        /// How long commits are kept.
        keep: Duration,
        /// What happens to a leaf once it is past `keep`.
        then: AfterWindow,
    },
    /// For long-lived streams: time leaves older than `keep` are dropped once every stream with commits in
    /// them has a snapshot at or after its last version there.
    SnapshotAndTruncate {
        /// How long commits are kept.
        keep: Duration,
    },
}

impl Retention {
    /// The longest `keep`: 1,000 years of 365 days. Retention compares a leaf's bound with the database
    /// clock minus `keep`, and a much longer `keep` would take that time out of PostgreSQL's range.
    pub const MAX_KEEP: Duration = Duration::from_secs(1_000 * 365 * 24 * 60 * 60);
}

/// What happens to a time leaf once [`Retention::Window`] no longer keeps it.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
#[non_exhaustive]
pub enum AfterWindow {
    /// The leaf is detached and dropped.
    #[default]
    Drop,
}

/// What [`admin::Admin::apply_retention`] must know about the deployment. The clock skew and transaction
/// age it allows for are the store's own ([`limit`]).
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct RetentionConfig {
    /// The roles that append. Each must have a `transaction_timeout` of at most the store's `max_txn_age`,
    /// set with `ALTER ROLE … SET` (optionally `IN DATABASE`) or `ALTER DATABASE … SET`.
    pub writer_roles: limit::WriterRoles,
}

impl RetentionConfig {
    /// Retention for writers in `writer_roles`.
    pub fn new(writer_roles: limit::WriterRoles) -> Self {
        Self { writer_roles }
    }
}

/// What one [`admin::Admin::apply_retention`] run did.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
#[non_exhaustive]
pub struct RetentionReport {
    /// The time leaves dropped, in the order they were dropped.
    pub dropped: Vec<DroppedLeaf>,
    /// The leaves, or categories, retention could not drop this time.
    pub refused: Vec<Refusal>,
    /// The log partitions dropped.
    pub log_partitions: Vec<PartitionName>,
}

/// A time leaf retention dropped.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct DroppedLeaf {
    /// The category.
    pub category: category::CategoryName,
    /// The leaf's table name.
    pub leaf: PartitionName,
    /// The category's retention horizon after the drop: every commit retention dropped is at or below it.
    /// A subscription whose cursor is behind it fails with `CursorBeforeHorizon`.
    pub horizon: position::Position,
    /// The category's start position after the drop: every commit at or below it was dropped. It is at
    /// most [`DroppedLeaf::horizon`]. Commits that stay can lie between the two, when they committed
    /// before the dropped leaf's last commit; a subscription created with `Start::Retained`, and a
    /// backfill, read the category from here.
    pub start: position::Position,
}

/// A leaf retention could not drop. Later leaves of the category were not tried.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct Refusal {
    /// The category.
    pub category: category::CategoryName,
    /// The leaf's table name; `None` when the category was refused as a whole.
    pub leaf: Option<PartitionName>,
    /// Why.
    pub reason: RefusalReason,
}

/// Why retention did not drop a leaf.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum RefusalReason {
    /// Some commits of the leaf have not been sequenced yet.
    NotSequenced,
    /// Streams with commits in the leaf have no snapshot at or after their last version there (up to
    /// [`REPORTED_STREAMS`] of them). Save snapshots of them, delete them, or wait for them to be loaded
    /// with [`store::Store::load_snapshotted`].
    MissingSnapshots(Vec<stream::StreamId>),
    /// A [`Retention::Window`] category has long-lived streams: streams with commits in the leaf that also
    /// had a commit within `keep` (up to [`REPORTED_STREAMS`] of them). Dropping the leaf would remove their
    /// first events. Use [`Retention::SnapshotAndTruncate`] for such categories.
    LiveStreams(Vec<stream::StreamId>),
    /// The category's policy (a window followed by an archive export) is not supported by this version of
    /// the crate.
    UnsupportedPolicy,
}

/// The most streams a [`RefusalReason`] lists.
pub const REPORTED_STREAMS: u16 = 100;

/// The table name of a partition in a store's schema: a category's partition or time leaf, or a log
/// partition. Its `Debug` is the quoted name, as for a string.
#[derive(Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct PartitionName(String);

impl PartitionName {
    /// The name, without the schema.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub(crate) fn of(table: &template::Ident) -> Self {
        Self(table.as_str().to_owned())
    }

    pub(crate) fn new(table: String) -> Self {
        Self(table)
    }
}

impl fmt::Debug for PartitionName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl fmt::Display for PartitionName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl AsRef<str> for PartitionName {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl PartialEq<str> for PartitionName {
    fn eq(&self, other: &str) -> bool {
        self.0 == other
    }
}

impl PartialEq<&str> for PartitionName {
    fn eq(&self, other: &&str) -> bool {
        self.0 == *other
    }
}

/// What one [`admin::Admin::prune_log`] run did.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
#[non_exhaustive]
pub struct PrunedLog {
    /// The category's log rows deleted.
    pub rows: u64,
    /// The log partitions dropped.
    pub log_partitions: Vec<PartitionName>,
}

/// A category's retention could not be set.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum SetRetentionError {
    /// The category is not registered in the store.
    #[error("category {0} is not registered in the store")]
    UnknownCategory(category::CategoryName),
    /// The policy's `keep` is longer than [`Retention::MAX_KEEP`]. To keep everything, use
    /// [`Retention::Forever`].
    #[error(
        "retention keep {0:?} is longer than Retention::MAX_KEEP; use Retention::Forever to keep everything"
    )]
    KeepTooLong(Duration),
    /// A database error.
    #[error(transparent)]
    Database(#[from] error::DbError),
    /// The store's maintenance lock stayed taken by another session for longer than the lock wait
    /// ([`admin::Admin::with_lock_wait`]). Nothing was changed.
    #[error(transparent)]
    LockTimeout(lock::LockTimeout),
}

/// The store's default retention could not be set.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum SetDefaultRetentionError {
    /// No store with this name exists.
    #[error("store {0} does not exist")]
    StoreNotFound(pges_core::store::StoreName),
    /// The policy's `keep` is longer than [`Retention::MAX_KEEP`]. To keep everything, use
    /// [`Retention::Forever`].
    #[error(
        "retention keep {0:?} is longer than Retention::MAX_KEEP; use Retention::Forever to keep everything"
    )]
    KeepTooLong(Duration),
    /// A database error.
    #[error(transparent)]
    Database(#[from] error::DbError),
    /// The store's maintenance lock stayed taken by another session for longer than the lock wait
    /// ([`admin::Admin::with_lock_wait`]). Nothing was changed.
    #[error(transparent)]
    LockTimeout(lock::LockTimeout),
}

/// Retention did not run, or stopped. What was dropped before the error stays dropped; running it again
/// continues.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum RetentionError {
    /// The sequencer is halted (`pges.sequencer.halted`), for example after losing its replication slot.
    /// Nothing is dropped until `resequence` clears it.
    #[error("the sequencer is halted: {0}")]
    SequencerHalted(String),
    /// No store with this name exists.
    #[error("store {0} does not exist")]
    StoreNotFound(pges_core::store::StoreName),
    /// A role in [`RetentionConfig::writer_roles`] does not cap its transactions at the store's
    /// `max_txn_age`.
    #[error(transparent)]
    WriterRole(limit::WriterRoleError),
    /// A database error, including DDL that kept timing out waiting for a lock.
    #[error(transparent)]
    Database(#[from] error::DbError),
    /// The store's maintenance lock stayed taken by another session for longer than the lock wait
    /// ([`admin::Admin::with_lock_wait`]). Nothing was changed.
    #[error(transparent)]
    LockTimeout(lock::LockTimeout),
}

/// A category's log could not be pruned.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum PruneLogError {
    /// The category is not registered in the store.
    #[error("category {0} is not registered in the store")]
    UnknownCategory(category::CategoryName),
    /// A database error, including DDL that kept timing out waiting for a lock.
    #[error(transparent)]
    Database(#[from] error::DbError),
    /// The store's maintenance lock stayed taken by another session for longer than the lock wait
    /// ([`admin::Admin::with_lock_wait`]). Nothing was changed.
    #[error(transparent)]
    LockTimeout(lock::LockTimeout),
}

impl From<sqlx::Error> for SetRetentionError {
    fn from(source: sqlx::Error) -> Self {
        Self::Database(source.into())
    }
}

impl From<KeepTooLong> for SetRetentionError {
    fn from(KeepTooLong(keep): KeepTooLong) -> Self {
        Self::KeepTooLong(keep)
    }
}

impl From<KeepTooLong> for SetDefaultRetentionError {
    fn from(KeepTooLong(keep): KeepTooLong) -> Self {
        Self::KeepTooLong(keep)
    }
}

impl From<sqlx::Error> for SetDefaultRetentionError {
    fn from(source: sqlx::Error) -> Self {
        Self::Database(source.into())
    }
}

impl From<sqlx::Error> for RetentionError {
    fn from(source: sqlx::Error) -> Self {
        Self::Database(source.into())
    }
}

impl From<limit::WriterRoleError> for RetentionError {
    fn from(source: limit::WriterRoleError) -> Self {
        match source {
            limit::WriterRoleError::Database(error) => Self::Database(error),
            other => Self::WriterRole(other),
        }
    }
}

impl From<sqlx::Error> for PruneLogError {
    fn from(source: sqlx::Error) -> Self {
        Self::Database(source.into())
    }
}

/// `categories.retention` and `pges.stores.retention`, mirrored from their CHECK constraints.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RetentionCode {
    Forever,
    WindowDrop,
    WindowArchive,
    SnapshotAndTruncate,
}

impl RetentionCode {
    pub(crate) const ALL: [Self; 4] = [
        Self::Forever,
        Self::WindowDrop,
        Self::WindowArchive,
        Self::SnapshotAndTruncate,
    ];

    pub(crate) fn code(self) -> i16 {
        match self {
            Self::Forever => 0,
            Self::WindowDrop => 1,
            Self::WindowArchive => 2,
            Self::SnapshotAndTruncate => 3,
        }
    }

    pub(crate) fn from_code(code: i16) -> Option<Self> {
        Self::ALL.into_iter().find(|c| c.code() == code)
    }
}

/// A policy's `keep` is longer than [`Retention::MAX_KEEP`].
#[derive(Debug)]
pub(crate) struct KeepTooLong(pub(crate) Duration);

/// The `retention` and `retain_for` columns for a policy.
pub(crate) fn to_columns(retention: &Retention) -> Result<(i16, Option<PgInterval>), KeepTooLong> {
    let (code, keep) = match *retention {
        Retention::Forever => return Ok((RetentionCode::Forever.code(), None)),
        Retention::Window {
            keep,
            then: AfterWindow::Drop,
        } => (RetentionCode::WindowDrop, keep),
        Retention::SnapshotAndTruncate { keep } => (RetentionCode::SnapshotAndTruncate, keep),
    };
    if keep > Retention::MAX_KEEP {
        return Err(KeepTooLong(keep));
    }

    Ok((code.code(), Some(store::interval(keep))))
}

/// The store's default retention columns; [`Retention::Forever`] if the store has no row.
pub(crate) async fn store_default(
    conn: &mut PgConnection,
    store: &str,
) -> Result<(i16, Option<PgInterval>), sqlx::Error> {
    let row = statement::store_default_retention::Query::fixed()
        .bind(statement::store_default_retention::Params { store })
        .fetch_optional(&mut *conn)
        .await?;

    Ok(row.map_or((RetentionCode::Forever.code(), None), |row| {
        (row.retention, row.retain_for)
    }))
}

/// Log rows deleted per statement by [`admin::Admin::prune_log`].
const PRUNE_BATCH: i64 = 10_000;

/// A position past every log row, for a scan with no stop row.
const END_OF_LOG: i64 = i64::MAX;

/// One category's retention as read from `categories`.
struct CategoryRetention {
    id: category::CategoryId,
    name: String,
    code: Option<RetentionCode>,
    retain_for: Option<PgInterval>,
    scan_from: i64,
}

/// The positions `resequence` gave a category's commits, in commit id order rather than commit order
/// (`categories.resequenced_from`, `resequenced_through`; both 0 when it never did).
#[derive(Debug, Clone, Copy)]
struct Resequenced {
    from: i64,
    through: i64,
}

/// A leaf past its category's retention.
struct Leaf {
    name: template::Ident,
    upper: Uuid,
    detaching: bool,
}

/// What the scan of the log found for one leaf (`retention/scan.sql`).
struct Scan {
    horizon: Option<i64>,
    first_kept: Option<i64>,
    sequenced: bool,
    next_scan_from: Option<i64>,
}

/// The outcome of one leaf.
enum LeafOutcome {
    Dropped(Horizon),
    Refused(RefusalReason),
}

/// A category's retention horizon and start position (`categories.horizon_position`, `start_position`).
struct Horizon {
    horizon: position::Position,
    start: position::Position,
}

/// The intervals the eligibility checks need.
struct Margins {
    skew: PgInterval,
    txn_age: PgInterval,
}

async fn apply_retention(
    conn: &mut PgConnection,
    admin: &admin::Admin,
    config: &RetentionConfig,
) -> Result<RetentionReport, RetentionError> {
    let store = admin.store();
    if let Some(reason) = halted(conn).await? {
        return Err(RetentionError::SequencerHalted(reason));
    }
    let limits = statement::writer_limits::Query::fixed()
        .bind(statement::writer_limits::Params { store })
        .fetch_optional(&mut *conn)
        .await?;
    let Some(statement::writer_limits::Row {
        max_clock_skew: skew,
        max_txn_age: txn_age,
    }) = limits
    else {
        return Err(RetentionError::StoreNotFound(admin.store_name().clone()));
    };
    limit::check_writer_roles(conn, &config.writer_roles, store::duration(txn_age)).await?;

    let margins = Margins { skew, txn_age };
    let rows = admin
        .sql()
        .retention_categories
        .bind(statement::retention_categories::Params {})
        .fetch_all(&mut *conn)
        .await?;
    let mut categories = Vec::with_capacity(rows.len());
    for row in rows {
        categories.push(CategoryRetention {
            id: category::CategoryId::new(row.category_id),
            name: row.name,
            code: RetentionCode::from_code(row.retention),
            retain_for: row.retain_for,
            scan_from: row.retention_scan_from,
        });
    }

    let mut report = RetentionReport::default();
    for category in categories {
        // Names become part of DDL; the categories table's CHECK already enforces this.
        let Some(category_name) = catalog::intern(&category.name) else {
            continue;
        };
        let (retain_for, policy) = match (category.code, &category.retain_for) {
            (Some(RetentionCode::Forever), _) | (_, None) => continue,
            (Some(RetentionCode::WindowDrop), Some(keep)) => (keep, Policy::Window),
            (Some(RetentionCode::SnapshotAndTruncate), Some(keep)) => {
                (keep, Policy::SnapshotAndTruncate)
            }
            (Some(RetentionCode::WindowArchive) | None, Some(_)) => {
                report.refused.push(Refusal {
                    category: category_name,
                    leaf: None,
                    reason: RefusalReason::UnsupportedPolicy,
                });
                continue;
            }
        };

        let parent = admin::plain(&ident::category_partition(category_name.as_str()))?;
        drop_detached_leaves(conn, admin.store_name(), category_name, &parent).await?;

        let leaves = statement::leaves_past_retention::Query::fixed()
            .bind(statement::leaves_past_retention::Params {
                parent: &format!("{store}.{parent}"),
                keep: *retain_for,
                max_clock_skew: margins.skew,
                max_txn_age: margins.txn_age,
            })
            .fetch_all(&mut *conn)
            .await?;
        let mut scan_from = category.scan_from;
        for row in leaves {
            // Leaves go oldest first: skipping one would move the horizon past commits that stay.
            let Some(name) = template::Ident::new(row.leaf) else {
                break;
            };
            let leaf = Leaf {
                name,
                upper: row.upper,
                detaching: row.detaching,
            };

            let outcome = retire_leaf(
                conn,
                admin,
                &category,
                &parent,
                &leaf,
                Retire {
                    policy,
                    retain_for,
                    margins: &margins,
                    scan_from: &mut scan_from,
                },
            )
            .await?;
            match outcome {
                LeafOutcome::Dropped(Horizon { horizon, start }) => {
                    report.dropped.push(DroppedLeaf {
                        category: category_name,
                        leaf: PartitionName::of(&leaf.name),
                        horizon,
                        start,
                    });
                }
                LeafOutcome::Refused(reason) => {
                    report.refused.push(Refusal {
                        category: category_name,
                        leaf: Some(PartitionName::of(&leaf.name)),
                        reason,
                    });
                    break;
                }
            }
        }
    }
    report.log_partitions = drop_log_partitions(conn, admin).await?;

    Ok(report)
}

/// The policy of a category that drops leaves.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Policy {
    Window,
    SnapshotAndTruncate,
}

/// What [`retire_leaf`] needs besides the leaf.
struct Retire<'a> {
    policy: Policy,
    retain_for: &'a PgInterval,
    margins: &'a Margins,
    /// The category's `retention_scan_from`, advanced as leaves go.
    scan_from: &'a mut i64,
}

/// Checks one leaf, records the horizon and the streams' floors, then detaches and drops it.
async fn retire_leaf(
    conn: &mut PgConnection,
    admin: &admin::Admin,
    category: &CategoryRetention,
    parent: &template::Ident,
    leaf: &Leaf,
    retire: Retire<'_>,
) -> Result<LeafOutcome, RetentionError> {
    let store = admin.store_name();
    let leaf_values = template::Values::new()
        .store(store)
        .ident(template::IdentPlaceholder::Leaf, &leaf.name);

    // An earlier run recorded the horizon and was interrupted while detaching.
    if leaf.detaching {
        detach_and_drop(conn, store, parent, &leaf.name).await?;
        return Ok(LeafOutcome::Dropped(
            horizon_of(conn, admin, category.id).await?,
        ));
    }

    let scan = scan_log(conn, admin, category, &leaf_values, leaf.upper, &retire).await?;
    if !scan.sequenced {
        return Ok(LeafOutcome::Refused(RefusalReason::NotSequenced));
    }

    let mut tx = conn.begin().await?;
    if let Some(reason) = halted(&mut tx).await? {
        tx.rollback().await?;
        return Err(RetentionError::SequencerHalted(reason));
    }
    let blocking = match retire.policy {
        Policy::Window => {
            let streams = live_streams(&mut tx, &leaf_values, retire.retain_for).await?;
            (!streams.is_empty()).then_some(RefusalReason::LiveStreams(streams))
        }
        Policy::SnapshotAndTruncate => {
            let streams = missing_snapshots(&mut tx, &leaf_values, category.id).await?;
            (!streams.is_empty()).then_some(RefusalReason::MissingSnapshots(streams))
        }
    };
    if let Some(reason) = blocking {
        tx.rollback().await?;
        return Ok(LeafOutcome::Refused(reason));
    }

    // Nested loops only: a hash or merge join would read every streams partition for each leaf.
    tx.execute(statement::PROBE_JOINS.sql()).await?;
    statement::raise_floors::Query::render(&leaf_values)
        .bind(statement::raise_floors::Params {})
        .execute(&mut *tx)
        .await?;
    tx.execute(statement::PLAN_JOINS.sql()).await?;
    let raised = admin
        .sql()
        .raise_horizon
        .bind(statement::raise_horizon::Params {
            category_id: category.id.get(),
            horizon: scan.horizon,
            first_kept: scan.first_kept,
        })
        .fetch_one(&mut *tx)
        .await?;
    let horizon = raised.horizon_position;
    // In the horizon's transaction: once it commits, no store-wide read reaches these log rows.
    admin
        .sql()
        .forget_deleted
        .bind(statement::forget_deleted::Params {
            category_id: category.id.get(),
            upper: leaf.upper,
            scan_from: *retire.scan_from,
            horizon,
        })
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;

    detach_and_drop(conn, store, parent, &leaf.name).await?;

    // Only once the leaf is gone: a run interrupted before the drop scans the leaf again from the old
    // start, and its commits below the new start would look unsequenced. `prune_log` deletes up to it too.
    if let Some(next) = scan.next_scan_from {
        admin
            .sql()
            .raise_scan_from
            .bind(statement::raise_scan_from::Params {
                category_id: category.id.get(),
                scan_from: next,
            })
            .execute(&mut *conn)
            .await?;
        *retire.scan_from = (*retire.scan_from).max(next);
    }

    Ok(LeafOutcome::Dropped(Horizon {
        horizon: to_position(horizon),
        start: to_position(raised.start_position),
    }))
}

/// Finds the leaf's horizon and whether every commit in it has a log row, scanning the category's log
/// rows from `retention_scan_from` to the first row of a commit that started after the leaf's last commit
/// ended, or to the end of the positions `resequence` gave the category if those lie further.
///
/// Positions follow commit order everywhere except inside the range `resequence` wrote, which is in commit
/// id order and may hold commits older than rows before it. So the scan covers that range whole, and the
/// next scan start ignores rows inside it: a row there with an old id can sit after commits of later
/// leaves.
///
/// The range and the log are read in one snapshot. A `resequence` that commits between reading the range
/// and scanning would otherwise add rows the scan sees as if in commit order, and the next scan start could
/// land past rows of commits that stay. One that commits after the snapshot only adds rows past the log
/// tail, which this scan does not see and the next one covers.
async fn scan_log(
    conn: &mut PgConnection,
    admin: &admin::Admin,
    category: &CategoryRetention,
    leaf_values: &template::Values,
    upper: Uuid,
    retire: &Retire<'_>,
) -> Result<Scan, sqlx::Error> {
    let mut tx = conn.begin().await?;
    tx.execute(statement::SCAN_SNAPSHOT.sql()).await?;
    let scan = scan_log_in_snapshot(&mut tx, admin, category, leaf_values, upper, retire).await?;
    tx.commit().await?;

    Ok(scan)
}

async fn scan_log_in_snapshot(
    conn: &mut PgConnection,
    admin: &admin::Admin,
    category: &CategoryRetention,
    leaf_values: &template::Values,
    upper: Uuid,
    retire: &Retire<'_>,
) -> Result<Scan, sqlx::Error> {
    let category_id = category.id;
    let row = admin
        .sql()
        .resequenced
        .bind(statement::resequenced::Params {
            category_id: category_id.get(),
        })
        .fetch_one(&mut *conn)
        .await?;
    let resequenced = Resequenced {
        from: row.resequenced_from,
        through: row.resequenced_through,
    };
    let stop = admin
        .sql()
        .scan_stop
        .bind(statement::scan_stop::Params {
            category_id: category_id.get(),
            scan_from: *retire.scan_from,
            upper,
            max_txn_age: retire.margins.txn_age,
            max_clock_skew: retire.margins.skew,
        })
        .fetch_optional(&mut *conn)
        .await?;
    let mut stop = stop.map_or(END_OF_LOG, |row| row.last_position);
    if resequenced.through > *retire.scan_from {
        stop = stop.max(resequenced.through);
    }

    let row = statement::scan::Query::render(leaf_values)
        .bind(statement::scan::Params {
            category_id: category_id.get(),
            scan_from: *retire.scan_from,
            stop,
            upper,
            max_txn_age: retire.margins.txn_age,
            max_clock_skew: retire.margins.skew,
            resequenced_from: resequenced.from,
            resequenced_through: resequenced.through,
        })
        .fetch_one(&mut *conn)
        .await?;

    Ok(Scan {
        horizon: row.horizon,
        first_kept: row.first_kept,
        sequenced: row.sequenced,
        next_scan_from: row.next_scan_from,
    })
}

/// Streams with commits in the leaf that are not deleted and had a commit within `keep`.
async fn live_streams(
    conn: &mut PgConnection,
    leaf_values: &template::Values,
    keep: &PgInterval,
) -> Result<Vec<stream::StreamId>, sqlx::Error> {
    let rows = statement::live_streams::Query::render(leaf_values)
        .bind(statement::live_streams::Params {
            keep: *keep,
            limit: i64::from(REPORTED_STREAMS),
        })
        .fetch_all(&mut *conn)
        .await?;

    Ok(rows
        .into_iter()
        .map(|row| stream::StreamId::from_uuid(row.stream_id))
        .collect())
}

/// Streams with commits in the leaf that are not deleted and have no snapshot at or after their last version
/// in it.
async fn missing_snapshots(
    conn: &mut PgConnection,
    leaf_values: &template::Values,
    category_id: category::CategoryId,
) -> Result<Vec<stream::StreamId>, sqlx::Error> {
    let rows = statement::missing_snapshots::Query::render(leaf_values)
        .bind(statement::missing_snapshots::Params {
            category_id: category_id.get(),
            limit: i64::from(REPORTED_STREAMS),
        })
        .fetch_all(&mut *conn)
        .await?;

    Ok(rows
        .into_iter()
        .map(|row| stream::StreamId::from_uuid(row.stream_id))
        .collect())
}

async fn horizon_of(
    conn: &mut PgConnection,
    admin: &admin::Admin,
    category_id: category::CategoryId,
) -> Result<Horizon, sqlx::Error> {
    let row = admin
        .sql()
        .horizon
        .bind(statement::horizon::Params {
            category_id: category_id.get(),
        })
        .fetch_one(&mut *conn)
        .await?;

    Ok(Horizon {
        horizon: to_position(row.horizon_position),
        start: to_position(row.start_position),
    })
}

/// `pges.sequencer.halted`.
async fn halted(conn: &mut PgConnection) -> Result<Option<String>, sqlx::Error> {
    let row = statement::sequencer_halted::Query::fixed()
        .bind(statement::sequencer_halted::Params {})
        .fetch_optional(&mut *conn)
        .await?;

    Ok(row.and_then(|row| row.halted))
}

/// Detaches a partition of `parent` without blocking appends (or finishes a detach an earlier run started),
/// then drops it.
async fn detach_and_drop(
    conn: &mut PgConnection,
    store: &pges_core::store::StoreName,
    parent: &template::Ident,
    table: &template::Ident,
) -> Result<(), sqlx::Error> {
    let values = template::Values::new()
        .store(store)
        .ident(template::IdentPlaceholder::Parent, parent)
        .ident(template::IdentPlaceholder::Table, table);
    let mut backoff = Duration::from_millis(50);
    let mut attempt = 1;

    loop {
        let pending = statement::detach_pending::Query::fixed()
            .bind(statement::detach_pending::Params {
                store: store.as_str(),
                table: table.as_str(),
            })
            .fetch_optional(&mut *conn)
            .await?
            .map(|row| row.inhdetachpending);
        let statement = match pending {
            None => break,
            // A concurrent detach that was interrupted after its first transaction must be finalized; it
            // cannot be started again.
            Some(true) => statement::DETACH_FINALIZE.render_text(&values),
            Some(false) => statement::DETACH_CONCURRENTLY.render_text(&values),
        };

        match conn.execute(statement.clone()).await {
            Ok(_) => break,
            Err(e)
                if attempt < admin::DDL_ATTEMPTS
                    && error::sqlstate(&e).as_deref() == Some(admin::LOCK_NOT_AVAILABLE) =>
            {
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(Duration::from_secs(2));
                attempt += 1;
            }
            Err(e) => return Err(e),
        }
    }

    if admin::relation_exists(conn, store.as_str(), table.as_str()).await? {
        admin::ddl(
            conn,
            statement::DROP_PARTITION.render_text(&values),
            admin::OnLockTimeout::RetryStatement,
        )
        .await?;
    }

    Ok(())
}

/// Drops leaves an earlier run detached but did not drop.
async fn drop_detached_leaves(
    conn: &mut PgConnection,
    store: &pges_core::store::StoreName,
    category: category::CategoryName,
    parent: &template::Ident,
) -> Result<(), sqlx::Error> {
    let pattern = ident::leaf_pattern(category.as_str());
    let leaves = statement::detached_leaves::Query::fixed()
        .bind(statement::detached_leaves::Params {
            store: store.as_str(),
            pattern: &pattern,
            parent: &format!("{store}.{parent}"),
        })
        .fetch_all(&mut *conn)
        .await?;

    for row in leaves {
        let Some(leaf) = template::Ident::new(row.relname) else {
            continue;
        };
        let values = template::Values::new()
            .store(store)
            .ident(template::IdentPlaceholder::Table, &leaf);
        admin::ddl(
            conn,
            statement::DROP_PARTITION.render_text(&values),
            admin::OnLockTimeout::RetryStatement,
        )
        .await?;
    }

    Ok(())
}

/// Drops the full log partitions whose rows every category's retention has passed: each row is at or below
/// its category's `retention_scan_from`, so it only names commits retention dropped. The newest partition
/// is never dropped.
async fn drop_log_partitions(
    conn: &mut PgConnection,
    admin: &admin::Admin,
) -> Result<Vec<PartitionName>, sqlx::Error> {
    let store = admin.store_name();
    let parent = admin::plain("log")?;

    let mut dropped = Vec::new();
    for partition in closed_log_partitions(conn, admin).await? {
        let droppable = partition.detaching || {
            let values = template::Values::new()
                .store(store)
                .ident(template::IdentPlaceholder::LogPartition, &partition.name);
            let pinned = statement::log_partition_pinned::Query::render(&values)
                .bind(statement::log_partition_pinned::Params {
                    store: store.as_str(),
                })
                .fetch_one(&mut *conn)
                .await?;
            !pinned.pinned
        };
        if droppable {
            detach_and_drop(conn, store, &parent, &partition.name).await?;
            dropped.push(PartitionName::of(&partition.name));
        }
    }

    Ok(dropped)
}

/// The names of the store's full log partitions other than the newest, oldest first, leaving out any being
/// detached. The sequencer no longer writes to them.
pub(crate) async fn full_log_partitions(
    conn: &mut PgConnection,
    admin: &admin::Admin,
) -> Result<Vec<String>, sqlx::Error> {
    Ok(closed_log_partitions(conn, admin)
        .await?
        .into_iter()
        .filter(|partition| !partition.detaching)
        .map(|partition| partition.name.as_str().to_owned())
        .collect())
}

/// The store's full log partitions other than the newest, oldest first.
async fn closed_log_partitions(
    conn: &mut PgConnection,
    admin: &admin::Admin,
) -> Result<Vec<LogPartition>, sqlx::Error> {
    let partitions = log_partitions(conn, admin.store_name()).await?;
    let Some(newest) = partitions.last().map(|p| p.number) else {
        return Ok(Vec::new());
    };
    let last_position = admin
        .sql()
        .last_position
        .bind(statement::last_position::Params {})
        .fetch_one(&mut *conn)
        .await?
        .coalesce;

    // Positions are gap-free, so a partition is full once the log reaches its last position.
    Ok(partitions
        .into_iter()
        .take_while(|partition| {
            let upper = (partition.number + 1).saturating_mul(admin::LOG_PARTITION_SPAN);
            partition.number != newest && last_position >= upper - 1
        })
        .collect())
}

struct LogPartition {
    name: template::Ident,
    number: i64,
    detaching: bool,
}

/// The store's log partitions, oldest first.
async fn log_partitions(
    conn: &mut PgConnection,
    store: &pges_core::store::StoreName,
) -> Result<Vec<LogPartition>, sqlx::Error> {
    let rows = statement::log_partitions::Query::fixed()
        .bind(statement::log_partitions::Params {
            log: &format!("{store}.log"),
        })
        .fetch_all(&mut *conn)
        .await?;

    Ok(rows
        .into_iter()
        .filter_map(|row| {
            Some(LogPartition {
                name: template::Ident::new(row.relname)?,
                number: row.n,
                detaching: row.inhdetachpending,
            })
        })
        .collect())
}

async fn prune_log(
    conn: &mut PgConnection,
    admin: &admin::Admin,
    name: category::CategoryName,
) -> Result<PrunedLog, PruneLogError> {
    let row = admin
        .sql()
        .prune_category
        .bind(statement::prune_category::Params {
            name: name.as_str(),
        })
        .fetch_optional(&mut *conn)
        .await?;
    let Some(statement::prune_category::Row {
        category_id,
        retention_scan_from: scan_from,
    }) = row
    else {
        return Err(PruneLogError::UnknownCategory(name));
    };

    let mut pruned = PrunedLog::default();
    for partition in log_partitions(conn, admin.store_name()).await? {
        let lower = partition.number.saturating_mul(admin::LOG_PARTITION_SPAN);
        if lower > scan_from {
            break;
        }
        if partition.detaching {
            continue;
        }

        let values = template::Values::new()
            .store(admin.store_name())
            .category(name)
            .ident(template::IdentPlaceholder::LogPartition, &partition.name);
        let deleted =
            delete_log_rows(conn, &values, admin.store_name(), category_id, scan_from).await?;
        if deleted > 0 {
            conn.execute(statement::VACUUM_LOG_PARTITION.render_text(&values))
                .await?;
        }
        pruned.rows += deleted;
    }
    pruned.log_partitions = drop_log_partitions(conn, admin).await?;

    Ok(pruned)
}

/// Deletes the category's rows at or below `through` from one log partition, in batches, except those
/// whose commit still exists in the category's partition of `commits` and those above the store's coarse
/// high-water mark.
async fn delete_log_rows(
    conn: &mut PgConnection,
    partition_values: &template::Values,
    store: &pges_core::store::StoreName,
    category_id: i32,
    through: i64,
) -> Result<u64, sqlx::Error> {
    // Every row at or below retention_scan_from names a dropped commit as long as writers keep to the
    // store's limits. A row whose commit is still there would make resequence sequence it again.
    let prune = statement::prune_log::Query::render(partition_values);

    let mut deleted = 0;
    loop {
        let rows = prune
            .bind(statement::prune_log::Params {
                category_id,
                scan_from: through,
                batch: PRUNE_BATCH,
                store: store.as_str(),
            })
            .execute(&mut *conn)
            .await?
            .rows_affected();
        deleted += rows;
        if rows < PRUNE_BATCH.unsigned_abs() {
            return Ok(deleted);
        }
    }
}

fn to_position(position: i64) -> position::Position {
    position::Position::from_i64(position).unwrap_or(position::Position::BEGINNING)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn partition_names_print_as_their_text() {
        let leaf = PartitionName("commits_jobs__202611".to_owned());

        assert_eq!(format!("{leaf:?}"), "\"commits_jobs__202611\"");
        assert_eq!(
            format!("{:?}", Some(&leaf)),
            "Some(\"commits_jobs__202611\")"
        );
        assert_eq!(leaf.to_string(), "commits_jobs__202611");
    }

    #[test]
    fn codes_round_trip() {
        for code in RetentionCode::ALL {
            assert_eq!(RetentionCode::from_code(code.code()), Some(code));
        }
        assert_eq!(RetentionCode::from_code(4), None);
    }

    #[test]
    fn policies_map_to_their_codes() {
        let keep = Duration::from_secs(90);
        assert_eq!(to_columns(&Retention::Forever).unwrap(), (0, None));
        let (code, retain_for) = to_columns(&Retention::Window {
            keep,
            then: AfterWindow::Drop,
        })
        .unwrap();
        assert_eq!(code, 1);
        assert_eq!(retain_for.map(|i| i.microseconds), Some(90_000_000));
        assert_eq!(
            to_columns(&Retention::SnapshotAndTruncate { keep })
                .unwrap()
                .0,
            3
        );
    }
}
