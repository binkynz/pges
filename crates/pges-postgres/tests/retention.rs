//! Retention: which leaves `Admin::apply_retention` drops, the horizon and floors it records, and
//! `Admin::prune_log`.
//!
//! Commits are written into leaves months in the past through a store whose clock skew tolerance spans
//! them, and a hand-written "sequencer" gives them log rows. Before retention runs, the store's recorded
//! skew goes back to the default. `Admin::set_writer_limits` refuses that, so the fixture writes the column
//! itself: the history it builds is one honest writers produce, compressed from months into seconds.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

mod common;

use std::time::Duration;

use anyhow::Result;
use futures_util::TryStreamExt;
use sqlx::{Executor, PgPool};
use uuid::Uuid;

use pges_core::{commit, position, stream, version};
use pges_postgres::{admin, append, limit, load, read, retention, store};

use common::{Job, JobEvent, OrderEvent, Placed, STORE, STORE_NAME, v};

const DAY: Duration = Duration::from_secs(86_400);
/// The store's `max_txn_age`; the test role's `transaction_timeout` matches it.
const MAX_TXN_AGE: Duration = Duration::from_secs(60);

struct Fixture {
    db: pges_testing::TestDb,
    pool: PgPool,
    store: store::Store,
    admin: admin::Admin,
    config: retention::RetentionConfig,
    ms: u32,
}

impl Fixture {
    /// A store with `jobs` and `orders`, each with monthly leaves from three months back to two ahead, log
    /// partition 0, and the test's role as the writer, its transactions capped at a minute in this database.
    async fn new() -> Result<Self> {
        let db = pges_testing::TestDb::new().await?;
        let mut conn = db.connect().await?;
        pges_testing::create_store(
            &mut conn,
            &STORE_NAME,
            &pges_testing::definition(pges_postgres::store::Outbox::Disabled),
        )
        .await?;
        pges_testing::register_category(&mut conn, &STORE_NAME, pges_core::category!("jobs"), 3, 2)
            .await?;
        pges_testing::register_category(
            &mut conn,
            &STORE_NAME,
            pges_core::category!("orders"),
            3,
            2,
        )
        .await?;
        pges_testing::create_log_partition(&mut conn, &STORE_NAME, 0).await?;
        conn.execute(sqlx::AssertSqlSafe(format!(
            "ALTER ROLE CURRENT_USER IN DATABASE {} SET transaction_timeout = '1min'",
            db.name()
        )))
        .await?;
        let role: String = sqlx::query_scalar("SELECT current_user::text")
            .fetch_one(&mut conn)
            .await?;

        let pool = db.pool().await?;
        let admin = admin::Admin::new(pool.clone(), STORE_NAME);
        // Lets the test write commits months in the past.
        admin
            .set_writer_limits(limit::WriterLimits::new(
                200 * DAY,
                pges_core::interval!(pges_core::store::DEFAULT_MAX_TXN_AGE),
            ))
            .await?;
        let mut config = store::StoreConfig::new(STORE_NAME);
        config.max_clock_skew = Some(200 * DAY);
        let store = store::Store::open(pool.clone(), config).await?;

        Ok(Self {
            db,
            pool,
            store,
            admin,
            config: retention::RetentionConfig::new(limit::WriterRoles::one(role)),
            ms: 0,
        })
    }

    /// Records the default skew and `max_txn_age` as the store's limits, lowering them past
    /// `Admin::set_writer_limits`.
    async fn limits(&self, max_txn_age: Duration) -> Result<()> {
        sqlx::query(
            "UPDATE pges.stores SET max_clock_skew = $2, max_txn_age = $3 WHERE schema_name = $1",
        )
        .bind(STORE)
        .bind(interval(pges_core::store::DEFAULT_MAX_CLOCK_SKEW))
        .bind(interval(max_txn_age))
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// Runs retention with the default skew and a `max_txn_age` of a minute.
    async fn apply(&self) -> Result<retention::RetentionReport, retention::RetentionError> {
        self.limits(MAX_TXN_AGE).await.unwrap();
        self.admin.apply_retention(&self.config).await
    }

    async fn finish(self) -> Result<()> {
        self.pool.close().await;
        self.db.cleanup().await
    }

    /// A commit id `months_back` months before the start of the current month, plus `day` days; each call
    /// adds a millisecond, so ids increase in call order.
    async fn id_at(&mut self, months_back: i32, day: i32) -> Result<commit::CommitId> {
        self.ms += 1;
        let millis: i64 = sqlx::query_scalar(
            "SELECT (extract(epoch FROM date_trunc('month', now() AT TIME ZONE 'UTC') AT TIME ZONE 'UTC'
                     - make_interval(months => $1) + make_interval(days => $2)) * 1000)::int8 + $3",
        )
        .bind(months_back)
        .bind(day)
        .bind(i64::from(self.ms))
        .fetch_one(&self.pool)
        .await?;

        Ok(id_from_millis(millis))
    }

    fn id_now(&mut self) -> commit::CommitId {
        commit::CommitId::generate()
    }

    async fn job(
        &self,
        id: stream::StreamId,
        expected: version::ExpectedVersion,
        at: commit::CommitId,
        event: JobEvent,
    ) -> Result<append::AppendResult> {
        Ok(self
            .store
            .append(id, expected, &[event])
            .commit_id(at)
            .await?)
    }

    async fn order(&self, id: stream::StreamId, at: commit::CommitId) -> Result<()> {
        self.store
            .append(
                id,
                version::ExpectedVersion::NoStream,
                &[OrderEvent::Placed(Placed { total: 1 })],
            )
            .commit_id(at)
            .await?;
        Ok(())
    }

    /// Gives every commit without a log row one, in commit id order, from `first` on (or after the newest
    /// position when there is one).
    async fn sequence_from(&self, first: i64) -> Result<()> {
        self.pool
            .execute(
                sqlx::query(
                    "INSERT INTO es.log (last_position, commit_id, category_id, event_count)
                     SELECT greatest(coalesce((SELECT max(last_position) FROM es.log), 0), $1 - 1)
                            + sum(c.event_count) OVER (ORDER BY c.commit_id),
                            c.commit_id, c.category_id, c.event_count
                     FROM es.commits c
                     WHERE NOT EXISTS (SELECT 1 FROM es.log l WHERE l.commit_id = c.commit_id)",
                )
                .bind(first),
            )
            .await?;
        Ok(())
    }

    async fn sequence(&self) -> Result<()> {
        self.sequence_from(1).await
    }

    async fn leaf(&self, months_back: i32) -> Result<String> {
        Ok(sqlx::query_scalar(
            "SELECT 'commits_jobs__' || to_char(now() AT TIME ZONE 'UTC' - make_interval(months => $1), 'YYYYMM')",
        )
        .bind(months_back)
        .fetch_one(&self.pool)
        .await?)
    }

    async fn exists(&self, table: &str) -> Result<bool> {
        Ok(
            sqlx::query_scalar("SELECT to_regclass('es.' || $1) IS NOT NULL")
                .bind(table)
                .fetch_one(&self.pool)
                .await?,
        )
    }

    async fn horizon(&self, category: &str) -> Result<(i64, i64)> {
        Ok(sqlx::query_as(
            "SELECT horizon_position, retention_scan_from FROM es.categories WHERE name = $1",
        )
        .bind(category)
        .fetch_one(&self.pool)
        .await?)
    }

    async fn window(&self, keep: Duration) -> Result<()> {
        self.admin
            .set_retention(
                common::JOBS,
                retention::Retention::Window {
                    keep,
                    then: retention::AfterWindow::Drop,
                },
            )
            .await?;
        Ok(())
    }
}

fn id_from_millis(millis: i64) -> commit::CommitId {
    let millis = u64::try_from(millis).unwrap();
    let ts = uuid::Timestamp::from_unix(
        uuid::NoContext,
        millis / 1000,
        u32::try_from(millis % 1000).unwrap() * 1_000_000,
    );
    commit::CommitId::from_uuid(Uuid::new_v7(ts)).unwrap()
}

/// The commit ids recorded in `deleted_commits`, in order.
async fn deleted_commits(pool: &PgPool) -> Result<Vec<Uuid>> {
    Ok(
        sqlx::query_scalar("SELECT commit_id FROM es.deleted_commits ORDER BY commit_id")
            .fetch_all(pool)
            .await?,
    )
}

fn refused(
    report: &retention::RetentionReport,
) -> Vec<(String, Option<String>, retention::RefusalReason)> {
    report
        .refused
        .iter()
        .map(|r| {
            (
                r.category.as_str().to_owned(),
                r.leaf.as_ref().map(|leaf| leaf.as_str().to_owned()),
                r.reason.clone(),
            )
        })
        .collect()
}

fn dropped(report: &retention::RetentionReport) -> Vec<&str> {
    report.dropped.iter().map(|d| d.leaf.as_str()).collect()
}

#[tokio::test]
async fn window_drops_only_old_fully_sequenced_leaves() -> Result<()> {
    let mut f = Fixture::new().await?;
    f.window(7 * DAY).await?;
    let (a, b, c, order) = (
        stream::StreamId::new_v7(),
        stream::StreamId::new_v7(),
        stream::StreamId::new_v7(),
        stream::StreamId::new_v7(),
    );
    let at = f.id_at(3, 1).await?;
    f.order(order, at).await?;
    let at = f.id_at(3, 2).await?;
    f.job(
        a,
        version::ExpectedVersion::NoStream,
        at,
        JobEvent::created("a"),
    )
    .await?;
    let at = f.id_at(2, 2).await?;
    f.job(
        b,
        version::ExpectedVersion::NoStream,
        at,
        JobEvent::created("b"),
    )
    .await?;
    let at = f.id_now();
    f.job(
        c,
        version::ExpectedVersion::NoStream,
        at,
        JobEvent::created("c"),
    )
    .await?;
    let (oldest, older, current) = (f.leaf(3).await?, f.leaf(2).await?, f.leaf(0).await?);

    // Nothing is sequenced yet: the oldest leaf is refused, and the later ones are not tried.
    let report = f.apply().await?;
    assert!(report.dropped.is_empty(), "{report:?}");
    assert_eq!(
        refused(&report),
        [(
            "jobs".into(),
            Some(oldest.clone()),
            retention::RefusalReason::NotSequenced
        )],
        "{report:?}"
    );
    f.sequence().await?;

    // A halted sequencer stops retention before anything is dropped.
    f.pool
        .execute("UPDATE pges.sequencer SET halted = 'slot invalidated'")
        .await?;
    let error = f.apply().await.unwrap_err();
    assert!(
        matches!(&error, retention::RetentionError::SequencerHalted(reason) if reason == "slot invalidated"),
        "{error:?}"
    );
    assert!(f.exists(&oldest).await?);
    f.pool
        .execute("UPDATE pges.sequencer SET halted = NULL")
        .await?;

    let report = f.apply().await?;
    assert!(report.refused.is_empty(), "{report:?}");
    let leaves = dropped(&report);
    assert_eq!(&leaves[..2], [oldest.as_str(), older.as_str()]);
    assert!(!leaves.contains(&current.as_str()));
    assert!(!f.exists(&oldest).await? && !f.exists(&older).await?);
    assert!(f.exists(&current).await?);
    // In commit id order: the order took position 1, a's job 2, b's 3.
    let (horizon, scan_from) = f.horizon("jobs").await?;
    assert_eq!(horizon, 3);
    assert_eq!(scan_from, 3);
    assert_eq!(report.dropped[1].horizon, position!(3), "{report:?}");
    // Orders keep everything.
    assert_eq!(f.horizon("orders").await?, (0, 0));
    assert!(f.exists(&oldest.replace("jobs", "orders")).await?);

    // a's only commit is gone: reading or loading it says so instead of returning nothing.
    let error = f
        .store
        .read_stream::<JobEvent>(a, version::Version::FIRST)
        .try_collect::<Vec<_>>()
        .await
        .unwrap_err();
    assert!(
        matches!(error, read::ReadError::Truncated { stream_id, floor } if stream_id == a && floor == v(1)),
        "{error:?}"
    );
    let error = f.store.load::<Job>(a).await.unwrap_err();
    assert!(
        matches!(
            error,
            load::LoadError::Read(read::ReadError::Truncated { .. })
        ),
        "{error:?}"
    );
    // c is untouched.
    assert_eq!(f.store.load::<Job>(c).await?.version(), Some(v(1)));

    // Running it again finds nothing more to drop.
    let again = f.apply().await?;
    assert!(!dropped(&again).contains(&oldest.as_str()), "{again:?}");

    f.finish().await
}

#[tokio::test]
async fn window_refuses_a_leaf_with_long_lived_streams() -> Result<()> {
    let mut f = Fixture::new().await?;
    f.window(7 * DAY).await?;
    let long_lived = stream::StreamId::new_v7();
    let at = f.id_at(3, 2).await?;
    let first = f
        .job(
            long_lived,
            version::ExpectedVersion::NoStream,
            at,
            JobEvent::created("a"),
        )
        .await?;
    let at = f.id_now();
    f.job(long_lived, first.expected(), at, JobEvent::assigned())
        .await?;
    f.sequence().await?;

    let report = f.apply().await?;
    assert!(report.dropped.is_empty(), "{report:?}");
    assert_eq!(
        refused(&report),
        [(
            "jobs".into(),
            Some(f.leaf(3).await?),
            retention::RefusalReason::LiveStreams(vec![long_lived])
        )]
    );
    assert_eq!(f.horizon("jobs").await?, (0, 0));
    assert_eq!(f.store.load::<Job>(long_lived).await?.version(), Some(v(2)));

    f.finish().await
}

#[tokio::test]
async fn snapshot_and_truncate_needs_a_snapshot_of_every_stream() -> Result<()> {
    let mut f = Fixture::new().await?;
    f.admin
        .set_retention(
            common::JOBS,
            retention::Retention::SnapshotAndTruncate { keep: 7 * DAY },
        )
        .await?;
    let (job, deleted) = (stream::StreamId::new_v7(), stream::StreamId::new_v7());
    let at = f.id_at(3, 2).await?;
    let created = f
        .job(
            job,
            version::ExpectedVersion::NoStream,
            at,
            JobEvent::created("long"),
        )
        .await?;
    let at = f.id_at(3, 3).await?;
    let gone = f
        .job(
            deleted,
            version::ExpectedVersion::NoStream,
            at,
            JobEvent::created("gone"),
        )
        .await?;
    let at = f.id_at(2, 2).await?;
    let assigned = f
        .job(job, created.expected(), at, JobEvent::assigned())
        .await?;
    let at = f.id_now();
    f.job(job, assigned.expected(), at, JobEvent::assigned())
        .await?;
    f.sequence().await?;
    // A deleted stream needs no snapshot: its commits are gone.
    f.store
        .delete_stream::<JobEvent>(deleted, gone.last_version())
        .await?;
    // Deleted before it was sequenced: its log row may still come, after any horizon.
    let unsequenced = stream::StreamId::new_v7();
    let at = f.id_at(3, 4).await?;
    let late = f
        .job(
            unsequenced,
            version::ExpectedVersion::NoStream,
            at,
            JobEvent::created("late"),
        )
        .await?;
    f.store
        .delete_stream::<JobEvent>(unsequenced, late.last_version())
        .await?;
    assert_eq!(
        deleted_commits(&f.pool).await?,
        [gone.commit_id.as_uuid(), late.commit_id.as_uuid()]
    );

    let report = f.apply().await?;
    assert!(report.dropped.is_empty(), "{report:?}");
    assert_eq!(
        refused(&report),
        [(
            "jobs".into(),
            Some(f.leaf(3).await?),
            retention::RefusalReason::MissingSnapshots(vec![job])
        )]
    );
    assert!(f.exists(&f.leaf(3).await?).await?);

    // A snapshot at version 3 covers the stream's last version in both old leaves.
    let loaded = f.store.load_snapshotted::<Job>(job).await?;
    f.store.save_snapshot(&loaded).await?;
    f.sequence().await?;
    let report = f.apply().await?;
    assert!(report.refused.is_empty(), "{report:?}");
    assert_eq!(
        &dropped(&report)[..2],
        [f.leaf(3).await?.as_str(), f.leaf(2).await?.as_str()]
    );
    // The horizon covers the deleted commit's log row, so its record goes with the leaf.
    assert_eq!(deleted_commits(&f.pool).await?, [late.commit_id.as_uuid()]);

    // Versions up to 2 are gone: a plain load or a read from the start fails, the snapshot loads.
    let error = f.store.load::<Job>(job).await.unwrap_err();
    assert!(
        matches!(
            error,
            load::LoadError::Read(read::ReadError::Truncated { floor, .. }) if floor == v(2)
        ),
        "{error:?}"
    );
    let error = f
        .store
        .read_stream::<JobEvent>(job, version::Version::FIRST)
        .try_collect::<Vec<_>>()
        .await
        .unwrap_err();
    assert!(
        matches!(error, read::ReadError::Truncated { .. }),
        "{error:?}"
    );
    let loaded = f.store.load_snapshotted::<Job>(job).await?;
    assert_eq!(loaded.version(), Some(v(3)));
    assert_eq!(loaded.state().titles, ["long"]);
    assert_eq!(loaded.state().assignments, 2);
    let tail: Vec<_> = f
        .store
        .read_stream::<JobEvent>(job, v(3))
        .try_collect()
        .await?;
    assert_eq!(tail.len(), 1);

    f.finish().await
}

#[tokio::test]
async fn writer_roles_must_cap_their_transactions() -> Result<()> {
    let f = Fixture::new().await?;
    f.limits(MAX_TXN_AGE).await?;
    let role = f.config.writer_roles.iter().next().unwrap().to_owned();
    let config = |roles: limit::WriterRoles| {
        let mut config = f.config.clone();
        config.writer_roles = roles;
        config
    };
    let too_long = |error: &retention::RetentionError, timeout: Duration, age: Duration| {
        matches!(
            error,
            retention::RetentionError::WriterRole(limit::WriterRoleError::TransactionTimeoutTooLong {
                transaction_timeout, max_txn_age, ..
            }) if *transaction_timeout == timeout && *max_txn_age == age
        )
    };

    let error = f
        .admin
        .apply_retention(&config(limit::WriterRoles::new(
            role.clone(),
            ["pges_no_such_role"],
        )))
        .await
        .unwrap_err();
    assert!(
        matches!(
            error,
            retention::RetentionError::WriterRole(limit::WriterRoleError::UnknownRole(ref r))
                if r == "pges_no_such_role"
        ),
        "{error:?}"
    );

    // The role's minute is longer than the store's max_txn_age.
    f.limits(Duration::from_secs(30)).await?;
    let error = f.admin.apply_retention(&f.config).await.unwrap_err();
    assert!(
        too_long(&error, MAX_TXN_AGE, Duration::from_secs(30)),
        "{error:?}"
    );
    f.limits(MAX_TXN_AGE).await?;

    f.pool
        .execute(sqlx::AssertSqlSafe(format!(
            "ALTER ROLE CURRENT_USER IN DATABASE {} RESET transaction_timeout",
            f.db.name()
        )))
        .await?;
    let error = f.admin.apply_retention(&f.config).await.unwrap_err();
    assert!(
        matches!(
            &error,
            retention::RetentionError::WriterRole(limit::WriterRoleError::TransactionTimeoutNotSet { role: r })
                if *r == role
        ),
        "{error:?}"
    );

    // A database-wide setting applies to every role without one of its own.
    f.pool
        .execute(sqlx::AssertSqlSafe(format!(
            "ALTER DATABASE {} SET transaction_timeout = '30s'",
            f.db.name()
        )))
        .await?;
    f.admin.apply_retention(&f.config).await?;
    f.limits(Duration::from_secs(30)).await?;
    f.admin.apply_retention(&f.config).await?;
    f.limits(Duration::from_secs(10)).await?;
    let error = f.admin.apply_retention(&f.config).await.unwrap_err();
    assert!(
        too_long(&error, Duration::from_secs(30), Duration::from_secs(10)),
        "{error:?}"
    );

    f.finish().await
}

#[tokio::test]
async fn categories_take_the_store_default_when_registered() -> Result<()> {
    let f = Fixture::new().await?;
    let keep = 30 * DAY;
    f.admin
        .set_default_retention(retention::Retention::SnapshotAndTruncate { keep })
        .await?;
    let parts = pges_core::category!("parts");
    f.admin
        .register_category(parts, admin::CategoryConfig::default())
        .await?;
    let mut config = admin::CategoryConfig::default();
    config.retention = Some(retention::Retention::Forever);
    let tools = pges_core::category!("tools");
    f.admin.register_category(tools, config).await?;

    let rows: Vec<(String, i16, Option<f64>)> = sqlx::query_as(
        "SELECT name, retention, extract(epoch FROM retain_for)::float8 FROM es.categories
         WHERE name IN ('jobs', 'parts', 'tools') ORDER BY name",
    )
    .fetch_all(&f.pool)
    .await?;
    assert_eq!(
        rows,
        [
            ("jobs".to_owned(), 0, None),
            ("parts".to_owned(), 3, Some(keep.as_secs_f64())),
            ("tools".to_owned(), 0, None),
        ]
    );

    let missing = pges_core::category!("missing");
    let error = f
        .admin
        .set_retention(missing, retention::Retention::Forever)
        .await
        .unwrap_err();
    assert!(
        matches!(error, retention::SetRetentionError::UnknownCategory(name) if name == missing),
        "{error:?}"
    );

    // The CHECK constraints mirror the policy codes 0 to 3.
    for code in 0_i16..=4 {
        let result = sqlx::query(
            "UPDATE es.categories SET retention = $1,
                    retain_for = CASE WHEN $1 = 0 THEN NULL ELSE interval '1 day' END
             WHERE name = 'tools'",
        )
        .bind(code)
        .execute(&f.pool)
        .await;
        assert_eq!(result.is_ok(), code <= 3, "code {code}");
    }

    f.finish().await
}

#[tokio::test]
async fn an_archive_policy_is_refused() -> Result<()> {
    let f = Fixture::new().await?;
    f.pool
        .execute("UPDATE es.categories SET retention = 2, retain_for = interval '1 day' WHERE name = 'jobs'")
        .await?;

    let report = f.apply().await?;
    assert_eq!(
        refused(&report),
        [(
            "jobs".into(),
            None,
            retention::RefusalReason::UnsupportedPolicy
        )]
    );
    assert!(f.exists(&f.leaf(3).await?).await?);

    f.finish().await
}

#[tokio::test]
async fn prune_log_removes_a_window_categorys_rows_where_another_pins_the_log() -> Result<()> {
    let mut f = Fixture::new().await?;
    f.window(7 * DAY).await?;
    let mut conn = f.db.connect().await?;
    pges_testing::create_log_partition(&mut conn, &STORE_NAME, 1).await?;
    pges_testing::create_log_partition(&mut conn, &STORE_NAME, 2).await?;

    let at = f.id_at(3, 1).await?;
    f.order(stream::StreamId::new_v7(), at).await?;
    for (months_back, day) in [(3, 2), (3, 3), (2, 2)] {
        let at = f.id_at(months_back, day).await?;
        f.job(
            stream::StreamId::new_v7(),
            version::ExpectedVersion::NoStream,
            at,
            JobEvent::created("old"),
        )
        .await?;
    }
    f.sequence().await?;
    // Newer commits land in log partition 1, so partition 0 is full.
    let at = f.id_now();
    f.job(
        stream::StreamId::new_v7(),
        version::ExpectedVersion::NoStream,
        at,
        JobEvent::created("new"),
    )
    .await?;
    f.sequence_from(100_000_000).await?;

    // The order keeps log partition 0.
    let report = f.apply().await?;
    assert!(report.log_partitions.is_empty(), "{report:?}");
    assert_eq!(f.horizon("jobs").await?, (4, 4));

    let pruned = f.admin.prune_log(common::JOBS).await?;
    assert_eq!(pruned.rows, 3);
    assert!(pruned.log_partitions.is_empty());
    let left: Vec<(i64, String)> = sqlx::query_as(
        "SELECT l.last_position, c.name FROM es.log l JOIN es.categories c USING (category_id)
         ORDER BY l.last_position",
    )
    .fetch_all(&f.pool)
    .await?;
    assert_eq!(
        left,
        [(1, "orders".to_owned()), (100_000_000, "jobs".to_owned())]
    );
    assert!(f.exists("log_0000").await?);

    // Nothing more to prune.
    assert_eq!(f.admin.prune_log(common::JOBS).await?.rows, 0);
    let unknown = pges_core::category!("unknown");
    let error = f.admin.prune_log(unknown).await.unwrap_err();
    assert!(
        matches!(error, retention::PruneLogError::UnknownCategory(_)),
        "{error:?}"
    );

    f.finish().await
}

#[tokio::test]
async fn a_log_partition_retention_has_passed_for_every_category_is_dropped() -> Result<()> {
    let mut f = Fixture::new().await?;
    f.window(7 * DAY).await?;
    let mut conn = f.db.connect().await?;
    pges_testing::create_log_partition(&mut conn, &STORE_NAME, 1).await?;
    pges_testing::create_log_partition(&mut conn, &STORE_NAME, 2).await?;

    for (months_back, day) in [(3, 2), (2, 2)] {
        let at = f.id_at(months_back, day).await?;
        f.job(
            stream::StreamId::new_v7(),
            version::ExpectedVersion::NoStream,
            at,
            JobEvent::created("old"),
        )
        .await?;
    }
    f.sequence().await?;
    let at = f.id_now();
    f.order(stream::StreamId::new_v7(), at).await?;
    f.sequence_from(100_000_000).await?;

    let report = f.apply().await?;
    assert_eq!(report.log_partitions, ["log_0000"], "{report:?}");
    assert!(!f.exists("log_0000").await?);
    assert!(f.exists("log_0001").await? && f.exists("log_0002").await?);

    // Partition 1 is not full, and partition 2 is the newest.
    let again = f.apply().await?;
    assert!(again.log_partitions.is_empty(), "{again:?}");

    f.finish().await
}

#[tokio::test]
async fn a_run_interrupted_before_the_drop_is_finished_by_the_next() -> Result<()> {
    let mut f = Fixture::new().await?;
    f.window(7 * DAY).await?;
    for day in [2, 20] {
        let at = f.id_at(3, day).await?;
        f.job(
            stream::StreamId::new_v7(),
            version::ExpectedVersion::NoStream,
            at,
            JobEvent::created("old"),
        )
        .await?;
    }
    f.sequence().await?;
    let oldest = f.leaf(3).await?;

    // A lock on the category partition makes every detach attempt time out.
    let mut blocker = f.db.connect().await?;
    blocker.execute("BEGIN").await?;
    blocker
        .execute("LOCK TABLE es.commits_jobs IN SHARE UPDATE EXCLUSIVE MODE")
        .await?;
    let error = f.apply().await.unwrap_err();
    assert!(
        matches!(error, retention::RetentionError::Database(_)),
        "{error:?}"
    );
    blocker.execute("ROLLBACK").await?;

    // The horizon is recorded, the scan start is not: the leaf is still there.
    assert!(f.exists(&oldest).await?);
    assert_eq!(f.horizon("jobs").await?, (2, 0));
    assert_eq!(f.admin.prune_log(common::JOBS).await?.rows, 0);

    let report = f.apply().await?;
    assert!(report.refused.is_empty(), "{report:?}");
    assert_eq!(dropped(&report)[0], oldest.as_str());
    assert!(!f.exists(&oldest).await?);
    assert_eq!(f.horizon("jobs").await?, (2, 2));

    f.finish().await
}

/// Retention allows for the skew the store records, not a default of its own: while the store lets
/// writers append commits months in the past, a leaf from three months ago is not past retention.
#[tokio::test]
async fn retention_allows_for_the_stores_recorded_skew() -> Result<()> {
    let mut f = Fixture::new().await?;
    f.window(7 * DAY).await?;
    let at = f.id_at(3, 2).await?;
    f.job(
        stream::StreamId::new_v7(),
        version::ExpectedVersion::NoStream,
        at,
        JobEvent::created("old"),
    )
    .await?;
    f.sequence().await?;
    let oldest = f.leaf(3).await?;

    let report = f.admin.apply_retention(&f.config).await?;
    assert!(
        report.dropped.is_empty() && report.refused.is_empty(),
        "{report:?}"
    );
    assert!(f.exists(&oldest).await?);

    let report = f.apply().await?;
    assert_eq!(dropped(&report)[0], oldest.as_str(), "{report:?}");

    f.finish().await
}

/// `prune_log` keeps a row at or below the scan start whose commit still exists: a resequence would
/// otherwise take that commit for one the sequencer never saw.
#[tokio::test]
async fn prune_log_keeps_the_rows_of_commits_that_still_exist() -> Result<()> {
    let mut f = Fixture::new().await?;
    for _ in 0..2 {
        let at = f.id_now();
        f.job(
            stream::StreamId::new_v7(),
            version::ExpectedVersion::NoStream,
            at,
            JobEvent::created("kept"),
        )
        .await?;
    }
    f.sequence().await?;
    f.pool
        .execute("UPDATE es.categories SET retention_scan_from = 2 WHERE name = 'jobs'")
        .await?;

    let pruned = f.admin.prune_log(common::JOBS).await?;
    assert_eq!(pruned.rows, 0);
    let left: i64 = sqlx::query_scalar("SELECT count(*) FROM es.log")
        .fetch_one(&f.pool)
        .await?;
    assert_eq!(left, 2);

    f.finish().await
}

fn interval(duration: Duration) -> sqlx::postgres::types::PgInterval {
    sqlx::postgres::types::PgInterval {
        months: 0,
        days: 0,
        microseconds: i64::try_from(duration.as_micros()).unwrap(),
    }
}
