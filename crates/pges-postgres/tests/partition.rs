//! Partition names, the parent check of maintenance, appends that no partition takes, and partition headroom.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

mod common;

use std::time::{Duration, SystemTime};

use anyhow::Result;
use sqlx::Connection;

use pges_core::{category, commit, stream, version};
use pges_postgres::{admin, append, store};

use common::{Fixture, JOBS, JobEvent, ORDERS, OrderEvent, Placed, STORE_NAME};

/// A category named like another category's leaf, registered first, does not stop that leaf from being
/// created: `jobs_<this month>` and `jobs`'s leaf for this month have different names.
#[tokio::test]
async fn a_category_named_like_a_leaf_registered_first_leaves_no_hole() -> Result<()> {
    let db = pges_testing::TestDb::new().await?;
    let mut conn = db.connect().await?;
    pges_testing::create_store(
        &mut conn,
        &STORE_NAME,
        &pges_testing::definition(store::Outbox::Disabled),
    )
    .await?;
    let pool = db.pool().await?;
    let admin = admin::Admin::new(pool.clone(), STORE_NAME);
    let like_a_leaf = category::CategoryName::leak(format!("jobs_{}", this_month(&pool).await?))?;

    admin
        .register_category(like_a_leaf, admin::CategoryConfig::default())
        .await?;
    admin
        .register_category(JOBS, admin::CategoryConfig::default())
        .await?;
    admin.maintain().await?;
    let store = store::Store::open(pool.clone(), store::StoreConfig::new(STORE_NAME)).await?;

    store
        .append(
            stream::StreamId::new_v7(),
            version::ExpectedVersion::NoStream,
            &[JobEvent::created("a")],
        )
        .await?;
    assert_eq!(
        parent(&pool, &format!("commits_{like_a_leaf}")).await?,
        Some("commits".to_owned())
    );
    conn.close().await?;
    pool.close().await;
    db.cleanup().await
}

/// Categories named like an existing category's leaf, its leaf's index, its partition's primary key, or the
/// primary key of `commits` register, get their own partitions and leaves, and leave the existing category's
/// partitions alone.
#[tokio::test]
async fn a_category_named_like_a_leaf_registered_after_gets_its_own_partition() -> Result<()> {
    let f = Fixture::new().await?;
    let admin = f.store.admin();
    let month = this_month(&f.pool).await?;
    let names = [
        format!("jobs_{month}"),
        format!("jobs_{month}_s"),
        "jobs_pkey".to_owned(),
        "pkey".to_owned(),
    ];

    for name in names.clone() {
        let category = category::CategoryName::leak(name)?;
        admin
            .register_category(category, admin::CategoryConfig::default())
            .await?;
    }
    admin.maintain().await?;

    for name in &names {
        let partition = format!("commits_{name}");
        assert_eq!(
            parent(&f.pool, &partition).await?,
            Some("commits".to_owned()),
            "{partition}"
        );
        assert_eq!(
            parent(&f.pool, &format!("commits_{name}__{month}")).await?,
            Some(partition)
        );
    }
    assert_eq!(
        parent(&f.pool, &format!("commits_jobs__{month}")).await?,
        Some("commits_jobs".to_owned())
    );
    f.store
        .append(
            stream::StreamId::new_v7(),
            version::ExpectedVersion::NoStream,
            &[JobEvent::created("a")],
        )
        .await?;
    f.finish().await
}

/// A partition's name taken by a relation that is not that partition fails registration with `NameTaken`;
/// nothing is dropped or attached under it.
#[tokio::test]
async fn a_partition_name_taken_by_another_relation_is_refused() -> Result<()> {
    let f = Fixture::new().await?;
    let admin = f.store.admin();
    let month = this_month(&f.pool).await?;
    let taken = format!("commits_parts__{month}");
    sqlx::query(sqlx::AssertSqlSafe(format!("CREATE SEQUENCE es.{taken}")))
        .execute(&f.pool)
        .await?;

    let error = admin
        .register_category(category!("parts"), admin::CategoryConfig::default())
        .await
        .unwrap_err();

    assert!(
        matches!(&error, admin::RegisterCategoryError::NameTaken { name } if name.as_str() == taken),
        "{error}"
    );
    let kind: String =
        sqlx::query_scalar("SELECT relkind::text FROM pg_class WHERE oid = to_regclass($1)")
            .bind(format!("es.{taken}"))
            .fetch_one(&f.pool)
            .await?;
    assert_eq!(kind, "S", "the sequence is left alone");
    f.finish().await
}

/// An append whose category has no leaf for its commit id fails with `NoPartition`, in every append path,
/// and works again once maintenance has created the leaves.
#[tokio::test]
async fn an_append_no_leaf_takes_fails_with_no_partition() -> Result<()> {
    let f = Fixture::new().await?;
    drop_leaves(&f, "commits_jobs").await?;
    let created = [JobEvent::created("a")];
    let placed = [OrderEvent::Placed(Placed { total: 1 })];

    let single = f
        .store
        .append(
            stream::StreamId::new_v7(),
            version::ExpectedVersion::NoStream,
            &created,
        )
        .await
        .unwrap_err();
    assert!(
        matches!(single, append::AppendError::NoPartition { category, .. } if category == JOBS),
        "{single}"
    );

    let orders = stream::StreamId::new_v7();
    let batch = f
        .store
        .append_many([
            append::AppendEntry::new(orders, version::ExpectedVersion::NoStream, &placed),
            append::AppendEntry::new(
                stream::StreamId::new_v7(),
                version::ExpectedVersion::NoStream,
                &created,
            ),
        ])
        .await
        .unwrap_err();
    assert!(
        matches!(batch, append::AppendError::NoPartition { category, .. } if category == JOBS),
        "{batch}"
    );

    let mut uow = f.store.begin().await?;
    let in_work = uow
        .append(
            stream::StreamId::new_v7(),
            version::ExpectedVersion::NoStream,
            &created,
        )
        .await
        .unwrap_err();
    assert!(
        matches!(in_work, append::AppendError::NoPartition { category, .. } if category == JOBS),
        "{in_work}"
    );
    drop(uow);

    f.store.admin().maintain().await?;
    f.store
        .append(
            stream::StreamId::new_v7(),
            version::ExpectedVersion::NoStream,
            &created,
        )
        .await?;
    f.store
        .append(orders, version::ExpectedVersion::NoStream, &placed)
        .await?;
    f.finish().await
}

/// A category whose partition of `commits` is gone fails appends with `NoPartition` too.
#[tokio::test]
async fn an_append_without_its_category_partition_fails_with_no_partition() -> Result<()> {
    let f = Fixture::new().await?;
    sqlx::query("ALTER TABLE es.commits DETACH PARTITION es.commits_orders")
        .execute(&f.pool)
        .await?;

    let error = f
        .store
        .append(
            stream::StreamId::new_v7(),
            version::ExpectedVersion::NoStream,
            &[OrderEvent::Placed(Placed { total: 1 })],
        )
        .await
        .unwrap_err();

    assert!(
        matches!(error, append::AppendError::NoPartition { category, .. } if category == ORDERS),
        "{error}"
    );
    f.finish().await
}

/// Headroom reports each category's newest leaf bound and the log's remaining positions, and flags a
/// category whose leaves reach less than one bucket ahead.
#[tokio::test]
async fn partition_headroom_reports_how_far_leaves_and_the_log_reach() -> Result<()> {
    let f = Fixture::new().await?;
    let admin = f.store.admin();
    let mut daily = admin::CategoryConfig::default();
    daily.bucket = admin::Bucket::Day;
    admin.register_category(category!("parts"), daily).await?;

    let before = admin.partition_headroom().await?;
    assert_eq!(before.log.end, None, "no log partition before maintain");
    assert_eq!(before.log.remaining, 0);

    admin.maintain().await?;
    let headroom = admin.partition_headroom().await?;
    let names: Vec<_> = headroom.categories.iter().map(|c| c.category).collect();
    assert_eq!(names, [JOBS, ORDERS, category!("parts")]);
    let month_ahead: i64 = sqlx::query_scalar(
        "SELECT extract(epoch FROM date_trunc('month', now() AT TIME ZONE 'UTC') AT TIME ZONE 'UTC'
                                   + 4 * interval '1 month')::int8",
    )
    .fetch_one(&f.pool)
    .await?;
    let jobs = &headroom.categories[0];
    assert_eq!(jobs.bucket, admin::Bucket::Month);
    assert_eq!(
        jobs.leaves_until,
        Some(SystemTime::UNIX_EPOCH + Duration::from_secs(month_ahead.try_into()?))
    );
    assert!(
        jobs.ahead > Duration::from_secs(3 * 28 * 24 * 3600),
        "{jobs:?}"
    );
    assert!(!jobs.below_one_bucket());
    let parts = &headroom.categories[2];
    assert_eq!(parts.bucket, admin::Bucket::Day);
    assert!(
        parts.ahead > Duration::from_secs(3 * 24 * 3600),
        "{parts:?}"
    );
    assert!(!parts.below_one_bucket());
    assert_eq!(
        headroom.log.end,
        pges_core::position::Position::new(admin::LOG_PARTITION_POSITIONS)
    );
    assert_eq!(
        headroom.log.remaining,
        admin::LOG_PARTITION_POSITIONS - 1,
        "positions 1 to 99,999,999"
    );

    // Keep only the day leaf holding now: it ends less than a day ahead.
    let later: Vec<String> = sqlx::query_scalar(
        "SELECT c.relname::text FROM pg_inherits i JOIN pg_class c ON c.oid = i.inhrelid
         WHERE i.inhparent = 'es.commits_parts'::regclass
           AND uuid_extract_timestamp(substring(pg_get_expr(c.relpartbound, c.oid)
                 FROM 'FROM \\(''([0-9a-f-]+)''\\)')::uuid) > now()",
    )
    .fetch_all(&f.pool)
    .await?;
    for leaf in later {
        drop_leaf(&f, "commits_parts", &leaf).await?;
    }
    let low = admin.partition_headroom().await?;
    assert!(low.categories[2].below_one_bucket(), "{low:?}");
    assert!(low.categories[2].ahead <= Duration::from_secs(24 * 3600));
    assert!(!low.categories[0].below_one_bucket(), "{low:?}");

    drop_leaves(&f, "commits_jobs").await?;
    let none = admin.partition_headroom().await?;
    assert_eq!(none.categories[0].leaves_until, None);
    assert_eq!(none.categories[0].ahead, Duration::ZERO);
    assert!(none.categories[0].below_one_bucket());
    f.finish().await
}

/// The current UTC month as a leaf suffix, by the database's clock.
async fn this_month(pool: &sqlx::PgPool) -> Result<String> {
    Ok(
        sqlx::query_scalar("SELECT to_char(now() AT TIME ZONE 'UTC', 'YYYYMM')")
            .fetch_one(pool)
            .await?,
    )
}

/// The parent of a table of the store, if it is a partition.
async fn parent(pool: &sqlx::PgPool, table: &str) -> Result<Option<String>> {
    Ok(sqlx::query_scalar(
        "SELECT p.relname::text FROM pg_inherits i JOIN pg_class p ON p.oid = i.inhparent
         WHERE i.inhrelid = to_regclass($1)",
    )
    .bind(format!("es.{table}"))
    .fetch_optional(pool)
    .await?)
}

/// Detaches and drops every leaf of a category partition.
async fn drop_leaves(f: &Fixture, partition: &str) -> Result<()> {
    let leaves: Vec<String> = sqlx::query_scalar(
        "SELECT c.relname::text FROM pg_inherits i JOIN pg_class c ON c.oid = i.inhrelid
         WHERE i.inhparent = to_regclass($1)",
    )
    .bind(format!("es.{partition}"))
    .fetch_all(&f.pool)
    .await?;
    for leaf in leaves {
        drop_leaf(f, partition, &leaf).await?;
    }

    Ok(())
}

async fn drop_leaf(f: &Fixture, partition: &str, leaf: &str) -> Result<()> {
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "ALTER TABLE es.{partition} DETACH PARTITION es.{leaf}"
    )))
    .execute(&f.pool)
    .await?;
    sqlx::query(sqlx::AssertSqlSafe(format!("DROP TABLE es.{leaf}")))
        .execute(&f.pool)
        .await?;

    Ok(())
}

/// `NoPartition` is reported the same in a caller's transaction, on a caller's connection in autocommit,
/// with the outbox on, and by `delete_stream`.
#[tokio::test]
async fn no_partition_is_reported_in_every_transaction_mode() -> Result<()> {
    let f = Fixture::with_outbox(store::Outbox::Enabled).await?;
    let created = [JobEvent::created("a")];
    let existing = stream::StreamId::new_v7();
    f.store
        .append(existing, version::ExpectedVersion::NoStream, &created)
        .await?;
    drop_leaves(&f, "commits_jobs").await?;
    let is_no_partition = |error: &append::AppendError| matches!(error, append::AppendError::NoPartition { category, .. } if *category == JOBS);

    let pooled = f
        .store
        .append(
            stream::StreamId::new_v7(),
            version::ExpectedVersion::NoStream,
            &created,
        )
        .await
        .unwrap_err();
    assert!(is_no_partition(&pooled), "{pooled}");

    let mut conn = f.pool.acquire().await?;
    let autocommit = f
        .store
        .append_in(
            &mut conn,
            stream::StreamId::new_v7(),
            version::ExpectedVersion::NoStream,
            &created,
        )
        .await
        .unwrap_err();
    assert!(is_no_partition(&autocommit), "{autocommit}");

    let mut tx = conn.begin().await?;
    let in_tx = f
        .store
        .append_in(
            &mut tx,
            stream::StreamId::new_v7(),
            version::ExpectedVersion::NoStream,
            &created,
        )
        .await
        .unwrap_err();
    assert!(is_no_partition(&in_tx), "{in_tx}");
    tx.rollback().await?;
    drop(conn);

    let deleted = f
        .store
        .delete_stream::<JobEvent>(existing, pges_postgres::delete::DeleteExpected::Any)
        .await
        .unwrap_err();
    assert!(
        matches!(&deleted, pges_postgres::delete::DeleteStreamError::Write(e) if is_no_partition(e)),
        "{deleted}"
    );
    f.finish().await
}

/// Day buckets and a bucket change name their leaves with the separator too: a day-bucketed category named
/// like another's day leaf, registered first, leaves no hole, and the bridging leaf of a bucket change does
/// not collide either.
#[tokio::test]
async fn day_leaves_and_bucket_changes_do_not_collide() -> Result<()> {
    let f = Fixture::new().await?;
    let admin = f.store.admin();
    let today: String = sqlx::query_scalar("SELECT to_char(now() AT TIME ZONE 'UTC', 'YYYYMMDD')")
        .fetch_one(&f.pool)
        .await?;
    let mut daily = admin::CategoryConfig::default();
    daily.bucket = admin::Bucket::Day;
    let like_a_leaf = category::CategoryName::leak(format!("parts_{today}"))?;
    admin.register_category(like_a_leaf, daily).await?;
    admin.register_category(category!("parts"), daily).await?;
    admin.maintain().await?;
    assert_eq!(
        parent(&f.pool, &format!("commits_parts__{today}")).await?,
        Some("commits_parts".to_owned())
    );
    assert_eq!(
        parent(&f.pool, &format!("commits_parts_{today}")).await?,
        Some("commits".to_owned())
    );

    admin.change_bucket(JOBS, admin::Bucket::Day).await?;
    admin.maintain().await?;
    let headroom = admin.partition_headroom().await?;
    for category in &headroom.categories {
        assert!(!category.below_one_bucket(), "{category:?}");
    }
    f.store
        .append(
            stream::StreamId::new_v7(),
            version::ExpectedVersion::NoStream,
            &[JobEvent::created("a")],
        )
        .await?;
    f.finish().await
}

/// `commits` has no key of its own: each category partition's primary key, on the commit id, is copied to
/// its leaves, so a commit id is unique within a category and may repeat in another. A category partition
/// that maintenance attaches again keeps its key.
#[tokio::test]
async fn commit_ids_are_unique_per_category() -> Result<()> {
    let f = Fixture::new().await?;
    let root_indexes: i64 =
        sqlx::query_scalar("SELECT count(*) FROM pg_index WHERE indrelid = 'es.commits'::regclass")
            .fetch_one(&f.pool)
            .await?;
    assert_eq!(root_indexes, 0);
    for category in [JOBS, ORDERS] {
        let partition = format!("es.commits_{category}");
        let key: (String, String) = sqlx::query_as(
            "SELECT conname::text, pg_get_constraintdef(oid) FROM pg_constraint
             WHERE conrelid = $1::regclass AND contype = 'p'",
        )
        .bind(&partition)
        .fetch_one(&f.pool)
        .await?;
        assert_eq!(
            key,
            (
                format!("commits_{category}__pkey"),
                "PRIMARY KEY (commit_id)".to_owned()
            )
        );
        let leaf_keys: Vec<String> = sqlx::query_scalar(
            "SELECT pg_get_constraintdef(k.oid) FROM pg_inherits i
             JOIN pg_constraint k ON k.conrelid = i.inhrelid AND k.contype = 'p'
             WHERE i.inhparent = $1::regclass",
        )
        .bind(&partition)
        .fetch_all(&f.pool)
        .await?;
        assert!(!leaf_keys.is_empty());
        assert!(
            leaf_keys.iter().all(|key| key == "PRIMARY KEY (commit_id)"),
            "{leaf_keys:?}"
        );
    }

    sqlx::query("ALTER TABLE es.commits DETACH PARTITION es.commits_orders")
        .execute(&f.pool)
        .await?;
    f.store.admin().maintain().await?;
    let commit_id = commit::CommitId::generate();
    f.store
        .append(
            stream::StreamId::new_v7(),
            version::ExpectedVersion::NoStream,
            &[JobEvent::created("a")],
        )
        .commit_id(commit_id)
        .await?;
    f.store
        .append(
            stream::StreamId::new_v7(),
            version::ExpectedVersion::NoStream,
            &[OrderEvent::Placed(Placed { total: 1 })],
        )
        .commit_id(commit_id)
        .await?;
    let error = f
        .store
        .append(
            stream::StreamId::new_v7(),
            version::ExpectedVersion::NoStream,
            &[OrderEvent::Placed(Placed { total: 2 })],
        )
        .commit_id(commit_id)
        .await
        .unwrap_err();
    assert!(
        matches!(error, append::AppendError::DuplicateCommit(c) if c == commit_id),
        "{error}"
    );

    // A lookup by category and commit id, as the append's classification makes, reads one leaf's key.
    let mut conn = f.db.connect().await?;
    sqlx::query("SET enable_seqscan = off")
        .execute(&mut conn)
        .await?;
    let orders: i32 =
        sqlx::query_scalar("SELECT category_id FROM es.categories WHERE name = 'orders'")
            .fetch_one(&mut conn)
            .await?;
    let plan: Vec<String> = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
        "EXPLAIN (COSTS OFF) SELECT last_version FROM es.commits
         WHERE category_id = {orders} AND commit_id = '{commit_id}'"
    )))
    .fetch_all(&mut conn)
    .await?;
    let plan = plan.join("\n");
    assert!(plan.contains("Index Scan using commits_orders__"), "{plan}");
    assert!(!plan.contains("commits_jobs"), "{plan}");
    conn.close().await?;
    f.finish().await
}
