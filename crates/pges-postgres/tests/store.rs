//! Opening stores, registering event types and categories, and partition maintenance.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

mod common;

use anyhow::Result;
use sqlx::Connection;

use pges_core::{category, store_name, stream, version};
use pges_postgres::{admin, migrate, store};

use common::{Fixture, JobEvent, OrderEvent, Placed};

#[tokio::test]
async fn open_checks_the_schema_and_the_store() -> Result<()> {
    let db = pges_testing::TestDb::new().await?;
    let pool = db.pool().await?;

    let error = store::Store::open(
        pool.clone(),
        store::StoreConfig::new(store_name!("missing")),
    )
    .await
    .unwrap_err();
    assert!(
        matches!(error, store::OpenError::StoreNotFound(_)),
        "{error}"
    );

    let mut config = store::StoreConfig::new(store_name!("auto"));
    config.migrate = store::AutoMigrate::CreateOrMigrate(store::StoreDefinition::default());
    let opened = store::Store::open(pool.clone(), config).await?;
    assert_eq!(opened.name(), &store_name!("auto"));
    let id: i32 = sqlx::query_scalar("SELECT store_id FROM pges.stores WHERE schema_name = 'auto'")
        .fetch_one(&pool)
        .await?;
    assert_eq!(opened.store_id(), pges_core::store::StoreId::new(id));

    sqlx::query(
        "UPDATE pges.scopes SET schema_version = 0, min_compatible = 0 WHERE scope = 'auto'",
    )
    .execute(&pool)
    .await?;
    let error = store::Store::open(pool.clone(), store::StoreConfig::new(store_name!("auto")))
        .await
        .unwrap_err();
    assert!(
        matches!(&error, store::OpenError::SchemaBehind { scope, database, required } if scope == "auto" && *database == migrate::SchemaVersion::NONE && *required == migrate::STORE.last().unwrap().version()),
        "{error}"
    );

    sqlx::query(
        "UPDATE pges.scopes SET schema_version = 3, min_compatible = 2 WHERE scope = 'pges'",
    )
    .execute(&pool)
    .await?;
    let error = store::Store::open(pool.clone(), store::StoreConfig::new(store_name!("auto")))
        .await
        .unwrap_err();
    assert!(
        matches!(&error, store::OpenError::CrateTooOld { scope, min_compatible, .. } if scope == "pges" && min_compatible.get() == 2),
        "{error}"
    );

    pool.close().await;
    db.cleanup().await
}

#[tokio::test]
async fn open_detects_edited_migrations() -> Result<()> {
    let f = Fixture::new().await?;
    sqlx::query("UPDATE pges.migrations SET checksum = '\\x01' WHERE scope = 'es'")
        .execute(&f.pool)
        .await?;

    let error = store::Store::open(f.pool.clone(), store::StoreConfig::new(common::STORE_NAME))
        .await
        .unwrap_err();

    assert!(
        matches!(&error, store::OpenError::ChecksumMismatch { scope, version } if scope == "es" && version.get() == 1),
        "{error}"
    );
    f.finish().await
}

/// No public option creates a DCB store; one created by another build is refused.
#[tokio::test]
async fn stores_that_need_dcb_are_refused() -> Result<()> {
    let db = pges_testing::TestDb::new().await?;
    let mut conn = db.connect().await?;
    pges_testing::create_store(
        &mut conn,
        &pges_core::store_name!("tagged"),
        &pges_testing::definition(pges_postgres::store::Outbox::Disabled),
    )
    .await?;
    let mut tx = conn.begin().await?;
    sqlx::query("SET LOCAL search_path TO tagged")
        .execute(&mut *tx)
        .await?;
    sqlx::raw_sql(include_str!("../migrations/store/opt_dcb.sql"))
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    let pool = db.pool().await?;

    let error = store::Store::open(pool.clone(), store::StoreConfig::new(store_name!("tagged")))
        .await
        .unwrap_err();

    assert!(
        matches!(error, store::OpenError::StoreRequiresFeature("dcb")),
        "{error}"
    );
    conn.close().await?;
    pool.close().await;
    db.cleanup().await
}

/// Event type registration is idempotent and committed outside the caller's transaction.
#[tokio::test]
async fn event_types_are_registered_once_outside_the_callers_transaction() -> Result<()> {
    let f = Fixture::new().await?;
    f.store.register::<JobEvent>().await?;
    f.store.register::<JobEvent>().await?;

    let mut uow = f.store.begin().await?;
    uow.append(
        stream::StreamId::new_v7(),
        version::ExpectedVersion::NoStream,
        &[OrderEvent::Placed(Placed { total: 1 })],
    )
    .await?;
    uow.rollback().await?;

    let names: Vec<String> = sqlx::query_scalar("SELECT name FROM es.event_types ORDER BY name")
        .fetch_all(&f.pool)
        .await?;
    assert_eq!(names, ["job.assigned", "job.created", "order.placed"]);
    f.finish().await
}

#[tokio::test]
async fn categories_registered_after_open_are_found() -> Result<()> {
    struct Invoice;
    const INVOICES: category::CategoryName = category!("invoices");
    impl pges_core::event::Event for Invoice {
        const CATEGORY: category::CategoryName = INVOICES;
        const TYPES: &'static [&'static str] = &["invoice.sent"];
        fn type_name(&self) -> &'static str {
            "invoice.sent"
        }
        fn schema_version(&self) -> pges_core::event::SchemaVersion {
            pges_core::event::SchemaVersion::FIRST
        }
        fn encode(&self, out: &mut Vec<u8>) -> Result<(), pges_core::event::CodecError> {
            out.extend_from_slice(b"{}");
            Ok(())
        }
        fn decode(
            _: &str,
            _: pges_core::event::SchemaVersion,
            _: &[u8],
        ) -> Result<pges_core::event::Decoded<Self>, pges_core::event::CodecError> {
            Ok(pges_core::event::Decoded::Event(Self))
        }
    }

    let f = Fixture::new().await?;
    assert!(f.store.register::<Invoice>().await.is_err());

    f.store
        .admin()
        .register_category(INVOICES, admin::CategoryConfig::default())
        .await?;
    f.store
        .append(
            stream::StreamId::new_v7(),
            version::ExpectedVersion::NoStream,
            &[Invoice],
        )
        .await?;
    f.finish().await
}

#[tokio::test]
async fn categories_get_partitions_ahead_and_maintenance_is_idempotent() -> Result<()> {
    let f = Fixture::new().await?;
    let admin = f.store.admin();
    let mut daily = admin::CategoryConfig::default();
    daily.bucket = admin::Bucket::Day;
    let parts = pges_core::category!("parts");

    admin.register_category(parts, daily).await?;
    admin.register_category(parts, daily).await?;
    admin.maintain().await?;
    admin.maintain().await?;

    let leaves = |parent: &'static str| {
        let pool = f.pool.clone();
        async move {
            sqlx::query_scalar::<_, String>(
                "SELECT c.relname::text FROM pg_inherits i JOIN pg_class c ON c.oid = i.inhrelid
                 WHERE i.inhparent = $1::regclass ORDER BY 1",
            )
            .bind(parent)
            .fetch_all(&pool)
            .await
        }
    };
    let daily_leaves = leaves("es.commits_parts").await?;
    let monthly_leaves = leaves("es.commits_jobs").await?;
    assert!(
        (4..=5).contains(&daily_leaves.len()),
        "current day (maybe yesterday) plus 3 ahead: {daily_leaves:?}"
    );
    assert!(
        daily_leaves
            .iter()
            .all(|l| l.len() == "commits_parts__yyyymmdd".len())
    );
    assert!(
        (4..=5).contains(&monthly_leaves.len()),
        "{monthly_leaves:?}"
    );
    assert_eq!(leaves("es.log").await?, ["log_0000"]);
    let constraints: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM pg_constraint WHERE conname = 'bound' AND connamespace = 'es'::regnamespace",
    )
    .fetch_one(&f.pool)
    .await?;
    assert_eq!(constraints, 0, "the attach constraints are dropped");
    f.finish().await
}

/// A second maintainer waits for the first instead of failing on `lock_timeout`.
#[tokio::test]
async fn maintenance_waits_for_another_maintainer() -> Result<()> {
    let f = Fixture::new().await?;
    let mut holder = f.pool.acquire().await?;
    sqlx::query("SELECT pg_advisory_lock(hashtext('pges.maintain.' || $1))")
        .bind(common::STORE)
        .execute(&mut *holder)
        .await?;

    let maintain = {
        let admin = f.store.admin();
        tokio::spawn(async move { admin.maintain().await })
    };
    tokio::time::sleep(std::time::Duration::from_millis(1500)).await;
    sqlx::query("SELECT pg_advisory_unlock(hashtext('pges.maintain.' || $1))")
        .bind(common::STORE)
        .execute(&mut *holder)
        .await?;

    maintain.await??;
    drop(holder);
    f.finish().await
}

/// Maintenance of leaves that already exist runs no DDL on them, so a reader holding a lock on the current
/// leaf does not block it.
#[tokio::test]
async fn maintenance_leaves_existing_leaves_alone() -> Result<()> {
    let f = Fixture::new().await?;
    let admin = f.store.admin();
    admin.maintain().await?;

    let current: String = sqlx::query_scalar(
        "SELECT 'es.commits_jobs__' || to_char(now() AT TIME ZONE 'UTC', 'YYYYMM')",
    )
    .fetch_one(&f.pool)
    .await?;
    let mut reader = f.pool.acquire().await?;
    let mut tx = reader.begin().await?;
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "SELECT count(*) FROM {current}"
    )))
    .execute(&mut *tx)
    .await?;

    tokio::time::timeout(std::time::Duration::from_secs(5), admin.maintain()).await??;

    tx.rollback().await?;
    drop(reader);
    f.finish().await
}

const PARTS: category::CategoryName = category!("parts");

/// An event of the `parts` category, registered by the tests themselves.
struct PartMade;

impl pges_core::event::Event for PartMade {
    const CATEGORY: category::CategoryName = PARTS;
    const TYPES: &'static [&'static str] = &["part.made"];

    fn type_name(&self) -> &'static str {
        "part.made"
    }

    fn schema_version(&self) -> pges_core::event::SchemaVersion {
        pges_core::event::SchemaVersion::FIRST
    }

    fn encode(&self, out: &mut Vec<u8>) -> Result<(), pges_core::event::CodecError> {
        out.extend_from_slice(b"{}");
        Ok(())
    }

    fn decode(
        _: &str,
        _: pges_core::event::SchemaVersion,
        _: &[u8],
    ) -> Result<pges_core::event::Decoded<Self>, pges_core::event::CodecError> {
        Ok(pges_core::event::Decoded::Event(Self))
    }
}

/// A leaf of a category partition and its bounds, in seconds since the epoch.
#[derive(Debug)]
struct Leaf {
    name: String,
    lower: i64,
    upper: i64,
}

async fn leaves_of(pool: &sqlx::PgPool, parent: &str) -> Result<Vec<Leaf>> {
    let rows: Vec<(String, i64, i64)> = sqlx::query_as(
        "SELECT c.relname::text,
                extract(epoch FROM uuid_extract_timestamp(substring(pg_get_expr(c.relpartbound, c.oid)
                        FROM 'FROM \\(''([0-9a-f-]+)''\\)')::uuid))::int8,
                extract(epoch FROM uuid_extract_timestamp(substring(pg_get_expr(c.relpartbound, c.oid)
                        FROM 'TO \\(''([0-9a-f-]+)''\\)')::uuid))::int8
         FROM pg_inherits i JOIN pg_class c ON c.oid = i.inhrelid
         WHERE i.inhparent = $1::regclass
         ORDER BY 2",
    )
    .bind(parent)
    .fetch_all(pool)
    .await?;

    Ok(rows
        .into_iter()
        .map(|(name, lower, upper)| Leaf { name, lower, upper })
        .collect())
}

/// The category row and its partitions are committed together. While the attach of the category partition
/// waits for a lock, no store sees the category: an append fails with `UnknownCategory`, not with a database
/// error about a missing partition.
#[tokio::test]
async fn categories_appear_only_with_their_partitions() -> Result<()> {
    let f = Fixture::new().await?;
    let mut holder = f.pool.acquire().await?;
    let mut tx = holder.begin().await?;
    // Blocks ATTACH PARTITION on commits, not CREATE TABLE … (LIKE commits).
    sqlx::query("LOCK TABLE es.commits IN SHARE UPDATE EXCLUSIVE MODE")
        .execute(&mut *tx)
        .await?;

    let register = {
        let admin = f.store.admin();
        tokio::spawn(async move {
            admin
                .register_category(PARTS, admin::CategoryConfig::default())
                .await
        })
    };
    // Longer than lock_timeout, so the registration has timed out at least once and is retrying.
    tokio::time::sleep(std::time::Duration::from_millis(1500)).await;

    let visible: bool =
        sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM es.categories WHERE name = 'parts')")
            .fetch_one(&f.pool)
            .await?;
    assert!(!visible, "the category is visible before its partitions");
    let other =
        store::Store::open(f.pool.clone(), store::StoreConfig::new(common::STORE_NAME)).await?;
    let id = stream::StreamId::new_v7();
    let error = other
        .append(id, version::ExpectedVersion::NoStream, &[PartMade])
        .await
        .unwrap_err();
    assert!(
        matches!(error, pges_postgres::append::AppendError::UnknownCategory(c) if c == PARTS),
        "{error}"
    );

    tx.rollback().await?;
    drop(holder);
    register.await??;
    other
        .append(id, version::ExpectedVersion::NoStream, &[PartMade])
        .await?;
    f.finish().await
}

/// The registration's connection is terminated while its transaction waits for a lock, so the rollback fails
/// too. The error says why the registration failed (57P01), not why the rollback did.
#[tokio::test]
async fn a_failed_rollback_keeps_the_registration_error() -> Result<()> {
    let f = Fixture::new().await?;
    let mut holder = f.pool.acquire().await?;
    let mut tx = holder.begin().await?;
    sqlx::query("LOCK TABLE es.commits IN SHARE UPDATE EXCLUSIVE MODE")
        .execute(&mut *tx)
        .await?;

    let register = {
        let admin = f.store.admin();
        tokio::spawn(async move {
            admin
                .register_category(PARTS, admin::CategoryConfig::default())
                .await
        })
    };
    let waiting = loop {
        let pid: Option<i32> = sqlx::query_scalar(
            "SELECT pid FROM pg_stat_activity
             WHERE datname = current_database() AND wait_event_type = 'Lock'
               AND state = 'active' AND query LIKE '%ATTACH PARTITION%'",
        )
        .fetch_optional(&f.pool)
        .await?;
        if let Some(pid) = pid {
            break pid;
        }
        assert!(
            !register.is_finished(),
            "the registration ended before it was seen waiting"
        );
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    };
    let terminated: bool = sqlx::query_scalar("SELECT pg_terminate_backend($1)")
        .bind(waiting)
        .fetch_one(&f.pool)
        .await?;
    assert!(terminated);

    let error = register.await?.unwrap_err();
    assert!(
        matches!(&error, admin::RegisterCategoryError::Database(db) if db.code().as_deref() == Some("57P01")),
        "{error:?}"
    );

    tx.rollback().await?;
    drop(holder);
    f.finish().await
}

/// Every index the crate creates on a leaf keeps its full name for the longest category with the longest
/// (day) suffix: PostgreSQL would silently truncate a name over 63 bytes.
#[tokio::test]
async fn leaf_names_fit_for_the_longest_category() -> Result<()> {
    let f = Fixture::new().await?;
    let longest = category::CategoryName::leak("c".repeat(40)).unwrap();
    let mut daily = admin::CategoryConfig::default();
    daily.bucket = admin::Bucket::Day;
    f.store.admin().register_category(longest, daily).await?;

    let leaves = leaves_of(&f.pool, &format!("es.commits_{longest}")).await?;
    assert!(!leaves.is_empty());
    for leaf in &leaves {
        assert_eq!(leaf.name.len(), "commits_".len() + 40 + "__yyyymmdd".len());
        let mut indexes: Vec<String> = sqlx::query_scalar(
            "SELECT c.relname::text FROM pg_index x JOIN pg_class c ON c.oid = x.indexrelid
             WHERE x.indrelid = $1::regclass ORDER BY 1",
        )
        .bind(format!("es.{}", leaf.name))
        .fetch_all(&f.pool)
        .await?;
        indexes.sort();
        assert_eq!(
            indexes,
            [format!("{}_pkey", leaf.name), format!("{}_s", leaf.name)],
        );
    }
    f.finish().await
}

/// A bucket change applies from the upper bound of the newest leaf: leaves never overlap or leave a gap, the
/// first leaf of the new bucket bridges to its next bound, and appends keep working.
#[tokio::test]
async fn bucket_changes_apply_after_the_newest_leaf() -> Result<()> {
    let f = Fixture::new().await?;
    let admin = f.store.admin();
    let mut daily = admin::CategoryConfig::default();
    daily.bucket = admin::Bucket::Day;
    admin.register_category(PARTS, daily).await?;
    let days = leaves_of(&f.pool, "es.commits_parts").await?;
    let id = stream::StreamId::new_v7();
    f.store
        .append(id, version::ExpectedVersion::NoStream, &[PartMade])
        .await?;

    admin.change_bucket(PARTS, admin::Bucket::Month).await?;
    admin.maintain().await?;
    admin.maintain().await?;

    let leaves = leaves_of(&f.pool, "es.commits_parts").await?;
    assert_eq!(
        leaves[..days.len()]
            .iter()
            .map(|l| &l.name)
            .collect::<Vec<_>>(),
        days.iter().map(|l| &l.name).collect::<Vec<_>>(),
        "the day leaves are kept"
    );
    for pair in leaves.windows(2) {
        assert_eq!(
            pair[0].upper, pair[1].lower,
            "no overlap, no gap: {leaves:?}"
        );
    }
    let month_ahead: i64 = sqlx::query_scalar(
        "SELECT extract(epoch FROM date_trunc('month', now() AT TIME ZONE 'UTC') AT TIME ZONE 'UTC'
                                   + 4 * interval '1 month')::int8",
    )
    .fetch_one(&f.pool)
    .await?;
    assert_eq!(leaves.last().map(|l| l.upper), Some(month_ahead));
    let added = &leaves[days.len()..];
    assert!(added.len() >= 3, "{leaves:?}");
    assert!(
        added
            .iter()
            .skip(1)
            .all(|l| l.name.len() == "commits_parts__yyyymm".len()),
        "after the bridge, one leaf per month: {added:?}"
    );

    f.store
        .append(
            id,
            version::ExpectedVersion::Exact(common::v(1)),
            &[PartMade],
        )
        .await?;

    admin.change_bucket(PARTS, admin::Bucket::Day).await?;
    admin.maintain().await?;
    let unchanged = leaves_of(&f.pool, "es.commits_parts").await?;
    assert_eq!(unchanged.len(), leaves.len(), "months ahead already exist");

    let error = admin
        .change_bucket(pges_core::category!("missing"), admin::Bucket::Day)
        .await
        .unwrap_err();
    assert!(
        matches!(error, admin::ChangeBucketError::UnknownCategory(_)),
        "{error}"
    );
    f.finish().await
}

/// Store names go into SQL unquoted, so `StoreName` refuses exactly the keywords that cannot name a schema:
/// the server's reserved ones and those that cannot name a type or function (`pg_get_keywords()` with
/// `catcode` `R` or `T`). A PostgreSQL release that adds, removes or recategorizes such a keyword fails this
/// test, and the list in `pges_core::store` must follow.
#[tokio::test]
async fn store_names_refuse_the_servers_reserved_keywords() -> Result<()> {
    let db = pges_testing::TestDb::new().await?;
    let mut conn = db.connect().await?;
    let keywords: Vec<(String, String)> =
        sqlx::query_as("SELECT word, catcode::text FROM pg_get_keywords() ORDER BY word")
            .fetch_all(&mut conn)
            .await?;

    let mut mismatched = Vec::new();
    let mut reserved = 0;
    for (word, catcode) in &keywords {
        let cannot_name_a_schema = matches!(catcode.as_str(), "R" | "T");
        reserved += usize::from(cannot_name_a_schema);
        let refused = pges_core::store::StoreName::parse(word.as_str())
            == Err(pges_core::store::StoreNameError::Keyword);
        if refused != cannot_name_a_schema {
            mismatched.push(format!("{word} ({catcode})"));
        }
    }
    assert!(
        mismatched.is_empty(),
        "StoreName and the server disagree on: {mismatched:?}"
    );
    // The length of pges_core::store's keyword list: a keyword the server dropped would still be refused.
    assert_eq!(reserved, 101, "the server has {reserved} R and T keywords");

    conn.close().await?;
    db.cleanup().await
}
