//! A store's writer limits: recorded at creation, enforced on writers at open, changed through `Admin`, and
//! the defaults the database and `pges-core` share.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::time::Duration;

use anyhow::Result;
use sqlx::postgres::types::PgInterval;

use pges_core::{interval, store_name};
use pges_postgres::{admin, limit, store};

/// A store named `lim` created with a skew of 30 s and a `max_txn_age` of 10 minutes.
fn lim() -> store::StoreConfig {
    let mut definition = store::StoreDefinition::default();
    definition.max_clock_skew = Duration::from_secs(30);
    definition.max_txn_age = interval!(10 min);
    let mut config = store::StoreConfig::new(store_name!("lim"));
    config.max_clock_skew = Some(Duration::from_secs(30));
    config.migrate = store::AutoMigrate::CreateOrMigrate(definition);
    config
}

#[tokio::test]
async fn a_store_records_its_writer_limits_and_refuses_wider_writers() -> Result<()> {
    let db = pges_testing::TestDb::new().await?;
    let pool = db.pool().await?;

    store::Store::open(pool.clone(), lim()).await?;
    assert_eq!(
        limits(&pool, "lim").await?,
        (Duration::from_secs(30), Duration::from_secs(600))
    );

    // A narrower writer is fine; a wider one is refused until the store's skew is raised.
    let mut narrower = store::StoreConfig::new(store_name!("lim"));
    narrower.max_clock_skew = Some(Duration::from_secs(1));
    store::Store::open(pool.clone(), narrower).await?;
    let mut wider = store::StoreConfig::new(store_name!("lim"));
    wider.max_clock_skew = Some(Duration::from_secs(31));
    let error = store::Store::open(pool.clone(), wider.clone())
        .await
        .unwrap_err();
    assert!(
        matches!(
            error,
            store::OpenError::ClockSkewAboveStore { configured, store }
                if configured == Duration::from_secs(31) && store == Duration::from_secs(30)
        ),
        "{error:?}"
    );

    let admin = admin::Admin::new(pool.clone(), store_name!("lim"));
    admin
        .set_writer_limits(limit::WriterLimits::new(
            Duration::from_secs(31),
            interval!(10 min),
        ))
        .await?;
    store::Store::open(pool.clone(), wider).await?;
    assert_eq!(
        limits(&pool, "lim").await?,
        (Duration::from_secs(31), Duration::from_secs(600))
    );

    let error = admin::Admin::new(pool.clone(), store_name!("nostore"))
        .set_writer_limits(limit::WriterLimits::default())
        .await
        .unwrap_err();
    assert!(
        matches!(error, limit::SetWriterLimitsError::StoreNotFound(ref s) if s.as_str() == "nostore"),
        "{error:?}"
    );

    pool.close().await;
    db.cleanup().await
}

/// The column defaults of `pges.stores` are `pges-core`'s constants, which `StoreDefinition` and
/// `WriterLimits` default to.
#[tokio::test]
async fn the_default_limits_agree_with_the_database() -> Result<()> {
    let db = pges_testing::TestDb::new().await?;
    let pool = db.pool().await?;

    let mut tx = pool.begin().await?;
    let (skew, age): (PgInterval, PgInterval) = sqlx::query_as(
        "INSERT INTO pges.stores (schema_name) VALUES ('defaults')
         RETURNING max_clock_skew, max_txn_age",
    )
    .fetch_one(&mut *tx)
    .await?;
    tx.rollback().await?;
    assert_eq!(
        (micros(skew), micros(age)),
        (
            pges_core::store::DEFAULT_MAX_CLOCK_SKEW,
            pges_core::store::DEFAULT_MAX_TXN_AGE
        )
    );
    let definition = store::StoreDefinition::default();
    assert_eq!(
        (definition.max_clock_skew, definition.max_txn_age.get()),
        (
            pges_core::store::DEFAULT_MAX_CLOCK_SKEW,
            pges_core::store::DEFAULT_MAX_TXN_AGE
        )
    );
    assert_eq!(
        store::StoreConfig::new(store_name!("defaults")).max_clock_skew,
        None
    );
    let limits = limit::WriterLimits::default();
    assert_eq!(
        (limits.max_clock_skew, limits.max_txn_age),
        (definition.max_clock_skew, definition.max_txn_age)
    );

    pool.close().await;
    db.cleanup().await
}

/// Neither limit goes down: commits written under the larger one stay in the log, and an open writer may
/// still use it.
#[tokio::test]
async fn writer_limits_are_only_raised() -> Result<()> {
    let db = pges_testing::TestDb::new().await?;
    let pool = db.pool().await?;
    let writer = store::Store::open(pool.clone(), lim()).await?;
    let admin = admin::Admin::new(pool.clone(), store_name!("lim"));
    let recorded = (Duration::from_secs(30), Duration::from_secs(600));

    for (skew, age) in [(29, 600), (30, 599), (1, 1)] {
        let lower = limit::WriterLimits::new(
            Duration::from_secs(skew),
            interval::Interval::from_secs(age).unwrap(),
        );
        let error = admin.set_writer_limits(lower).await.unwrap_err();
        assert!(
            matches!(
                error,
                limit::SetWriterLimitsError::Lowering { max_clock_skew, max_txn_age }
                    if (max_clock_skew, max_txn_age) == recorded
            ),
            "{error:?}"
        );
        assert_eq!(limits(&pool, "lim").await?, recorded);
    }

    admin
        .set_writer_limits(limit::WriterLimits::new(recorded.0, interval!(10 min)))
        .await?;
    admin
        .set_writer_limits(limit::WriterLimits::new(
            Duration::from_secs(30),
            interval!(1 h),
        ))
        .await?;
    assert_eq!(
        limits(&pool, "lim").await?,
        (Duration::from_secs(30), Duration::from_secs(3600))
    );

    drop(writer);
    pool.close().await;
    db.cleanup().await
}

async fn limits(pool: &sqlx::PgPool, store: &str) -> Result<(Duration, Duration)> {
    let (skew, age): (PgInterval, PgInterval) = sqlx::query_as(
        "SELECT max_clock_skew, max_txn_age FROM pges.stores WHERE schema_name = $1",
    )
    .bind(store)
    .fetch_one(pool)
    .await?;
    Ok((micros(skew), micros(age)))
}

fn micros(interval: PgInterval) -> Duration {
    assert_eq!((interval.months, interval.days), (0, 0), "{interval:?}");
    Duration::from_micros(u64::try_from(interval.microseconds).unwrap())
}
