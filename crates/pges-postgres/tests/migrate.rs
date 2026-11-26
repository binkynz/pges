//! The migration runner: idempotent, concurrent-safe, checksummed, and refusing incompatible databases.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::num::NonZeroU16;

use anyhow::Result;
use sqlx::Connection;

use pges_core::{store as core_store, store_name};
use pges_postgres::{migrate, store};

fn definition(outbox: store::Outbox) -> store::StoreDefinition {
    let mut definition = store::StoreDefinition::default();
    definition.stream_partitions = NonZeroU16::new(4).unwrap();
    definition.outbox = outbox;
    definition
}

/// Runs the migrations for a store named `name` without the outbox.
async fn run(
    conn: &mut sqlx::PgConnection,
    name: core_store::StoreName,
) -> Result<(), migrate::MigrateError> {
    migrate::run(conn, &name, &definition(store::Outbox::Disabled)).await
}

async fn run_with_outbox(
    conn: &mut sqlx::PgConnection,
    name: core_store::StoreName,
    outbox: store::Outbox,
) -> Result<(), migrate::MigrateError> {
    migrate::run(conn, &name, &definition(outbox)).await
}

#[tokio::test]
async fn creates_a_store_once_and_records_checksums() -> Result<()> {
    let db = pges_testing::TestDb::new().await?;
    let mut conn = db.connect().await?;

    run(&mut conn, store_name!("m1")).await?;
    run(&mut conn, store_name!("m1")).await?;

    let rows: Vec<(String, i32, Vec<u8>)> = sqlx::query_as(
        "SELECT scope, version, checksum FROM pges.migrations ORDER BY scope, version",
    )
    .fetch_all(&mut conn)
    .await?;
    assert_eq!(
        rows,
        vec![
            ("m1".into(), 1, migrate::STORE[0].checksum().to_vec()),
            ("pges".into(), 1, migrate::SHARED[0].checksum().to_vec()),
        ]
    );
    let partitions: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM pg_inherits WHERE inhparent = 'm1.streams'::regclass",
    )
    .fetch_one(&mut conn)
    .await?;
    assert_eq!(partitions, 4);
    let tuned: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM pg_inherits i JOIN pg_class c ON c.oid = i.inhrelid
         WHERE i.inhparent = 'm1.streams'::regclass
           AND c.reloptions @> ARRAY['autovacuum_vacuum_scale_factor=0.02',
                                     'autovacuum_vacuum_insert_scale_factor=0.05']",
    )
    .fetch_one(&mut conn)
    .await?;
    assert_eq!(
        tuned, 4,
        "every streams partition gets the autovacuum settings"
    );
    let scope: (i32, i32) =
        sqlx::query_as("SELECT schema_version, min_compatible FROM pges.scopes WHERE scope = 'm1'")
            .fetch_one(&mut conn)
            .await?;
    assert_eq!(scope, (1, 1));

    conn.close().await?;
    db.cleanup().await
}

#[tokio::test]
async fn concurrent_runners_create_the_store_once() -> Result<()> {
    let db = pges_testing::TestDb::new().await?;
    let mut runners = Vec::new();
    for _ in 0..4 {
        let mut conn = db.connect().await?;
        runners.push(tokio::spawn(async move {
            run(&mut conn, store_name!("m2")).await
        }));
    }
    for runner in runners {
        runner.await??;
    }

    let mut conn = db.connect().await?;
    let stores: i64 =
        sqlx::query_scalar("SELECT count(*) FROM pges.stores WHERE schema_name = 'm2'")
            .fetch_one(&mut conn)
            .await?;
    assert_eq!(stores, 1);

    conn.close().await?;
    db.cleanup().await
}

#[tokio::test]
async fn creation_options_set_the_store_flags() -> Result<()> {
    let db = pges_testing::TestDb::new().await?;
    let mut conn = db.connect().await?;
    run_with_outbox(&mut conn, store_name!("m3"), store::Outbox::Enabled).await?;

    let (outbox, dcb): (bool, bool) =
        sqlx::query_as("SELECT outbox, dcb FROM pges.stores WHERE schema_name = 'm3'")
            .fetch_one(&mut conn)
            .await?;
    assert!(outbox);
    assert!(!dcb);
    let pending: bool = sqlx::query_scalar("SELECT to_regclass('m3.pending') IS NOT NULL")
        .fetch_one(&mut conn)
        .await?;
    assert!(pending);

    conn.close().await?;
    db.cleanup().await
}

#[tokio::test]
async fn an_edited_migration_is_refused() -> Result<()> {
    let db = pges_testing::TestDb::new().await?;
    let mut conn = db.connect().await?;
    run(&mut conn, store_name!("m4")).await?;
    sqlx::query("UPDATE pges.migrations SET checksum = '\\x00' WHERE scope = 'm4'")
        .execute(&mut conn)
        .await?;

    let error = run(&mut conn, store_name!("m4")).await.unwrap_err();

    assert!(
        matches!(&error, migrate::MigrateError::ChecksumMismatch { scope, version, .. } if scope == "m4" && version.get() == 1),
        "{error}"
    );
    conn.close().await?;
    db.cleanup().await
}

#[tokio::test]
async fn a_database_beyond_the_compatibility_window_is_refused() -> Result<()> {
    let db = pges_testing::TestDb::new().await?;
    let mut conn = db.connect().await?;
    run(&mut conn, store_name!("m5")).await?;
    sqlx::query("UPDATE pges.scopes SET schema_version = 9, min_compatible = 7 WHERE scope = 'm5'")
        .execute(&mut conn)
        .await?;

    let error = run(&mut conn, store_name!("m5")).await.unwrap_err();

    assert!(
        matches!(
            &error,
            migrate::MigrateError::CrateTooOld {
                min_compatible,
                crate_version,
                ..
            } if min_compatible.get() == 7 && crate_version.get() == 1
        ),
        "{error}"
    );
    conn.close().await?;
    db.cleanup().await
}

async fn store_names(conn: &mut sqlx::PgConnection) -> Result<Vec<String>> {
    Ok(
        sqlx::query_scalar("SELECT schema_name FROM pges.stores ORDER BY schema_name")
            .fetch_all(conn)
            .await?,
    )
}

/// A database holds stores of one kind: with the outbox or without. Creating a store of the other kind fails
/// with a typed error and creates nothing, in both directions; more stores of the same kind are fine.
#[tokio::test]
async fn stores_of_both_outbox_kinds_are_refused_in_one_database() -> Result<()> {
    for (first, other) in [
        (store::Outbox::Disabled, store::Outbox::Enabled),
        (store::Outbox::Enabled, store::Outbox::Disabled),
    ] {
        let db = pges_testing::TestDb::new().await?;
        let mut conn = db.connect().await?;
        run_with_outbox(&mut conn, store_name!("k1"), first).await?;
        run_with_outbox(&mut conn, store_name!("k2"), first).await?;

        let error = run_with_outbox(&mut conn, store_name!("k3"), other)
            .await
            .unwrap_err();

        assert!(
            matches!(&error, migrate::MigrateError::OutboxMismatch { store, outbox } if store.as_str() == "k3" && *outbox == other),
            "{error}"
        );
        assert_eq!(store_names(&mut conn).await?, ["k1", "k2"]);
        let schema: bool = sqlx::query_scalar("SELECT to_regnamespace('k3') IS NOT NULL")
            .fetch_one(&mut conn)
            .await?;
        assert!(!schema, "the store's schema was rolled back");

        conn.close().await?;
        db.cleanup().await?;
    }
    Ok(())
}

/// The check lives in the database: auto-migrating `Store::open` and pges-testing's `create_store` refuse a
/// mixed database too, and so does changing the flag by hand.
#[tokio::test]
async fn every_way_of_creating_a_store_refuses_mixed_kinds() -> Result<()> {
    let db = pges_testing::TestDb::new().await?;
    let mut conn = db.connect().await?;
    pges_testing::create_store(
        &mut conn,
        &pges_core::store_name!("k1"),
        &pges_testing::definition(pges_postgres::store::Outbox::Enabled),
    )
    .await?;

    let error = pges_testing::create_store(
        &mut conn,
        &pges_core::store_name!("k2"),
        &pges_testing::definition(pges_postgres::store::Outbox::Disabled),
    )
    .await
    .unwrap_err();
    assert!(
        matches!(
            error.downcast_ref::<migrate::MigrateError>(),
            Some(migrate::MigrateError::OutboxMismatch { .. })
        ),
        "{error:?}"
    );

    let pool = db.pool().await?;
    let mut auto = store::StoreConfig::new(store_name!("k3"));
    auto.migrate = store::AutoMigrate::CreateOrMigrate(definition(store::Outbox::Disabled));
    let error = store::Store::open(pool.clone(), auto).await.unwrap_err();
    assert!(
        matches!(
            error,
            store::OpenError::Migrate(migrate::MigrateError::OutboxMismatch { .. })
        ),
        "{error}"
    );

    run_with_outbox(&mut conn, store_name!("k4"), store::Outbox::Enabled).await?;
    let error = sqlx::query("UPDATE pges.stores SET outbox = false WHERE schema_name = 'k4'")
        .execute(&mut conn)
        .await
        .unwrap_err();
    assert_eq!(
        error.as_database_error().and_then(|e| e.code()).as_deref(),
        Some("PGE02")
    );
    assert_eq!(store_names(&mut conn).await?, ["k1", "k4"]);

    pool.close().await;
    conn.close().await?;
    db.cleanup().await
}

/// Two transactions add stores of different kinds without the runner. The check waits for the first to
/// commit, then refuses the second.
#[tokio::test]
async fn concurrent_stores_of_different_kinds_are_refused() -> Result<()> {
    let db = pges_testing::TestDb::new().await?;
    let mut first = db.connect().await?;
    let mut second = db.connect().await?;
    let insert = "INSERT INTO pges.stores (schema_name, outbox) VALUES ($1, $2)";

    let mut tx = first.begin().await?;
    sqlx::query(insert)
        .bind("k1")
        .bind(true)
        .execute(&mut *tx)
        .await?;
    let refused = tokio::spawn(async move {
        let result = sqlx::query(insert)
            .bind("k2")
            .bind(false)
            .execute(&mut second)
            .await;
        (result.map(|_| ()), second)
    });
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    assert!(
        !refused.is_finished(),
        "the second insert waits for the first transaction"
    );
    tx.commit().await?;

    let (result, second) = refused.await?;
    let error = result.unwrap_err();
    assert_eq!(
        error.as_database_error().and_then(|e| e.code()).as_deref(),
        Some("PGE02")
    );
    assert_eq!(store_names(&mut first).await?, ["k1"]);

    second.close().await?;
    first.close().await?;
    db.cleanup().await
}
