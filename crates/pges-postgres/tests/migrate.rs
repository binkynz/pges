//! The migration runner: idempotent, concurrent-safe, checksummed, and refusing incompatible databases.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::num::NonZeroU16;

use anyhow::Result;
use sqlx::{Connection, Executor};

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
    migrate::run(
        conn,
        &name,
        &definition(store::Outbox::Disabled),
        &migrate::MigrateConfig::default(),
    )
    .await
}

async fn run_with_outbox(
    conn: &mut sqlx::PgConnection,
    name: core_store::StoreName,
    outbox: store::Outbox,
) -> Result<(), migrate::MigrateError> {
    migrate::run(
        conn,
        &name,
        &definition(outbox),
        &migrate::MigrateConfig::default(),
    )
    .await
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

fn waiting(lock_wait: pges_core::interval::Interval) -> migrate::MigrateConfig {
    let mut config = migrate::MigrateConfig::default();
    config.lock_wait = lock_wait;
    config
}

/// The migration lock is a row only roles with `UPDATE` on `pges.scopes` can lock. A role that can only
/// connect, holding the advisory key the lock used to be, blocks neither store creation nor the
/// one-source-kind trigger, and a role that reads `pges.scopes` cannot lock the row. Before, both waited
/// forever.
#[tokio::test]
async fn a_role_that_can_only_connect_cannot_block_migrations() -> Result<()> {
    let db = pges_testing::TestDb::new().await?;
    let role = format!("{}_squatter", db.name());
    let mut squatter = db.connect().await?;
    squatter
        .execute(sqlx::AssertSqlSafe(format!(
            "CREATE ROLE {role};
             GRANT USAGE ON SCHEMA pges TO {role};
             GRANT SELECT ON pges.scopes TO {role};
             SET ROLE {role};
             SELECT pg_advisory_lock(hashtext('pges.migrate'));"
        )))
        .await?;
    let error = sqlx::query("SELECT FROM pges.scopes WHERE scope = 'pges' FOR UPDATE")
        .execute(&mut squatter)
        .await
        .unwrap_err();
    assert_eq!(
        error.as_database_error().and_then(|e| e.code()).as_deref(),
        Some("42501")
    );

    let mut conn = db.connect().await?;
    tokio::time::timeout(
        std::time::Duration::from_secs(10),
        run(&mut conn, store_name!("free")),
    )
    .await
    .expect("migrations do not wait for the advisory key")?;
    assert_eq!(store_names(&mut conn).await?, ["free"]);

    squatter.close().await?;
    conn.execute(sqlx::AssertSqlSafe(format!(
        "DROP OWNED BY {role}; DROP ROLE {role}"
    )))
    .await?;
    conn.close().await?;
    db.cleanup().await
}

/// A runner waits for the migration lock while another session holds it, up to its lock wait, then fails
/// with `LockTimeout` naming that session's pid and role and creates nothing. Once the lock is free it
/// creates the store, and a runner whose wait is longer than the hold waits for it.
#[tokio::test]
async fn a_runner_gives_up_waiting_for_the_migration_lock_and_names_the_holder() -> Result<()> {
    let db = pges_testing::TestDb::new().await?;
    let mut holder = db.connect().await?;
    let mut tx = holder.begin().await?;
    let (pid, role): (i32, String) = sqlx::query_as(
        "SELECT pg_backend_pid(), session_user::text
         FROM pges.scopes WHERE scope = 'pges' FOR UPDATE",
    )
    .fetch_one(&mut *tx)
    .await?;

    let mut conn = db.connect().await?;
    let started = std::time::Instant::now();
    let error = migrate::run(
        &mut conn,
        &store_name!("late"),
        &definition(store::Outbox::Disabled),
        &waiting(pges_core::interval!(300 ms)),
    )
    .await
    .unwrap_err();
    let migrate::MigrateError::LockTimeout(timeout) = &error else {
        panic!("{error}");
    };
    assert!(started.elapsed() >= std::time::Duration::from_millis(300));
    assert_eq!(timeout.lock, pges_postgres::lock::Lock::Migration);
    let found = timeout.holder.as_ref().expect("the holder is visible");
    assert_eq!(found.pid.get(), pid);
    assert_eq!(found.role.as_deref(), Some(role.as_str()));
    assert!(store_names(&mut conn).await?.is_empty());

    let patient = tokio::spawn(async move {
        let result = migrate::run(
            &mut conn,
            &store_name!("late"),
            &definition(store::Outbox::Disabled),
            &waiting(pges_core::interval!(30 s)),
        )
        .await;
        (result, conn)
    });
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    assert!(!patient.is_finished(), "the runner waits for the lock");
    tx.commit().await?;
    let (result, mut conn) = patient.await?;
    result?;
    assert_eq!(store_names(&mut conn).await?, ["late"]);

    holder.close().await?;
    conn.close().await?;
    db.cleanup().await
}

/// Before the shared schema exists, an advisory lock stands for the migration lock: concurrent first runs
/// apply the shared migrations once, and a run whose wait runs out while another session holds the key fails
/// with `LockTimeout` naming it.
#[tokio::test]
async fn the_first_shared_migration_is_serialised_by_the_bootstrap_lock() -> Result<()> {
    let db = pges_testing::TestDb::new().await?;
    let mut conn = db.connect().await?;
    conn.execute("DROP PUBLICATION pges; DROP SCHEMA pges CASCADE")
        .await?;
    let mut holder = db.connect().await?;
    let pid: i32 = sqlx::query_scalar(
        "SELECT pg_backend_pid() FROM (SELECT pg_advisory_lock(hashtext('pges.migrate'))) AS locked",
    )
    .fetch_one(&mut holder)
    .await?;

    let error = migrate::run_shared(&mut conn, &waiting(pges_core::interval!(300 ms)))
        .await
        .unwrap_err();
    let migrate::MigrateError::LockTimeout(timeout) = &error else {
        panic!("{error}");
    };
    assert_eq!(timeout.holder.as_ref().map(|h| h.pid.get()), Some(pid));
    holder.close().await?;

    let mut runners = Vec::new();
    for _ in 0..4 {
        let mut conn = db.connect().await?;
        runners.push(tokio::spawn(async move {
            migrate::run_shared(&mut conn, &migrate::MigrateConfig::default()).await
        }));
    }
    for runner in runners {
        runner.await??;
    }
    let applied: i64 =
        sqlx::query_scalar("SELECT count(*) FROM pges.migrations WHERE scope = 'pges'")
            .fetch_one(&mut conn)
            .await?;
    assert_eq!(applied, 1);

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

/// `crate_too_old` reads the shared scope first, then one store or every store, each with its outbox scope,
/// which is compared with the crate's outbox migrations. Scopes of options this crate does not know are not
/// compared.
#[tokio::test]
async fn crate_too_old_names_the_first_scope_past_the_crate() -> Result<()> {
    let db = pges_testing::TestDb::new().await?;
    let mut conn = db.connect().await?;
    run(&mut conn, store_name!("m6")).await?;
    run(&mut conn, store_name!("m7")).await?;
    let (m6, m7) = (store_name!("m6"), store_name!("m7"));
    sqlx::query(
        "INSERT INTO pges.scopes (schema_version, min_compatible, scope) VALUES (9, 9, 'm6.other')",
    )
    .execute(&mut conn)
    .await?;
    assert_eq!(
        migrate::crate_too_old(&mut conn, migrate::Scopes::AllStores).await?,
        None
    );

    sqlx::query(
        "INSERT INTO pges.scopes (schema_version, min_compatible, scope) VALUES (9, 9, 'm6.outbox')",
    )
    .execute(&mut conn)
    .await?;
    let too_old = migrate::crate_too_old(&mut conn, migrate::Scopes::OneStore(&m6))
        .await?
        .unwrap();
    assert_eq!(
        (
            too_old.scope.as_str(),
            too_old.min_compatible.get(),
            too_old.crate_version
        ),
        ("m6.outbox", 9, migrate::latest_version(migrate::OUTBOX))
    );
    assert_eq!(
        migrate::crate_too_old(&mut conn, migrate::Scopes::OneStore(&m7)).await?,
        None
    );
    sqlx::query("DELETE FROM pges.scopes WHERE scope = 'm6.outbox'")
        .execute(&mut conn)
        .await?;

    sqlx::query("UPDATE pges.scopes SET schema_version = 9, min_compatible = 8 WHERE scope = 'm7'")
        .execute(&mut conn)
        .await?;
    assert_eq!(
        migrate::crate_too_old(&mut conn, migrate::Scopes::OneStore(&m6)).await?,
        None
    );
    let too_old = migrate::crate_too_old(&mut conn, migrate::Scopes::OneStore(&m7))
        .await?
        .unwrap();
    assert_eq!(
        (
            too_old.scope.as_str(),
            too_old.min_compatible.get(),
            too_old.crate_version
        ),
        ("m7", 8, migrate::latest_version(migrate::STORE))
    );
    assert_eq!(
        migrate::crate_too_old(&mut conn, migrate::Scopes::AllStores).await?,
        Some(too_old)
    );

    sqlx::query(
        "UPDATE pges.scopes SET schema_version = 9, min_compatible = 9 WHERE scope = 'pges'",
    )
    .execute(&mut conn)
    .await?;
    let too_old = migrate::crate_too_old(&mut conn, migrate::Scopes::OneStore(&m6))
        .await?
        .unwrap();
    assert_eq!(
        (too_old.scope.as_str(), too_old.min_compatible.get()),
        ("pges", 9)
    );
    assert_eq!(
        too_old.crate_version,
        migrate::latest_version(migrate::SHARED)
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

/// Hash partitions above 1000 widen to the digits of the highest remainder, for `streams` and `snapshots`.
/// With 3 digits, as before, `streams_p100` was created twice.
#[tokio::test]
async fn a_store_with_over_1000_stream_partitions_names_them_by_the_widest_remainder() -> Result<()>
{
    let db = pges_testing::TestDb::new().await?;
    let mut conn = db.connect().await?;
    let mut wide = definition(store::Outbox::Disabled);
    wide.stream_partitions = NonZeroU16::new(1001).unwrap();
    migrate::run(
        &mut conn,
        &store_name!("wide"),
        &wide,
        &migrate::MigrateConfig::default(),
    )
    .await?;
    migrate::run(
        &mut conn,
        &store_name!("narrow"),
        &definition(store::Outbox::Disabled),
        &migrate::MigrateConfig::default(),
    )
    .await?;

    for (parent, partitions, first, last) in [
        ("wide.streams", 1001, "streams_p0000", "streams_p1000"),
        ("wide.snapshots", 1001, "snapshots_p0000", "snapshots_p1000"),
        ("narrow.streams", 4, "streams_p000", "streams_p003"),
        ("narrow.snapshots", 4, "snapshots_p000", "snapshots_p003"),
    ] {
        let (count, low, high): (i64, String, String) = sqlx::query_as(
            "SELECT count(*), min(c.relname)::text, max(c.relname)::text
             FROM pg_inherits i JOIN pg_class c ON c.oid = i.inhrelid
             WHERE i.inhparent = $1::regclass",
        )
        .bind(parent)
        .fetch_one(&mut conn)
        .await?;
        assert_eq!(
            (count, low.as_str(), high.as_str()),
            (partitions, first, last),
            "{parent}"
        );
    }
    // Each name's number is its partition's hash remainder.
    let misnamed: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM pg_inherits i JOIN pg_class c ON c.oid = i.inhrelid
         WHERE i.inhparent IN ('wide.streams'::regclass, 'wide.snapshots'::regclass)
           AND substring(pg_get_expr(c.relpartbound, c.oid) FROM 'remainder ([0-9]+)')::int
               <> substring(c.relname FROM '_p([0-9]+)$')::int",
    )
    .fetch_one(&mut conn)
    .await?;
    assert_eq!(misnamed, 0);

    conn.close().await?;
    db.cleanup().await
}

/// The outbox script is version 1 of the store's scope `<store>.outbox`: recorded with its checksum, checked
/// by the runner and by `Store::open`. A store without the outbox has no such scope.
#[tokio::test]
async fn the_outbox_script_is_recorded_and_checked() -> Result<()> {
    let db = pges_testing::TestDb::new().await?;
    let mut conn = db.connect().await?;
    run_with_outbox(&mut conn, store_name!("o1"), store::Outbox::Enabled).await?;
    run_with_outbox(&mut conn, store_name!("o1"), store::Outbox::Enabled).await?;

    let rows: Vec<(String, i32, String, Vec<u8>)> = sqlx::query_as(
        "SELECT scope, version, name, checksum FROM pges.migrations WHERE scope LIKE 'o1%' ORDER BY scope",
    )
    .fetch_all(&mut conn)
    .await?;
    assert_eq!(
        rows,
        vec![
            (
                "o1".into(),
                1,
                "0001_init".into(),
                migrate::STORE[0].checksum().to_vec()
            ),
            (
                "o1.outbox".into(),
                1,
                "opt_outbox".into(),
                migrate::OUTBOX[0].checksum().to_vec()
            ),
        ]
    );
    let scope: (i32, i32) = sqlx::query_as(
        "SELECT schema_version, min_compatible FROM pges.scopes WHERE scope = 'o1.outbox'",
    )
    .fetch_one(&mut conn)
    .await?;
    assert_eq!(scope, (1, 1));
    let pool = db.pool().await?;
    store::Store::open(pool.clone(), store::StoreConfig::new(store_name!("o1"))).await?;

    sqlx::query("UPDATE pges.migrations SET checksum = '\\x00' WHERE scope = 'o1.outbox'")
        .execute(&mut conn)
        .await?;
    let error = run_with_outbox(&mut conn, store_name!("o1"), store::Outbox::Enabled)
        .await
        .unwrap_err();
    assert!(
        matches!(&error, migrate::MigrateError::ChecksumMismatch { scope, version, name } if scope == "o1.outbox" && version.get() == 1 && *name == "opt_outbox"),
        "{error}"
    );
    let error = store::Store::open(pool.clone(), store::StoreConfig::new(store_name!("o1")))
        .await
        .unwrap_err();
    assert!(
        matches!(&error, store::OpenError::ChecksumMismatch { scope, version } if scope == "o1.outbox" && version.get() == 1),
        "{error}"
    );

    sqlx::query(
        "UPDATE pges.scopes SET schema_version = 9, min_compatible = 9 WHERE scope = 'o1.outbox'",
    )
    .execute(&mut conn)
    .await?;
    let error = store::Store::open(pool.clone(), store::StoreConfig::new(store_name!("o1")))
        .await
        .unwrap_err();
    assert!(
        matches!(&error, store::OpenError::CrateTooOld { scope, .. } if scope == "o1.outbox"),
        "{error}"
    );

    pool.close().await;
    conn.close().await?;
    db.cleanup().await
}

/// A store without the outbox has no outbox scope.
#[tokio::test]
async fn a_store_without_the_outbox_has_no_outbox_scope() -> Result<()> {
    let db = pges_testing::TestDb::new().await?;
    let mut conn = db.connect().await?;
    run(&mut conn, store_name!("o2")).await?;

    let scopes: Vec<String> =
        sqlx::query_scalar("SELECT scope FROM pges.scopes WHERE scope LIKE 'o2%' ORDER BY scope")
            .fetch_all(&mut conn)
            .await?;
    assert_eq!(scopes, ["o2"]);

    conn.close().await?;
    db.cleanup().await
}

/// Store creation adds `commits` to the publication when the creating role holds the publication owner's
/// privileges, and skips it when the publication is missing.
#[tokio::test]
async fn store_creation_publishes_commits_only_where_it_can() -> Result<()> {
    let db = pges_testing::TestDb::new().await?;
    let mut conn = db.connect().await?;

    run(&mut conn, store_name!("p1")).await?;
    assert_eq!(published(&mut conn).await?, ["p1"]);

    sqlx::query("DROP PUBLICATION pges")
        .execute(&mut conn)
        .await?;
    run(&mut conn, store_name!("p2")).await?;
    let publication: bool =
        sqlx::query_scalar("SELECT EXISTS (SELECT FROM pg_publication WHERE pubname = 'pges')")
            .fetch_one(&mut conn)
            .await?;
    assert!(
        !publication,
        "store creation does not create the publication"
    );

    conn.close().await?;
    db.cleanup().await
}

/// A role without the publication owner's privileges creates an outbox store, which is left out of the
/// publication, and is refused a store for the logical source with SQLSTATE 42501: an existing slot would
/// never deliver that store's commits made before they joined the publication. Nothing is created then.
#[tokio::test]
async fn a_role_that_does_not_own_the_publication_creates_only_outbox_stores() -> Result<()> {
    for outbox in [store::Outbox::Enabled, store::Outbox::Disabled] {
        let db = pges_testing::TestDb::new().await?;
        let mut conn = db.connect().await?;
        let role = format!("{}_creator", db.name());
        conn.execute(sqlx::AssertSqlSafe(format!(
            "CREATE ROLE {role};
             GRANT CREATE ON DATABASE {database} TO {role};
             GRANT USAGE ON SCHEMA pges TO {role};
             GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA pges TO {role};",
            database = db.name()
        )))
        .await?;
        sqlx::query(sqlx::AssertSqlSafe(format!("SET ROLE {role}")))
            .execute(&mut conn)
            .await?;

        // The shared schema belongs to the owner, who migrated it: the creator only creates the store.
        let created = migrate::run_store(
            &mut conn,
            &store_name!("n1"),
            &definition(outbox),
            &migrate::MigrateConfig::default(),
        )
        .await;
        let shared = migrate::run_shared(&mut conn, &migrate::MigrateConfig::default()).await;
        assert!(
            matches!(
                shared,
                Err(migrate::MigrateError::SharedSchemaNotOwned { .. })
            ),
            "{shared:?}"
        );

        sqlx::query("RESET ROLE").execute(&mut conn).await?;
        match outbox {
            store::Outbox::Enabled => {
                created?;
                assert_eq!(store_names(&mut conn).await?, ["n1"]);
                assert!(published(&mut conn).await?.is_empty());
            }
            _ => {
                let error = created.unwrap_err();
                let migrate::MigrateError::Failed { source, .. } = &error else {
                    panic!("{error}");
                };
                assert_eq!(source.code().as_deref(), Some("42501"), "{error}");
                assert!(store_names(&mut conn).await?.is_empty());
            }
        }
        conn.execute(sqlx::AssertSqlSafe(format!(
            "DROP SCHEMA IF EXISTS n1 CASCADE; DROP OWNED BY {role}; DROP ROLE {role}"
        )))
        .await?;

        conn.close().await?;
        db.cleanup().await?;
    }
    Ok(())
}

/// The stores whose `commits` the publication `pges` holds.
async fn published(conn: &mut sqlx::PgConnection) -> Result<Vec<String>> {
    Ok(sqlx::query_scalar(
        "SELECT schemaname::text FROM pg_publication_tables WHERE pubname = 'pges' ORDER BY 1",
    )
    .fetch_all(conn)
    .await?)
}

/// A role with `CREATE` on the database takes the name `pges` before the first migration. As the schema's
/// owner it could later drop `pges.stores` and put a table of its own in its place, whose trigger would run
/// as the role creating the next store, a superuser included. The runner creates nothing in a schema that
/// belongs to another role, whoever runs it; the schema's owner may migrate into it.
#[tokio::test]
async fn the_shared_tables_are_not_created_in_a_schema_of_another_role() -> Result<()> {
    let db = pges_testing::TestDb::new().await?;
    let mut conn = db.connect().await?;
    let squatter = format!("{}_squatter", db.name());
    conn.execute(sqlx::AssertSqlSafe(format!(
        "DROP PUBLICATION pges; DROP SCHEMA pges CASCADE;
         CREATE ROLE {squatter}; GRANT CREATE ON DATABASE {database} TO {squatter};
         SET ROLE {squatter}; CREATE SCHEMA pges; RESET ROLE;",
        database = db.name()
    )))
    .await?;

    let error = run(&mut conn, store_name!("q1")).await.unwrap_err();

    assert!(
        matches!(&error, migrate::MigrateError::SharedSchemaNotOwned { owner } if *owner == squatter),
        "{error}"
    );
    let tables: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM pg_class WHERE relnamespace = 'pges'::regnamespace",
    )
    .fetch_one(&mut conn)
    .await?;
    assert_eq!(tables, 0);

    // Its owner migrates into it.
    conn.execute(sqlx::AssertSqlSafe(format!("SET ROLE {squatter}")))
        .await?;
    run_with_outbox(&mut conn, store_name!("q1"), store::Outbox::Enabled).await?;
    conn.execute("RESET ROLE").await?;
    assert_eq!(store_names(&mut conn).await?, ["q1"]);

    conn.execute(sqlx::AssertSqlSafe(format!(
        "DROP SCHEMA q1, pges CASCADE; DROP PUBLICATION IF EXISTS pges;
         DROP OWNED BY {squatter}; DROP ROLE {squatter}"
    )))
    .await?;
    conn.close().await?;
    db.cleanup().await
}

/// The owner check does not rely on anything inside the schema: a squatter whose `pges` already records
/// every shared migration with the right checksums (here it ran them itself) is refused on every run,
/// though no shared migration is left to apply, and no store is created there. Its owner runs them again,
/// and so does a login role that inherits from a NOLOGIN owner after `SET ROLE` to it, while the login role
/// itself is refused.
#[tokio::test]
async fn a_schema_of_another_role_is_refused_with_its_migrations_applied() -> Result<()> {
    let db = pges_testing::TestDb::new().await?;
    let mut conn = db.connect().await?;
    let squatter = format!("{}_squatter", db.name());
    let member = format!("{}_member", db.name());
    conn.execute(sqlx::AssertSqlSafe(format!(
        "DROP PUBLICATION pges; DROP SCHEMA pges CASCADE;
         CREATE ROLE {squatter} NOLOGIN; GRANT CREATE ON DATABASE {database} TO {squatter};
         CREATE ROLE {member} LOGIN IN ROLE {squatter};",
        database = db.name()
    )))
    .await?;
    conn.execute(sqlx::AssertSqlSafe(format!("SET ROLE {squatter}")))
        .await?;
    migrate::run_shared(&mut conn, &migrate::MigrateConfig::default()).await?;
    conn.execute("RESET ROLE").await?;

    let error = run(&mut conn, store_name!("q1")).await.unwrap_err();
    assert!(
        matches!(&error, migrate::MigrateError::SharedSchemaNotOwned { owner } if *owner == squatter),
        "{error}"
    );
    let error = migrate::run_shared(&mut conn, &migrate::MigrateConfig::default())
        .await
        .unwrap_err();
    assert!(
        matches!(&error, migrate::MigrateError::SharedSchemaNotOwned { .. }),
        "{error}"
    );
    assert!(store_names(&mut conn).await?.is_empty());
    let schema: Option<String> =
        sqlx::query_scalar("SELECT nspname::text FROM pg_namespace WHERE nspname = 'q1'")
            .fetch_optional(&mut conn)
            .await?;
    assert_eq!(schema, None, "no store schema was created");

    // A member of the owner is refused as itself, and migrates after SET ROLE to the owner.
    conn.execute(sqlx::AssertSqlSafe(format!("SET ROLE {member}")))
        .await?;
    let error = migrate::run_shared(&mut conn, &migrate::MigrateConfig::default())
        .await
        .unwrap_err();
    assert!(
        matches!(&error, migrate::MigrateError::SharedSchemaNotOwned { .. }),
        "{error}"
    );
    conn.execute(sqlx::AssertSqlSafe(format!("SET ROLE {squatter}")))
        .await?;
    run_with_outbox(&mut conn, store_name!("q1"), store::Outbox::Enabled).await?;
    run_with_outbox(&mut conn, store_name!("q1"), store::Outbox::Enabled).await?;
    conn.execute("RESET ROLE").await?;
    assert_eq!(store_names(&mut conn).await?, ["q1"]);

    conn.execute(sqlx::AssertSqlSafe(format!(
        "DROP SCHEMA q1, pges CASCADE; DROP PUBLICATION IF EXISTS pges;
         DROP OWNED BY {squatter}, {member}; DROP ROLE {member}; DROP ROLE {squatter}"
    )))
    .await?;
    conn.close().await?;
    db.cleanup().await
}
