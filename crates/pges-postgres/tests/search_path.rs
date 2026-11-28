//! The functions the migrations create pin their `search_path`, so neither a caller's `search_path` nor an
//! object another role creates in `public` changes what they call.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use anyhow::Result;
use sqlx::{Connection, Executor};

use pges_core::store_name;
use pges_postgres::store;

/// Another role shadows `uuid_extract_version` in `public`, and a caller puts `public` before `pg_catalog`.
/// `pges.check_commit_id` still calls the built-in and refuses a v4 id with 22023. Before its `search_path`
/// was pinned, it called the impostor, which says 7, and accepted the id.
#[tokio::test]
async fn a_function_in_public_does_not_hijack_the_commit_id_check() -> Result<()> {
    let db = pges_testing::TestDb::new().await?;
    let mut admin = db.connect().await?;
    let hostile = format!("{}_hostile", db.name());
    admin
        .execute(sqlx::AssertSqlSafe(format!(
            "CREATE ROLE {hostile};
             GRANT CREATE ON SCHEMA public TO {hostile};
             SET ROLE {hostile};
             CREATE FUNCTION public.uuid_extract_version(uuid) RETURNS int2
               LANGUAGE sql IMMUTABLE RETURN 7;
             RESET ROLE;"
        )))
        .await?;

    let mut caller = db.connect().await?;
    caller
        .execute("SET search_path = public, pg_catalog")
        .await?;
    let impostor: i16 = sqlx::query_scalar("SELECT uuid_extract_version(gen_random_uuid())")
        .fetch_one(&mut caller)
        .await?;
    assert_eq!(
        impostor, 7,
        "the caller's own calls resolve to the impostor"
    );

    let error = sqlx::query("SELECT pges.check_commit_id(gen_random_uuid(), interval '5 seconds')")
        .execute(&mut caller)
        .await
        .unwrap_err();
    assert_eq!(
        error.as_database_error().and_then(|e| e.code()).as_deref(),
        Some("22023"),
        "{error}"
    );

    caller.close().await?;
    admin
        .execute(sqlx::AssertSqlSafe(format!(
            "DROP OWNED BY {hostile}; DROP ROLE {hostile}"
        )))
        .await?;
    admin.close().await?;
    db.cleanup().await
}

/// Every function in `pges` and in a store schema, the outbox trigger's included, sets `search_path` to
/// `pg_catalog, pg_temp`. A function added to a migration without it fails here.
#[tokio::test]
async fn every_pges_function_pins_its_search_path() -> Result<()> {
    let db = pges_testing::TestDb::new().await?;
    let mut conn = db.connect().await?;
    pges_testing::create_store(
        &mut conn,
        &store_name!("es"),
        &pges_testing::definition(store::Outbox::Enabled),
    )
    .await?;

    let functions: Vec<(String, Option<Vec<String>>)> = sqlx::query_as(
        "SELECT p.oid::regprocedure::text, p.proconfig
         FROM pg_proc AS p
         JOIN pg_namespace AS n ON n.oid = p.pronamespace
         WHERE n.nspname IN ('pges', 'es')
         ORDER BY 1",
    )
    .fetch_all(&mut conn)
    .await?;
    let names: Vec<&str> = functions.iter().map(|(name, _)| name.as_str()).collect();
    assert_eq!(
        names,
        [
            "es.notify_pending()",
            "pges.check_commit_id(uuid,interval)",
            "pges.check_one_source_kind()",
            "pges.uuidv7_floor(timestamp with time zone)",
        ]
    );
    for (name, config) in &functions {
        assert_eq!(
            config.as_deref(),
            Some(&["search_path=pg_catalog, pg_temp".to_owned()][..]),
            "{name}"
        );
    }

    conn.close().await?;
    db.cleanup().await
}
