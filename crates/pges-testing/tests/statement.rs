//! The statement registry against PostgreSQL 18: every query is checked against the Rust types of its parameters
//! and columns, every DDL statement runs, and no SQL or untyped bind or read bypasses the registry.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::{collections::BTreeSet, path::Path};

use anyhow::Result;
use sqlx::{Connection, Executor, PgConnection};

use pges_postgres::sql::{template, typed};
use pges_testing::sql::statement;

const STORE_NAME: pges_core::store::StoreName = pges_core::store_name!("reg");

#[test]
fn files_match_the_registry() -> Result<()> {
    let values = template::Values::new()
        .store(&STORE_NAME)
        .ident(template::IdentPlaceholder::Database, &ident("t_a"))
        .ident(template::IdentPlaceholder::Template, &ident("t_b"));

    pges_testing::statement::check_files(
        Path::new(env!("CARGO_MANIFEST_DIR")),
        statement::ALL,
        &values,
    )
}

#[test]
fn all_sql_comes_from_the_registry() -> Result<()> {
    pges_testing::statement::check_no_inline_sql(&Path::new(env!("CARGO_MANIFEST_DIR")).join("src"))
}

#[tokio::test]
async fn queries_match_their_registry_types() -> Result<()> {
    let db = pges_testing::TestDb::new().await?;
    let mut conn = db.connect().await?;
    pges_testing::create_store(
        &mut conn,
        &STORE_NAME,
        &pges_testing::definition(pges_postgres::store::Outbox::Disabled),
    )
    .await?;
    let values = template::Values::new().store(&STORE_NAME);

    let mut failures = Vec::new();
    for shape in statement::TYPED {
        if let Err(error) = pges_testing::statement::check_typed(&mut conn, shape, &values).await {
            failures.push(format!("{error:#}"));
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n\n"));

    drop(conn);
    db.cleanup().await
}

#[test]
fn every_query_is_typed() {
    let queries: BTreeSet<&str> = statement::ALL
        .iter()
        .filter(|s| s.kind == template::Kind::Query)
        .map(|s| s.name)
        .collect();
    let typed: Vec<&str> = statement::TYPED.iter().map(|s| s.statement.name).collect();

    assert_eq!(typed.iter().copied().collect::<BTreeSet<_>>(), queries);
    assert_eq!(typed.len(), queries.len(), "a query is listed twice");
}

/// `statement.rs` names the untyped calls it looks for.
#[test]
fn binds_and_reads_are_typed() -> Result<()> {
    pges_testing::statement::check_typed_calls(
        &Path::new(env!("CARGO_MANIFEST_DIR")).join("src"),
        &["statement.rs"],
    )
}

/// The registry check rejects a typed statement whose Rust types do not match its SQL, or with a placeholder
/// without a value, so a registry that drifts from its SQL fails its crate's test.
#[tokio::test]
async fn check_typed_rejects_wrong_rust_types() -> Result<()> {
    let db = pges_testing::TestDb::new().await?;
    let mut conn = db.connect().await?;
    let values = template::Values::new();

    for right in [
        typed::Shape::of::<typed_case::sum::Query>(),
        typed::Shape::of::<typed_case::relname::Query>(),
        typed::Shape::of::<typed_case::proconfig::Query>(),
    ] {
        pges_testing::statement::check_typed(&mut conn, &right, &values).await?;
    }
    for wrong in [
        typed::Shape::of::<typed_case::sum_narrow_param::Query>(),
        typed::Shape::of::<typed_case::sum_narrow_column::Query>(),
        typed::Shape::of::<typed_case::sum_misnamed::Query>(),
        typed::Shape::of::<typed_case::sum_missing_param::Query>(),
        typed::Shape::of::<typed_case::relname_optional::Query>(),
        typed::Shape::of::<typed_case::proconfig_not_null::Query>(),
        typed::Shape::of::<typed_case::unfilled::Query>(),
    ] {
        assert!(
            pges_testing::statement::check_typed(&mut conn, &wrong, &values)
                .await
                .is_err(),
            "{} passed",
            wrong.statement.name
        );
    }

    conn.close().await?;
    db.cleanup().await
}

/// The typed statements of `check_typed_rejects_wrong_rust_types`: right ones, and wrong variants of them.
mod typed_case {
    use pges_postgres::sql::template;

    const SUM: template::File = template::File {
        path: "sum.sql",
        text: "-- One more than $1.\nSELECT $1::int8 + 1 AS n",
    };
    const RELNAME: template::File = template::File {
        path: "relname.sql",
        text: "-- A relation's name, never NULL.\nSELECT relname FROM pg_class WHERE relname = $1",
    };
    const UNFILLED: template::File = template::File {
        path: "unfilled.sql",
        text: "-- The log of a store.\nSELECT 1 FROM {store}.log",
    };
    const PROCONFIG: template::File = template::File {
        path: "proconfig.sql",
        text: "-- A function's settings, NULL for most.\nSELECT proconfig FROM pg_proc WHERE proname = $1",
    };

    pges_postgres::typed_query! {
        /// Right.
        pub SUM_RIGHT, mod sum {
            name: "sum",
            files: [SUM],
            params { n: i64 },
            row { n: i64 },
        }
    }
    pges_postgres::typed_query! {
        /// `$1` is `int8`.
        pub SUM_NARROW_PARAM, mod sum_narrow_param {
            name: "sum_narrow_param",
            files: [SUM],
            params { n: i32 },
            row { n: i64 },
        }
    }
    pges_postgres::typed_query! {
        /// `n` is `int8`.
        pub SUM_NARROW_COLUMN, mod sum_narrow_column {
            name: "sum_narrow_column",
            files: [SUM],
            params { n: i64 },
            row { n: i32 },
        }
    }
    pges_postgres::typed_query! {
        /// The column is `n`.
        pub SUM_MISNAMED, mod sum_misnamed {
            name: "sum_misnamed",
            files: [SUM],
            params { n: i64 },
            row { m: i64 },
        }
    }
    pges_postgres::typed_query! {
        /// `$1` has no field.
        pub SUM_MISSING_PARAM, mod sum_missing_param {
            name: "sum_missing_param",
            files: [SUM],
            params {},
            row { n: i64 },
        }
    }
    pges_postgres::typed_query! {
        /// Right: `relname` is `name`, which `String` decodes.
        pub RELNAME_RIGHT, mod relname {
            name: "relname",
            files: [RELNAME],
            params<'q> { relname: &'q str },
            row { relname: String },
        }
    }
    pges_postgres::typed_query! {
        /// `relname` is never NULL.
        pub RELNAME_OPTIONAL, mod relname_optional {
            name: "relname_optional",
            files: [RELNAME],
            params<'q> { relname: &'q str },
            row { relname: Option<String> },
        }
    }
    pges_postgres::typed_query! {
        /// Right.
        pub PROCONFIG_RIGHT, mod proconfig {
            name: "proconfig",
            files: [PROCONFIG],
            params<'q> { proname: &'q str },
            row { proconfig: Option<Vec<String>> },
        }
    }
    pges_postgres::typed_query! {
        /// `proconfig` is nullable.
        pub PROCONFIG_NOT_NULL, mod proconfig_not_null {
            name: "proconfig_not_null",
            files: [PROCONFIG],
            params<'q> { proname: &'q str },
            row { proconfig: Vec<String> },
        }
    }
    pges_postgres::typed_query! {
        /// `{store}` has no value.
        pub UNFILLED_STORE, mod unfilled {
            name: "unfilled",
            files: [UNFILLED],
            params {},
            row { one: i32 as "?column?" },
        }
    }
}

/// The template's life cycle on databases of this test: built under one name, renamed, marked as a template,
/// cloned, and dropped.
#[tokio::test]
async fn ddl_statements_run() -> Result<()> {
    let db = pges_testing::TestDb::new().await?;
    let mut conn = db.connect().await?;
    let mut ran = BTreeSet::new();
    run(
        &mut conn,
        &mut ran,
        &statement::UTC,
        &template::Values::new(),
    )
    .await?;
    let mut admin = pges_testing::server().await?.admin().await?;

    let building = test_database();
    let built = test_database();
    let clone = test_database();
    let values = template::Values::new()
        .ident(template::IdentPlaceholder::Database, &ident(&building))
        .ident(template::IdentPlaceholder::Template, &ident(&built));
    let clone_values = template::Values::new()
        .ident(template::IdentPlaceholder::Database, &ident(&clone))
        .ident(template::IdentPlaceholder::Template, &ident(&built));
    let mut life_cycle = async {
        for (statement, values) in [
            (&statement::DROP_DATABASE_IF_EXISTS, &values),
            (&statement::CREATE_DATABASE, &values),
            (&statement::RENAME_TO_TEMPLATE, &values),
            (&statement::MARK_TEMPLATE, &values),
            (&statement::CREATE_FROM_TEMPLATE, &clone_values),
            (&statement::DROP_DATABASE, &clone_values),
        ] {
            run(&mut admin, &mut ran, statement, values).await?;
        }
        anyhow::Ok(())
    }
    .await;
    // Whatever failed, no database of this test is left: the sweep cannot drop a template.
    for name in [&building, &built, &clone] {
        let unmark = admin
            .execute(sqlx::AssertSqlSafe(format!(
                r#"ALTER DATABASE "{name}" WITH IS_TEMPLATE false"#
            )))
            .await;
        if unmark.is_ok() {
            let dropped = admin
                .execute(sqlx::AssertSqlSafe(format!(
                    r#"DROP DATABASE IF EXISTS "{name}" WITH (FORCE)"#
                )))
                .await;
            life_cycle = life_cycle.and(dropped.map(drop).map_err(anyhow::Error::from));
        }
    }
    life_cycle?;

    let ddl: BTreeSet<&str> = statement::ALL
        .iter()
        .filter(|s| s.kind == template::Kind::Ddl)
        .map(|s| s.name)
        .collect();
    assert_eq!(ran, ddl, "every DDL statement runs in this test");

    admin.close().await?;
    conn.close().await?;
    db.cleanup().await
}

/// A `t_<uuidv7>` name, which a later run's sweep drops if this test is killed.
fn test_database() -> String {
    format!("t_{}", uuid::Uuid::now_v7().simple())
}

fn ident(name: &str) -> template::Ident {
    template::Ident::new(name).unwrap()
}

async fn run(
    conn: &mut PgConnection,
    ran: &mut BTreeSet<&'static str>,
    statement: &'static template::Statement,
    values: &template::Values,
) -> Result<()> {
    pges_testing::statement::run_ddl(conn, statement, values).await?;
    ran.insert(statement.name);

    Ok(())
}
