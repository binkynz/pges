//! The facade runs no SQL of its own: every statement comes from the registries of the crates it re-exports,
//! and it binds and reads nothing outside their typed statements.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::path::Path;

#[test]
fn no_sql_outside_the_registries() -> anyhow::Result<()> {
    pges_testing::statement::check_no_inline_sql(&Path::new(env!("CARGO_MANIFEST_DIR")).join("src"))
}

#[test]
fn binds_and_reads_are_typed() -> anyhow::Result<()> {
    pges_testing::statement::check_typed_calls(
        &Path::new(env!("CARGO_MANIFEST_DIR")).join("src"),
        &[],
    )
}
