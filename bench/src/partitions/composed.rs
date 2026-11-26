//! Reads the result of a statement composed at run time from registry fragments and SQL literals
//! (`EXPLAIN ... EXECUTE bench_read (..)`). No typed statement describes it, so its one column is read here
//! without one; `tests/statement.rs` exempts this file from the typed-call check.

use anyhow::Result;
use serde_json::Value;
use sqlx::PgConnection;

/// The JSON plan of `explain`, an `EXPLAIN (FORMAT JSON)` statement.
pub(super) async fn plan(conn: &mut PgConnection, explain: &str) -> Result<Value> {
    Ok(sqlx::query_scalar(sqlx::AssertSqlSafe(explain))
        .fetch_one(conn)
        .await?)
}
