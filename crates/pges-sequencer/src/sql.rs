//! The sequencer's SQL statements. This module supports the pges crates and is not a stable API: it can
//! change in any release.
//!
//! The SQL lives in `.sql` files under the crate's `sql/` directory. [`statement`] declares every statement
//! the crate runs, and statements that name a store are rendered with
//! [`pges_postgres::sql::template::Values`] once per store.

pub mod statement;
