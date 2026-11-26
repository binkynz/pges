//! SQL statements. This module supports the pges crates and is not a stable API: it can change in any
//! release.
//!
//! The crate keeps its SQL in `.sql` files under its `sql/` directory and declares every statement in one
//! registry ([`statement`]). Its fixtures also render statements of `pges_postgres::sql::statement`, the ones
//! `Admin` runs to create partitions.

pub mod statement;
