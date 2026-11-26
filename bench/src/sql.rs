//! SQL statements. This module supports the pges crates and is not a stable API: it can change in any
//! release.
//!
//! The benchmarks keep their SQL in `.sql` files under `sql/` and declare every statement in one registry
//! ([`statement`]). Where a benchmark measures a statement of a pges crate, it renders that crate's statement
//! instead of a copy.

pub mod statement;
