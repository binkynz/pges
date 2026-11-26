//! PostgreSQL storage for pges, an event store on PostgreSQL 18+.
//!
//! - [`store::Store`] opens a store (one schema), appends events to streams with optimistic concurrency, and
//!   reads streams and aggregates back. Appends are one statement and safe to retry: commit ids are
//!   generated on the client, and a retry of an append that already committed returns its original result.
//! - [`unit_of_work::UnitOfWork`] puts appends to several streams and the caller's own writes in one
//!   transaction.
//! - [`migrate`] holds the embedded, checksummed migrations and the runner that applies them and creates
//!   stores.
//! - [`admin::Admin`] registers categories, keeps their partitions ahead of time and applies each category's
//!   [`retention::Retention`], and records the store's writer limits ([`limit`]). It also freezes closed
//!   partitions and reports how far each partition is from transaction id wraparound ([`freeze`]).
//! - [`store::Store::delete_stream`] deletes a stream and its events, for the exceptions where personal data
//!   cannot be crypto-shredded.
//! - [`ambient::scope`] sets request metadata (correlation, actor, trace) once, for every append inside it.
//!   With the `otel` feature, appends without a trace record the current `tracing` span's.
//!
//! ```no_run
//! # async fn example(pool: sqlx::PgPool) -> Result<(), Box<dyn std::error::Error>> {
//! use pges_core::store_name;
//! use pges_postgres::store;
//!
//! let mut config = store::StoreConfig::new(store_name!("ops_es"));
//! config.migrate = store::AutoMigrate::CreateOrMigrate(store::StoreDefinition::default());
//! let store = store::Store::open(pool, config).await?;
//! # Ok(())
//! # }
//! ```
//!
//! Every statement uses qualified names built once per store, so the connection's `search_path` does not
//! matter. Errors are one enum per operation; database errors are wrapped in [`error::DbError`].
#![cfg_attr(docsrs, feature(doc_cfg))]

pub mod admin;
pub mod ambient;
pub mod append;
pub mod delete;
pub mod error;
pub mod freeze;
pub mod limit;
pub mod load;
pub mod migrate;
pub mod read;
pub mod retention;
#[doc(hidden)]
pub mod sql;
pub mod store;
pub mod unit_of_work;

#[cfg(doctest)]
#[doc = include_str!("../README.md")]
struct ReadmeDoctests;

mod catalog;
mod ident;
mod route;
