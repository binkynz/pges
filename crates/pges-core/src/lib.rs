//! Core types for pges, an event store on PostgreSQL 18+.
//!
//! This crate has no IO. It defines what the other crates exchange:
//!
//! - identities and counters: [`stream::StreamId`], [`commit::CommitId`], [`version::Version`],
//!   [`position::Position`], [`category::CategoryName`], [`category::CategoryId`], [`store::StoreName`],
//!   [`store::StoreId`], [`commit::EventCount`], WAL positions ([`lsn::Lsn`]);
//! - checked option values: [`interval::Interval`], a duration from 1 ms to 30 days, and
//!   [`non_empty::NonEmpty`], a list with at least one item;
//! - literal macros that check their argument at compile time: [`category!`], [`store_name!`],
//!   [`interval!`] and [`non_empty!`];
//! - [`high_water::HighWater`], how far a store's log has been sequenced;
//! - the [`event::Event`] and [`aggregate::Aggregate`] traits that application types implement (usually through
//!   `#[derive(Event)]` from `pges-derive`);
//! - per-commit [`metadata::Metadata`];
//! - the payload [`frame`] stored in `commits.payload`, and the [`codec`]s used for event data;
//! - [`tls::TlsError`], returned by the gRPC watch server's and the gRPC waker's TLS settings.
#![cfg_attr(docsrs, feature(doc_cfg))]

pub mod aggregate;
pub mod category;
pub mod codec;
pub mod commit;
pub mod event;
pub mod frame;
pub mod high_water;
pub mod interval;
pub mod lsn;
pub mod metadata;
pub mod non_empty;
pub mod position;
pub mod store;
pub mod stream;
pub mod tls;
pub mod version;

#[cfg(doctest)]
#[doc = include_str!("../README.md")]
struct ReadmeDoctests;

#[doc(hidden)]
#[path = "private.rs"]
pub mod __private;
