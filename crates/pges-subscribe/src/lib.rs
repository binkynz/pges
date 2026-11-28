//! Subscriptions for pges: follow one or several categories of a store, or all of them, in log order, and
//! keep read models up to date.
//!
//! - [`subscription`] configures and runs a subscription: a named cursor in the store's `subscriptions`
//!   table, with a lease so one instance runs at a time and others stand by. Names, categories, shard
//!   counts and durations are typed, so a subscription without categories, with an invalid name literal or
//!   with a zero interval does not compile.
//! - [`handler::PgHandler`] applies batches inside the transaction that also moves the cursor, so a
//!   projection in the same database sees every event exactly once. [`handler::Handler`] is for anything
//!   else, at least once.
//! - [`batch::Batch`] is what a handler receives: events in position order, decoded per event type.
//! - [`waker`] tells a caught-up subscription that new events may exist, so it does not have to poll.
//! - [`replica`] reads a subscription's log pages from a streaming replica, waiting until it has replayed
//!   what the waker announced, with an opt-in, capped fallback to the primary.
//! - [`backfill`] replays some categories up to a position once, with parallel lanes by stream, to fill a
//!   new read-model field from old events.
//!
//! ```no_run
//! # async fn example(
//! #     store: pges_postgres::store::Store,
//! # ) -> Result<(), Box<dyn std::error::Error>> {
//! use sqlx::PgConnection;
//!
//! use pges_core::{category, interval, non_empty};
//! use pges_subscribe::{batch, handler, subscription::{self, StoreExt}, subscription_name, waker};
//!
//! struct JobCount;
//!
//! impl handler::PgHandler for JobCount {
//!     async fn handle(
//!         &mut self,
//!         tx: &mut PgConnection,
//!         batch: &batch::Batch,
//!     ) -> Result<(), handler::HandlerError> {
//!         let events = batch.len() as i64;
//!         sqlx::query("UPDATE job_count SET n = n + $1")
//!             .bind(events)
//!             .execute(&mut *tx)
//!             .await?;
//!         Ok(())
//!     }
//! }
//!
//! let handle = store
//!     .subscription(subscription_name!("job-count.v1"))
//!     .categories(non_empty![category!("jobs")])
//!     .start(subscription::Start::Beginning)
//!     .lease(interval!(15 s))
//!     .waker(waker::recheck::Recheck::every(interval!(200 ms)))   // or a push waker
//!     .run_pg(JobCount)
//!     .spawn();
//!
//! println!("at {}", handle.status().position);
//! handle.shutdown().await?;
//! # Ok(())
//! # }
//! ```
//!
//! # Leases
//!
//! One instance of a subscription holds its lease at a time; the others stand by. The holder renews it once
//! less than half is left: with a checkpoint where possible, otherwise on another connection of the store's
//! pool, including while a handler runs and while a failed batch waits for its retry. Every checkpoint is
//! fenced, so an instance that lost the lease without noticing cannot move the cursor; its batch rolls back
//! (`run_pg`) or is delivered again by the new holder (`run`).
//!
//! So the store's pool needs room for the renewal: `run_pg` holds 2 connections at once (the handler's
//! transaction and the renewal), `run` 1, and a `NotifyWaker` on the store's pool keeps 1 more. A
//! subscription whose store's pool allows fewer fails at start with
//! [`subscription::SubscribeError::PoolTooSmall`]. That check sees only the one subscription; the pool must
//! also fit the store's other users ([`subscription::Builder`], Pool size).
//!
//! # Order and the merge cut
//!
//! Positions are gap-free per store, and every category's events have increasing positions. A subscription
//! over one category reads that category's log rows after its cursor. Over several categories it reads each
//! category separately, each up to `H`, the store's newest position read first, and merges them at a cut no
//! category can skip: the lowest last position of the categories that filled a page, or `H` when none did.
//! Batches are therefore in position order, and no event of the subscribed categories is skipped or
//! delivered twice, whatever commits and sequencer batches happen between the reads. A batch can advance the
//! cursor without events, when other categories took the positions in between.
//!
//! A store-wide subscription ([`subscription::Builder::all_categories`]) reads the log alone, up to `H`, and
//! then fetches the commits of each category in the page, so it needs no merge. It also reads categories
//! registered after it started.
//!
//! # Shards
//!
//! A sharded subscription ([`subscription::Builder::sharded`]) splits its events by stream into shards, each
//! with its own cursor and lease. Running instances share the shards fairly and take over those of an
//! instance that died. Each stream's events stay in order; there is no order between streams of different
//! shards.
//!
//! # Retention
//!
//! Each read checks, in the same statement, that the cursor is not behind the category's retention horizon
//! (for a store-wide subscription, the highest horizon of any category).
//! A cursor that is behind stops the subscription with
//! [`SubscribeError::CursorBeforeHorizon`](subscription::SubscribeError::CursorBeforeHorizon): events
//! after it are gone, and the read model must be rebuilt, from snapshots or another projection for the
//! state they built. A new subscription created with
//! [`Start::Retained`](subscription::Start::Retained) reads every event retention kept and skips those it
//! dropped; a later position is not enough, since a kept commit can be positioned below the horizon when it
//! committed before the dropped leaf's last commit. A backfill does not stop either: it reads each category
//! from its start position (see [`backfill`]).
//!
//! # Features
//!
#![cfg_attr(
    feature = "wake-grpc",
    doc = " - `wake-grpc` (default): [`waker::grpc::GrpcWaker`], woken by the sequencer's gRPC watch (its `watch-grpc` feature). It adds tonic."
)]
#![cfg_attr(
    not(feature = "wake-grpc"),
    doc = " - `wake-grpc` (default): `waker::grpc::GrpcWaker`, woken by the sequencer's gRPC watch (its `watch-grpc` feature). It adds tonic."
)]
//! - `wake-grpc-tls`: `https` endpoints for the gRPC waker, verified against the system's root
//!   certificates or a given CA, with an optional client certificate (rustls with ring).
#![cfg_attr(
    feature = "wake-poll",
    doc = " - `wake-poll`: [`waker::poll::PollWaker`]."
)]
#![cfg_attr(
    not(feature = "wake-poll"),
    doc = " - `wake-poll`: `waker::poll::PollWaker`."
)]
#![cfg_attr(
    feature = "wake-inproc",
    doc = " - `wake-inproc`: [`waker::inproc::InprocWaker`], for a sequencer running in the same process."
)]
#![cfg_attr(
    not(feature = "wake-inproc"),
    doc = " - `wake-inproc`: `waker::inproc::InprocWaker`, for a sequencer running in the same process."
)]
#![cfg_attr(
    feature = "wake-notify",
    doc = " - `wake-notify`: [`waker::notify::NotifyWaker`], woken by the sequencer's `NOTIFY` (its `watch-notify` feature)."
)]
#![cfg_attr(
    not(feature = "wake-notify"),
    doc = " - `wake-notify`: `waker::notify::NotifyWaker`, woken by the sequencer's `NOTIFY` (its `watch-notify` feature)."
)]
#![cfg_attr(docsrs, feature(doc_cfg))]

pub mod backfill;
pub mod batch;
pub mod handler;
pub mod replica;
#[doc(hidden)]
pub mod sql;
pub mod subscription;
pub mod waker;

#[cfg(doctest)]
#[doc = include_str!("../README.md")]
struct ReadmeDoctests;

/// Support for this crate's macros. Not part of the API.
#[doc(hidden)]
pub mod __private {
    use crate::subscription;

    /// Whether `name` can name a subscription or backfill, for `subscription_name!`.
    pub const fn is_valid_subscription_name(name: &str) -> bool {
        subscription::validate_name(name).is_ok()
    }

    /// The name behind `subscription_name!`, which checks it with [`is_valid_subscription_name`] first and
    /// calls this in a `const` item, so an invalid name is a compile error.
    pub const fn subscription_name(name: &'static str) -> subscription::SubscriptionName {
        match subscription::validate_name(name) {
            Ok(()) => subscription::SubscriptionName::from_validated(name),
            // `subscription_name!` evaluates this in a `const` item, so the panic is a compile error. A `const
            // fn` cannot return the `Result` instead: `SubscriptionName` has a destructor, which `const`
            // evaluation cannot run.
            #[allow(clippy::panic)]
            Err(_) => panic!("invalid subscription name: it must match ^[a-z][a-z0-9_.-]{{0,99}}$"),
        }
    }
}

mod ident;
mod read;
