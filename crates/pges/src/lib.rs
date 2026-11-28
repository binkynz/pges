//! pges is an event store on PostgreSQL 18+.
//!
//! Applications append events to streams with optimistic concurrency, load aggregates back, and follow
//! categories of events through subscriptions. A sequencer orders committed appends into a gap-free log per
//! store, so subscribers keep a plain integer cursor and a projection in the same database sees every event
//! exactly once.
//!
//! This crate gathers the pges crates as modules, so an application depends on `pges` alone:
//!
//! | Module | Crate | What it holds |
//! |---|---|---|
//! | [`core`] | `pges-core` | ids, versions, positions, the [`Event`](core::event::Event) and [`Aggregate`](core::aggregate::Aggregate) traits, codecs, metadata; no IO |
#![cfg_attr(
    feature = "derive",
    doc = " | [`derive`](mod@derive) | `pges-derive` | `#[derive(Event)]`, also at the root as [`pges::Event`](macro@Event) |"
)]
#![cfg_attr(
    not(feature = "derive"),
    doc = " | `derive` | `pges-derive` | `#[derive(Event)]`, also at the root as `pges::Event` |"
)]
//! | [`postgres`] | `pges-postgres` | [`Store`](postgres::store::Store): appends, reads, loads, snapshots, units of work; migrations; [`Admin`](postgres::admin::Admin) for categories and partitions |
#![cfg_attr(
    feature = "subscribe",
    doc = " | [`subscribe`] | `pges-subscribe` | subscriptions: `run_pg` (exactly once, in the projection's transaction) and `run` (at least once); wakers |"
)]
#![cfg_attr(
    not(feature = "subscribe"),
    doc = " | `subscribe` | `pges-subscribe` | subscriptions: `run_pg` (exactly once, in the projection's transaction) and `run` (at least once); wakers |"
)]
#![cfg_attr(
    feature = "sequencer-outbox",
    doc = " | [`sequencer`] | `pges-sequencer` | the sequencer, embedded in an application; also a binary of its own |"
)]
#![cfg_attr(
    not(feature = "sequencer-outbox"),
    doc = " | `sequencer` | `pges-sequencer` | the sequencer, embedded in an application; also a binary of its own |"
)]
#![cfg_attr(
    feature = "replication",
    doc = " | [`replication`] | `pges-replication` | the minimal logical replication client the sequencer's logical source uses |"
)]
#![cfg_attr(
    not(feature = "replication"),
    doc = " | `replication` | `pges-replication` | the minimal logical replication client the sequencer's logical source uses |"
)]
//!
//! Each module is the crate itself, so its documentation is the crate's, and a path such as
//! `pges::postgres::store::Store` names the same type as `pges_postgres::store::Store`.
//!
//! # Example
//!
//! Define events with the derive, open a store, append, load an aggregate, read a stream, and keep a
//! read model up to date with a subscription.
//!
//! ```no_run
//! # #[cfg(all(feature = "derive", feature = "subscribe"))]
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! use futures_util::TryStreamExt;
//! use sqlx::PgConnection;
//!
//! use pges::core::{aggregate, event::Event as _, stream, version};
//! use pges::postgres::{admin, store};
//! use pges::subscribe::{batch, handler, subscription::{self, StoreExt}};
//!
//! // One enum per category; each variant is one stored event type.
//! #[derive(Debug, Clone, PartialEq, pges::Event)]
//! #[event(category = "jobs")]
//! enum JobEvent {
//!     #[event(name = "job.created")]
//!     Created { title: String },
//!     #[event(name = "job.assigned")]
//!     Assigned { assignee: String },
//! }
//!
//! // An aggregate folds a stream's events into state.
//! #[derive(Default)]
//! struct Job {
//!     assignee: Option<String>,
//! }
//!
//! impl aggregate::Aggregate for Job {
//!     type Event = JobEvent;
//!
//!     fn apply(&mut self, event: &JobEvent) {
//!         if let JobEvent::Assigned { assignee } = event {
//!             self.assignee = Some(assignee.clone());
//!         }
//!     }
//! }
//!
//! // A projection: it writes through the transaction that also moves its cursor.
//! struct JobBoard;
//!
//! impl handler::PgHandler for JobBoard {
//!     async fn handle(
//!         &mut self,
//!         tx: &mut PgConnection,
//!         batch: &batch::Batch,
//!     ) -> Result<(), handler::HandlerError> {
//!         for event in batch.decode::<JobEvent>() {
//!             let event = event?;
//!             match &event.event {
//!                 JobEvent::Created { title } => {
//!                     sqlx::query("INSERT INTO job_board (job_id, title) VALUES ($1, $2)")
//!                         .bind(event.stream_id.as_uuid())
//!                         .bind(title)
//!                         .execute(&mut *tx)
//!                         .await?;
//!                 }
//!                 JobEvent::Assigned { assignee } => {
//!                     sqlx::query("UPDATE job_board SET assignee = $2 WHERE job_id = $1")
//!                         .bind(event.stream_id.as_uuid())
//!                         .bind(assignee)
//!                         .execute(&mut *tx)
//!                         .await?;
//!                 }
//!             }
//!         }
//!         Ok(())
//!     }
//! }
//!
//! // Open the store (one schema), creating and migrating it if needed, and register the category.
//! let pool = sqlx::PgPool::connect("postgres://app@localhost/app").await?;
//! let mut config = store::StoreConfig::new(pges::core::store_name!("ops_es"));
//! config.migrate = store::AutoMigrate::CreateOrMigrate(store::StoreDefinition::default());
//! let store = store::Store::open(pool, config).await?;
//! store
//!     .admin()
//!     .register_category(JobEvent::CATEGORY, admin::CategoryConfig::default())
//!     .await?;
//!
//! // Append to a new stream, then load the aggregate and append what it decided.
//! let job_id = stream::StreamId::new_v7();
//! let created = [JobEvent::Created { title: "Replace brake pads".into() }];
//! store.append(job_id, version::ExpectedVersion::NoStream, &created).await?;
//!
//! let job = store.load::<Job>(job_id).await?;
//! if job.state().assignee.is_none() {
//!     let assigned = [JobEvent::Assigned { assignee: "ana".into() }];
//!     store.append(job_id, job.expected(), &assigned).await?;
//! }
//!
//! // Read the stream back.
//! let events: Vec<_> = store
//!     .read_stream::<JobEvent>(job_id, version::Version::FIRST)
//!     .try_collect()
//!     .await?;
//! assert_eq!(events.len(), 2);
//!
//! // Follow the category. The subscription receives events once a sequencer has ordered them into the
//! // store's log (see "Sequencer" below).
//! let board = store
//!     .subscription(pges::subscribe::subscription_name!("job-board.v1"))
//!     .category::<JobEvent>()
//!     .start(subscription::Start::Beginning)
//!     .run_pg(JobBoard)
//!     .spawn();
//!
//! // ... until the application stops:
//! board.shutdown().await?;
//! # Ok(())
//! # }
//! ```
//!
//! [`postgres::admin::Admin::maintain`] creates partitions ahead of time and must run on a schedule, for
//! example once an hour. [`postgres::admin::Admin::apply_retention`] drops what each category's
//! [`Retention`](postgres::retention::Retention) no longer keeps.
//!
//! # Sequencer
//!
//! Appends become visible to subscriptions once the sequencer has written them to the store's log. Run it
//! as the `pges-sequencer` binary, or embed it with the `sequencer` feature. One sequencer leads per
//! database; others stand by. Once the crate is published, `cargo install pges-sequencer --features bin`
//! installs the binary; until then, install it from a checkout of the repository with
//! `cargo install --path crates/pges-sequencer --features bin`.
//!
//! The logical source reads commits from a logical replication slot and is the usual choice. It needs a
//! replication connection besides the pool:
//!
//! ```no_run
//! # #[cfg(feature = "source-logical")]
//! # async fn example(pool: sqlx::PgPool) -> Result<(), Box<dyn std::error::Error>> {
//! use pges::replication::config;
//! use pges::sequencer::{logical, sequencer};
//!
//! let replication = config::Config::from_url("postgres://sequencer:secret@db:5432/app")?;
//! let source = logical::LogicalSource::new(pool.clone(), replication, logical::LogicalConfig::default());
//! let sequencer = sequencer::Sequencer::new(pool, source, sequencer::SequencerConfig::default());
//! sequencer.run(std::future::pending()).await?;
//! # Ok(())
//! # }
//! ```
//!
//! The outbox source sequences stores created with `StoreDefinition::outbox` enabled, where logical
//! replication is not available:
//!
//! ```no_run
//! # #[cfg(feature = "sequencer-outbox")]
//! # async fn example(pool: sqlx::PgPool) -> Result<(), Box<dyn std::error::Error>> {
//! use pges::sequencer::{outbox, sequencer};
//!
//! let source = outbox::OutboxSource::new(pool.clone(), outbox::OutboxConfig::default());
//! let sequencer = sequencer::Sequencer::new(pool, source, sequencer::SequencerConfig::default());
//! sequencer.run(std::future::pending()).await?;
//! # Ok(())
//! # }
//! ```
//!
//! # Features
//!
#![cfg_attr(
    feature = "derive",
    doc = " - `derive` (default): `#[derive(pges::Event)]`, also reachable as [`derive`](mod@derive)`::Event`."
)]
#![cfg_attr(
    not(feature = "derive"),
    doc = " - `derive` (default): `#[derive(pges::Event)]`, also reachable as `derive::Event`."
)]
#![cfg_attr(
    feature = "subscribe",
    doc = " - `subscribe` (default): [`subscribe`], without wakers; a subscription then rechecks the log on a timer."
)]
#![cfg_attr(
    not(feature = "subscribe"),
    doc = " - `subscribe` (default): `subscribe`, without wakers; a subscription then rechecks the log on a timer."
)]
//! - `wake-grpc` (default), `wake-poll`, `wake-inproc`, `wake-notify`: the wakers of `pges-subscribe`'s
//!   features of the same names; each turns on `subscribe`. `wake-grpc` adds tonic. `wake-grpc-tls` adds
//!   `https` endpoints to it (rustls).
//! - `sequencer`: `pges::sequencer` with the outbox and logical sources, as a direct `pges-sequencer`
//!   dependency has by default. It turns on `sequencer-outbox` and `source-logical`.
//! - `sequencer-outbox`: `pges::sequencer` with the outbox source only, without `pges-replication`.
//! - `source-logical`: the logical source. It turns on `sequencer-outbox` and `replication`, for the
//!   connection config the source takes, and TLS for replication connections.
//! - `watch-notify`: `pges-sequencer`'s feature of the same name, the sequencer's `NOTIFY` after each
//!   batch; it turns on `sequencer-outbox`.
//! - `watch-grpc`: `pges-sequencer`'s feature of the same name, the gRPC watch server; it turns on
//!   `sequencer-outbox`. `watch-grpc-tls` serves it over TLS (rustls).
//! - `replication`: `pges::replication`; `tls` adds TLS to it.
//! - `otel`: W3C trace context through `tracing-opentelemetry`. An append whose metadata has no trace
//!   records the current `tracing` span's, and delivered events give theirs back as an OpenTelemetry
//!   parent context (`trace_parent()`). It turns on the `otel` features of `pges-core`, `pges-postgres` and,
//!   with `subscribe`, `pges-subscribe`.
//!
//! [`core`] and [`postgres`] are always available. Applications also depend on `sqlx` 0.9 for pools and
//! connections.
//!
//! # The derive's path to `pges-core`
//!
//! The code `#[derive(pges::Event)]` generates names `pges-core` through this crate, as `::pges::core`,
//! found in the calling crate's `Cargo.toml` under whatever name it gives `pges`. A crate that reaches
//! `pges` some other way, such as through a re-export of its own, names the path with
//! `#[event(crate = "...")]`.
#![cfg_attr(docsrs, feature(doc_cfg))]

#[doc(inline)]
pub use pges_core_dep as core;
#[cfg(feature = "derive")]
#[doc(inline)]
pub use pges_derive as derive;
#[cfg(feature = "derive")]
pub use pges_derive::Event;
#[doc(inline)]
pub use pges_postgres as postgres;
#[cfg(feature = "replication")]
#[doc(inline)]
pub use pges_replication as replication;
#[cfg(feature = "sequencer-outbox")]
#[doc(inline)]
pub use pges_sequencer as sequencer;
#[cfg(feature = "subscribe")]
#[doc(inline)]
pub use pges_subscribe as subscribe;

#[cfg(all(doctest, feature = "derive"))]
#[doc = include_str!("../README.md")]
struct ReadmeDoctests;
