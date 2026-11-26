//! The pges sequencer: it orders committed appends into each store's log, gap-free, so subscribers can
//! follow a store or a category with a plain integer cursor.
//!
//! - [`sequencer::Sequencer`] runs one [`source::Source`]. Several sequencers can run against one
//!   database; a lease elects one leader and the others stand by. A leader that lost its lease is fenced in
//!   the database and cannot write.
//! - [`source::Source`] is how the sequencer learns which commits exist. `logical::LogicalSource` (feature
//!   `source-logical`, on by default) reads them in WAL commit order from a logical replication slot;
//!   [`outbox::OutboxSource`] drains the `pending` queue of stores created with the outbox option. A
//!   database is sequenced by one kind of source.
//! - [`watch::Watch`] announces each store's [`HighWater`](pges_core::high_water::HighWater) and the
//!   sequencer's [`Status`](sequencer::Status) through `tokio::sync::watch` channels. With the
//!   `watch-notify` feature the sequencer also sends `NOTIFY pges_<store>` after each batch, with a
//!   [`Notice`](pges_core::high_water::Notice) as payload. With `watch-grpc`, `grpc::WatchService` streams
//!   the same values to other processes over gRPC. A run that stops on an error that needs an
//!   operator, such as a lost replication slot, ends in [`Status::Halted`](sequencer::Status::Halted):
//!   alert on it.
//!
//! # Deploying with the logical source
//!
//! A replication slot only delivers commits from its creation on. Create it before the first append: after
//! the migrations and before any writer starts, run `pges-sequencer bootstrap` (or
//! `logical::LogicalSource::bootstrap`). A sequencer that creates the slot itself refuses to start if a
//! store already holds commits the slot would never deliver, instead of skipping them.
//!
//! If the slot is lost (invalidated by the server, dropped, or created after the first commits), the
//! sequencer halts and every sequencer refuses to start. Stop them all, run `pges-sequencer resequence`
//! (`logical::LogicalSource::resequence`), then start them again: they resume from a new slot, and the
//! commits the old one never delivered are sequenced in commit id order.
//!
//! # Features
//!
//! - `source-logical` (default): the logical replication source, through `pges-replication` with TLS.
//! - `watch-notify`: `NOTIFY pges_<store>` after each batch (`SequencerConfig::notify`).
#![cfg_attr(
    feature = "watch-grpc",
    doc = "- `watch-grpc`: the gRPC watch server, [`grpc::WatchService`] (`pges.watch.v1.Watch`, tonic). With `bin`, `--grpc-listen <addr>` serves it."
)]
#![cfg_attr(
    not(feature = "watch-grpc"),
    doc = "- `watch-grpc`: the gRPC watch server, `grpc::WatchService` (`pges.watch.v1.Watch`, tonic). With `bin`, `--grpc-listen <addr>` serves it."
)]
//! - `watch-grpc-tls`: `watch-grpc` over TLS, optionally requiring client certificates (`grpc::ServerTls`;
//!   rustls with ring). With `bin`, `--grpc-tls-cert`, `--grpc-tls-key` and `--grpc-tls-client-ca`.
//! - `bin`: the `pges-sequencer` binary. It reads its settings from flags or `PGES_*` environment variables
//!   (`pges-sequencer --help`), logs status changes to stderr, shuts down gracefully on SIGINT or SIGTERM,
//!   and exits with status 1 when the run stops on an error. `pges-sequencer bootstrap` creates the logical
//!   source's slot and exits; `pges-sequencer resequence` recovers a halted database and exits.
//!
//! ```no_run
//! # async fn example(
//! #     pool: sqlx::PgPool,
//! #     store: pges_postgres::store::Store,
//! # ) -> Result<(), Box<dyn std::error::Error>> {
//! use pges_sequencer::{outbox, sequencer};
//!
//! let source = outbox::OutboxSource::new(pool.clone(), outbox::OutboxConfig::default());
//! let mut config = sequencer::SequencerConfig::default();
//! config.batch_size = std::num::NonZeroUsize::new(500).unwrap_or(std::num::NonZeroUsize::MIN);
//! let sequencer = sequencer::Sequencer::new(pool, source, config);
//!
//! let mut high_water = sequencer.watch().high_water(store.store_id());
//! let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
//! let running = tokio::spawn(sequencer.run(async {
//!     let _ = stopped.await;
//! }));
//!
//! high_water.changed().await?;
//! println!("{} is sequenced up to {}", store.name(), high_water.borrow().store());
//!
//! let _ = stop.send(());
//! running.await??;
//! # Ok(())
//! # }
//! ```
//!
//! The logical source needs a replication connection besides the pool:
//!
//! ```no_run
//! # #[cfg(feature = "source-logical")]
//! # async fn example(pool: sqlx::PgPool) -> Result<(), Box<dyn std::error::Error>> {
//! use pges_replication::config;
//! use pges_sequencer::{logical, sequencer};
//!
//! let replication = config::Config::from_url("postgres://sequencer:secret@db:5432/app")?;
//! let source = logical::LogicalSource::new(pool.clone(), replication, logical::LogicalConfig::default());
//! let sequencer = sequencer::Sequencer::new(pool, source, sequencer::SequencerConfig::default());
//! sequencer.run(std::future::pending()).await?;
//! # Ok(())
//! # }
//! ```
//!
//! # Positions
//!
//! Each batch is one transaction: the fence check, the source's reads (for the outbox, the `DELETE` from
//! `pending`), and one `INSERT` into each store's `log`. Positions continue from the newest log row of the
//! store, so the log has no gaps and no duplicates, also across restarts and takeovers. Within a store,
//! positions follow source order: the logical source delivers transactions in WAL commit order, and the
//! outbox orders by `pending.seq`. Both respect causality: if one append could see another's commit, it is
//! sequenced after it.
//!
//! # Coarse token
//!
//! The source's resume token and the coarse high-water marks (`pges.stores.last_position`,
//! `categories.last_position`) are written at most once per [`token_interval`], in a batch transaction or
//! alone when idle, and only then acknowledged to the source. After a restart a source may deliver again
//! what came after its stored token; the sequencer drops those commits because it seeds its dedupe window
//! from the log rows above `pges.stores.last_position`. The outbox needs no token (a drained row is gone in
//! the same transaction as its log row is written); the logical source stores the end LSN of the last
//! transaction of a batch and confirms it to its slot on acknowledgement. The transaction that stores the
//! token commits synchronously (`synchronous_commit = on`), so a server crash cannot lose log rows the slot
//! was already confirmed past.
//!
//! [`token_interval`]: sequencer::SequencerConfig::token_interval
#![cfg_attr(docsrs, feature(doc_cfg))]

#[cfg(feature = "watch-grpc")]
pub mod grpc;
#[cfg(feature = "source-logical")]
pub mod logical;
pub mod outbox;
pub mod sequencer;
pub mod source;
#[doc(hidden)]
pub mod sql;
pub mod watch;

#[cfg(all(doctest, feature = "source-logical"))]
#[doc = include_str!("../README.md")]
struct ReadmeDoctests;

mod lease;
mod log;
#[cfg(feature = "watch-notify")]
mod notify;
