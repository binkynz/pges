//! A minimal PostgreSQL logical replication client: the protocol subset the pges sequencer needs.
//!
//! - [`connection::ReplicationConnection`] opens a `replication=database` connection over TCP or a unix
//!   socket, optionally with TLS (below), and authenticates with trust, cleartext, MD5, SCRAM-SHA-256, or
//!   SCRAM-SHA-256-PLUS with channel binding over TLS.
//! - It runs `IDENTIFY_SYSTEM`, `CREATE_REPLICATION_SLOT … LOGICAL pgoutput` (optionally with `FAILOVER`),
//!   `DROP_REPLICATION_SLOT` and `START_REPLICATION … LOGICAL` with pgoutput protocol version 4 and
//!   `streaming 'off'`, so every transaction arrives whole, in commit order.
//! - [`stream::ReplicationStream`] yields XLogData decoded into typed [`pgoutput::Message`]s, answers
//!   keepalives, and confirms progress to the slot through [`stream::ReplicationStream::confirm`].
//! - WAL positions are [`pges_core::lsn::Lsn`]s; slot and publication names are validated ([`name`]).
//!
//! Message encoding and SCRAM come from the published `postgres-protocol` crate; IO is Tokio.
//!
//! ```no_run
//! use pges_core::lsn;
//! use pges_replication::{config, connection, pgoutput, stream};
//!
//! # async fn run() -> Result<(), Box<dyn std::error::Error>> {
//! let config = config::Config::from_url("postgres://sequencer:secret@db:5432/app")?;
//! let slot = pges_replication::slot_name!("pges_16384");
//! let publication = pges_replication::publication_name!("pges");
//!
//! let connection = connection::ReplicationConnection::connect(&config).await?;
//! let mut stream = connection
//!     .start(&connection::StartOptions::new(slot, publication))
//!     .await?;
//! let mut relations = pgoutput::Relations::new();
//! while let Some(message) = stream.next().await? {
//!     let stream::ReplicationMessage::XLogData(data) = message else {
//!         continue;
//!     };
//!     match data.message {
//!         pgoutput::Message::Relation(relation) => relations.insert(relation),
//!         pgoutput::Message::Insert(insert) => {
//!             // Route by relation id: leaf partitions announce Relations too, but with
//!             // publish_via_partition_root the Insert carries the root's id.
//!             let table = relations.get(insert.relation);
//!             println!("{:?}: {:?}", table.map(|t| &t.name), insert.new.text(0));
//!         }
//!         pgoutput::Message::Commit(commit) => {
//!             // Once the transaction's effects are durable:
//!             stream.confirm(commit.end_lsn).await?;
//!         }
//!         _ => {}
//!     }
//! }
//! # Ok(())
//! # }
//! ```
//!
//! # TLS
//!
//! The `tls` feature adds TLS through rustls with the ring crypto provider; no OpenSSL is involved. The
//! settings and their defaults follow libpq: `sslmode` (default `prefer` with the feature, `disable`
//! without), `sslrootcert` (a PEM file, or `system`), `sslnegotiation` (`postgres`, or `direct` for
//! PostgreSQL 17+) and `channel_binding` (default `prefer` with the feature). See [`config::Config`].
//!
//! ```no_run
//! # #[cfg(feature = "tls")]
//! # async fn run() -> Result<(), Box<dyn std::error::Error>> {
//! use pges_replication::{config, connection};
//!
//! let config = config::Config::from_url(
//!     "postgres://sequencer:secret@db.example:5432/app\
//!      ?sslmode=verify-full&sslrootcert=/etc/pges/ca.pem&channel_binding=require",
//! )?;
//! let connection = connection::ReplicationConnection::connect(&config).await?;
//! assert_eq!(connection.encryption(), connection::Encryption::Tls);
//! assert_eq!(
//!     connection.authentication(),
//!     connection::Authentication::ScramSha256Plus
//! );
//! # Ok(())
//! # }
//! ```
//!
//! Differences from libpq: without `sslrootcert`, `verify-ca` and `verify-full` use the platform's trust
//! store (libpq reads `~/.postgresql/root.crt`); `verify-full` matches the host against the certificate's
//! subject alternative names only, not its common name; an `sslrootcert` file that is missing or holds no
//! certificate is an error in every mode that tries TLS (libpq ignores a missing file below `verify-ca`);
//! client certificates (`sslcert`, `sslkey`) are not supported.
//!
//! # Cancellation
//!
//! Every async method documents whether it is cancel safe. In short: [`stream::ReplicationStream::next`],
//! [`stream::ReplicationStream::confirm`] and [`stream::ReplicationStream::send_status`] are, so they can sit
//! in a `tokio::select!` loop; replication commands on [`connection::ReplicationConnection`] are not, and a
//! connection whose command was cancelled refuses further commands.
#![cfg_attr(docsrs, feature(doc_cfg))]
#![warn(missing_docs)]

pub mod config;
pub mod connection;
pub mod error;
pub mod name;
pub mod pgoutput;
pub mod stream;

#[cfg(doctest)]
#[doc = include_str!("../README.md")]
struct ReadmeDoctests;

mod time;
#[cfg(feature = "tls")]
mod tls;
mod wire;
