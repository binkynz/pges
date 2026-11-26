//! The replication connection and its commands.

use std::{fmt, io, str};

use fallible_iterator::FallibleIterator;
use postgres_protocol::{
    authentication::{self, sasl},
    message::{backend, frontend},
};

use pges_core::{lsn, non_empty};

use crate::{config, error, name, stream, wire};

/// A connection in logical replication mode (`replication=database`), ready for replication commands.
///
/// [`ReplicationConnection::start`] turns it into a [`stream::ReplicationStream`].
///
/// Commands are not cancel safe: if a command's future is dropped before it completes, the connection is left
/// mid-command, and every later command fails with [`error::CommandError::Protocol`]. Drop it and connect
/// again.
pub struct ReplicationConnection {
    pub(crate) wire: wire::Wire,
    state: State,
    server_version: String,
    process_id: i32,
    authentication: Authentication,
}

impl ReplicationConnection {
    /// Connects and authenticates: trust, cleartext password, MD5, SCRAM-SHA-256, or, over TLS,
    /// SCRAM-SHA-256-PLUS with channel binding.
    ///
    /// TLS (with the `tls` feature) follows the config's [`config::SslMode`]. As in libpq,
    /// [`config::SslMode::Prefer`] tries TLS first and connects again without it when the handshake fails or
    /// the server rejects the encrypted connection with SQLSTATE `28000` (no `pg_hba.conf` entry), and
    /// [`config::SslMode::Allow`] does the reverse. Unix sockets never use TLS.
    ///
    /// The role needs the `REPLICATION` attribute, and `pg_hba.conf` must let it connect to the database (a
    /// logical replication connection is matched against the database name, not the `replication` keyword).
    ///
    /// Fails with [`error::ConnectError::InvalidConfig`] when the config fails [`config::Config::check`],
    /// [`error::ConnectError::Tls`] when TLS is required and cannot be established or the certificate is
    /// rejected, and [`error::ConnectError::ChannelBindingRequired`] when `channel_binding=require` cannot be
    /// met.
    ///
    /// Cancel safe in the sense that dropping the future drops the half-open connection and nothing else.
    pub async fn connect(config: &config::Config) -> Result<Self, error::ConnectError> {
        config.check()?;

        let (first, fallback) = attempts(config);
        match (Self::connect_with(config, first).await, fallback) {
            (Err(error), Some(fallback)) if worth_another_attempt(&error) => {
                Self::connect_with(config, fallback).await
            }
            (result, _) => result,
        }
    }

    /// Whether the connection is encrypted.
    pub fn encryption(&self) -> Encryption {
        self.wire.encryption()
    }

    /// How the server authenticated this client.
    pub fn authentication(&self) -> Authentication {
        self.authentication
    }

    /// The server's `server_version`, e.g. `18.3`.
    pub fn server_version(&self) -> &str {
        &self.server_version
    }

    /// The process id of the walsender serving this connection, as shown in `pg_stat_replication.pid` and
    /// `pg_replication_slots.active_pid`.
    pub fn process_id(&self) -> i32 {
        self.process_id
    }

    /// `IDENTIFY_SYSTEM`: the cluster's identity and current WAL flush position.
    ///
    /// Not cancel safe (see [`ReplicationConnection`]).
    pub async fn identify_system(&mut self) -> Result<SystemIdentity, error::CommandError> {
        let rows = self.simple_query("IDENTIFY_SYSTEM").await?;
        let [row] = rows.as_slice() else {
            return Err(protocol("IDENTIFY_SYSTEM returned no single row"));
        };

        Ok(SystemIdentity {
            system_id: SystemId(parse_column(row, 0, "systemid")?),
            timeline: TimelineId(parse_column(row, 1, "timeline")?),
            wal_flush: parse_column(row, 2, "xlogpos")?,
            database: column(row, 3).map(str::to_owned),
        })
    }

    /// `CREATE_REPLICATION_SLOT <slot> LOGICAL pgoutput (SNAPSHOT 'nothing')`, with `FAILOVER true` when
    /// asked, so the slot is synchronised to standbys and survives promotion (PostgreSQL 17+).
    ///
    /// Fails with SQLSTATE `42710` ([`error::SqlState::DUPLICATE_OBJECT`]) when the slot exists. Not cancel
    /// safe (see [`ReplicationConnection`]).
    pub async fn create_slot(
        &mut self,
        slot: &name::SlotName,
        failover: Failover,
    ) -> Result<CreatedSlot, error::CommandError> {
        self.create_slot_with(slot, failover, SlotSnapshot::Nothing)
            .await
    }

    /// [`ReplicationConnection::create_slot`], choosing what happens to the snapshot at the slot's
    /// consistent point (`SNAPSHOT 'nothing' | 'export'`). With [`SlotSnapshot::Export`] the result carries
    /// the snapshot's name: a transaction that imports it sees exactly the transactions that committed
    /// before the consistent point, which the slot will not stream. It stays importable until this
    /// connection runs its next command or closes.
    ///
    /// Fails with SQLSTATE `42710` ([`error::SqlState::DUPLICATE_OBJECT`]) when the slot exists. Not cancel
    /// safe (see [`ReplicationConnection`]).
    pub async fn create_slot_with(
        &mut self,
        slot: &name::SlotName,
        failover: Failover,
        snapshot: SlotSnapshot,
    ) -> Result<CreatedSlot, error::CommandError> {
        let failover = match failover {
            Failover::Enabled => ", FAILOVER true",
            Failover::Disabled => "",
        };
        let action = match snapshot {
            SlotSnapshot::Nothing => "nothing",
            SlotSnapshot::Export => "export",
        };
        let rows = self
            .simple_query(&format!(
                "CREATE_REPLICATION_SLOT {slot} LOGICAL pgoutput (SNAPSHOT '{action}'{failover})"
            ))
            .await?;
        let [row] = rows.as_slice() else {
            return Err(protocol("CREATE_REPLICATION_SLOT returned no single row"));
        };
        let exported = match snapshot {
            SlotSnapshot::Nothing => None,
            SlotSnapshot::Export => Some(parse_column(row, 2, "snapshot_name")?),
        };

        Ok(CreatedSlot {
            name: slot.clone(),
            consistent_point: parse_column(row, 1, "consistent_point")?,
            snapshot: exported,
        })
    }

    /// `ALTER_REPLICATION_SLOT <slot> (FAILOVER true | false)`: whether an existing logical slot is
    /// synchronised to physical standbys (PostgreSQL 17+).
    ///
    /// Waits while another process uses the slot; the server has no option to fail instead. Fails with
    /// SQLSTATE `42704` ([`error::SqlState::UNDEFINED_OBJECT`]) when the slot does not exist. Not cancel safe
    /// (see [`ReplicationConnection`]).
    pub async fn alter_slot_failover(
        &mut self,
        slot: &name::SlotName,
        failover: Failover,
    ) -> Result<(), error::CommandError> {
        let failover = match failover {
            Failover::Enabled => "true",
            Failover::Disabled => "false",
        };
        self.simple_query(&format!(
            "ALTER_REPLICATION_SLOT {slot} (FAILOVER {failover})"
        ))
        .await?;
        Ok(())
    }

    /// `DROP_REPLICATION_SLOT <slot>`, optionally waiting while another process uses the slot.
    ///
    /// Fails with SQLSTATE `42704` ([`error::SqlState::UNDEFINED_OBJECT`]) when the slot does not exist, and
    /// with `55006` ([`error::SqlState::OBJECT_IN_USE`]) when it is active and `wait` is
    /// [`DropWait::NoWait`]. Not cancel safe (see [`ReplicationConnection`]).
    pub async fn drop_slot(
        &mut self,
        slot: &name::SlotName,
        wait: DropWait,
    ) -> Result<(), error::CommandError> {
        let wait = match wait {
            DropWait::Wait => " WAIT",
            DropWait::NoWait => "",
        };
        self.simple_query(&format!("DROP_REPLICATION_SLOT {slot}{wait}"))
            .await?;
        Ok(())
    }

    /// `START_REPLICATION SLOT <slot> LOGICAL <lsn> (proto_version '4', publication_names '…', streaming
    /// 'off', …)`, then streams.
    ///
    /// The server starts at the later of `options`' start LSN and the slot's confirmed position. Transactions
    /// arrive whole and in commit order. Fails with SQLSTATE `55006` while another process streams from the
    /// slot.
    ///
    /// Not cancel safe: dropping the future drops the connection.
    pub async fn start(
        mut self,
        options: &StartOptions,
    ) -> Result<stream::ReplicationStream, error::CommandError> {
        self.ready()?;

        let publications = options
            .publications
            .iter()
            .map(name::PublicationName::as_str)
            .collect::<Vec<_>>()
            .join(",");
        let messages = match options.messages {
            Messages::Include => "true",
            Messages::Exclude => "false",
        };
        let command = format!(
            "START_REPLICATION SLOT {} LOGICAL {} (proto_version '4', publication_names '{publications}', \
             streaming 'off', messages '{messages}')",
            options.slot, options.start,
        );
        frontend::query(&command, &mut self.wire.write_buf)?;
        self.state = State::Busy;
        self.wire.flush().await?;

        let mut server_error = None;
        loop {
            match self.wire.read().await? {
                wire::Backend::CopyBothResponse => {
                    return Ok(stream::ReplicationStream::new(self, options.start));
                }
                wire::Backend::Message(backend::Message::ErrorResponse(body)) => {
                    let error = error::server_error(&body)?;
                    // FATAL and PANIC close the socket without ReadyForQuery.
                    if error.ends_session() {
                        return Err(error.into());
                    }
                    server_error = Some(error);
                }
                wire::Backend::Message(backend::Message::NoticeResponse(_)) => {}
                wire::Backend::Message(backend::Message::ReadyForQuery(_)) => {
                    return Err(match server_error {
                        Some(error) => error.into(),
                        None => protocol("START_REPLICATION ended without streaming"),
                    });
                }
                _ => return Err(protocol("unexpected reply to START_REPLICATION")),
            }
        }
    }

    /// Ends the session politely (`Terminate`) and closes the socket.
    ///
    /// Cancel safe: dropping the future closes the socket without the goodbye.
    pub async fn close(mut self) -> io::Result<()> {
        frontend::terminate(&mut self.wire.write_buf);
        self.wire.shutdown().await
    }

    /// Marks the connection ready for commands again, after streaming ended cleanly.
    pub(crate) fn set_idle(&mut self) {
        self.state = State::Idle;
    }

    async fn connect_with(
        config: &config::Config,
        tls: wire::Tls,
    ) -> Result<Self, error::ConnectError> {
        let mut wire = wire::Wire::connect(config, tls).await?;
        frontend::startup_message(
            [
                ("user", config.get_user()),
                ("database", config.get_database()),
                ("replication", "database"),
                ("application_name", config.get_application_name()),
                ("client_encoding", "UTF8"),
            ],
            &mut wire.write_buf,
        )?;
        wire.flush().await?;

        let authentication = authenticate(&mut wire, config).await?;

        let mut server_version = String::new();
        let mut process_id = 0;
        loop {
            match wire.read().await? {
                wire::Backend::Message(backend::Message::ReadyForQuery(_)) => break,
                wire::Backend::Message(backend::Message::ParameterStatus(body)) => {
                    if body.name()? == "server_version" {
                        server_version = body.value()?.to_owned();
                    }
                }
                wire::Backend::Message(backend::Message::BackendKeyData(body)) => {
                    process_id = body.process_id();
                }
                wire::Backend::Message(backend::Message::NoticeResponse(_)) => {}
                wire::Backend::Message(backend::Message::ErrorResponse(body)) => {
                    return Err(error::server_error(&body)?.into());
                }
                _ => {
                    return Err(error::ConnectError::Protocol(
                        "unexpected message after authentication".to_owned(),
                    ));
                }
            }
        }

        Ok(Self {
            wire,
            state: State::Idle,
            server_version,
            process_id,
            authentication,
        })
    }

    fn ready(&self) -> Result<(), error::CommandError> {
        match self.state {
            State::Idle => Ok(()),
            State::Busy => Err(protocol(
                "an earlier command was cancelled; the connection must be discarded",
            )),
        }
    }

    async fn simple_query(&mut self, sql: &str) -> Result<Vec<Row>, error::CommandError> {
        self.ready()?;

        frontend::query(sql, &mut self.wire.write_buf)?;
        self.state = State::Busy;
        self.wire.flush().await?;

        let mut rows = Vec::new();
        let mut server_error = None;
        loop {
            match self.wire.read().await? {
                wire::Backend::Message(backend::Message::DataRow(body)) => {
                    rows.push(row(&body)?);
                }
                wire::Backend::Message(backend::Message::ErrorResponse(body)) => {
                    let error = error::server_error(&body)?;
                    // FATAL and PANIC close the socket without ReadyForQuery.
                    if error.ends_session() {
                        return Err(error.into());
                    }
                    server_error = Some(error);
                }
                wire::Backend::Message(
                    backend::Message::RowDescription(_)
                    | backend::Message::CommandComplete(_)
                    | backend::Message::EmptyQueryResponse
                    | backend::Message::NoticeResponse(_)
                    | backend::Message::ParameterStatus(_),
                ) => {}
                wire::Backend::Message(backend::Message::ReadyForQuery(_)) => {
                    self.state = State::Idle;
                    return match server_error {
                        Some(error) => Err(error.into()),
                        None => Ok(rows),
                    };
                }
                _ => return Err(protocol("unexpected reply to a replication command")),
            }
        }
    }
}

impl fmt::Debug for ReplicationConnection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ReplicationConnection")
            .field("server_version", &self.server_version)
            .field("process_id", &self.process_id)
            .field("encryption", &self.encryption())
            .field("authentication", &self.authentication)
            .finish_non_exhaustive()
    }
}

/// Whether a connection is encrypted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum Encryption {
    /// Not encrypted: a unix socket, or TCP without TLS.
    Plain,
    /// TLS.
    Tls,
}

/// How the server authenticated the client.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum Authentication {
    /// The server accepted the client without a password (e.g. `trust` or `peer`).
    WithoutPassword,
    /// A cleartext password (`password`).
    Cleartext,
    /// An MD5 password hash (`md5`).
    Md5,
    /// SCRAM-SHA-256 without channel binding.
    ScramSha256,
    /// SCRAM-SHA-256-PLUS: SCRAM bound to the TLS channel (`tls-server-end-point`).
    ScramSha256Plus,
}

/// The answer to `IDENTIFY_SYSTEM`.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct SystemIdentity {
    /// The cluster's unique system identifier.
    pub system_id: SystemId,
    /// The current timeline.
    pub timeline: TimelineId,
    /// The current WAL flush position.
    pub wal_flush: lsn::Lsn,
    /// The database this connection is bound to.
    pub database: Option<String>,
}

/// A cluster's system identifier (`pg_control_system().system_identifier`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SystemId(pub u64);

/// A WAL timeline id.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct TimelineId(pub u32);

/// A slot created by [`ReplicationConnection::create_slot`].
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct CreatedSlot {
    /// The slot's name.
    pub name: name::SlotName,
    /// The LSN from which the slot decodes: transactions committing after it are streamed.
    pub consistent_point: lsn::Lsn,
    /// The exported snapshot, with [`SlotSnapshot::Export`].
    pub snapshot: Option<name::SnapshotName>,
}

/// What [`ReplicationConnection::create_slot_with`] does with the snapshot at the new slot's consistent
/// point.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum SlotSnapshot {
    /// Nothing (`SNAPSHOT 'nothing'`).
    Nothing,
    /// Export it (`SNAPSHOT 'export'`), so another connection can import it with `SET TRANSACTION SNAPSHOT`.
    Export,
}

/// Whether a slot is synchronised to physical standbys (`FAILOVER`): set when it is created
/// ([`ReplicationConnection::create_slot`]) or changed later ([`ReplicationConnection::alter_slot_failover`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum Failover {
    /// Synchronised; the slot survives promotion of a standby that has `sync_replication_slots` on.
    Enabled,
    /// Not synchronised.
    Disabled,
}

/// Whether dropping an active slot waits for it to become inactive.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum DropWait {
    /// Wait until no process uses the slot.
    Wait,
    /// Fail with `55006` if a process uses it.
    NoWait,
}

/// What [`ReplicationConnection::start`] asks for.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StartOptions {
    slot: name::SlotName,
    publications: non_empty::NonEmpty<name::PublicationName>,
    start: lsn::Lsn,
    messages: Messages,
}

impl StartOptions {
    /// Streams the changes of `publications` from `slot`, starting at the slot's confirmed position, without
    /// logical messages.
    ///
    /// `publications` is one [`PublicationName`](name::PublicationName), an array of one to eight, or a
    /// [`NonEmpty`](non_empty::NonEmpty) list, so an empty list does not compile:
    ///
    /// ```
    /// use pges_core::non_empty;
    /// use pges_replication::{connection, name};
    ///
    /// let slot = pges_replication::slot_name!("pges_16384");
    /// let jobs = pges_replication::publication_name!("jobs");
    /// let audit = pges_replication::publication_name!("audit");
    ///
    /// connection::StartOptions::new(slot.clone(), jobs.clone());
    /// connection::StartOptions::new(slot.clone(), [jobs.clone(), audit.clone()]);
    /// connection::StartOptions::new(slot, non_empty![jobs, audit]);
    /// ```
    ///
    /// ```compile_fail
    /// use pges_replication::{connection, name};
    ///
    /// let none: [name::PublicationName; 0] = [];
    /// connection::StartOptions::new(pges_replication::slot_name!("pges_16384"), none);
    /// ```
    pub fn new(slot: name::SlotName, publications: impl Publications) -> Self {
        Self {
            slot,
            publications: sealed::Sealed::into_non_empty(publications),
            start: lsn::Lsn::ZERO,
            messages: Messages::Exclude,
        }
    }

    /// Starts at `lsn` instead, when it is later than the slot's confirmed position.
    pub fn start_at(mut self, lsn: lsn::Lsn) -> Self {
        self.start = lsn;
        self
    }

    /// Whether `pg_logical_emit_message` messages are sent.
    pub fn messages(mut self, messages: Messages) -> Self {
        self.messages = messages;
        self
    }
}

/// At least one publication for [`StartOptions::new`]: a [`PublicationName`](name::PublicationName), an
/// array of one to eight, or a [`NonEmpty`](non_empty::NonEmpty) list of any length. Sealed: an empty
/// array has no implementation, so it is a compile error instead of a server error at `START_REPLICATION`.
#[diagnostic::on_unimplemented(
    message = "`{Self}` is not a list of at least one publication",
    label = "expected a `PublicationName`, an array of one to eight, or a `NonEmpty` list",
    note = "START_REPLICATION needs at least one publication; for a list known only at run time, use \
            `NonEmpty::try_from(vec)`"
)]
pub trait Publications: sealed::Sealed {}

impl Publications for name::PublicationName {}

impl Publications for non_empty::NonEmpty<name::PublicationName> {}

/// Whether logical messages (`pg_logical_emit_message`) are streamed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum Messages {
    /// Stream them as [`pgoutput::Message::Logical`](crate::pgoutput::Message::Logical).
    Include,
    /// Leave them out.
    Exclude,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum State {
    Idle,
    Busy,
}

type Row = Vec<Option<String>>;

/// The connection attempt to make first, and the one to make if it fails in a way that another transport
/// might fix.
fn attempts(config: &config::Config) -> (wire::Tls, Option<wire::Tls>) {
    if matches!(config.get_host(), config::Host::Unix(_)) {
        return (wire::Tls::Off, None);
    }

    match config.get_ssl_mode() {
        config::SslMode::Disable => (wire::Tls::Off, None),
        #[cfg(feature = "tls")]
        config::SslMode::Allow => (wire::Tls::Off, Some(wire::Tls::Required)),
        #[cfg(feature = "tls")]
        config::SslMode::Prefer => (wire::Tls::IfOffered, Some(wire::Tls::Off)),
        #[cfg(feature = "tls")]
        config::SslMode::Require | config::SslMode::VerifyCa | config::SslMode::VerifyFull => {
            (wire::Tls::Required, None)
        }
        // `Config::check` rejects the modes that need TLS.
        #[cfg(not(feature = "tls"))]
        _ => (wire::Tls::Off, None),
    }
}

/// Whether a failed attempt may succeed with the other transport: the TLS handshake failed, or
/// `pg_hba.conf` has no entry for this transport (`hostssl` or `hostnossl`).
fn worth_another_attempt(error: &error::ConnectError) -> bool {
    match error {
        error::ConnectError::Tls(
            error::TlsError::Certificate(_) | error::TlsError::Handshake(_),
        ) => true,
        error::ConnectError::Server(server) => {
            server.code == error::SqlState::INVALID_AUTHORIZATION_SPECIFICATION
        }
        _ => false,
    }
}

async fn authenticate(
    wire: &mut wire::Wire,
    config: &config::Config,
) -> Result<Authentication, error::ConnectError> {
    let binding_required = config.get_channel_binding() == config::ChannelBinding::Require;
    let mut method = Authentication::WithoutPassword;
    let mut scram: Option<sasl::ScramSha256> = None;
    loop {
        let message = match wire.read().await? {
            wire::Backend::Message(message) => message,
            wire::Backend::CopyBothResponse => {
                return Err(error::ConnectError::Protocol(
                    "unexpected CopyBothResponse during authentication".to_owned(),
                ));
            }
        };
        match message {
            backend::Message::AuthenticationOk => {
                // Without the server's final SCRAM message, the server never proved that it knows the
                // password, nor that it sees the same TLS channel.
                if scram.is_some() {
                    return Err(error::ConnectError::Protocol(
                        "authentication succeeded before the SCRAM exchange finished".to_owned(),
                    ));
                }
                if binding_required && method != Authentication::ScramSha256Plus {
                    return Err(error::ConnectError::ChannelBindingRequired);
                }
                return Ok(method);
            }
            backend::Message::AuthenticationCleartextPassword => {
                if binding_required {
                    return Err(error::ConnectError::ChannelBindingRequired);
                }
                let password = config
                    .password_bytes()
                    .ok_or(error::ConnectError::PasswordRequired)?;

                frontend::password_message(password, &mut wire.write_buf)?;
                method = Authentication::Cleartext;
            }
            backend::Message::AuthenticationMd5Password(body) => {
                if binding_required {
                    return Err(error::ConnectError::ChannelBindingRequired);
                }
                let password = config
                    .password_bytes()
                    .ok_or(error::ConnectError::PasswordRequired)?;

                let hash =
                    authentication::md5_hash(config.get_user().as_bytes(), password, body.salt());
                frontend::password_message(hash.as_bytes(), &mut wire.write_buf)?;
                method = Authentication::Md5;
            }
            backend::Message::AuthenticationSasl(body) => {
                let mut offered = Vec::new();
                let mut mechanisms = body.mechanisms();
                while let Some(mechanism) = mechanisms.next()? {
                    offered.push(mechanism.to_owned());
                }
                let (mechanism, binding) = scram_mechanism(&offered, wire, config)?;
                let password = config
                    .password_bytes()
                    .ok_or(error::ConnectError::PasswordRequired)?;

                let client = sasl::ScramSha256::new(password, binding);
                frontend::sasl_initial_response(mechanism, client.message(), &mut wire.write_buf)?;
                scram = Some(client);
                method = if mechanism == sasl::SCRAM_SHA_256_PLUS {
                    Authentication::ScramSha256Plus
                } else {
                    Authentication::ScramSha256
                };
            }
            backend::Message::AuthenticationSaslContinue(body) => {
                let client = scram.as_mut().ok_or_else(|| {
                    error::ConnectError::Protocol("SASL continue without SASL start".to_owned())
                })?;
                client
                    .update(body.data())
                    .map_err(error::ConnectError::Scram)?;
                frontend::sasl_response(client.message(), &mut wire.write_buf)?;
            }
            backend::Message::AuthenticationSaslFinal(body) => {
                let mut client = scram.take().ok_or_else(|| {
                    error::ConnectError::Protocol("SASL final without SASL start".to_owned())
                })?;
                client
                    .finish(body.data())
                    .map_err(error::ConnectError::Scram)?;
            }
            backend::Message::ErrorResponse(body) => {
                return Err(error::server_error(&body)?.into());
            }
            backend::Message::NoticeResponse(_) => {}
            backend::Message::AuthenticationKerberosV5 => {
                return Err(error::ConnectError::UnsupportedAuthentication(
                    "Kerberos V5".to_owned(),
                ));
            }
            backend::Message::AuthenticationGss
            | backend::Message::AuthenticationGssContinue(_) => {
                return Err(error::ConnectError::UnsupportedAuthentication(
                    "GSSAPI".to_owned(),
                ));
            }
            backend::Message::AuthenticationSspi => {
                return Err(error::ConnectError::UnsupportedAuthentication(
                    "SSPI".to_owned(),
                ));
            }
            backend::Message::AuthenticationScmCredential => {
                return Err(error::ConnectError::UnsupportedAuthentication(
                    "SCM credentials".to_owned(),
                ));
            }
            _ => {
                return Err(error::ConnectError::Protocol(
                    "unexpected message during authentication".to_owned(),
                ));
            }
        }
        wire.flush().await?;
    }
}

/// The SASL mechanism and the GS2 channel binding flag, as libpq chooses them (RFC 5802, section 6).
fn scram_mechanism(
    offered: &[String],
    wire: &wire::Wire,
    config: &config::Config,
) -> Result<(&'static str, sasl::ChannelBinding), error::ConnectError> {
    let offers = |mechanism: &str| offered.iter().any(|m| m == mechanism);
    let binding = config.get_channel_binding();

    if binding != config::ChannelBinding::Disable
        && offers(sasl::SCRAM_SHA_256_PLUS)
        && let Some(end_point) = wire.tls_server_end_point()
    {
        return Ok((
            sasl::SCRAM_SHA_256_PLUS,
            sasl::ChannelBinding::tls_server_end_point(end_point),
        ));
    }
    if binding == config::ChannelBinding::Require {
        return Err(error::ConnectError::ChannelBindingRequired);
    }
    if !offers(sasl::SCRAM_SHA_256) {
        return Err(error::ConnectError::UnsupportedAuthentication(format!(
            "SASL mechanisms {}",
            offered.join(", ")
        )));
    }

    // `y`: the client could bind but the server did not offer it. A server that did offer it treats `y` as
    // a downgrade attack, so a certificate the client cannot hash gets `n` instead.
    let tls = wire.encryption() == Encryption::Tls;
    let flag =
        if binding != config::ChannelBinding::Disable && tls && !offers(sasl::SCRAM_SHA_256_PLUS) {
            sasl::ChannelBinding::unrequested()
        } else {
            sasl::ChannelBinding::unsupported()
        };
    Ok((sasl::SCRAM_SHA_256, flag))
}

fn row(body: &backend::DataRowBody) -> io::Result<Row> {
    let buffer = body.buffer();
    let mut ranges = body.ranges();
    let mut values = Vec::new();
    while let Some(range) = ranges.next()? {
        values.push(range.map(|r| String::from_utf8_lossy(&buffer[r]).into_owned()));
    }
    Ok(values)
}

fn column(row: &Row, index: usize) -> Option<&str> {
    row.get(index).and_then(Option::as_deref)
}

fn parse_column<T: str::FromStr>(
    row: &Row,
    index: usize,
    what: &str,
) -> Result<T, error::CommandError> {
    column(row, index)
        .and_then(|text| text.parse().ok())
        .ok_or_else(|| protocol(&format!("invalid {what} column")))
}

fn protocol(message: &str) -> error::CommandError {
    error::CommandError::Protocol(message.to_owned())
}

/// `Publications` for arrays of each listed length; `[PublicationName; 0]` stays without one.
macro_rules! publication_arrays {
    ($($len:literal)*) => {$(
        impl Publications for [name::PublicationName; $len] {}

        impl sealed::Sealed for [name::PublicationName; $len] {
            fn into_non_empty(self) -> non_empty::NonEmpty<name::PublicationName> {
                let [first, rest @ ..] = self;
                non_empty::NonEmpty::new(first, rest)
            }
        }
    )*};
}

publication_arrays!(1 2 3 4 5 6 7 8);

mod sealed {
    use pges_core::non_empty;

    use crate::name;

    pub trait Sealed {
        fn into_non_empty(self) -> non_empty::NonEmpty<name::PublicationName>;
    }

    impl Sealed for name::PublicationName {
        fn into_non_empty(self) -> non_empty::NonEmpty<name::PublicationName> {
            non_empty::NonEmpty::one(self)
        }
    }

    impl Sealed for non_empty::NonEmpty<name::PublicationName> {
        fn into_non_empty(self) -> non_empty::NonEmpty<name::PublicationName> {
            self
        }
    }
}
