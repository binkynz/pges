//! Errors: one enum per operation, plus the server's own error report.

use std::{borrow::Cow, fmt, io};

use fallible_iterator::FallibleIterator;
use postgres_protocol::message::backend;

use crate::{config, pgoutput};

/// [`connection::ReplicationConnection::connect`](crate::connection::ReplicationConnection::connect) failed.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum ConnectError {
    /// The socket could not be opened, or broke during the handshake.
    #[error("connecting to the server: {0}")]
    Io(#[from] io::Error),
    /// The server rejected the connection, e.g. a wrong password (`28P01`) or a missing `REPLICATION`
    /// attribute (`42501`).
    #[error(transparent)]
    Server(#[from] ServerError),
    /// The server asks for a password and the config has none.
    #[error("the server asks for a password and none is configured")]
    PasswordRequired,
    /// The server asks for an authentication method this crate does not implement (GSSAPI, SSPI, Kerberos,
    /// or a SASL mechanism other than SCRAM-SHA-256 and SCRAM-SHA-256-PLUS).
    #[error("unsupported authentication method: {0}")]
    UnsupportedAuthentication(String),
    /// SCRAM-SHA-256 failed on the client side, e.g. the server's signature did not verify.
    #[error("SCRAM-SHA-256 authentication failed: {0}")]
    Scram(io::Error),
    /// The server sent something the handshake does not expect.
    #[error("protocol violation during the handshake: {0}")]
    Protocol(String),
    /// The config failed [`Config::check`](crate::config::Config::check).
    #[error(transparent)]
    InvalidConfig(#[from] config::ConfigError),
    /// TLS was required and could not be established, or the server's certificate was rejected.
    #[error(transparent)]
    Tls(#[from] TlsError),
    /// `channel_binding=require`, and the server would authenticate without channel binding: the connection
    /// is not TLS, the server does not offer `SCRAM-SHA-256-PLUS`, it asks for another method, or it accepts
    /// the client without SCRAM. No password was sent.
    #[error("channel binding is required, and the server would authenticate without it")]
    ChannelBindingRequired,
}

/// TLS could not be established (part of [`ConnectError`]).
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum TlsError {
    /// The `sslmode` requires TLS and the server does not offer it (it answered the `SSLRequest` with `N`).
    #[error("the server does not offer TLS, and the sslmode requires it")]
    NotOffered,
    /// The root certificates could not be loaded: the `sslrootcert` file is missing or holds no
    /// certificate, or the platform trust store is empty.
    #[error("loading root certificates: {0}")]
    RootCertificates(String),
    /// With `sslmode=verify-full`, the host is neither a valid DNS name nor an IP address, so no certificate
    /// can match it.
    #[error("{0:?} is not a valid TLS server name")]
    InvalidServerName(String),
    /// The server's certificate failed verification.
    #[error("the server's certificate was rejected: {0}")]
    Certificate(CertificateError),
    /// The TLS handshake failed for another reason, e.g. no common protocol version, or the server rejected
    /// the ALPN protocol.
    #[error("TLS handshake failed: {0}")]
    Handshake(String),
}

/// Why the server's certificate was rejected.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum CertificateError {
    /// No trusted root signed the certificate chain.
    #[error("it is not signed by a trusted root")]
    UnknownIssuer,
    /// The certificate does not name the host (`sslmode=verify-full`).
    #[error("it does not name the host")]
    WrongHostName,
    /// The certificate has expired.
    #[error("it has expired")]
    Expired,
    /// The certificate is not valid yet.
    #[error("it is not valid yet")]
    NotYetValid,
    /// Any other reason, as reported by the TLS library.
    #[error("{0}")]
    Other(String),
}

/// A replication command (`IDENTIFY_SYSTEM`, `CREATE_REPLICATION_SLOT`, `DROP_REPLICATION_SLOT`,
/// `START_REPLICATION`) failed.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum CommandError {
    /// The connection broke. The connection must be discarded.
    #[error("connection lost: {0}")]
    Io(#[from] io::Error),
    /// The server rejected the command, e.g. the slot already exists (`42710`), does not exist (`42704`) or is
    /// active for another process (`55006`). After an [`Severity::Error`], the connection stays usable for
    /// further commands (except after a failed `START_REPLICATION`, which consumes it); after a
    /// [`Severity::Fatal`] or [`Severity::Panic`], e.g. the walsender was terminated (`57P01`), the server has
    /// closed it.
    #[error(transparent)]
    Server(#[from] ServerError),
    /// The server answered with something the command does not expect. The connection must be discarded.
    #[error("protocol violation: {0}")]
    Protocol(String),
}

/// Receiving from or replying on a [`stream::ReplicationStream`](crate::stream::ReplicationStream) failed.
///
/// After any of these the stream must be discarded: reconnect and restart from the last confirmed LSN.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum StreamError {
    /// The connection broke or the server closed it.
    #[error("connection lost: {0}")]
    Io(#[from] io::Error),
    /// The server ended replication with an error, e.g. the walsender was terminated (`57P01`).
    #[error(transparent)]
    Server(#[from] ServerError),
    /// An XLogData payload was not a valid pgoutput message.
    #[error(transparent)]
    Decode(#[from] pgoutput::DecodeError),
    /// The server sent something the replication protocol does not expect.
    #[error("protocol violation: {0}")]
    Protocol(String),
}

/// An error report from the server (`ErrorResponse`).
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{severity:?}: {message} ({code})")]
#[non_exhaustive]
pub struct ServerError {
    /// How severe the error is.
    pub severity: Severity,
    /// The SQLSTATE code.
    pub code: SqlState,
    /// The primary message.
    pub message: String,
    /// Optional detail.
    pub detail: Option<String>,
    /// Optional hint.
    pub hint: Option<String>,
}

impl ServerError {
    /// Whether the server ends the session after this error (FATAL or PANIC).
    pub(crate) fn ends_session(&self) -> bool {
        matches!(self.severity, Severity::Fatal | Severity::Panic)
    }
}

/// Severity of a [`ServerError`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum Severity {
    /// The command failed; the session continues.
    Error,
    /// The session ends.
    Fatal,
    /// The server restarts.
    Panic,
}

/// A five-character SQLSTATE code, e.g. `42704`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SqlState(Cow<'static, str>);

impl SqlState {
    /// `28000`: e.g. `pg_hba.conf` has no entry for this host, user, database and encryption.
    pub const INVALID_AUTHORIZATION_SPECIFICATION: Self = Self(Cow::Borrowed("28000"));
    /// `28P01`: wrong password.
    pub const INVALID_PASSWORD: Self = Self(Cow::Borrowed("28P01"));
    /// `42704`: e.g. the replication slot does not exist.
    pub const UNDEFINED_OBJECT: Self = Self(Cow::Borrowed("42704"));
    /// `42710`: e.g. the replication slot already exists.
    pub const DUPLICATE_OBJECT: Self = Self(Cow::Borrowed("42710"));
    /// `55006`: e.g. the replication slot is active for another process.
    pub const OBJECT_IN_USE: Self = Self(Cow::Borrowed("55006"));
    /// `57P01`: the server or an administrator terminated the session, e.g. with `pg_terminate_backend`.
    pub const ADMIN_SHUTDOWN: Self = Self(Cow::Borrowed("57P01"));

    /// The code.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for SqlState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

pub(crate) fn server_error(body: &backend::ErrorResponseBody) -> io::Result<ServerError> {
    let mut error = ServerError {
        severity: Severity::Error,
        code: SqlState(Cow::Borrowed("")),
        message: String::new(),
        detail: None,
        hint: None,
    };
    let mut fields = body.fields();
    while let Some(field) = fields.next()? {
        let value = String::from_utf8_lossy(field.value_bytes()).into_owned();
        match field.type_() {
            b'V' => {
                error.severity = match value.as_str() {
                    "FATAL" => Severity::Fatal,
                    "PANIC" => Severity::Panic,
                    _ => Severity::Error,
                };
            }
            b'C' => error.code = SqlState(Cow::Owned(value)),
            b'M' => error.message = value,
            b'D' => error.detail = Some(value),
            b'H' => error.hint = Some(value),
            _ => {}
        }
    }
    Ok(error)
}
