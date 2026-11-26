//! Where and as whom to connect, and how to secure the connection.

use std::{fmt, path::PathBuf, str::FromStr};

use percent_encoding::percent_decode_str;

/// Connection settings for a replication connection.
///
/// Build one with [`Config::tcp`] or [`Config::unix`], or parse a `postgres://` URL with
/// [`Config::from_url`]. The connection always uses `replication=database`, so the server runs logical
/// replication commands against [`Config::database`].
///
/// # TLS
///
/// TLS needs the `tls` feature. The settings follow libpq: [`SslMode`] (`sslmode`), [`RootCertificates`]
/// (`sslrootcert`), [`SslNegotiation`] (`sslnegotiation`) and [`ChannelBinding`] (`channel_binding`). With
/// the feature, the defaults are [`SslMode::Prefer`] and [`ChannelBinding::Prefer`]; without it,
/// [`SslMode::Disable`] and [`ChannelBinding::Disable`], and any setting that needs TLS is rejected with
/// [`ConfigError::TlsFeatureRequired`]. Unix sockets never use TLS, whatever the settings say.
#[derive(Clone)]
pub struct Config {
    host: Host,
    port: u16,
    user: String,
    password: Option<String>,
    database: String,
    application_name: String,
    ssl_mode: SslMode,
    root_certificates: Option<RootCertificates>,
    ssl_negotiation: SslNegotiation,
    channel_binding: ChannelBinding,
}

/// How to reach the server.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum Host {
    /// A host name or IP address, over TCP.
    Tcp(String),
    /// The directory holding the server's unix socket, `.s.PGSQL.<port>`.
    Unix(PathBuf),
}

/// Whether and how strictly to use TLS (`sslmode`), as in libpq.
///
/// Only TCP connections use TLS. A mode stronger than [`SslMode::Prefer`] needs the `tls` feature.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, strum::EnumString, strum::IntoStaticStr)]
#[strum(serialize_all = "kebab-case")]
#[non_exhaustive]
pub enum SslMode {
    /// Never use TLS.
    Disable,
    /// Connect without TLS; if the server rejects that connection, try again with TLS. Without the `tls`
    /// feature there is no second attempt.
    Allow,
    /// Use TLS if the server offers it, otherwise connect without it. If the TLS handshake fails, or the
    /// server rejects the encrypted connection, try again without TLS. Without the `tls` feature, never use
    /// TLS. The certificate is checked only when [`RootCertificates::File`] is set.
    Prefer,
    /// Use TLS or fail. The certificate is not checked, unless [`RootCertificates::File`] is set, in which
    /// case this behaves as [`SslMode::VerifyCa`].
    Require,
    /// Use TLS and check that a trusted root signed the server's certificate. The host name is not checked.
    VerifyCa,
    /// Use TLS, check the certificate chain, and check that the certificate names the host (in its subject
    /// alternative names; the common name is not consulted).
    VerifyFull,
}

/// Which root certificates verify the server (`sslrootcert`).
///
/// When none is set, [`SslMode::VerifyCa`] and [`SslMode::VerifyFull`] use the platform's roots, and weaker
/// modes do not check the certificate.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum RootCertificates {
    /// The platform's trust store (`sslrootcert=system`): the operating system's roots, or the PEM file or
    /// directory named by `SSL_CERT_FILE` or `SSL_CERT_DIR`. Only allowed with [`SslMode::VerifyFull`], as in
    /// libpq.
    System,
    /// The certificates in a PEM file.
    File(PathBuf),
}

/// How TLS is started (`sslnegotiation`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, strum::EnumString, strum::IntoStaticStr)]
#[strum(serialize_all = "lowercase")]
#[non_exhaustive]
pub enum SslNegotiation {
    /// Ask the server first (`SSLRequest`), as every PostgreSQL version understands.
    Postgres,
    /// Start the TLS handshake right away, with ALPN `postgresql`, saving a round trip. Needs PostgreSQL 17+
    /// and [`SslMode::Require`] or stronger.
    Direct,
}

/// Whether SCRAM authentication binds to the TLS channel (`channel_binding`), as in libpq.
///
/// Channel binding (`SCRAM-SHA-256-PLUS` with `tls-server-end-point`) proves that the server that holds the
/// password verifier is the one at the other end of the TLS connection, so a man in the middle cannot relay
/// the authentication.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, strum::EnumString, strum::IntoStaticStr)]
#[strum(serialize_all = "lowercase")]
#[non_exhaustive]
pub enum ChannelBinding {
    /// Never use channel binding.
    Disable,
    /// Use it when the connection is TLS and the server offers `SCRAM-SHA-256-PLUS`.
    Prefer,
    /// Fail unless the server authenticates with channel binding; no password is sent by any other method.
    /// Needs the `tls` feature and an [`SslMode`] other than [`SslMode::Disable`].
    Require,
}

impl Config {
    /// TCP to `host:port`. The database defaults to the user name and the application name to `pges`.
    pub fn tcp(host: impl Into<String>, port: u16, user: impl Into<String>) -> Self {
        Self::new(Host::Tcp(host.into()), port, user.into())
    }

    /// The unix socket in `directory` for `port`. The database defaults to the user name.
    pub fn unix(directory: impl Into<PathBuf>, port: u16, user: impl Into<String>) -> Self {
        Self::new(Host::Unix(directory.into()), port, user.into())
    }

    /// Parses a libpq-style URL: `postgres://user:password@host:port/database?param=value`.
    ///
    /// Understood query parameters: `host` (a path starting with `/` selects a unix socket directory), `port`,
    /// `user`, `password`, `dbname`, `application_name`, `sslmode` ([`SslMode`]), `sslrootcert` (a PEM file,
    /// or `system`: [`RootCertificates`]), `sslnegotiation` ([`SslNegotiation`]) and `channel_binding`
    /// ([`ChannelBinding`]). Anything else is an error rather than silently ignored.
    ///
    /// As in libpq, `sslrootcert=system` without an `sslmode` selects [`SslMode::VerifyFull`]. The result
    /// passes [`Config::check`]: settings that need TLS fail with [`ConfigError::TlsFeatureRequired`] when
    /// the `tls` feature is off, and contradictory ones with [`ConfigError::IncompatibleSettings`].
    pub fn from_url(text: &str) -> Result<Self, ConfigError> {
        let url = url::Url::parse(text).map_err(|e| ConfigError::InvalidUrl(e.to_string()))?;
        if !matches!(url.scheme(), "postgres" | "postgresql") {
            return Err(ConfigError::InvalidUrl(format!(
                "scheme must be postgres or postgresql, not {}",
                url.scheme()
            )));
        }

        let mut host = match url.host() {
            Some(url::Host::Domain(name)) if !name.is_empty() => Some(Host::Tcp(decode(name)?)),
            Some(url::Host::Domain(_)) | None => None,
            Some(url::Host::Ipv4(ip)) => Some(Host::Tcp(ip.to_string())),
            Some(url::Host::Ipv6(ip)) => Some(Host::Tcp(ip.to_string())),
        };
        let mut port = url.port().unwrap_or(DEFAULT_PORT);
        let mut user = Some(decode(url.username())?).filter(|u| !u.is_empty());
        let mut password = url.password().map(decode).transpose()?;
        let mut database =
            Some(decode(url.path().trim_start_matches('/'))?).filter(|d| !d.is_empty());
        let mut application_name = None;
        let mut ssl_mode = None;
        let mut root_certificates = None;
        let mut ssl_negotiation = None;
        let mut channel_binding = None;

        // libpq percent-decodes query values but keeps `+`, unlike form encoding.
        let pairs = url
            .query()
            .unwrap_or_default()
            .split('&')
            .filter(|p| !p.is_empty());
        for pair in pairs {
            let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
            let (key, value) = (decode(key)?, decode(value)?);
            match key.as_str() {
                "host" if value.starts_with('/') => host = Some(Host::Unix(PathBuf::from(value))),
                "host" => host = Some(Host::Tcp(value)),
                "port" => {
                    port = value
                        .parse()
                        .map_err(|_| ConfigError::InvalidUrl(format!("invalid port {value:?}")))?;
                }
                "user" => user = Some(value),
                "password" => password = Some(value),
                "dbname" => database = Some(value),
                "application_name" => application_name = Some(value),
                "sslmode" => ssl_mode = Some(parse_value(&key, value)?),
                "sslrootcert" if value == "system" => {
                    root_certificates = Some(RootCertificates::System);
                }
                "sslrootcert" => root_certificates = Some(RootCertificates::File(value.into())),
                "sslnegotiation" => ssl_negotiation = Some(parse_value(&key, value)?),
                "channel_binding" => channel_binding = Some(parse_value(&key, value)?),
                other => return Err(ConfigError::UnsupportedParameter(other.to_owned())),
            }
        }

        let user = user.ok_or(ConfigError::MissingUser)?;
        let host = host.unwrap_or_else(|| Host::Tcp("localhost".to_owned()));
        let mut config = Self::new(host, port, user);
        config.password = password;
        if let Some(database) = database {
            config.database = database;
        }
        if let Some(application_name) = application_name {
            config.application_name = application_name;
        }
        if root_certificates == Some(RootCertificates::System) && ssl_mode.is_none() {
            ssl_mode = Some(SslMode::VerifyFull);
        }
        if let Some(ssl_mode) = ssl_mode {
            config.ssl_mode = ssl_mode;
        }
        config.root_certificates = root_certificates;
        if let Some(ssl_negotiation) = ssl_negotiation {
            config.ssl_negotiation = ssl_negotiation;
        }
        if let Some(channel_binding) = channel_binding {
            config.channel_binding = channel_binding;
        }

        config.check()?;
        Ok(config)
    }

    /// Sets the password, used for cleartext, MD5 and SCRAM-SHA-256 authentication.
    pub fn password(mut self, password: impl Into<String>) -> Self {
        self.password = Some(password.into());
        self
    }

    /// Sets the database. Logical replication slots belong to one database.
    pub fn database(mut self, database: impl Into<String>) -> Self {
        self.database = database.into();
        self
    }

    /// Sets `application_name`, shown in `pg_stat_replication` and `pg_stat_activity`.
    pub fn application_name(mut self, name: impl Into<String>) -> Self {
        self.application_name = name.into();
        self
    }

    /// Sets `sslmode`.
    pub fn ssl_mode(mut self, mode: SslMode) -> Self {
        self.ssl_mode = mode;
        self
    }

    /// Sets `sslrootcert`, the roots that verify the server's certificate.
    pub fn root_certificates(mut self, roots: RootCertificates) -> Self {
        self.root_certificates = Some(roots);
        self
    }

    /// Sets `sslnegotiation`.
    pub fn ssl_negotiation(mut self, negotiation: SslNegotiation) -> Self {
        self.ssl_negotiation = negotiation;
        self
    }

    /// Sets `channel_binding`.
    pub fn channel_binding(mut self, channel_binding: ChannelBinding) -> Self {
        self.channel_binding = channel_binding;
        self
    }

    /// Rejects settings that need TLS when the `tls` feature is off
    /// ([`ConfigError::TlsFeatureRequired`]), and combinations libpq rejects too
    /// ([`ConfigError::IncompatibleSettings`]): `sslnegotiation=direct` with an `sslmode` weaker than
    /// `require`, `channel_binding=require` with `sslmode=disable`, and `sslrootcert=system` with an `sslmode`
    /// other than `verify-full`.
    ///
    /// [`Config::from_url`] and
    /// [`ReplicationConnection::connect`](crate::connection::ReplicationConnection::connect) run this check,
    /// so a config built with the setters fails at the latest when it connects.
    pub fn check(&self) -> Result<(), ConfigError> {
        if !cfg!(feature = "tls") {
            let needs_tls = [
                (
                    "sslmode",
                    <&str>::from(self.ssl_mode),
                    matches!(
                        self.ssl_mode,
                        SslMode::Require | SslMode::VerifyCa | SslMode::VerifyFull
                    ),
                ),
                (
                    "channel_binding",
                    <&str>::from(self.channel_binding),
                    self.channel_binding == ChannelBinding::Require,
                ),
                (
                    "sslnegotiation",
                    <&str>::from(self.ssl_negotiation),
                    self.ssl_negotiation == SslNegotiation::Direct,
                ),
            ];
            if let Some((name, value, _)) = needs_tls.into_iter().find(|(_, _, needs)| *needs) {
                return Err(ConfigError::TlsFeatureRequired(format!("{name}={value}")));
            }
        }

        let weaker_than_require = matches!(
            self.ssl_mode,
            SslMode::Disable | SslMode::Allow | SslMode::Prefer
        );
        if self.ssl_negotiation == SslNegotiation::Direct && weaker_than_require {
            return Err(ConfigError::IncompatibleSettings(format!(
                "sslnegotiation=direct needs sslmode=require or stronger, not {}",
                <&str>::from(self.ssl_mode)
            )));
        }
        if self.channel_binding == ChannelBinding::Require && self.ssl_mode == SslMode::Disable {
            return Err(ConfigError::IncompatibleSettings(
                "channel_binding=require needs TLS, and sslmode=disable turns it off".to_owned(),
            ));
        }
        if self.root_certificates == Some(RootCertificates::System)
            && self.ssl_mode != SslMode::VerifyFull
        {
            return Err(ConfigError::IncompatibleSettings(format!(
                "sslrootcert=system needs sslmode=verify-full, not {}",
                <&str>::from(self.ssl_mode)
            )));
        }
        Ok(())
    }

    /// How the server is reached.
    pub fn get_host(&self) -> &Host {
        &self.host
    }

    /// The server port.
    pub fn get_port(&self) -> u16 {
        self.port
    }

    /// The role to connect as.
    pub fn get_user(&self) -> &str {
        &self.user
    }

    /// The database.
    pub fn get_database(&self) -> &str {
        &self.database
    }

    /// The `sslmode`.
    pub fn get_ssl_mode(&self) -> SslMode {
        self.ssl_mode
    }

    /// The `sslrootcert`, if set.
    pub fn get_root_certificates(&self) -> Option<&RootCertificates> {
        self.root_certificates.as_ref()
    }

    /// The `sslnegotiation`.
    pub fn get_ssl_negotiation(&self) -> SslNegotiation {
        self.ssl_negotiation
    }

    /// The `channel_binding`.
    pub fn get_channel_binding(&self) -> ChannelBinding {
        self.channel_binding
    }

    /// The `application_name`.
    pub fn get_application_name(&self) -> &str {
        &self.application_name
    }

    pub(crate) fn password_bytes(&self) -> Option<&[u8]> {
        self.password.as_deref().map(str::as_bytes)
    }

    fn new(host: Host, port: u16, user: String) -> Self {
        Self {
            host,
            port,
            database: user.clone(),
            user,
            password: None,
            application_name: "pges".to_owned(),
            ssl_mode: if cfg!(feature = "tls") {
                SslMode::Prefer
            } else {
                SslMode::Disable
            },
            root_certificates: None,
            ssl_negotiation: SslNegotiation::Postgres,
            channel_binding: if cfg!(feature = "tls") {
                ChannelBinding::Prefer
            } else {
                ChannelBinding::Disable
            },
        }
    }
}

impl fmt::Debug for Config {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Config")
            .field("host", &self.host)
            .field("port", &self.port)
            .field("user", &self.user)
            .field("password", &self.password.as_ref().map(|_| "<redacted>"))
            .field("database", &self.database)
            .field("application_name", &self.application_name)
            .field("ssl_mode", &self.ssl_mode)
            .field("root_certificates", &self.root_certificates)
            .field("ssl_negotiation", &self.ssl_negotiation)
            .field("channel_binding", &self.channel_binding)
            .finish()
    }
}

/// A connection URL or a combination of settings was rejected.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum ConfigError {
    /// The text is not a `postgres://` URL.
    #[error("invalid connection URL: {0}")]
    InvalidUrl(String),
    /// The URL names no user.
    #[error("the connection URL names no user")]
    MissingUser,
    /// A parameter has a value outside its closed set, e.g. `sslmode=maybe`.
    #[error("invalid value {value:?} for {parameter}")]
    InvalidValue {
        /// The parameter, e.g. `sslmode`.
        parameter: String,
        /// The rejected value.
        value: String,
    },
    /// A setting, e.g. `sslmode=require`, needs TLS, and the crate was built without its `tls` feature.
    #[error("{0} needs TLS: enable the `tls` feature of pges-replication")]
    TlsFeatureRequired(String),
    /// Two settings contradict each other, e.g. `sslnegotiation=direct` with `sslmode=prefer`.
    #[error("incompatible connection settings: {0}")]
    IncompatibleSettings(String),
    /// A query parameter this crate does not understand.
    #[error("unsupported connection parameter {0:?}")]
    UnsupportedParameter(String),
}

const DEFAULT_PORT: u16 = 5432;

fn parse_value<T: FromStr>(parameter: &str, value: String) -> Result<T, ConfigError> {
    value.parse().map_err(|_| ConfigError::InvalidValue {
        parameter: parameter.to_owned(),
        value,
    })
}

fn decode(text: &str) -> Result<String, ConfigError> {
    percent_decode_str(text)
        .decode_utf8()
        .map(|decoded| decoded.into_owned())
        .map_err(|_| ConfigError::InvalidUrl(format!("{text:?} is not UTF-8 once decoded")))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_tcp_url_with_password() {
        let config = Config::from_url("postgres://pg%40user:p%2Fw@db.example:6543/app").unwrap();
        assert_eq!(config.get_host(), &Host::Tcp("db.example".into()));
        assert_eq!(config.get_port(), 6543);
        assert_eq!(config.get_user(), "pg@user");
        assert_eq!(config.password_bytes(), Some(&b"p/w"[..]));
        assert_eq!(config.get_database(), "app");
    }

    #[test]
    fn parses_unix_socket_in_query() {
        let config =
            Config::from_url("postgres://postgres@localhost/postgres?host=/tmp/x/.pg&port=54318")
                .unwrap();
        assert_eq!(config.get_host(), &Host::Unix("/tmp/x/.pg".into()));
        assert_eq!(config.get_port(), 54318);
        assert_eq!(config.password_bytes(), None);
    }

    #[test]
    fn keeps_plus_in_query_values() {
        let config =
            Config::from_url("postgres://u@h/d?password=a+b%2Bc&application_name=x%20y").unwrap();
        assert_eq!(config.password_bytes(), Some(&b"a+b+c"[..]));
        assert_eq!(config.get_application_name(), "x y");
    }

    #[test]
    fn defaults_database_to_user() {
        let config = Config::from_url("postgresql://alice@[::1]").unwrap();
        assert_eq!(config.get_host(), &Host::Tcp("::1".into()));
        assert_eq!(config.get_port(), 5432);
        assert_eq!(config.get_database(), "alice");
    }

    #[test]
    fn rejects_unknown_parameters() {
        assert_eq!(
            Config::from_url("postgres://a@h/d?target_session_attrs=any").unwrap_err(),
            ConfigError::UnsupportedParameter("target_session_attrs".into())
        );
        assert_eq!(
            Config::from_url("postgres://a@h/d?sslcert=/c.pem").unwrap_err(),
            ConfigError::UnsupportedParameter("sslcert".into())
        );
        assert_eq!(
            Config::from_url("postgres://h/d").unwrap_err(),
            ConfigError::MissingUser
        );
        assert!(Config::from_url("mysql://a@h/d").is_err());
    }

    #[test]
    fn defaults_follow_the_tls_feature() {
        let (mode, binding) = if cfg!(feature = "tls") {
            (SslMode::Prefer, ChannelBinding::Prefer)
        } else {
            (SslMode::Disable, ChannelBinding::Disable)
        };

        let config = Config::from_url("postgres://a@h/d").unwrap();
        assert_eq!(config.get_ssl_mode(), mode);
        assert_eq!(config.get_channel_binding(), binding);
        assert_eq!(config.get_ssl_negotiation(), SslNegotiation::Postgres);
        assert_eq!(config.get_root_certificates(), None);
        assert_eq!(Config::tcp("h", 1, "u").get_ssl_mode(), mode);
        assert_eq!(Config::unix("/s", 1, "u").get_channel_binding(), binding);
    }

    #[test]
    fn parses_modes_that_need_no_tls() {
        for (text, mode) in [
            ("disable", SslMode::Disable),
            ("allow", SslMode::Allow),
            ("prefer", SslMode::Prefer),
        ] {
            let config = Config::from_url(&format!("postgres://a@h/d?sslmode={text}")).unwrap();
            assert_eq!(config.get_ssl_mode(), mode, "{text}");
        }
        for (text, binding) in [
            ("disable", ChannelBinding::Disable),
            ("prefer", ChannelBinding::Prefer),
        ] {
            let config =
                Config::from_url(&format!("postgres://a@h/d?channel_binding={text}")).unwrap();
            assert_eq!(config.get_channel_binding(), binding, "{text}");
        }
        let config = Config::from_url("postgres://a@h/d?sslnegotiation=postgres").unwrap();
        assert_eq!(config.get_ssl_negotiation(), SslNegotiation::Postgres);
    }

    #[test]
    fn rejects_invalid_values() {
        for (parameter, value) in [
            ("sslmode", "maybe"),
            ("sslmode", "REQUIRE"),
            ("sslmode", "verify_full"),
            ("channel_binding", "on"),
            ("sslnegotiation", "tls"),
        ] {
            assert_eq!(
                Config::from_url(&format!("postgres://a@h/d?{parameter}={value}")).unwrap_err(),
                ConfigError::InvalidValue {
                    parameter: parameter.into(),
                    value: value.into()
                },
            );
        }
    }

    #[cfg(not(feature = "tls"))]
    #[test]
    fn settings_that_need_tls_name_the_feature() {
        for (query, setting) in [
            ("sslmode=require", "sslmode=require"),
            ("sslmode=verify-ca", "sslmode=verify-ca"),
            ("sslmode=verify-full", "sslmode=verify-full"),
            ("sslrootcert=system", "sslmode=verify-full"),
            ("channel_binding=require", "channel_binding=require"),
            ("sslnegotiation=direct", "sslnegotiation=direct"),
        ] {
            let error = Config::from_url(&format!("postgres://a@h/d?{query}")).unwrap_err();
            assert_eq!(
                error,
                ConfigError::TlsFeatureRequired(setting.into()),
                "{query}"
            );
            assert!(error.to_string().contains("`tls` feature"), "{error}");
        }

        let built = Config::tcp("h", 1, "u").ssl_mode(SslMode::Require);
        assert_eq!(
            built.check().unwrap_err(),
            ConfigError::TlsFeatureRequired("sslmode=require".into())
        );
    }

    #[cfg(feature = "tls")]
    #[test]
    fn parses_every_tls_setting() {
        for (text, mode) in [
            ("disable", SslMode::Disable),
            ("allow", SslMode::Allow),
            ("prefer", SslMode::Prefer),
            ("require", SslMode::Require),
            ("verify-ca", SslMode::VerifyCa),
            ("verify-full", SslMode::VerifyFull),
        ] {
            let config = Config::from_url(&format!("postgres://a@h/d?sslmode={text}")).unwrap();
            assert_eq!(config.get_ssl_mode(), mode, "{text}");
        }
        for (text, binding) in [
            ("disable", ChannelBinding::Disable),
            ("prefer", ChannelBinding::Prefer),
            ("require", ChannelBinding::Require),
        ] {
            let config =
                Config::from_url(&format!("postgres://a@h/d?channel_binding={text}")).unwrap();
            assert_eq!(config.get_channel_binding(), binding, "{text}");
        }

        let config = Config::from_url(
            "postgres://a@h/d?sslmode=verify-full&sslrootcert=%2Fetc%2Fca.pem\
             &sslnegotiation=direct",
        )
        .unwrap();
        assert_eq!(config.get_ssl_mode(), SslMode::VerifyFull);
        assert_eq!(
            config.get_root_certificates(),
            Some(&RootCertificates::File("/etc/ca.pem".into()))
        );
        assert_eq!(config.get_ssl_negotiation(), SslNegotiation::Direct);

        let system = Config::from_url("postgres://a@h/d?sslrootcert=system").unwrap();
        assert_eq!(system.get_ssl_mode(), SslMode::VerifyFull);
        assert_eq!(
            system.get_root_certificates(),
            Some(&RootCertificates::System)
        );

        let require_with_file =
            Config::from_url("postgres://a@h/d?sslmode=require&sslrootcert=ca.pem").unwrap();
        assert_eq!(require_with_file.get_ssl_mode(), SslMode::Require);
    }

    #[cfg(feature = "tls")]
    #[test]
    fn rejects_combinations_libpq_rejects() {
        for query in [
            "sslmode=prefer&sslnegotiation=direct",
            "sslmode=allow&sslnegotiation=direct",
            "sslnegotiation=direct",
            "sslmode=disable&channel_binding=require",
            "sslmode=require&sslrootcert=system",
            "sslmode=verify-ca&sslrootcert=system",
        ] {
            let error = Config::from_url(&format!("postgres://a@h/d?{query}")).unwrap_err();
            assert!(
                matches!(error, ConfigError::IncompatibleSettings(_)),
                "{query}: {error:?}"
            );
        }
        assert!(
            Config::from_url("postgres://a@h/d?sslmode=verify-ca&sslnegotiation=direct").is_ok()
        );

        let built = Config::tcp("h", 1, "u")
            .ssl_mode(SslMode::Disable)
            .channel_binding(ChannelBinding::Require);
        assert!(matches!(
            built.check(),
            Err(ConfigError::IncompatibleSettings(_))
        ));
    }

    #[test]
    fn debug_hides_the_password() {
        let config = Config::tcp("h", 1, "u").password("secret");
        assert!(!format!("{config:?}").contains("secret"));
    }
}
