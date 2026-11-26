//! TLS: sslmode, root certificates, host name checks, direct negotiation and channel binding.
//!
//! `against_a_tls_server` starts its own PostgreSQL 18 container (unique name, removed at the end) with a
//! certificate from a throwaway CA generated here, and skips when Docker is unavailable. The other tests
//! need no Docker.

#![cfg(feature = "tls")]
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::{path::PathBuf, process::Command, sync::Arc, time::Duration};

use anyhow::{Context, Result, ensure};
use testcontainers::{CopyTargetOptions, ImageExt, runners::AsyncRunner};
use testcontainers_modules::postgres::Postgres;
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};

use pges_replication::{config, connection, error};

#[tokio::test]
async fn against_a_tls_server() -> Result<()> {
    if !docker_available() {
        eprintln!("skipped: Docker is not available, so no TLS-enabled PostgreSQL can be started");
        return Ok(());
    }

    let pki = Pki::generate()?;
    let server = TlsServer::start(&pki).await?;
    if server.host != "localhost" {
        eprintln!(
            "skipped: the container is reachable at {}, and the test certificate names only localhost",
            server.host
        );
        return Ok(());
    }

    let ca = pki.write("ca.pem", &pki.ca_pem)?;
    let wrong_ca = pki.write("wrong-ca.pem", &pki.wrong_ca_pem)?;
    let postgres = |host: &str| {
        config::Config::tcp(host, server.port, "postgres")
            .password("postgres")
            .database("postgres")
    };
    let role = |name: &str| {
        config::Config::tcp("localhost", server.port, name)
            .password("pw")
            .database("postgres")
    };

    // require: TLS without checking the certificate; channel binding by default.
    let require = postgres("localhost").ssl_mode(config::SslMode::Require);
    expect(
        &require,
        connection::Encryption::Tls,
        connection::Authentication::ScramSha256Plus,
    )
    .await
    .context("sslmode=require")?;

    // The default (prefer) uses TLS when offered, from a URL too.
    let url = format!(
        "postgres://postgres:postgres@localhost:{}/postgres",
        server.port
    );
    expect(
        &config::Config::from_url(&url)?,
        connection::Encryption::Tls,
        connection::Authentication::ScramSha256Plus,
    )
    .await
    .context("default sslmode")?;

    // verify-full with the right CA, from a URL.
    let verify_full = config::Config::from_url(&format!(
        "{url}?sslmode=verify-full&sslrootcert={}&channel_binding=require",
        ca.display()
    ))?;
    expect(
        &verify_full,
        connection::Encryption::Tls,
        connection::Authentication::ScramSha256Plus,
    )
    .await
    .context("verify-full with the right CA")?;

    // verify-full checks the host name: the certificate names localhost, not 127.0.0.1.
    let wrong_host = postgres("127.0.0.1")
        .ssl_mode(config::SslMode::VerifyFull)
        .root_certificates(config::RootCertificates::File(ca.clone()));
    assert_certificate_error(&wrong_host, error::CertificateError::WrongHostName).await;

    // verify-ca does not.
    let verify_ca = postgres("127.0.0.1")
        .ssl_mode(config::SslMode::VerifyCa)
        .root_certificates(config::RootCertificates::File(ca.clone()));
    expect(
        &verify_ca,
        connection::Encryption::Tls,
        connection::Authentication::ScramSha256Plus,
    )
    .await
    .context("verify-ca with another host name")?;

    // A CA that did not sign the certificate fails verify-full, verify-ca, and require with sslrootcert.
    for mode in [
        config::SslMode::VerifyFull,
        config::SslMode::VerifyCa,
        config::SslMode::Require,
    ] {
        let config = postgres("localhost")
            .ssl_mode(mode)
            .root_certificates(config::RootCertificates::File(wrong_ca.clone()));
        assert_certificate_error(&config, error::CertificateError::UnknownIssuer).await;
    }

    // A missing root file is a typed error, not a fallback.
    let missing = postgres("localhost")
        .ssl_mode(config::SslMode::VerifyFull)
        .root_certificates(config::RootCertificates::File(pki.dir.join("missing.pem")));
    match connection::ReplicationConnection::connect(&missing).await {
        Err(error::ConnectError::Tls(error::TlsError::RootCertificates(_))) => {}
        other => panic!("expected a root certificate error, got {other:?}"),
    }

    // Channel binding off: plain SCRAM over TLS.
    let unbound = require
        .clone()
        .channel_binding(config::ChannelBinding::Disable);
    expect(
        &unbound,
        connection::Encryption::Tls,
        connection::Authentication::ScramSha256,
    )
    .await
    .context("channel_binding=disable")?;

    // Direct TLS (PostgreSQL 17+).
    let direct = require
        .clone()
        .ssl_negotiation(config::SslNegotiation::Direct);
    expect(
        &direct,
        connection::Encryption::Tls,
        connection::Authentication::ScramSha256Plus,
    )
    .await
    .context("sslnegotiation=direct")?;

    // disable: no TLS, plain SCRAM.
    let disable = postgres("localhost")
        .ssl_mode(config::SslMode::Disable)
        .channel_binding(config::ChannelBinding::Disable);
    expect(
        &disable,
        connection::Encryption::Plain,
        connection::Authentication::ScramSha256,
    )
    .await
    .context("sslmode=disable")?;

    // pg_hba.conf: tls_only is rejected without TLS, plain_only with TLS.
    // prefer falls back to plain when the server rejects the TLS connection.
    let prefer_plain = role("plain_only").ssl_mode(config::SslMode::Prefer);
    expect(
        &prefer_plain,
        connection::Encryption::Plain,
        connection::Authentication::ScramSha256,
    )
    .await
    .context("prefer, falling back to plain")?;
    // allow starts plain and retries with TLS.
    let allow_tls = role("tls_only").ssl_mode(config::SslMode::Allow);
    expect(
        &allow_tls,
        connection::Encryption::Tls,
        connection::Authentication::ScramSha256Plus,
    )
    .await
    .context("allow, retrying with TLS")?;
    let allow_plain = role("plain_only").ssl_mode(config::SslMode::Allow);
    expect(
        &allow_plain,
        connection::Encryption::Plain,
        connection::Authentication::ScramSha256,
    )
    .await
    .context("allow, plain")?;
    // require does not fall back.
    match connection::ReplicationConnection::connect(
        &role("plain_only").ssl_mode(config::SslMode::Require),
    )
    .await
    {
        Err(error::ConnectError::Server(e)) => {
            assert_eq!(
                e.code,
                error::SqlState::INVALID_AUTHORIZATION_SPECIFICATION,
                "{e}"
            );
        }
        other => panic!("require fell back or succeeded: {other:?}"),
    }

    // channel_binding=require fails over a connection that ends up without TLS, before any password.
    let bound_plain = role("plain_only")
        .ssl_mode(config::SslMode::Prefer)
        .channel_binding(config::ChannelBinding::Require);
    match connection::ReplicationConnection::connect(&bound_plain).await {
        Err(error::ConnectError::ChannelBindingRequired) => {}
        other => panic!("expected ChannelBindingRequired, got {other:?}"),
    }

    server.stop().await
}

/// The shared test server has no TLS (a unix socket, or the Docker container without `ssl`), so
/// `channel_binding=require` must fail there.
#[tokio::test]
async fn channel_binding_require_fails_without_tls() -> Result<()> {
    let server = pges_testing::server().await?;
    let config = config::Config::from_url(server.admin_url())?
        .channel_binding(config::ChannelBinding::Require);

    match connection::ReplicationConnection::connect(&config).await {
        Err(error::ConnectError::ChannelBindingRequired) => Ok(()),
        other => panic!("expected ChannelBindingRequired, got {other:?}"),
    }
}

/// A server that answers the `SSLRequest` with `N`: `require` fails with a typed error.
#[tokio::test]
async fn require_fails_when_the_server_declines_tls() -> Result<()> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let port = listener.local_addr()?.port();
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await?;
        let mut request = [0; 8];
        socket.read_exact(&mut request).await?;
        socket.write_all(b"N").await?;
        anyhow::Ok((request, socket))
    });

    let config = config::Config::tcp("127.0.0.1", port, "u").ssl_mode(config::SslMode::Require);
    let result = connection::ReplicationConnection::connect(&config).await;
    let (request, _socket) = server.await??;

    assert_eq!(request, [0, 0, 0, 8, 0x04, 0xd2, 0x16, 0x2f], "SSLRequest");
    match result {
        Err(error::ConnectError::Tls(error::TlsError::NotOffered)) => Ok(()),
        other => panic!("expected NotOffered, got {other:?}"),
    }
}

/// A server that answers the `SSLRequest` with `N`: `prefer` continues without TLS on the same socket.
#[tokio::test]
async fn prefer_continues_in_plain_text_when_the_server_declines_tls() -> Result<()> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let port = listener.local_addr()?.port();
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await?;
        let mut request = [0; 8];
        socket.read_exact(&mut request).await?;
        socket.write_all(b"N").await?;
        let startup = read_startup(&mut socket).await?;
        socket.write_all(AUTHENTICATION_OK).await?;
        socket.write_all(READY_FOR_QUERY).await?;
        anyhow::Ok((startup, socket))
    });

    let config = config::Config::tcp("127.0.0.1", port, "u").ssl_mode(config::SslMode::Prefer);
    let connection = connection::ReplicationConnection::connect(&config).await?;
    let (startup, _socket) = server.await??;

    assert_eq!(startup[..4], [0, 3, 0, 0], "protocol 3.0 startup message");
    assert_eq!(connection.encryption(), connection::Encryption::Plain);
    assert_eq!(
        connection.authentication(),
        connection::Authentication::WithoutPassword
    );
    Ok(())
}

/// Over TLS, `channel_binding=require` refuses a server that accepts the client without SCRAM, as a man in
/// the middle holding a certificate of its own would.
#[tokio::test]
async fn channel_binding_require_refuses_a_server_that_skips_scram() -> Result<()> {
    let server = FakeTlsServer::start(Vec::new()).await?;

    let config = config::Config::tcp("localhost", server.port, "u")
        .password("pw")
        .ssl_mode(config::SslMode::Require)
        .channel_binding(config::ChannelBinding::Require);
    let result = connection::ReplicationConnection::connect(&config).await;
    server.finish().await?;

    match result {
        Err(error::ConnectError::ChannelBindingRequired) => Ok(()),
        other => panic!("expected ChannelBindingRequired, got {other:?}"),
    }
}

/// The client binds to the certificate when the server offers SCRAM-SHA-256-PLUS, and rejects an
/// `AuthenticationOk` that skips the server's final SCRAM message, which proves the binding.
#[tokio::test]
async fn scram_plus_needs_the_servers_final_message() -> Result<()> {
    let server =
        FakeTlsServer::start(sasl_request(&["SCRAM-SHA-256-PLUS", "SCRAM-SHA-256"])).await?;

    let config = config::Config::tcp("localhost", server.port, "u")
        .password("pw")
        .ssl_mode(config::SslMode::Require);
    let result = connection::ReplicationConnection::connect(&config).await;
    let initial = server.finish().await?.context("no SASL initial response")?;

    let (mechanism, client_first) = split_initial_response(&initial)?;
    assert_eq!(mechanism, "SCRAM-SHA-256-PLUS");
    assert!(
        client_first.starts_with("p=tls-server-end-point,,"),
        "{client_first}"
    );
    match result {
        Err(error::ConnectError::Protocol(_)) => Ok(()),
        other => panic!("expected a protocol error, got {other:?}"),
    }
}

/// Over TLS, a server that offers only SCRAM-SHA-256 gets the GS2 flag `y` (the client could bind), and
/// `channel_binding=require` sends no SASL response at all.
#[tokio::test]
async fn scram_without_plus_over_tls() -> Result<()> {
    let config = |port| {
        config::Config::tcp("localhost", port, "u")
            .password("pw")
            .ssl_mode(config::SslMode::Require)
    };

    let server = FakeTlsServer::start(sasl_request(&["SCRAM-SHA-256"])).await?;
    let result = connection::ReplicationConnection::connect(&config(server.port)).await;
    let initial = server.finish().await?.context("no SASL initial response")?;
    let (mechanism, client_first) = split_initial_response(&initial)?;
    assert_eq!(mechanism, "SCRAM-SHA-256");
    assert!(client_first.starts_with("y,,"), "{client_first}");
    assert!(
        matches!(result, Err(error::ConnectError::Protocol(_))),
        "{result:?}"
    );

    let server = FakeTlsServer::start(sasl_request(&["SCRAM-SHA-256"])).await?;
    let required = config(server.port).channel_binding(config::ChannelBinding::Require);
    let result = connection::ReplicationConnection::connect(&required).await;
    assert_eq!(server.finish().await?, None, "a SASL response was sent");
    match result {
        Err(error::ConnectError::ChannelBindingRequired) => Ok(()),
        other => panic!("expected ChannelBindingRequired, got {other:?}"),
    }
}

/// The sequencer runs the connection inside `tokio::spawn`, so the TLS path must keep the future `Send`.
#[test]
fn connect_future_is_send() {
    fn assert_send<T: Send>(_: T) {}

    let config = config::Config::tcp("localhost", 5432, "u").ssl_mode(config::SslMode::VerifyFull);
    assert_send(connection::ReplicationConnection::connect(&config));
}

async fn expect(
    config: &config::Config,
    encryption: connection::Encryption,
    authentication: connection::Authentication,
) -> Result<()> {
    let mut connection = connection::ReplicationConnection::connect(config).await?;
    ensure!(
        connection.encryption() == encryption,
        "encryption {:?}, expected {encryption:?}",
        connection.encryption()
    );
    ensure!(
        connection.authentication() == authentication,
        "authentication {:?}, expected {authentication:?}",
        connection.authentication()
    );

    let identity = connection.identify_system().await?;
    ensure!(identity.database.as_deref() == Some(config.get_database()));
    connection.close().await?;
    Ok(())
}

async fn assert_certificate_error(config: &config::Config, expected: error::CertificateError) {
    match connection::ReplicationConnection::connect(config).await {
        Err(error::ConnectError::Tls(error::TlsError::Certificate(reason))) => {
            assert_eq!(reason, expected, "{config:?}");
        }
        other => panic!("expected {expected:?} for {config:?}, got {other:?}"),
    }
}

fn docker_available() -> bool {
    Command::new("docker")
        .arg("info")
        .output()
        .is_ok_and(|output| output.status.success())
}

/// A throwaway CA (ECDSA P-384, so the server certificate is signed with SHA-384 and channel binding hashes
/// with SHA-384), a server certificate for `localhost`, and an unrelated CA.
struct Pki {
    ca_pem: String,
    wrong_ca_pem: String,
    server_cert_pem: String,
    server_key_pem: String,
    dir: PathBuf,
}

impl Pki {
    fn generate() -> Result<Self> {
        let ca = Self::ca("pges test CA")?;
        let wrong_ca = Self::ca("pges unrelated CA")?;

        let server_key = rcgen::KeyPair::generate_for(&rcgen::PKCS_ECDSA_P256_SHA256)?;
        let mut params = rcgen::CertificateParams::new(vec!["localhost".to_owned()])?;
        params
            .distinguished_name
            .push(rcgen::DnType::CommonName, "localhost");
        let server_cert = params.signed_by(&server_key, &ca)?;

        let dir = std::env::temp_dir().join(format!("pges-tls-{}", uuid::Uuid::now_v7().simple()));
        std::fs::create_dir_all(&dir)?;
        Ok(Self {
            ca_pem: ca.pem(),
            wrong_ca_pem: wrong_ca.pem(),
            server_cert_pem: server_cert.pem(),
            server_key_pem: server_key.serialize_pem(),
            dir,
        })
    }

    fn ca(name: &str) -> Result<rcgen::CertifiedIssuer<'static, rcgen::KeyPair>> {
        let key = rcgen::KeyPair::generate_for(&rcgen::PKCS_ECDSA_P384_SHA384)?;
        let mut params = rcgen::CertificateParams::new(Vec::<String>::new())?;
        params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
        params
            .distinguished_name
            .push(rcgen::DnType::CommonName, name);
        params.key_usages = vec![
            rcgen::KeyUsagePurpose::KeyCertSign,
            rcgen::KeyUsagePurpose::CrlSign,
        ];
        Ok(rcgen::CertifiedIssuer::self_signed(params, key)?)
    }

    fn write(&self, name: &str, pem: &str) -> Result<PathBuf> {
        let path = self.dir.join(name);
        std::fs::write(&path, pem)?;
        Ok(path)
    }
}

impl Drop for Pki {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

/// Runs once while the image initialises the cluster, as the `postgres` user: installs the certificate,
/// turns TLS on, and sets up `pg_hba.conf` with one role that needs TLS and one that must not use it.
const INIT_SCRIPT: &str = r#"#!/bin/sh
set -e
cp /pges-tls/server.crt /pges-tls/server.key "$PGDATA/"
chmod 600 "$PGDATA/server.key"
echo "ssl = on" >> "$PGDATA/postgresql.conf"
cat > "$PGDATA/pg_hba.conf" <<'HBA'
local all all trust
hostnossl all tls_only all reject
hostssl all plain_only all reject
host all all all scram-sha-256
HBA
psql -v ON_ERROR_STOP=1 -U postgres -d postgres \
  -c "CREATE ROLE tls_only LOGIN REPLICATION PASSWORD 'pw'" \
  -c "CREATE ROLE plain_only LOGIN REPLICATION PASSWORD 'pw'"
"#;

/// A PostgreSQL 18 container with TLS on, removed when stopped or dropped.
struct TlsServer {
    container: testcontainers::ContainerAsync<Postgres>,
    host: String,
    port: u16,
}

impl TlsServer {
    async fn start(pki: &Pki) -> Result<Self> {
        let name = format!("pges-test-tls-{}", uuid::Uuid::now_v7().simple());
        let container = Postgres::default()
            .with_tag("18")
            .with_copy_to(
                "/pges-tls/server.crt",
                pki.server_cert_pem.clone().into_bytes(),
            )
            .with_copy_to(
                "/pges-tls/server.key",
                pki.server_key_pem.clone().into_bytes(),
            )
            .with_copy_to(
                CopyTargetOptions::new("/docker-entrypoint-initdb.d/pges-tls.sh").with_mode(0o755),
                INIT_SCRIPT.as_bytes().to_vec(),
            )
            .with_container_name(&name)
            .start()
            .await
            .with_context(|| format!("starting the TLS container {name}"))?;
        let host = container.get_host().await?.to_string();
        let port = container.get_host_port_ipv4(5432).await?;
        Ok(Self {
            container,
            host,
            port,
        })
    }

    async fn stop(self) -> Result<()> {
        self.container.rm().await?;
        Ok(())
    }
}

const AUTHENTICATION_OK: &[u8] = b"R\0\0\0\x08\0\0\0\0";
const READY_FOR_QUERY: &[u8] = b"Z\0\0\0\x05I";

/// `AuthenticationSASL` offering `mechanisms`.
fn sasl_request(mechanisms: &[&str]) -> Vec<u8> {
    let mut body = 10_i32.to_be_bytes().to_vec();
    for mechanism in mechanisms {
        body.extend_from_slice(mechanism.as_bytes());
        body.push(0);
    }
    body.push(0);

    let mut message = vec![b'R'];
    message.extend_from_slice(&(i32::try_from(body.len()).unwrap() + 4).to_be_bytes());
    message.extend_from_slice(&body);
    message
}

/// The mechanism and the client-first-message of a `SASLInitialResponse` body.
fn split_initial_response(body: &[u8]) -> Result<(String, String)> {
    let nul = body
        .iter()
        .position(|b| *b == 0)
        .context("no mechanism terminator")?;
    let mechanism = String::from_utf8(body[..nul].to_vec())?;
    let data = body.get(nul + 5..).context("no client-first-message")?;
    Ok((mechanism, String::from_utf8(data.to_vec())?))
}

/// The body of a startup message (protocol version and parameters).
async fn read_startup(socket: &mut (impl AsyncRead + Unpin)) -> Result<Vec<u8>> {
    let length = usize::try_from(socket.read_i32().await?)?;
    let mut body = vec![0; length - 4];
    socket.read_exact(&mut body).await?;
    Ok(body)
}

/// A TLS server with a self-signed P-256 certificate for `localhost`. It reads the startup message, sends
/// `auth_request` (if any), reads the client's next message, answers `AuthenticationOk` and hangs up.
struct FakeTlsServer {
    port: u16,
    task: tokio::task::JoinHandle<Result<Option<Vec<u8>>>>,
}

impl FakeTlsServer {
    async fn start(auth_request: Vec<u8>) -> Result<Self> {
        let certified = rcgen::generate_simple_self_signed(vec!["localhost".to_owned()])?;
        let key = rustls_pki_types::PrivateKeyDer::try_from(certified.signing_key.serialize_der())
            .map_err(anyhow::Error::msg)?;
        let tls = rustls::ServerConfig::builder_with_provider(Arc::new(
            rustls::crypto::ring::default_provider(),
        ))
        .with_safe_default_protocol_versions()?
        .with_no_client_auth()
        .with_single_cert(vec![certified.cert.der().clone()], key)?;
        let acceptor = tokio_rustls::TlsAcceptor::from(Arc::new(tls));

        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let port = listener.local_addr()?.port();
        let task = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await?;
            let mut request = [0; 8];
            socket.read_exact(&mut request).await?;
            socket.write_all(b"S").await?;
            let mut stream = acceptor.accept(socket).await?;
            read_startup(&mut stream).await?;

            let mut answer = None;
            if !auth_request.is_empty() {
                stream.write_all(&auth_request).await?;
                stream.flush().await?;
                let mut header = [0; 5];
                if stream.read_exact(&mut header).await.is_ok() {
                    ensure!(header[0] == b'p', "unexpected message {}", header[0]);
                    let length = i32::from_be_bytes(header[1..].try_into()?);
                    let mut body = vec![0; usize::try_from(length)? - 4];
                    stream.read_exact(&mut body).await?;
                    answer = Some(body);
                }
            }
            // The client may already have hung up.
            let _ = stream.write_all(AUTHENTICATION_OK).await;
            let _ = stream.flush().await;
            Ok(answer)
        });
        Ok(Self { port, task })
    }

    /// What the client sent after the authentication request, if anything.
    async fn finish(self) -> Result<Option<Vec<u8>>> {
        tokio::time::timeout(Duration::from_secs(10), self.task)
            .await
            .context("the fake server is still waiting for the client")??
    }
}
