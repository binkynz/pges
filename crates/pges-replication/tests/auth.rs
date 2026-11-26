//! Authentication: every supported method, over TCP and the unix socket.
//!
//! The shared test server allows only one method (trust for `pg-up`, SCRAM-SHA-256 over TCP for the Docker
//! container), so `methods_on_a_private_cluster` starts its own throwaway cluster with a `pg_hba.conf` that
//! assigns one method per role. It needs `initdb` and `pg_ctl` on the PATH (the Nix dev shell has them) and
//! skips otherwise.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::{path::PathBuf, process::Command};

use anyhow::{Context, Result, ensure};
use sqlx::{Connection, Executor, PgConnection};

use pges_replication::{config, connection, error};

#[tokio::test]
async fn connects_to_the_test_server() -> Result<()> {
    let server = pges_testing::server().await?;
    let config = config::Config::from_url(server.admin_url())?;
    let mut connection = connection::ReplicationConnection::connect(&config).await?;
    let identity = connection.identify_system().await?;
    assert_eq!(identity.database.as_deref(), Some("postgres"));
    assert!(connection.server_version().starts_with("18"));
    assert!(connection.process_id() > 0);
    connection.close().await?;
    Ok(())
}

/// SCRAM-SHA-256 over TCP, against a test server that asks for passwords: the Docker container
/// (`PGES_TEST_BACKEND=docker`). Skips on servers that trust TCP connections.
#[tokio::test]
async fn scram_over_tcp_on_the_test_server() -> Result<()> {
    let server = pges_testing::server().await?;
    let admin_config = config::Config::from_url(server.admin_url())?;
    let config::Host::Tcp(host) = admin_config.get_host().clone() else {
        eprintln!(
            "skipped: the test server is on a unix socket; run with PGES_TEST_BACKEND=docker for SCRAM over TCP"
        );
        return Ok(());
    };

    let role = format!("r_{}", uuid::Uuid::now_v7().simple());
    let mut admin = server.admin().await?;
    admin
        .execute(sqlx::AssertSqlSafe(format!(
            "SET password_encryption = 'scram-sha-256'; \
                 CREATE ROLE {role} LOGIN REPLICATION PASSWORD 'right horse battery'"
        )))
        .await?;

    let result = async {
        let base = config::Config::tcp(host.as_str(), admin_config.get_port(), role.as_str())
            .database("postgres");
        let wrong =
            connection::ReplicationConnection::connect(&base.clone().password("wrong")).await;
        let Err(error) = wrong else {
            eprintln!("skipped: the test server trusts TCP connections; SCRAM was not exercised");
            return Ok(());
        };

        assert_invalid_password(error);
        let mut connection =
            connection::ReplicationConnection::connect(&base.password("right horse battery"))
                .await?;
        connection.identify_system().await?;
        connection.close().await?;
        anyhow::Ok(())
    }
    .await;

    admin
        .execute(sqlx::AssertSqlSafe(format!("DROP ROLE {role}")))
        .await?;
    admin.close().await?;
    result
}

#[tokio::test]
async fn methods_on_a_private_cluster() -> Result<()> {
    let Some(cluster) = Cluster::start()? else {
        eprintln!("skipped: initdb and pg_ctl are not on the PATH");
        return Ok(());
    };

    let mut admin = PgConnection::connect(&format!(
        "postgres://postgres@localhost/postgres?host={}&port={}",
        cluster.dir.display(),
        cluster.port
    ))
    .await?;
    for (role, encryption) in [
        ("password_user", "scram-sha-256"),
        ("md5_user", "md5"),
        ("scram_user", "scram-sha-256"),
    ] {
        admin
            .execute(sqlx::AssertSqlSafe(format!(
                "SET password_encryption = '{encryption}'; \
                     CREATE ROLE {role} LOGIN REPLICATION PASSWORD 'pw-{role}'"
            )))
            .await?;
    }
    admin
        .execute("CREATE ROLE trust_user LOGIN REPLICATION; CREATE ROLE plain_user LOGIN")
        .await?;
    admin.close().await?;

    let unix = config::Config::unix(&cluster.dir, cluster.port, "postgres");
    identify(&unix)
        .await
        .context("trust over the unix socket")?;
    let tcp =
        |role: &str| config::Config::tcp("127.0.0.1", cluster.port, role).database("postgres");
    identify(&tcp("trust_user"))
        .await
        .context("trust over TCP")?;

    for role in ["password_user", "md5_user", "scram_user"] {
        identify(&tcp(role).password(format!("pw-{role}")))
            .await
            .with_context(|| format!("{role} with the right password"))?;
        let wrong = connection::ReplicationConnection::connect(&tcp(role).password("wrong")).await;
        assert_invalid_password(
            wrong
                .err()
                .unwrap_or_else(|| panic!("{role} accepted a wrong password")),
        );
        let missing = connection::ReplicationConnection::connect(&tcp(role)).await;
        assert!(
            matches!(missing, Err(error::ConnectError::PasswordRequired)),
            "{role}: {missing:?}"
        );
    }

    let non_replication = connection::ReplicationConnection::connect(&config::Config::unix(
        &cluster.dir,
        cluster.port,
        "plain_user",
    ))
    .await;
    match non_replication {
        Err(error::ConnectError::Server(e)) => assert_eq!(e.code.as_str(), "42501", "{e}"),
        other => panic!("a role without REPLICATION connected: {other:?}"),
    }
    Ok(())
}

async fn identify(config: &config::Config) -> Result<()> {
    let mut connection = connection::ReplicationConnection::connect(config).await?;
    let identity = connection.identify_system().await?;
    ensure!(identity.database.as_deref() == Some(config.get_database()));
    connection.close().await?;
    Ok(())
}

fn assert_invalid_password(error: error::ConnectError) {
    match error {
        error::ConnectError::Server(e) => {
            assert_eq!(e.code, error::SqlState::INVALID_PASSWORD, "{e}")
        }
        other => panic!("expected a password failure, got {other:?}"),
    }
}

/// A throwaway cluster in a temporary directory, stopped and removed on drop.
struct Cluster {
    dir: PathBuf,
    port: u16,
}

const HBA: &str = "\
local all postgres trust
local all plain_user trust
host all trust_user 127.0.0.1/32 trust
host all password_user 127.0.0.1/32 password
host all md5_user 127.0.0.1/32 md5
host all scram_user 127.0.0.1/32 scram-sha-256
";

impl Cluster {
    fn start() -> Result<Option<Self>> {
        let available = |tool: &str| Command::new(tool).arg("--version").output().is_ok();
        if !available("initdb") || !available("pg_ctl") {
            return Ok(None);
        }

        let dir = std::env::temp_dir().join(format!("pges-auth-{}", uuid::Uuid::now_v7().simple()));
        std::fs::create_dir_all(&dir)?;
        let port = std::net::TcpListener::bind("127.0.0.1:0")?
            .local_addr()?
            .port();
        let cluster = Self { dir, port };

        let data = cluster.dir.join("data");
        run(Command::new("initdb").arg("-D").arg(&data).args([
            "-U",
            "postgres",
            "--auth=trust",
            "--no-locale",
            "-E",
            "UTF8",
        ]))?;
        std::fs::write(data.join("pg_hba.conf"), HBA)?;
        run(Command::new("pg_ctl")
            .arg("-D")
            .arg(&data)
            .arg("-l")
            .arg(cluster.dir.join("log"))
            .arg("-o")
            .arg(format!(
                "-p {} -k {} -c listen_addresses=127.0.0.1",
                cluster.port,
                cluster.dir.display()
            ))
            .args(["-w", "start"]))?;
        Ok(Some(cluster))
    }
}

impl Drop for Cluster {
    fn drop(&mut self) {
        let _ = Command::new("pg_ctl")
            .arg("-D")
            .arg(self.dir.join("data"))
            .args(["-m", "immediate", "-w", "stop"])
            .output();
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

fn run(command: &mut Command) -> Result<()> {
    let output = command.output().with_context(|| format!("{command:?}"))?;
    ensure!(
        output.status.success(),
        "{command:?} failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    Ok(())
}
