//! The `pges-sequencer` binary: it sequences a test database, logs its status, and stops cleanly on SIGTERM,
//! releasing the lease.

#![cfg(all(feature = "bin", unix))]
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

mod common;

use std::{process::Stdio, sync::Arc, time::Duration};

use anyhow::{Context, Result, ensure};
use tokio::{io::AsyncReadExt, process::Command, task::JoinHandle};

use pges_core::stream;

use common::{Fixture, Kind, Target};

#[tokio::test]
async fn the_binary_sequences_and_stops_on_sigterm() -> Result<()> {
    smoke(Kind::Outbox).await
}

#[cfg(feature = "source-logical")]
#[tokio::test]
async fn the_binary_runs_the_logical_source() -> Result<()> {
    smoke(Kind::Logical).await
}

async fn smoke(kind: Kind) -> Result<()> {
    let f = Fixture::with(kind).await?;
    let source = match kind {
        Kind::Outbox => "outbox",
        Kind::Logical => "logical",
    };
    // The slot must exist before the first append: the appends below can otherwise commit while the
    // sequencer creates it, before its consistent point, and the sequencer would refuse to start.
    if kind == Kind::Logical {
        let output = run_to_end(&f, &["bootstrap"]).await?;
        ensure!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
    let mut child = Command::new(env!("CARGO_BIN_EXE_pges-sequencer"))
        .args(["--source", source, "--token-interval", "100ms"])
        .args(["--lease-ttl", "2s", "--renew-interval", "500ms"])
        .env("PGES_DATABASE_URL", f.db.url())
        .env("RUST_LOG", "info")
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()?;

    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    loop {
        let leading: bool =
            sqlx::query_scalar("SELECT lease_until IS NOT NULL FROM pges.sequencer")
                .fetch_one(&f.pool)
                .await?;
        let streaming = match kind {
            Kind::Outbox => true,
            Kind::Logical => sqlx::query_scalar(
                "SELECT EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = $1 AND active)",
            )
            .bind(f.slot().await?)
            .fetch_one(&f.pool)
            .await?,
        };
        if leading && streaming {
            break;
        }
        ensure!(
            tokio::time::Instant::now() < deadline,
            "the binary never took the lease"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    for _ in 0..3 {
        f.append(Target::Job(stream::StreamId::new_v7()), 2).await?;
    }
    common::wait_sequenced(&f.pool, 3).await?;

    let pid = child.id().context("the binary exited early")?;
    let killed = std::process::Command::new("kill")
        .args(["-TERM", &pid.to_string()])
        .status()?;
    ensure!(killed.success(), "kill failed");
    let status = tokio::time::timeout(Duration::from_secs(20), child.wait())
        .await
        .context("the binary did not stop")??;
    let mut stderr = String::new();
    child
        .stderr
        .take()
        .unwrap()
        .read_to_string(&mut stderr)
        .await?;
    ensure!(status.success(), "exit status {status}: {stderr}");
    ensure!(stderr.contains("leading"), "{stderr}");
    ensure!(stderr.contains("shutting down"), "{stderr}");
    ensure!(!stderr.contains("recovering"), "{stderr}");

    let released: bool = sqlx::query_scalar("SELECT lease_until IS NULL FROM pges.sequencer")
        .fetch_one(&f.pool)
        .await?;
    ensure!(released, "the lease was not released");
    assert_eq!(common::check_log(&f.pool).await?.len(), 3);

    f.finish().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_binary_killed_mid_load_loses_and_repeats_nothing() -> Result<()> {
    killed_mid_load(Kind::Outbox).await
}

#[cfg(feature = "source-logical")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_logical_binary_killed_mid_load_loses_and_repeats_nothing() -> Result<()> {
    killed_mid_load(Kind::Logical).await
}

/// Writers append throughout. Three binaries in turn are killed with SIGKILL while they sequence; each
/// next one takes over once the lease expires. A last one drains the rest and stops on SIGTERM.
async fn killed_mid_load(kind: Kind) -> Result<()> {
    let f = Arc::new(Fixture::with(kind).await?);
    if kind == Kind::Logical {
        let output = run_to_end(&f, &["bootstrap"]).await?;
        ensure!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
    let load = common::Load::start(&f, 4);

    // Every other binary stores its token with each batch, so kills land both before and after the token.
    for token_interval in ["100ms", "0s", "100ms"] {
        let (mut child, stderr) = spawn_sequencer(&f, kind, token_interval)?;
        let from = log_count(&f).await?;
        let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
        while log_count(&f).await? < from + 30 {
            ensure!(
                tokio::time::Instant::now() < deadline,
                "the binary did not sequence"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        child.kill().await?;
        let stderr = stderr.await??;
        ensure!(!stderr.contains("recovering"), "{stderr}");
    }

    let (child, stderr) = spawn_sequencer(&f, kind, "100ms")?;
    tokio::time::sleep(Duration::from_millis(300)).await;
    let appended = load.stop().await?;
    let commits = common::commit_count(&f.pool).await?;
    ensure!(commits == appended.len());
    common::wait_sequenced(&f.pool, commits).await?;
    let log = common::check_log(&f.pool).await?;
    common::check_every_commit_once(&f.pool).await?;
    common::check_commit_order(&log, &appended)?;

    let pid = child.id().context("the binary exited early")?;
    let killed = std::process::Command::new("kill")
        .args(["-TERM", &pid.to_string()])
        .status()?;
    ensure!(killed.success(), "kill failed");
    let status = tokio::time::timeout(Duration::from_secs(20), child.wait_with_output())
        .await
        .context("the binary did not stop")??
        .status;
    let stderr = stderr.await??;
    ensure!(status.success(), "exit status {status}: {stderr}");
    ensure!(!stderr.contains("recovering"), "{stderr}");

    Arc::into_inner(f).unwrap().finish().await
}

/// A sequencer binary with a short lease, and a task collecting what it logs.
fn spawn_sequencer(
    f: &Fixture,
    kind: Kind,
    token_interval: &str,
) -> Result<(tokio::process::Child, JoinHandle<Result<String>>)> {
    let source = match kind {
        Kind::Outbox => "outbox",
        Kind::Logical => "logical",
    };
    let mut child = Command::new(env!("CARGO_BIN_EXE_pges-sequencer"))
        .args(["--source", source, "--token-interval", token_interval])
        .args(["--lease-ttl", "1s", "--renew-interval", "300ms"])
        .args(["--acquire-interval", "100ms", "--batch-size", "8"])
        .env("PGES_DATABASE_URL", f.db.url())
        .env("RUST_LOG", "info")
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()?;
    let mut stderr = child.stderr.take().context("no stderr")?;
    let read = tokio::spawn(async move {
        let mut text = String::new();
        stderr.read_to_string(&mut text).await?;
        Ok(text)
    });
    Ok((child, read))
}

async fn log_count(f: &Fixture) -> Result<i64> {
    Ok(sqlx::query_scalar("SELECT count(*) FROM es.log")
        .fetch_one(&f.pool)
        .await?)
}

/// `--grpc-listen` serves the watch while the binary runs, here on a host name; SIGTERM ends the stream and
/// the process.
#[cfg(feature = "watch-grpc")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_binary_serves_the_grpc_watch() -> Result<()> {
    use pges_watch_proto::v1;

    let f = Fixture::new().await?;
    let addr = {
        let listener = std::net::TcpListener::bind("127.0.0.1:0")?;
        listener.local_addr()?
    };
    let mut child = Command::new(env!("CARGO_BIN_EXE_pges-sequencer"))
        .args(["--source", "outbox", "--token-interval", "100ms"])
        // A host name: the binary serves on each address it resolves to (127.0.0.1 among them).
        .env("PGES_GRPC_LISTEN", format!("localhost:{}", addr.port()))
        .env("PGES_DATABASE_URL", f.db.url())
        .env("RUST_LOG", "info")
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()?;

    let endpoint = tonic::transport::Endpoint::from_shared(format!("http://{addr}"))?;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    let mut client = loop {
        if let Ok(channel) = endpoint.connect().await {
            let mut client = v1::watch_client::WatchClient::new(channel);
            if let Ok(status) = client.status(v1::StatusRequest {}).await
                && status.into_inner().state() == v1::SequencerState::Leading
            {
                break client;
            }
        }
        ensure!(
            tokio::time::Instant::now() < deadline,
            "the binary never served as the leader"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    };
    let mut stream = client
        .watch(v1::WatchRequest {
            store_id: f.store_id.get(),
        })
        .await?
        .into_inner();
    f.append(Target::Job(stream::StreamId::new_v7()), 2).await?;
    let reached = tokio::time::timeout(Duration::from_secs(20), async {
        while let Some(message) = stream.message().await? {
            if message.store >= 2 {
                return anyhow::Ok(());
            }
        }
        anyhow::bail!("the stream ended early")
    })
    .await;
    reached.context("no value reached position 2")??;

    let pid = child.id().context("the binary exited early")?;
    let killed = std::process::Command::new("kill")
        .args(["-TERM", &pid.to_string()])
        .status()?;
    ensure!(killed.success(), "kill failed");
    let ended = tokio::time::timeout(Duration::from_secs(20), stream.message())
        .await
        .context("the stream did not end")?;
    ensure!(
        matches!(&ended, Err(status) if status.code() == tonic::Code::Unavailable),
        "{ended:?}"
    );
    let status = tokio::time::timeout(Duration::from_secs(20), child.wait())
        .await
        .context("the binary did not stop")??;
    let mut stderr = String::new();
    child
        .stderr
        .take()
        .unwrap()
        .read_to_string(&mut stderr)
        .await?;
    ensure!(status.success(), "exit status {status}: {stderr}");
    ensure!(stderr.contains("serving the gRPC watch"), "{stderr}");
    ensure!(!stderr.contains("recovering"), "{stderr}");

    f.finish().await
}

/// `--grpc-tls-cert`, `--grpc-tls-key` and `--grpc-tls-client-ca` serve the watch over mutual TLS; unusable
/// files, or TLS without `--grpc-listen`, exit with status 2.
#[cfg(feature = "watch-grpc-tls")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_binary_serves_the_grpc_watch_over_tls() -> Result<()> {
    use pges_watch_proto::v1;

    let f = Fixture::new().await?;
    let pki = pges_testing::tls::Pki::generate()?;
    let dir = std::env::temp_dir().join(format!("pges-grpc-tls-{}", uuid::Uuid::now_v7().simple()));
    std::fs::create_dir_all(&dir)?;
    let write = |name: &str, pem: &str| -> Result<String> {
        let path = dir.join(name);
        std::fs::write(&path, pem)?;
        Ok(path.display().to_string())
    };
    let cert = write("server.crt", &pki.server_cert)?;
    let key = write("server.key", &pki.server_key)?;
    let ca = write("ca.crt", &pki.ca)?;
    let garbage = write("garbage.crt", "not a certificate")?;
    let addr = {
        let listener = std::net::TcpListener::bind("127.0.0.1:0")?;
        listener.local_addr()?
    };
    let listen = addr.to_string();

    for args in [
        &[
            "--grpc-listen",
            &listen,
            "--grpc-tls-cert",
            &garbage,
            "--grpc-tls-key",
            &key,
        ][..],
        &["--grpc-listen", &listen, "--grpc-tls-cert", &cert][..],
        &["--grpc-tls-cert", &cert, "--grpc-tls-key", &key][..],
    ] {
        let output = run_to_end(&f, &[&["--source", "outbox"][..], args].concat()).await?;
        let stderr = String::from_utf8_lossy(&output.stderr);
        ensure!(
            output.status.code() == Some(2),
            "{args:?}: {:?}: {stderr}",
            output.status
        );
    }

    let mut child = Command::new(env!("CARGO_BIN_EXE_pges-sequencer"))
        .args(["--source", "outbox", "--grpc-listen", &listen])
        .args(["--grpc-tls-cert", &cert, "--grpc-tls-key", &key])
        .env("PGES_GRPC_TLS_CLIENT_CA", &ca)
        .env("PGES_DATABASE_URL", f.db.url())
        .env("RUST_LOG", "info")
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()?;

    let endpoint = |tls: tonic::transport::ClientTlsConfig| {
        tonic::transport::Endpoint::from_shared(format!("https://localhost:{}", addr.port()))?
            .tls_config(tls)
    };
    let trusted = tonic::transport::ClientTlsConfig::new()
        .ca_certificate(tonic::transport::Certificate::from_pem(&pki.ca))
        .identity(tonic::transport::Identity::from_pem(
            &pki.client_cert,
            &pki.client_key,
        ));
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    loop {
        if let Ok(channel) = endpoint(trusted.clone())?.connect().await {
            let mut client = v1::watch_client::WatchClient::new(channel);
            if let Ok(status) = client.status(v1::StatusRequest {}).await
                && status.into_inner().state() == v1::SequencerState::Leading
            {
                break;
            }
        }
        ensure!(
            tokio::time::Instant::now() < deadline,
            "the binary never served as the leader"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let anonymous = tonic::transport::ClientTlsConfig::new()
        .ca_certificate(tonic::transport::Certificate::from_pem(&pki.ca));
    let refused = async {
        let channel = endpoint(anonymous)?.connect().await?;
        v1::watch_client::WatchClient::new(channel)
            .status(v1::StatusRequest {})
            .await?;
        anyhow::Ok(())
    };
    ensure!(
        refused.await.is_err(),
        "a client without a certificate was served"
    );

    let pid = child.id().context("the binary exited early")?;
    let killed = std::process::Command::new("kill")
        .args(["-TERM", &pid.to_string()])
        .status()?;
    ensure!(killed.success(), "kill failed");
    let status = tokio::time::timeout(Duration::from_secs(20), child.wait())
        .await
        .context("the binary did not stop")??;
    let mut stderr = String::new();
    child
        .stderr
        .take()
        .unwrap()
        .read_to_string(&mut stderr)
        .await?;
    ensure!(status.success(), "exit status {status}: {stderr}");
    ensure!(stderr.contains("serving the gRPC watch"), "{stderr}");
    std::fs::remove_dir_all(&dir)?;

    f.finish().await
}

#[tokio::test]
async fn a_halted_database_ends_the_binary_with_an_error() -> Result<()> {
    let f = Fixture::new().await?;
    sqlx::query("UPDATE pges.sequencer SET halted = 'slot invalidated'")
        .execute(&f.pool)
        .await?;

    let output = run_to_end(&f, &["--source", "outbox"]).await?;
    let stderr = String::from_utf8_lossy(&output.stderr);
    ensure!(
        output.status.code() == Some(1),
        "{:?}: {stderr}",
        output.status
    );
    ensure!(stderr.contains("slot invalidated"), "{stderr}");

    f.finish().await
}

/// `resequence` refuses a database that is not halted (exit 1), recovers a halted one (exit 0), and the
/// binary then sequences it again.
#[cfg(feature = "source-logical")]
#[tokio::test]
async fn resequence_recovers_a_halted_database_and_exits() -> Result<()> {
    let f = Fixture::logical().await?;
    let slot = f.slot().await?;

    let role = f.writer_role().await?;
    let output = run_to_end(&f, &["resequence", "--writer-role", &role]).await?;
    let stderr = String::from_utf8_lossy(&output.stderr);
    ensure!(
        output.status.code() == Some(1),
        "{:?}: {stderr}",
        output.status
    );
    ensure!(stderr.contains("not halted"), "{stderr}");

    f.append(Target::Job(stream::StreamId::new_v7()), 1).await?;
    sqlx::query("UPDATE pges.sequencer SET halted = 'slot invalidated'")
        .execute(&f.pool)
        .await?;
    let output = run_to_end(&f, &["resequence", "--writer-role", &role]).await?;
    let stderr = String::from_utf8_lossy(&output.stderr);
    ensure!(output.status.success(), "{:?}: {stderr}", output.status);
    ensure!(stderr.contains("resequenced"), "{stderr}");
    ensure!(stderr.contains(&slot), "{stderr}");
    let halted: Option<String> = sqlx::query_scalar("SELECT halted FROM pges.sequencer")
        .fetch_one(&f.pool)
        .await?;
    ensure!(halted.is_none(), "halted is still {halted:?}");
    common::check_log(&f.pool).await?;
    common::check_every_commit_once(&f.pool).await?;

    f.finish().await
}

#[cfg(feature = "source-logical")]
#[tokio::test]
async fn bootstrap_creates_the_slot_and_exits() -> Result<()> {
    let f = Fixture::logical().await?;
    let slot = f.slot().await?;

    for expected in [
        "created the replication slot",
        "the replication slot exists",
    ] {
        let output = run_to_end(&f, &["bootstrap"]).await?;
        let stderr = String::from_utf8_lossy(&output.stderr);
        ensure!(output.status.success(), "{:?}: {stderr}", output.status);
        ensure!(stderr.contains(expected), "{stderr}");
        ensure!(stderr.contains(&slot), "{stderr}");
    }
    let active: bool =
        sqlx::query_scalar("SELECT active FROM pg_replication_slots WHERE slot_name = $1")
            .bind(&slot)
            .fetch_one(&f.pool)
            .await?;
    ensure!(!active, "the bootstrap left the slot active");
    f.finish().await?;

    // Commits from before the slot are refused, and no slot is left behind.
    let f = Fixture::logical().await?;
    f.append(Target::Job(stream::StreamId::new_v7()), 1).await?;
    let output = run_to_end(&f, &["bootstrap"]).await?;
    let stderr = String::from_utf8_lossy(&output.stderr);
    ensure!(
        output.status.code() == Some(1),
        "{:?}: {stderr}",
        output.status
    );
    ensure!(stderr.contains("holds commits from before"), "{stderr}");

    f.finish().await
}

#[cfg(feature = "source-logical")]
#[tokio::test]
async fn bootstrap_creates_a_failover_slot_only_when_asked() -> Result<()> {
    for (args, env, expected) in [
        (&["bootstrap", "--failover"][..], None, true),
        (&["bootstrap"][..], Some("on"), true),
        (&["bootstrap"][..], None, false),
        (&["bootstrap", "--no-failover"][..], None, false),
    ] {
        let f = Fixture::logical().await?;
        let slot = f.slot().await?;

        let output = run_to_end_with(&f, args, env).await?;
        let stderr = String::from_utf8_lossy(&output.stderr);
        ensure!(output.status.success(), "{:?}: {stderr}", output.status);
        let failover = slot_failover(&f, &slot).await?;
        ensure!(
            failover == expected,
            "{args:?} {env:?}: failover is {failover}"
        );

        f.finish().await?;
    }

    Ok(())
}

/// Without a flag, `bootstrap` keeps an existing slot's failover setting; `--failover` and `--no-failover`
/// change it.
#[cfg(feature = "source-logical")]
#[tokio::test]
async fn bootstrap_changes_failover_on_an_existing_slot_only_when_asked() -> Result<()> {
    const CREATED: &str = "created the replication slot";
    const CHANGED: &str = "changed the replication slot's failover setting";
    const EXISTS: &str = "the replication slot exists";
    let f = Fixture::logical().await?;
    let slot = f.slot().await?;

    for (args, env, expected, logged) in [
        (&["bootstrap"][..], None, false, CREATED),
        (&["bootstrap", "--failover"][..], None, true, CHANGED),
        (&["bootstrap"][..], None, true, EXISTS),
        (&["bootstrap"][..], Some("on"), true, EXISTS),
        (&["bootstrap", "--failover"][..], None, true, EXISTS),
        (
            &["bootstrap", "--no-failover"][..],
            Some("on"),
            false,
            CHANGED,
        ),
        (&["bootstrap"][..], None, false, EXISTS),
        (&["bootstrap"][..], Some("on"), true, CHANGED),
        (&["bootstrap"][..], Some("off"), false, CHANGED),
    ] {
        let output = run_to_end_with(&f, args, env).await?;
        let stderr = String::from_utf8_lossy(&output.stderr);
        ensure!(output.status.success(), "{:?}: {stderr}", output.status);
        ensure!(stderr.contains(logged), "{args:?} {env:?}: {stderr}");
        let failover = slot_failover(&f, &slot).await?;
        ensure!(
            failover == expected,
            "{args:?} {env:?}: failover is {failover}"
        );
    }

    let output = run_to_end(&f, &["bootstrap", "--failover", "--no-failover"]).await?;
    ensure!(output.status.code() == Some(2), "{:?}", output.status);
    ensure!(!slot_failover(&f, &slot).await?);

    f.finish().await
}

#[cfg(feature = "source-logical")]
async fn slot_failover(f: &Fixture, slot: &str) -> Result<bool> {
    Ok(
        sqlx::query_scalar("SELECT failover FROM pg_replication_slots WHERE slot_name = $1")
            .bind(slot)
            .fetch_one(&f.pool)
            .await?,
    )
}

/// Runs the binary with `args` against the fixture's database until it exits by itself.
async fn run_to_end(f: &Fixture, args: &[&str]) -> Result<std::process::Output> {
    run_to_end_with(f, args, None).await
}

/// [`run_to_end`] with `PGES_FAILOVER` set to `failover`, or unset.
async fn run_to_end_with(
    f: &Fixture,
    args: &[&str],
    failover: Option<&str>,
) -> Result<std::process::Output> {
    let mut command = Command::new(env!("CARGO_BIN_EXE_pges-sequencer"));
    command.env_remove("PGES_FAILOVER");
    if let Some(failover) = failover {
        command.env("PGES_FAILOVER", failover);
    }
    let child = command
        .args(args)
        .env("PGES_DATABASE_URL", f.db.url())
        .env("RUST_LOG", "info")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()?;
    tokio::time::timeout(Duration::from_secs(20), child.wait_with_output())
        .await
        .context("the binary did not exit")?
        .context("waiting for the binary")
}
