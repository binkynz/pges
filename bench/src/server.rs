//! A private PostgreSQL 18 in Docker with production-like durability, and optionally a streaming replica,
//! for numbers that hold on a durable server.
//!
//! `pges-bench server start` creates the containers, waits until they accept connections and prints the
//! URL to pass as `--database-url`. The server keeps running across benchmark commands until
//! `pges-bench server stop` removes it, with its data and network. Its settings are [`DURABLE`], sized for a
//! 24-thread, 91 GiB, NVMe dev box; `--set name=value` overrides one.

use std::{process::Command, time::Duration};

use anyhow::{Context, Result, bail, ensure};
use serde_json::json;
use sqlx::{Connection, Executor, PgConnection};

use pges::postgres::sql::typed::Query as _;

use crate::{report, sql};

/// Options of `pges-bench server`.
#[derive(Debug, Clone, clap::Args)]
pub struct Args {
    /// What to do.
    #[command(subcommand)]
    pub action: Action,
}

/// What `pges-bench server` does.
#[derive(Debug, Clone, clap::Subcommand)]
pub enum Action {
    /// Starts the server (and its replica) and prints the URL to benchmark with.
    Start(StartArgs),
    /// Stops and removes the server, its replica, their data and their network.
    Stop(NameArgs),
    /// Prints the settings `start` passes to the server.
    Settings,
}

/// Options of `pges-bench server start`.
#[derive(Debug, Clone, clap::Args)]
pub struct StartArgs {
    /// Container name; the replica is `<name>-replica` and the network `<name>`.
    #[arg(long, default_value = DEFAULT_NAME)]
    pub name: String,
    /// Host port of the primary, bound to 127.0.0.1; the replica listens on the next one.
    #[arg(long, default_value_t = 54330)]
    pub port: u16,
    /// Whether a streaming replica runs, and whether commits wait for it.
    #[arg(long, value_enum, default_value = "none")]
    pub replica: Replica,
    /// A setting to pass instead of the default, as `name=value`; repeatable.
    #[arg(long = "set", value_name = "NAME=VALUE")]
    pub settings: Vec<String>,
    /// The `postgres` image tag.
    #[arg(long, default_value = "18")]
    pub tag: String,
}

/// Options naming the server to stop.
#[derive(Debug, Clone, clap::Args)]
pub struct NameArgs {
    /// Container name given to `start`.
    #[arg(long, default_value = DEFAULT_NAME)]
    pub name: String,
}

/// The replica `start` adds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
pub enum Replica {
    /// No replica.
    None,
    /// A hot standby the primary does not wait for.
    Async,
    /// A hot standby every commit waits for (`synchronous_standby_names`, `synchronous_commit = on`, so
    /// the standby has flushed the commit's WAL).
    Sync,
}

/// Server settings: durability on, WAL kept for logical decoding, and memory and checkpoint sizes for the
/// dev box (24 threads, 91 GiB shared with other work, NVMe). Checkpoints every 5 minutes, so a 10-minute
/// run sees two, with the full-page images that follow each. Above about 30 MB/s of WAL, `max_wal_size`
/// starts them sooner.
pub const DURABLE: &[(&str, &str)] = &[
    ("fsync", "on"),
    ("synchronous_commit", "on"),
    ("full_page_writes", "on"),
    ("wal_level", "logical"),
    ("wal_compression", "lz4"),
    ("wal_buffers", "64MB"),
    ("max_wal_size", "16GB"),
    ("min_wal_size", "2GB"),
    ("checkpoint_timeout", "5min"),
    ("checkpoint_completion_target", "0.9"),
    ("shared_buffers", "8GB"),
    ("effective_cache_size", "32GB"),
    ("work_mem", "16MB"),
    ("maintenance_work_mem", "1GB"),
    ("logical_decoding_work_mem", "256MB"),
    ("max_connections", "300"),
    ("max_wal_senders", "16"),
    ("max_replication_slots", "16"),
    ("max_slot_wal_keep_size", "50GB"),
    ("max_locks_per_transaction", "256"),
    ("random_page_cost", "1.1"),
    ("effective_io_concurrency", "64"),
    ("io_workers", "8"),
    ("autovacuum_max_workers", "6"),
    ("autovacuum_vacuum_cost_limit", "2000"),
    ("hot_standby", "on"),
    ("hot_standby_feedback", "on"),
    ("track_io_timing", "on"),
    ("track_wal_io_timing", "on"),
    ("shared_preload_libraries", "pg_stat_statements"),
    ("log_checkpoints", "on"),
    ("log_autovacuum_min_duration", "0"),
    ("log_lock_waits", "on"),
];

/// Runs `pges-bench server`.
pub async fn run(args: &Args) -> Result<()> {
    match &args.action {
        Action::Start(start_args) => start(start_args).await,
        Action::Stop(name) => {
            remove(&name.name);
            eprintln!("removed {0}, {0}-replica and their network", name.name);
            Ok(())
        }
        Action::Settings => {
            for (name, value) in DURABLE {
                println!("{name} = {value}");
            }
            Ok(())
        }
    }
}

const DEFAULT_NAME: &str = "pges-bench-durable";

/// Run by the primary's entrypoint on first start: the image's `pg_hba.conf` accepts replication
/// connections only from inside the container.
const ALLOW_REPLICATION: &str =
    "echo 'host replication all all trust' >> \"$PGDATA/pg_hba.conf\"\n";

async fn start(args: &StartArgs) -> Result<()> {
    let settings = settings(&args.settings)?;
    let replica_port = args
        .port
        .checked_add(1)
        .context("--port must leave room for the replica's port")?;
    ensure!(
        !exists(&args.name),
        "a container named {} exists; run `pges-bench server stop --name {}` first",
        args.name,
        args.name
    );

    let started = start_containers(args, &settings, replica_port).await;
    if let Err(error) = started {
        remove(&args.name);
        return Err(error);
    }
    let (url, replica_url) = started?;

    report::emit(&json!({
        "server": args.name,
        "url": url,
        "replica": format!("{:?}", args.replica).to_lowercase(),
        "replica_url": replica_url,
        "settings": settings.iter().cloned().collect::<serde_json::Map<_, _>>(),
    }));
    eprintln!("started {}: --database-url {url}", args.name);
    if let Some(replica_url) = replica_url {
        eprintln!("replica: {replica_url}");
    }
    eprintln!("stop it with: pges-bench server stop --name {}", args.name);

    Ok(())
}

async fn start_containers(
    args: &StartArgs,
    settings: &[(String, serde_json::Value)],
    replica_port: u16,
) -> Result<(String, Option<String>)> {
    let name = &args.name;
    let image = format!("postgres:{}", args.tag);
    let server_args: Vec<String> = settings
        .iter()
        .flat_map(|(setting, value)| {
            let value = value.as_str().unwrap_or_default();
            ["-c".to_owned(), format!("{setting}={value}")]
        })
        .collect();

    docker(&["network", "create", name])?;
    let mut create = vec![
        "create".to_owned(),
        "--name".into(),
        name.clone(),
        "--network".into(),
        name.clone(),
        "--publish".into(),
        format!("127.0.0.1:{}:5432", args.port),
        "--shm-size".into(),
        "2g".into(),
        "--env".into(),
        "POSTGRES_PASSWORD=postgres".into(),
        image.clone(),
    ];
    create.extend(server_args.iter().cloned());
    docker(&create.iter().map(String::as_str).collect::<Vec<_>>())?;
    let script = std::env::temp_dir().join(format!("{name}-allow-replication.sh"));
    std::fs::write(&script, ALLOW_REPLICATION).context("writing the pg_hba script")?;
    let copied = docker(&[
        "cp",
        &script.to_string_lossy(),
        &format!("{name}:/docker-entrypoint-initdb.d/allow-replication.sh"),
    ]);
    let _removed = std::fs::remove_file(&script);
    copied?;
    docker(&["start", name])?;

    let url = database_url(args.port, "postgres");
    let mut conn = connect(&url, Duration::from_secs(120))
        .await
        .context("waiting for the primary")?;
    // Scratch databases are created from template1, so they get the extensions the benchmarks read.
    for database in ["postgres", "template1"] {
        let mut db = PgConnection::connect(&database_url(args.port, database)).await?;
        db.execute(sql::statement::STAT_STATEMENTS.sql()).await?;
        db.execute(sql::statement::WALINSPECT.sql()).await?;
        db.close().await?;
    }

    if args.replica == Replica::None {
        return Ok((url, None));
    }

    // The replica gets the primary's settings: a hot standby needs at least its max_connections,
    // max_wal_senders and max_locks_per_transaction. pg_basebackup is retried until the primary's name
    // resolves.
    let replica = format!("{name}-replica");
    let script = format!(
        "set -e
         mkdir -p /var/lib/postgresql/replica
         chown postgres:postgres /var/lib/postgresql/replica
         until gosu postgres pg_basebackup -d 'host={name} user=postgres application_name=replica' \
               -D /var/lib/postgresql/replica/data -R -X stream -c fast; do
           rm -rf /var/lib/postgresql/replica/data
           sleep 0.5
         done
         exec gosu postgres postgres -D /var/lib/postgresql/replica/data {}",
        server_args
            .iter()
            .map(|arg| shell_quoted(arg))
            .collect::<Vec<_>>()
            .join(" ")
    );
    docker(&[
        "run",
        "--detach",
        "--name",
        &replica,
        "--network",
        name,
        "--publish",
        &format!("127.0.0.1:{replica_port}:5432"),
        "--shm-size",
        "2g",
        "--entrypoint",
        "bash",
        &image,
        "-c",
        &script,
    ])?;
    let replica_url = database_url(replica_port, "postgres");
    let mut replica_conn = connect(&replica_url, Duration::from_secs(300))
        .await
        .context("waiting for the replica")?;
    let in_recovery = sql::statement::in_recovery::Query::fixed()
        .bind(sql::statement::in_recovery::Params {})
        .fetch_one(&mut replica_conn)
        .await?
        .in_recovery;
    ensure!(in_recovery, "the replica is not in recovery");

    if args.replica == Replica::Sync {
        conn.execute(sql::statement::SYNCHRONOUS_STANDBY.sql())
            .await?;
        conn.execute(sql::statement::RELOAD.sql()).await?;
    }

    Ok((url, Some(replica_url)))
}

/// `text` as one word of a POSIX shell command line.
fn shell_quoted(text: &str) -> String {
    format!("'{}'", text.replace('\'', r"'\''"))
}

fn database_url(port: u16, database: &str) -> String {
    format!("postgres://postgres:postgres@127.0.0.1:{port}/{database}")
}

/// The default settings with `overrides` (`name=value`) applied, in order.
fn settings(overrides: &[String]) -> Result<Vec<(String, serde_json::Value)>> {
    let mut settings: Vec<(String, serde_json::Value)> = DURABLE
        .iter()
        .map(|(name, value)| ((*name).to_owned(), json!(value)))
        .collect();
    for item in overrides {
        let Some((name, value)) = item.split_once('=') else {
            bail!("--set takes name=value, not {item:?}");
        };
        ensure!(
            name.chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '.'),
            "not a setting name: {name:?}"
        );
        match settings.iter_mut().find(|(known, _)| known == name) {
            Some(setting) => setting.1 = json!(value),
            None => settings.push((name.to_owned(), json!(value))),
        }
    }

    Ok(settings)
}

/// Connects to `url`, retrying until `limit`: the entrypoint restarts the server once after initialising it.
async fn connect(url: &str, limit: Duration) -> Result<PgConnection> {
    let deadline = tokio::time::Instant::now() + limit;
    loop {
        match PgConnection::connect(url).await {
            Ok(conn) => return Ok(conn),
            Err(error) if tokio::time::Instant::now() >= deadline => {
                return Err(error).with_context(|| format!("connecting to {url}"));
            }
            Err(_) => tokio::time::sleep(Duration::from_millis(500)).await,
        }
    }
}

fn exists(name: &str) -> bool {
    Command::new("docker")
        .args(["container", "inspect", name])
        .output()
        .is_ok_and(|output| output.status.success())
}

/// Removes the containers with their anonymous volumes, then the network; missing ones are skipped.
fn remove(name: &str) {
    let replica = format!("{name}-replica");
    let _replica = docker(&["rm", "--force", "--volumes", &replica]);
    let _primary = docker(&["rm", "--force", "--volumes", name]);
    let _network = docker(&["network", "rm", name]);
}

fn docker(args: &[&str]) -> Result<()> {
    let output = Command::new("docker")
        .args(args)
        .output()
        .context("running docker (is it installed and running?)")?;
    ensure!(
        output.status.success(),
        "docker {} failed: {}",
        args.first().copied().unwrap_or_default(),
        String::from_utf8_lossy(&output.stderr).trim()
    );

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn overrides_replace_or_add_settings() {
        let settings = settings(&["shared_buffers=1GB".into(), "jit=off".into()]).unwrap();
        let value = |name: &str| {
            settings
                .iter()
                .find(|(known, _)| known == name)
                .map(|(_, value)| value.clone())
        };

        assert_eq!(value("shared_buffers"), Some(json!("1GB")));
        assert_eq!(value("jit"), Some(json!("off")));
        assert_eq!(value("fsync"), Some(json!("on")));
        assert!(super::settings(&["nonsense".into()]).is_err());
        assert!(super::settings(&["a;b=1".into()]).is_err());
    }

    #[test]
    fn shell_quoting_keeps_a_value_one_word() {
        assert_eq!(shell_quoted("-c"), "'-c'");
        assert_eq!(shell_quoted("prefix=%m 'x'"), r"'prefix=%m '\''x'\'''");
    }
}
