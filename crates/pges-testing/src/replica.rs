//! A private PostgreSQL 18 primary with a streaming hot standby, for tests of replica reads.
//!
//! [`Pair::start`] follows the backend [`server`](crate::server) would use:
//!
//! - Docker (also when `PGES_TEST_DATABASE_URL` names a server, which cannot be cloned): two containers on a
//!   network of their own. The primary's `pg_hba.conf` accepts replication connections; the replica takes
//!   a `pg_basebackup` of it and starts as a hot standby.
//! - The local backend (`pg-up`): two clusters in a temporary directory, started with `initdb`, `pg_ctl` and
//!   `pg_basebackup` from `PATH` (the Nix dev shell has them), listening on unix sockets only.
//!
//! The pair is private to the test: the shared server is never touched. Both servers run with the same
//! settings as the shared one (logical WAL, no fsync). [`Pair::stop`] removes it; a pair dropped without
//! `stop` is removed too, best effort.

use std::{path::PathBuf, process::Command, time::Duration};

use anyhow::{Context, Result, bail, ensure};
use sqlx::{Connection, Executor, PgConnection};
use testcontainers::{
    ContainerAsync, GenericImage, ImageExt,
    core::{IntoContainerPort, WaitFor},
    runners::AsyncRunner,
};
use testcontainers_modules::postgres::Postgres;

use pges_postgres::sql::{template, typed, typed::Query as _};

use crate::{Backend, SERVER_ARGS, TestDb, sql, url_parts};

/// A primary and its streaming replica.
pub struct Pair {
    primary: String,
    replica: String,
    servers: Servers,
}

impl Pair {
    /// Starts a primary and a replica of it, and waits until the replica accepts read-only connections.
    pub async fn start() -> Result<Self> {
        match Backend::select()? {
            Backend::Local { .. } => start_local().await,
            Backend::Docker | Backend::Url(_) => start_docker().await,
        }
    }

    /// The URL of `database` on the primary.
    pub fn primary_url(&self, database: &str) -> Result<String> {
        with_database(&self.primary, database)
    }

    /// The URL of `database` on the replica.
    pub fn replica_url(&self, database: &str) -> Result<String> {
        with_database(&self.replica, database)
    }

    /// Creates `name` on the primary with the shared `pges` migrations applied, and waits until the replica
    /// has it. Removing the pair removes the database.
    pub async fn create_database(&self, name: &str) -> Result<TestDb> {
        let create = sql::statement::CREATE_DATABASE.render(&crate::database(name)?);
        let mut admin = PgConnection::connect(&self.primary_url("postgres")?).await?;
        admin
            .execute(template::text(create.as_str()))
            .await
            .with_context(|| format!("creating {name} on the primary"))?;
        let url = self.primary_url(name)?;
        let mut conn = PgConnection::connect(&url).await?;
        crate::apply_pges(&mut conn).await?;
        conn.close().await?;
        self.caught_up().await?;

        Ok(TestDb::existing(name, url))
    }

    /// Waits until the replica has replayed everything the primary had written when it was called.
    pub async fn caught_up(&self) -> Result<()> {
        let mut primary = PgConnection::connect(&self.primary_url("postgres")?).await?;
        let lsn = sql::statement::insert_lsn::Query::fixed()
            .bind(sql::statement::insert_lsn::Params {})
            .fetch_one(&mut primary)
            .await?
            .lsn;
        let target: typed::PgLsn = lsn.parse()?;
        let mut replica = PgConnection::connect(&self.replica_url("postgres")?).await?;

        tokio::time::timeout(Duration::from_secs(30), async {
            loop {
                let reached = sql::statement::replayed::Query::fixed()
                    .bind(sql::statement::replayed::Params { lsn: target })
                    .fetch_one(&mut replica)
                    .await?
                    .reached;
                if reached == Some(true) {
                    return anyhow::Ok(());
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .with_context(|| format!("waiting for the replica to replay {lsn}"))?
    }

    /// Pauses WAL replay on the replica: it keeps receiving WAL but shows nothing new until
    /// [`Pair::resume_replay`].
    pub async fn pause_replay(&self) -> Result<()> {
        self.on_replica(&sql::statement::PAUSE_REPLAY).await
    }

    /// Resumes WAL replay on the replica.
    pub async fn resume_replay(&self) -> Result<()> {
        self.on_replica(&sql::statement::RESUME_REPLAY).await
    }

    /// Stops and removes both servers.
    pub async fn stop(mut self) -> Result<()> {
        match std::mem::replace(&mut self.servers, Servers::Stopped) {
            Servers::Docker { primary, replica } => {
                replica.rm().await?;
                primary.rm().await?;
                Ok(())
            }
            Servers::Local { dir } => {
                tokio::task::spawn_blocking(move || remove_local(&dir)).await?
            }
            Servers::Stopped => Ok(()),
        }
    }

    async fn on_replica(&self, statement: &template::Statement) -> Result<()> {
        let mut replica = PgConnection::connect(&self.replica_url("postgres")?).await?;
        replica.execute(statement.sql()).await?;

        Ok(())
    }
}

impl Drop for Pair {
    fn drop(&mut self) {
        // Containers remove themselves when dropped; local clusters are stopped and deleted here.
        if let Servers::Local { dir } = std::mem::replace(&mut self.servers, Servers::Stopped)
            && let Err(error) = remove_local(&dir)
        {
            eprintln!(
                "pges-testing: removing the replica pair in {}: {error:#}",
                dir.display()
            );
        }
    }
}

impl std::fmt::Debug for Pair {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Pair")
            .field("primary", &self.primary)
            .field("replica", &self.replica)
            .finish_non_exhaustive()
    }
}

enum Servers {
    Docker {
        primary: Box<ContainerAsync<Postgres>>,
        replica: Box<ContainerAsync<GenericImage>>,
    },
    Local {
        dir: PathBuf,
    },
    Stopped,
}

/// Run by the primary's entrypoint on first start: the image's `pg_hba.conf` accepts replication
/// connections only from inside the container.
const ALLOW_REPLICATION: &str =
    "echo 'host replication all all trust' >> \"$PGDATA/pg_hba.conf\"\n";

const LOCAL_PRIMARY_PORT: u16 = 54401;
const LOCAL_REPLICA_PORT: u16 = 54402;

async fn start_docker() -> Result<Pair> {
    let id = uuid::Uuid::now_v7().simple().to_string();
    let network = format!("pges-test-replica-{id}");
    let primary_name = format!("pges-test-primary-{id}");
    let primary = Postgres::default()
        .with_tag("18")
        .with_cmd(SERVER_ARGS.iter().copied())
        .with_copy_to(
            "/docker-entrypoint-initdb.d/allow-replication.sh",
            ALLOW_REPLICATION.as_bytes().to_vec(),
        )
        .with_network(&network)
        .with_container_name(&primary_name)
        .start()
        .await
        .context("starting the primary container (is Docker running?)")?;

    // The replica needs at least the primary's max_connections and max_wal_senders, so it gets the same
    // settings. pg_basebackup is retried: the primary's name may take a moment to resolve.
    let args = SERVER_ARGS.join(" ");
    let script = format!(
        "set -e
         mkdir -p /var/lib/postgresql/replica
         chown postgres:postgres /var/lib/postgresql/replica
         until gosu postgres pg_basebackup -h {primary_name} -U postgres \
               -D /var/lib/postgresql/replica/data -R -X stream -c fast; do
           rm -rf /var/lib/postgresql/replica/data
           sleep 0.5
         done
         exec gosu postgres postgres -D /var/lib/postgresql/replica/data {args}"
    );
    let replica = GenericImage::new("postgres", "18")
        .with_exposed_port(5432.tcp())
        .with_wait_for(WaitFor::message_on_stderr(
            "database system is ready to accept read-only connections",
        ))
        .with_entrypoint("bash")
        .with_cmd(["-c", script.as_str()])
        .with_network(&network)
        .with_container_name(format!("pges-test-replica-{id}"))
        .with_startup_timeout(Duration::from_secs(120))
        .start()
        .await
        .context("starting the replica container")?;

    let url =
        |host: &str, port: u16| format!("postgres://postgres:postgres@{host}:{port}/postgres");
    Ok(Pair {
        primary: url(
            &primary.get_host().await?.to_string(),
            primary.get_host_port_ipv4(5432).await?,
        ),
        replica: url(
            &replica.get_host().await?.to_string(),
            replica.get_host_port_ipv4(5432).await?,
        ),
        servers: Servers::Docker {
            primary: Box::new(primary),
            replica: Box::new(replica),
        },
    })
}

async fn start_local() -> Result<Pair> {
    let dir = std::env::temp_dir().join(format!(
        "pges-replica-{}",
        &uuid::Uuid::now_v7().simple().to_string()[20..]
    ));
    let started = {
        let dir = dir.clone();
        tokio::task::spawn_blocking(move || start_local_clusters(&dir)).await?
    };
    if let Err(error) = started {
        let _ = remove_local(&dir);
        return Err(error);
    }

    let url = |port: u16| {
        format!(
            "postgres://postgres@localhost/postgres?host={}&port={port}",
            dir.display()
        )
    };
    Ok(Pair {
        primary: url(LOCAL_PRIMARY_PORT),
        replica: url(LOCAL_REPLICA_PORT),
        servers: Servers::Local { dir },
    })
}

fn start_local_clusters(dir: &std::path::Path) -> Result<()> {
    std::fs::create_dir_all(dir)?;
    let primary = dir.join("primary");
    let replica = dir.join("replica");
    let socket_dir = dir.display().to_string();
    let options = |port: u16| {
        format!(
            "-k {socket_dir} -c listen_addresses= -p {port} {}",
            SERVER_ARGS.join(" ")
        )
    };

    run(Command::new("initdb").arg("-D").arg(&primary).args([
        "-U",
        "postgres",
        "--auth=trust",
        "--no-locale",
        "-E",
        "UTF8",
    ]))?;
    run(Command::new("pg_ctl")
        .arg("-D")
        .arg(&primary)
        .arg("-l")
        .arg(dir.join("primary.log"))
        .args(["-w", "start", "-o", &options(LOCAL_PRIMARY_PORT)]))?;
    run(Command::new("pg_basebackup")
        .args(["-h", &socket_dir, "-p", &LOCAL_PRIMARY_PORT.to_string()])
        .args(["-U", "postgres", "-D"])
        .arg(&replica)
        .args(["-R", "-X", "stream", "-c", "fast"]))?;
    run(Command::new("pg_ctl")
        .arg("-D")
        .arg(&replica)
        .arg("-l")
        .arg(dir.join("replica.log"))
        .args(["-w", "start", "-o", &options(LOCAL_REPLICA_PORT)]))
}

/// Stops both local clusters, if running, and deletes the directory.
fn remove_local(dir: &std::path::Path) -> Result<()> {
    for cluster in ["replica", "primary"] {
        let data = dir.join(cluster);
        if data.join("postmaster.pid").exists() {
            let _ = Command::new("pg_ctl")
                .arg("-D")
                .arg(&data)
                .args(["-m", "immediate", "-w", "stop"])
                .output();
        }
    }
    if dir.exists() {
        std::fs::remove_dir_all(dir).with_context(|| format!("deleting {}", dir.display()))?;
    }

    Ok(())
}

fn run(command: &mut Command) -> Result<()> {
    let program = command.get_program().to_string_lossy().into_owned();
    let output = command.output().with_context(|| {
        format!("running {program}; the local backend needs it on PATH (nix develop)")
    })?;
    if !output.status.success() {
        bail!(
            "{program} failed ({}): {}",
            output.status,
            String::from_utf8_lossy(&output.stderr)
        );
    }

    Ok(())
}

fn with_database(url: &str, database: &str) -> Result<String> {
    ensure!(
        !database.is_empty() && !database.contains(['/', '?']),
        "invalid database name {database:?}"
    );
    let mut parts = url_parts(url)?;
    parts.path = format!("/{database}");

    Ok(parts.join())
}
