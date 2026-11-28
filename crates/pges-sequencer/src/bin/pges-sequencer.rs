//! `pges-sequencer`: runs a [`Sequencer`](pges_sequencer::sequencer::Sequencer) for one database until
//! SIGINT or SIGTERM, then finishes its batch, stores its token and releases the lease. A second SIGINT or
//! SIGTERM during that shutdown exits at once, with status 130 (SIGINT) or 143 (SIGTERM), without waiting
//! for the release: the lease may be left to expire. It exits with status 1 when the sequencer stops on an error (`Status::Halted`). A panic in the run ends the process with
//! Rust's status 101 and the run as `Status::Abandoned`: the lease is not released, and a standby takes
//! over once it expires.
//!
//! `pges-sequencer resequence` recovers a halted database (`LogicalSource::resequence`): stop every
//! sequencer, run it, then start the sequencers again. It replaces the lost slot, sequences the commits the
//! sequencers never saw, clears `pges.sequencer.halted` and exits 0, or exits 1 if it refused or failed.
//!
//! `pges-sequencer bootstrap` creates the logical source's publication and replication slot if they are
//! missing, then exits (`LogicalSource::bootstrap`). Run it after the migrations and before writers start.
//! With `--failover` the slot is created with `FAILOVER` (PostgreSQL 17+), so standbys with
//! `sync_replication_slots` on keep a copy of it. On an existing slot, `bootstrap --failover` turns
//! `FAILOVER` on and `bootstrap --no-failover` turns it off, with `ALTER_REPLICATION_SLOT`; stop the
//! sequencers first, because a slot in use cannot be altered. Without either flag, an existing slot keeps
//! its setting and a new slot is created without `FAILOVER`.
//!
//! With `--grpc-listen` (feature `watch-grpc`) it also serves the gRPC watch, `pges.watch.v1.Watch`, on that
//! address while it runs; a standby answers `UNAVAILABLE`. A host name (`localhost:50051`) is resolved at
//! start, and the watch is served on each of its addresses; an address the host does not have is skipped
//! with a warning. If the gRPC server fails, the sequencer shuts down and the process exits with status 1.
//! `--grpc-max-streams` caps the streams served at once (default 10 000). With `--grpc-tls-cert` and
//! `--grpc-tls-key` (feature `watch-grpc-tls`) it serves over TLS; `--grpc-tls-client-ca` also requires client
//! certificates signed by that CA. The files are read, and checked, before anything connects.
//!
//! Every option can also be set through the environment variable named in `--help`. Durations take a unit:
//! `250ms`, `10s`, `1m`; each is at most 30 days, and every one except `--token-interval` at least 1 ms. An
//! invalid option, or an option of the other source (`--slot`, `--replication-url`, `--failover`,
//! `--no-failover` or `PGES_FAILOVER` with `--source outbox`; `--recheck-interval` with `--source logical`),
//! exits with status 2 before anything connects. Logs go to stderr; `RUST_LOG` filters them (default
//! `info`).

#[cfg(feature = "watch-grpc")]
use std::net::SocketAddr;
#[cfg(feature = "watch-grpc-tls")]
use std::path::PathBuf;
use std::{num::NonZeroUsize, process::ExitCode, time::Duration};

use clap::{CommandFactory, FromArgMatches, Parser, ValueEnum, error::ErrorKind};
use sqlx::postgres::PgPoolOptions;
use tracing_subscriber::EnvFilter;

use pges_core::interval;
#[cfg(feature = "watch-grpc")]
use pges_core::non_empty;
#[cfg(feature = "watch-grpc")]
use pges_sequencer::grpc;
#[cfg(feature = "source-logical")]
use pges_sequencer::logical;
use pges_sequencer::{outbox, sequencer, source, watch};

/// The command line: the run's options, or a subcommand.
#[derive(Debug)]
enum Cli {
    Run(Box<Args>),
    #[cfg(feature = "source-logical")]
    Command(Command),
}

// Written out, not derived: clap derives no enum of a flattened `Args` and subcommands, a derived
// `Option<Args>` stays `None` whenever `Args` flattens another group, and a derived `Args` would need its
// required options to be optional for the subcommands' sake.
impl CommandFactory for Cli {
    fn command() -> clap::Command {
        let command = clap::Command::new("pges-sequencer")
            .version(env!("CARGO_PKG_VERSION"))
            .about(
                "Orders committed appends into each store's log. Run one or more per database; one \
                 leads.",
            );
        let command = <Args as clap::Args>::augment_args(command);
        #[cfg(feature = "source-logical")]
        let command = <Command as clap::Subcommand>::augment_subcommands(command)
            .args_conflicts_with_subcommands(true)
            .subcommand_negates_reqs(true);

        command
    }

    fn command_for_update() -> clap::Command {
        Self::command()
    }
}

impl FromArgMatches for Cli {
    fn from_arg_matches(matches: &clap::ArgMatches) -> Result<Self, clap::Error> {
        Self::from_arg_matches_mut(&mut matches.clone())
    }

    fn from_arg_matches_mut(matches: &mut clap::ArgMatches) -> Result<Self, clap::Error> {
        #[cfg(feature = "source-logical")]
        if matches.subcommand_name().is_some() {
            return Ok(Self::Command(Command::from_arg_matches_mut(matches)?));
        }

        Ok(Self::Run(Box::new(Args::from_arg_matches_mut(matches)?)))
    }

    fn update_from_arg_matches(&mut self, matches: &clap::ArgMatches) -> Result<(), clap::Error> {
        *self = Self::from_arg_matches(matches)?;

        Ok(())
    }
}

impl Parser for Cli {}

#[cfg(feature = "source-logical")]
#[derive(Debug, clap::Subcommand)]
enum Command {
    /// Creates the logical source's publication and replication slot if they are missing, then exits. Run it
    /// after the migrations and before writers start, so the slot exists before the first append.
    Bootstrap(Bootstrap),
    /// Recovers a halted database: replaces the lost replication slot, sequences the commits it never
    /// delivered, and clears pges.sequencer.halted. Stop every sequencer first.
    Resequence(Resequence),
}

#[cfg(feature = "source-logical")]
#[derive(Debug, clap::Args)]
struct Bootstrap {
    #[command(flatten)]
    database: Database,

    #[command(flatten)]
    logical: Logical,
}

#[cfg(feature = "source-logical")]
#[derive(Debug, clap::Args)]
struct Resequence {
    #[command(flatten)]
    database: Database,

    #[command(flatten)]
    logical: Logical,

    /// A role that appends; repeat it, or separate roles with commas. Each must cap its transactions
    /// (transaction_timeout) at the stores' max_txn_age, which bounds the search together with their
    /// max_clock_skew (both recorded in pges.stores).
    #[arg(
        long = "writer-role",
        env = "PGES_WRITER_ROLES",
        value_delimiter = ',',
        required = true
    )]
    writer_roles: Vec<String>,

    /// How long the lease taken while resequencing lasts without renewal; it is renewed every half of it.
    #[arg(long, env = "PGES_LEASE_TTL", default_value = "10s", value_parser = interval_arg)]
    lease_ttl: interval::Interval,
}

/// Runs the sequencer.
#[derive(Debug, clap::Args)]
struct Args {
    #[command(flatten)]
    database: Database,

    #[cfg(feature = "source-logical")]
    #[command(flatten)]
    logical: Logical,

    /// Where commits come from: `logical` (a replication slot; needs wal_level=logical and a REPLICATION
    /// role) or `outbox` (the pending queues of outbox stores).
    #[arg(long, env = "PGES_SOURCE", value_enum, default_value_t = SourceKind::default())]
    source: SourceKind,

    /// The address announced in pges.sequencer.leader while leading.
    #[arg(long, env = "PGES_LEADER")]
    leader: Option<String>,

    /// Most commits per batch transaction.
    #[arg(long, env = "PGES_BATCH_SIZE", default_value = "1000")]
    batch_size: NonZeroUsize,

    /// How long a lease lasts without renewal.
    #[arg(long, env = "PGES_LEASE_TTL", default_value = "10s", value_parser = interval_arg)]
    lease_ttl: interval::Interval,

    /// How often the leader renews its lease; shorter than the lease. Default: half the lease.
    #[arg(long, env = "PGES_RENEW_INTERVAL", value_parser = interval_arg)]
    renew_interval: Option<interval::Interval>,

    /// How often a standby tries to take the lease.
    #[arg(long, env = "PGES_ACQUIRE_INTERVAL", default_value = "1s", value_parser = interval_arg)]
    acquire_interval: interval::Interval,

    /// How often the resume token and the coarse high-water marks are stored; 0s stores them with every
    /// batch.
    #[arg(long, env = "PGES_TOKEN_INTERVAL", default_value = "1s", value_parser = token_interval)]
    token_interval: sequencer::TokenInterval,

    /// Pause before retrying after an error.
    #[arg(long, env = "PGES_RETRY_INTERVAL", default_value = "1s", value_parser = interval_arg)]
    retry_interval: interval::Interval,

    /// Outbox source only: how long it waits without a notification before it checks the queues itself, a
    /// fallback for lost notifications. Default: 1s.
    #[arg(long, env = "PGES_RECHECK_INTERVAL", value_parser = interval_arg)]
    recheck_interval: Option<interval::Interval>,

    /// Send NOTIFY pges_<store> after each batch.
    #[cfg(feature = "watch-notify")]
    #[arg(long, env = "PGES_NOTIFY", value_enum, default_value_t = Switch::On)]
    notify: Switch,

    /// Serve the gRPC watch (pges.watch.v1.Watch) on this address, for example `0.0.0.0:50051`, or on every
    /// address a host name resolves to, for example `localhost:50051`.
    #[cfg(feature = "watch-grpc")]
    #[arg(long, env = "PGES_GRPC_LISTEN", value_parser = listen_address)]
    grpc_listen: Option<ListenAddress>,

    /// Most gRPC watch streams open at once; more are refused with RESOURCE_EXHAUSTED. Default: 10000.
    #[cfg(feature = "watch-grpc")]
    #[arg(long, env = "PGES_GRPC_MAX_STREAMS")]
    grpc_max_streams: Option<NonZeroUsize>,

    /// The gRPC watch's certificate chain, a PEM file. With --grpc-tls-key, the watch is served over TLS.
    #[cfg(feature = "watch-grpc-tls")]
    #[arg(long, env = "PGES_GRPC_TLS_CERT", requires = "grpc_tls_key")]
    grpc_tls_cert: Option<PathBuf>,

    /// The private key of --grpc-tls-cert, a PEM file.
    #[cfg(feature = "watch-grpc-tls")]
    #[arg(
        long,
        env = "PGES_GRPC_TLS_KEY",
        requires = "grpc_tls_cert",
        hide_env_values = true
    )]
    grpc_tls_key: Option<PathBuf>,

    /// Require every gRPC client to present a certificate signed by this CA, a PEM file (mutual TLS).
    #[cfg(feature = "watch-grpc-tls")]
    #[arg(long, env = "PGES_GRPC_TLS_CLIENT_CA", requires = "grpc_tls_cert")]
    grpc_tls_client_ca: Option<PathBuf>,

    /// Connections in the sequencer's pool; at least 2.
    #[arg(
        long,
        env = "PGES_MAX_CONNECTIONS",
        default_value = "4",
        value_parser = clap::value_parser!(u32).range(2..)
    )]
    max_connections: u32,
}

/// Where the database is.
#[derive(Debug, clap::Args)]
struct Database {
    /// The database to sequence.
    #[arg(long, env = "PGES_DATABASE_URL", hide_env_values = true)]
    database_url: String,
}

/// The logical source's replication connection and slot.
#[cfg(feature = "source-logical")]
#[derive(Debug, clap::Args)]
struct Logical {
    /// Logical source only: the replication connection. Default: the database URL.
    #[arg(long, env = "PGES_REPLICATION_URL", hide_env_values = true)]
    replication_url: Option<String>,

    /// Logical source only: the replication slot. Default: pges_<oid of the database>.
    #[arg(long, env = "PGES_SLOT", value_parser = slot_name)]
    slot: Option<pges_replication::name::SlotName>,

    /// Logical source only: turn FAILOVER on for the slot, so it is synchronised to physical standbys with
    /// sync_replication_slots on and survives their promotion (PostgreSQL 17+). Running the sequencer
    /// applies it only to a slot it creates; `bootstrap` also changes an existing slot. Without
    /// `--failover` or `--no-failover`, an existing slot keeps its setting and a new one is created
    /// without FAILOVER. Environment: PGES_FAILOVER=on (or off for `--no-failover`); either flag
    /// overrides it.
    #[arg(
        long,
        value_enum,
        num_args = 0,
        default_missing_value = "on",
        conflicts_with = "no_failover"
    )]
    failover: Option<Switch>,

    /// Logical source only: turn FAILOVER off for the slot: a slot this command creates gets none, and
    /// `bootstrap` also turns it off on an existing slot. Environment: PGES_FAILOVER=off.
    // An Option<Switch> that only `--no-failover` sets to off, so the struct holds no bool.
    #[arg(long, value_enum, num_args = 0, default_missing_value = "off")]
    no_failover: Option<Switch>,
}

#[cfg(feature = "source-logical")]
impl Logical {
    /// The source's configuration, with PGES_FAILOVER's value `env`.
    fn config(&self, env: Option<&str>) -> Result<logical::LogicalConfig, clap::Error> {
        let mut config = logical::LogicalConfig::default();
        config.slot = self.slot.clone();
        config.failover = self
            .failover(env)
            .map_err(|message| usage_error(ErrorKind::ValueValidation, message))?;

        Ok(config)
    }

    /// Whether any of these options is set, by a flag, the environment or `env` (PGES_FAILOVER).
    fn is_set(&self, env: Option<&str>) -> bool {
        self.replication_url.is_some()
            || self.slot.is_some()
            || self.failover.is_some()
            || self.no_failover.is_some()
            || env.is_some_and(|env| !env.is_empty())
    }

    /// `--failover` or `--no-failover`, whichever was given (clap refuses both), else `env`, the value of
    /// PGES_FAILOVER: `on` or `off`, and empty as if unset.
    // PGES_FAILOVER is read here, not by clap, so that either flag overrides it: clap counts a value from
    // the environment as given, and would refuse `--no-failover` with PGES_FAILOVER=on.
    fn failover(&self, env: Option<&str>) -> Result<logical::SlotFailover, String> {
        let from_env = match env {
            None | Some("") => None,
            Some(value) => Some(
                Switch::from_str(value, false)
                    .map_err(|_| format!("{FAILOVER_ENV} must be on or off, not {value:?}"))?,
            ),
        };

        Ok(match self.failover.or(self.no_failover).or(from_env) {
            None => logical::SlotFailover::Keep,
            Some(Switch::On) => {
                logical::SlotFailover::Set(pges_replication::connection::Failover::Enabled)
            }
            Some(Switch::Off) => {
                logical::SlotFailover::Set(pges_replication::connection::Failover::Disabled)
            }
        })
    }
}

/// The logical source as the options describe it, checked before anything connects.
#[cfg(feature = "source-logical")]
#[derive(Debug)]
struct LogicalPlan {
    replication_url: Option<String>,
    config: logical::LogicalConfig,
}

#[cfg(feature = "source-logical")]
impl LogicalPlan {
    fn new(options: &Logical, env: Option<&str>) -> Result<Self, clap::Error> {
        Ok(Self {
            replication_url: options.replication_url.clone(),
            config: options.config(env)?,
        })
    }

    fn source(
        self,
        database: &Database,
        pool: sqlx::PgPool,
    ) -> Result<logical::LogicalSource, Box<dyn std::error::Error>> {
        let url = self
            .replication_url
            .as_deref()
            .unwrap_or(&database.database_url);
        let replication = pges_replication::config::Config::from_url(url)?;

        Ok(logical::LogicalSource::new(pool, replication, self.config))
    }
}

/// A run as the options describe it, checked before anything connects.
#[derive(Debug)]
struct Plan {
    database: Database,
    source: SourcePlan,
    sequencer: sequencer::SequencerConfig,
    max_connections: u32,
    #[cfg(feature = "watch-grpc")]
    grpc: Option<GrpcPlan>,
}

/// The gRPC watch server's address and settings.
#[cfg(feature = "watch-grpc")]
#[derive(Debug)]
struct GrpcPlan {
    listen: ListenAddress,
    config: grpc::WatchConfig,
}

/// `--grpc-listen`: a socket address, or a host name and a port, resolved when the server starts.
#[cfg(feature = "watch-grpc")]
#[derive(Debug, Clone, PartialEq, Eq)]
struct ListenAddress(String);

#[derive(Debug)]
enum SourcePlan {
    #[cfg(feature = "source-logical")]
    Logical(LogicalPlan),
    Outbox(outbox::OutboxConfig),
}

impl Plan {
    /// Checks the options together; `failover_env` is PGES_FAILOVER. The error exits with status 2.
    fn new(args: Args, failover_env: Option<&str>) -> Result<Self, clap::Error> {
        #[cfg(not(feature = "source-logical"))]
        let _ = failover_env;

        let lease = match args.renew_interval {
            None => sequencer::Lease::new(args.lease_ttl),
            Some(renew) => {
                sequencer::Lease::with_renew(args.lease_ttl, renew).ok_or_else(|| {
                    usage_error(
                        ErrorKind::ArgumentConflict,
                        format!(
                            "--renew-interval {:?} must be shorter than --lease-ttl {:?}",
                            renew.get(),
                            args.lease_ttl.get()
                        ),
                    )
                })?
            }
        };

        let source = match args.source {
            #[cfg(feature = "source-logical")]
            SourceKind::Logical => {
                if args.recheck_interval.is_some() {
                    return Err(usage_error(
                        ErrorKind::ArgumentConflict,
                        "--recheck-interval (PGES_RECHECK_INTERVAL) applies to --source outbox only",
                    ));
                }
                SourcePlan::Logical(LogicalPlan::new(&args.logical, failover_env)?)
            }
            SourceKind::Outbox => {
                #[cfg(feature = "source-logical")]
                if args.logical.is_set(failover_env) {
                    return Err(usage_error(
                        ErrorKind::ArgumentConflict,
                        "--replication-url, --slot, --failover, --no-failover and their environment \
                         variables (PGES_REPLICATION_URL, PGES_SLOT, PGES_FAILOVER) apply to --source \
                         logical only",
                    ));
                }
                let mut config = outbox::OutboxConfig::default();
                if let Some(recheck) = args.recheck_interval {
                    config.recheck_interval = recheck;
                }
                SourcePlan::Outbox(config)
            }
        };

        #[cfg(feature = "watch-grpc")]
        let grpc = GrpcPlan::new(&args)?;

        let mut sequencer = sequencer::SequencerConfig::default();
        sequencer.batch_size = args.batch_size;
        sequencer.lease = lease;
        sequencer.acquire_interval = args.acquire_interval;
        sequencer.token_interval = args.token_interval;
        sequencer.retry_interval = args.retry_interval;
        sequencer.leader = args.leader;
        #[cfg(feature = "watch-notify")]
        {
            sequencer.notify = match args.notify {
                Switch::On => sequencer::Notify::Enabled,
                Switch::Off => sequencer::Notify::Disabled,
            };
        }

        Ok(Self {
            database: args.database,
            source,
            sequencer,
            max_connections: args.max_connections,
            #[cfg(feature = "watch-grpc")]
            grpc,
        })
    }
}

#[cfg(feature = "watch-grpc")]
impl GrpcPlan {
    /// `None` without `--grpc-listen`; the gRPC options need it.
    fn new(args: &Args) -> Result<Option<Self>, clap::Error> {
        let mut config = grpc::WatchConfig::default();
        if let Some(max_streams) = args.grpc_max_streams {
            config.max_streams = max_streams;
        }
        #[cfg(feature = "watch-grpc-tls")]
        {
            config.tls = server_tls(args)?;
        }

        let Some(listen) = args.grpc_listen.clone() else {
            if args.grpc_max_streams.is_some() {
                return Err(usage_error(
                    ErrorKind::MissingRequiredArgument,
                    "--grpc-max-streams needs --grpc-listen",
                ));
            }
            #[cfg(feature = "watch-grpc-tls")]
            if config.tls.is_some() {
                return Err(usage_error(
                    ErrorKind::MissingRequiredArgument,
                    "--grpc-tls-cert and --grpc-tls-key need --grpc-listen",
                ));
            }
            return Ok(None);
        };

        Ok(Some(Self { listen, config }))
    }
}

/// Reads and checks the TLS files, so a missing or invalid one exits with status 2 before anything connects.
#[cfg(feature = "watch-grpc-tls")]
fn server_tls(args: &Args) -> Result<Option<grpc::ServerTls>, clap::Error> {
    let (Some(cert), Some(key)) = (&args.grpc_tls_cert, &args.grpc_tls_key) else {
        return Ok(None);
    };
    let read = |path: &PathBuf| {
        std::fs::read(path).map_err(|error| {
            usage_error(
                ErrorKind::Io,
                format!("cannot read {}: {error}", path.display()),
            )
        })
    };
    let invalid = |error: grpc::TlsError| {
        usage_error(
            ErrorKind::InvalidValue,
            format!("the gRPC watch's TLS files cannot be used: {error}"),
        )
    };

    let tls = grpc::ServerTls::new(read(cert)?, read(key)?).map_err(invalid)?;
    match &args.grpc_tls_client_ca {
        Some(ca) => Ok(Some(tls.client_ca(read(ca)?).map_err(invalid)?)),
        None => Ok(Some(tls)),
    }
}

impl Database {
    fn pool(&self, max_connections: u32) -> Result<sqlx::PgPool, Box<dyn std::error::Error>> {
        Ok(PgPoolOptions::new()
            .max_connections(max_connections)
            .connect_lazy(&self.database_url)?)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum SourceKind {
    /// A logical replication slot.
    #[cfg(feature = "source-logical")]
    Logical,
    /// The pending queues of outbox stores.
    Outbox,
}

impl Default for SourceKind {
    fn default() -> Self {
        #[cfg(feature = "source-logical")]
        return Self::Logical;
        #[cfg(not(feature = "source-logical"))]
        return Self::Outbox;
    }
}

#[cfg(feature = "source-logical")]
const FAILOVER_ENV: &str = "PGES_FAILOVER";

#[cfg(any(feature = "watch-notify", feature = "source-logical"))]
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum Switch {
    On,
    Off,
}

#[tokio::main]
async fn main() -> ExitCode {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .with_writer(std::io::stderr)
        .init();
    let cli = Cli::parse();
    #[cfg(feature = "source-logical")]
    let failover_env = std::env::var(FAILOVER_ENV).ok();
    #[cfg(not(feature = "source-logical"))]
    let failover_env: Option<String> = None;

    // Without `source-logical` the run is the only command.
    #[allow(clippy::infallible_destructuring_match)]
    let args = match cli {
        Cli::Run(args) => *args,
        #[cfg(feature = "source-logical")]
        Cli::Command(Command::Bootstrap(bootstrap)) => {
            let logical = LogicalPlan::new(&bootstrap.logical, failover_env.as_deref())
                .unwrap_or_else(|error| error.exit());
            return match run_bootstrap(&bootstrap.database, logical).await {
                Ok(()) => ExitCode::SUCCESS,
                Err(error) => {
                    tracing::error!(%error, "bootstrap failed");
                    ExitCode::FAILURE
                }
            };
        }
        #[cfg(feature = "source-logical")]
        Cli::Command(Command::Resequence(resequence)) => {
            let logical = LogicalPlan::new(&resequence.logical, failover_env.as_deref())
                .unwrap_or_else(|error| error.exit());
            let config = resequence_config(&resequence).unwrap_or_else(|error| error.exit());
            return match run_resequence(&resequence.database, logical, &config).await {
                Ok(()) => ExitCode::SUCCESS,
                Err(error) => {
                    tracing::error!(%error, "resequence failed");
                    ExitCode::FAILURE
                }
            };
        }
    };
    let plan = Plan::new(args, failover_env.as_deref()).unwrap_or_else(|error| error.exit());
    match run(plan).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            tracing::error!(%error, "the sequencer stopped");
            ExitCode::FAILURE
        }
    }
}

#[cfg(feature = "source-logical")]
async fn run_bootstrap(
    database: &Database,
    logical: LogicalPlan,
) -> Result<(), Box<dyn std::error::Error>> {
    let pool = database.pool(2)?;
    let mut source = logical.source(database, pool.clone())?;

    let outcome = source.bootstrap().await;
    pool.close().await;
    let slot = source.slot().map(ToString::to_string).unwrap_or_default();
    match outcome? {
        logical::Bootstrap::Created => tracing::info!(%slot, "created the replication slot"),
        logical::Bootstrap::Exists => tracing::info!(%slot, "the replication slot exists"),
        logical::Bootstrap::FailoverChanged => {
            tracing::info!(%slot, "changed the replication slot's failover setting")
        }
        other => tracing::info!(%slot, outcome = ?other, "bootstrapped"),
    }

    Ok(())
}

#[cfg(feature = "source-logical")]
fn resequence_config(resequence: &Resequence) -> Result<logical::ResequenceConfig, clap::Error> {
    let writer_roles = pges_postgres::limit::WriterRoles::try_from(resequence.writer_roles.clone())
        .map_err(|_| {
            usage_error(
                ErrorKind::MissingRequiredArgument,
                "--writer-role is required",
            )
        })?;
    let mut config = logical::ResequenceConfig::new(writer_roles);
    config.lease = sequencer::Lease::new(resequence.lease_ttl);

    Ok(config)
}

#[cfg(feature = "source-logical")]
async fn run_resequence(
    database: &Database,
    logical: LogicalPlan,
    config: &logical::ResequenceConfig,
) -> Result<(), Box<dyn std::error::Error>> {
    let pool = database.pool(3)?;
    let mut source = logical.source(database, pool.clone())?;

    let outcome = source.resequence(config).await;
    pool.close().await;
    let resequenced = outcome?;
    for store in &resequenced.stores {
        tracing::info!(
            store = %store.store,
            commits = store.commits,
            last_position = %store.last_position,
            "sequenced the commits the lost slot never delivered"
        );
    }
    tracing::info!(
        slot = %resequenced.slot,
        consistent_point = %resequenced.consistent_point,
        "resequenced; start the sequencers"
    );

    Ok(())
}

async fn run(plan: Plan) -> Result<(), Box<dyn std::error::Error>> {
    let pool = plan.database.pool(plan.max_connections)?;
    let kind = match &plan.source {
        #[cfg(feature = "source-logical")]
        SourcePlan::Logical(_) => SourceKind::Logical,
        SourcePlan::Outbox(_) => SourceKind::Outbox,
    };
    let source = source(plan.source, &plan.database, pool.clone())?;
    let sequencer = sequencer::Sequencer::new(pool, source, plan.sequencer);
    log_status(sequencer.watch());

    #[cfg(feature = "watch-grpc")]
    let (server, server_failed) = match plan.grpc {
        Some(grpc) => {
            let (server, failed) = serve_grpc(sequencer.watch(), grpc).await?;
            (Some(server), Some(failed))
        }
        None => (None, None),
    };
    let mut signals = Signals::listen();
    #[cfg(feature = "watch-grpc")]
    let stopping = async move {
        let failed = async {
            match server_failed {
                Some(failed) => {
                    let _ = failed.await;
                }
                None => std::future::pending().await,
            }
        };
        tokio::select! {
            signal = signals.recv() => log_shutdown(signal),
            () = failed => tracing::error!("the gRPC watch server stopped; shutting down"),
        }
        exit_on_next_signal(signals);
    };
    #[cfg(not(feature = "watch-grpc"))]
    let stopping = async move {
        log_shutdown(signals.recv().await);
        exit_on_next_signal(signals);
    };

    tracing::info!(source = ?kind, "starting");
    let result = sequencer.run(stopping).await;

    #[cfg(feature = "watch-grpc")]
    let served = match server {
        Some(server) => server.stop().await,
        None => Ok(()),
    };
    result?;
    #[cfg(feature = "watch-grpc")]
    served?;
    tracing::info!("stopped");
    Ok(())
}

/// The gRPC watch server, running on a task.
#[cfg(feature = "watch-grpc")]
struct GrpcServer {
    stop: tokio::sync::oneshot::Sender<()>,
    task: tokio::task::JoinHandle<Result<(), grpc::ServeError>>,
}

#[cfg(feature = "watch-grpc")]
impl GrpcServer {
    /// Stops the server once the sequencer has stopped, so open streams have already ended.
    async fn stop(self) -> Result<(), Box<dyn std::error::Error>> {
        let _ = self.stop.send(());

        Ok(self.task.await??)
    }
}

/// Starts serving the gRPC watch on every address `--grpc-listen` resolves to. The receiver resolves when the
/// server stops on its own.
#[cfg(feature = "watch-grpc")]
async fn serve_grpc(
    watch: watch::Watch,
    plan: GrpcPlan,
) -> Result<(GrpcServer, tokio::sync::oneshot::Receiver<()>), Box<dyn std::error::Error>> {
    let listeners = bind_all(&plan.listen).await?;
    #[cfg(feature = "watch-grpc-tls")]
    let tls = plan.config.tls.is_some();
    #[cfg(not(feature = "watch-grpc-tls"))]
    let tls = false;
    for listener in listeners.iter() {
        tracing::info!(addr = %listener.local_addr()?, tls, "serving the gRPC watch");
    }
    let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
    let (ended, ended_rx) = tokio::sync::oneshot::channel::<()>();
    let task = tokio::spawn(async move {
        let served = grpc::WatchService::with_config(watch, plan.config)
            .serve_all(listeners, async {
                let _ = stopped.await;
            })
            .await;
        let _ = ended.send(());
        served
    });

    Ok((GrpcServer { stop, task }, ended_rx))
}

/// Binds every address `listen` resolves to. An address this host does not have (`::1` without IPv6, say)
/// is skipped with a warning; any other failure, or no address bound, fails.
#[cfg(feature = "watch-grpc")]
async fn bind_all(
    listen: &ListenAddress,
) -> Result<non_empty::NonEmpty<tokio::net::TcpListener>, Box<dyn std::error::Error>> {
    let mut addrs: Vec<SocketAddr> = tokio::net::lookup_host(listen.0.as_str())
        .await
        .map_err(|error| format!("--grpc-listen {}: {error}", listen.0))?
        .collect();
    addrs.sort();
    addrs.dedup();

    let mut listeners = Vec::with_capacity(addrs.len());
    for addr in addrs {
        match tokio::net::TcpListener::bind(addr).await {
            Ok(listener) => listeners.push(listener),
            Err(error) if error.kind() == std::io::ErrorKind::AddrNotAvailable => {
                tracing::warn!(%addr, %error, "cannot serve the gRPC watch on this address; skipped");
            }
            Err(error) => {
                return Err(format!("--grpc-listen {} ({addr}): {error}", listen.0).into());
            }
        }
    }

    non_empty::NonEmpty::try_from(listeners)
        .map_err(|_| format!("--grpc-listen {}: no address could be bound", listen.0).into())
}

fn source(
    plan: SourcePlan,
    database: &Database,
    pool: sqlx::PgPool,
) -> Result<Box<dyn source::DynSource>, Box<dyn std::error::Error>> {
    #[cfg(not(feature = "source-logical"))]
    let _ = database;

    Ok(match plan {
        #[cfg(feature = "source-logical")]
        SourcePlan::Logical(logical) => Box::new(logical.source(database, pool)?),
        SourcePlan::Outbox(config) => Box::new(outbox::OutboxSource::new(pool, config)),
    })
}

/// Logs every status change.
fn log_status(watch: watch::Watch) {
    let mut status = watch.status();
    tokio::spawn(async move {
        while status.changed().await.is_ok() {
            match &*status.borrow_and_update() {
                sequencer::Status::Standby => tracing::info!("standing by"),
                sequencer::Status::Leading(fence) => tracing::info!(%fence, "leading"),
                sequencer::Status::Recovering(error) => {
                    tracing::warn!(%error, "recovering");
                }
                // main logs the error run() returned.
                sequencer::Status::Halted(_) => {}
                sequencer::Status::Stopped => tracing::info!("run ended"),
                sequencer::Status::Abandoned => {
                    tracing::error!("run abandoned; the lease stays until it expires");
                }
                other => tracing::info!(status = ?other, "status"),
            }
        }
    });
}

/// The shutdown signals: SIGINT and, on Unix, SIGTERM. Listening replaces a signal's default action, which
/// ends the process, for the rest of the run, so a signal that arrives once shutdown has begun must be
/// handled too ([`exit_on_next_signal`]).
struct Signals {
    #[cfg(unix)]
    interrupt: Option<tokio::signal::unix::Signal>,
    #[cfg(unix)]
    terminate: Option<tokio::signal::unix::Signal>,
}

/// A shutdown signal that arrived.
#[derive(Debug, Clone, Copy)]
enum Received {
    Sigint,
    #[cfg(unix)]
    Sigterm,
}

impl Signals {
    /// Starts listening. A signal that cannot be listened for keeps its default action, with a warning.
    fn listen() -> Self {
        #[cfg(unix)]
        {
            use tokio::signal::unix::{SignalKind, signal};

            let listen = |kind: SignalKind, name: &str| match signal(kind) {
                Ok(listener) => Some(listener),
                Err(error) => {
                    tracing::warn!(%error, signal = name, "cannot listen for the signal");
                    None
                }
            };
            Self {
                interrupt: listen(SignalKind::interrupt(), "SIGINT"),
                terminate: listen(SignalKind::terminate(), "SIGTERM"),
            }
        }
        #[cfg(not(unix))]
        {
            Self {}
        }
    }

    /// Waits for the next signal. One that arrived since [`Signals::listen`] and was not yet returned is
    /// returned at once.
    async fn recv(&mut self) -> Received {
        #[cfg(unix)]
        {
            tokio::select! {
                () = next(&mut self.interrupt) => Received::Sigint,
                () = next(&mut self.terminate) => Received::Sigterm,
            }
        }
        #[cfg(not(unix))]
        {
            match tokio::signal::ctrl_c().await {
                Ok(()) => Received::Sigint,
                Err(_) => std::future::pending().await,
            }
        }
    }
}

impl Received {
    /// The status a shell gives a process this signal ended: 128 plus the signal's number.
    fn exit_code(self) -> i32 {
        match self {
            Self::Sigint => 130,
            #[cfg(unix)]
            Self::Sigterm => 143,
        }
    }
}

fn log_shutdown(signal: Received) {
    tracing::info!(
        ?signal,
        "shutting down; a second SIGINT or SIGTERM exits at once"
    );
}

/// Once shutdown has begun, the next SIGINT or SIGTERM exits the process at once with
/// [`Received::exit_code`]. It does not wait for the batch, the token or the lease release; a statement
/// already sent may still complete on the server, and otherwise a standby takes over once the lease expires.
/// This ends a shutdown that hangs, such as releasing the lease while the database is unreachable.
fn exit_on_next_signal(mut signals: Signals) {
    tokio::spawn(async move {
        let signal = signals.recv().await;
        tracing::error!(
            ?signal,
            "shutdown forced by a second signal; exiting without waiting to release the lease"
        );
        std::process::exit(signal.exit_code());
    });
}

/// Resolves on the listener's next signal, or never without a listener.
#[cfg(unix)]
async fn next(listener: &mut Option<tokio::signal::unix::Signal>) {
    let received = match listener {
        Some(listener) => listener.recv().await,
        None => None,
    };
    if received.is_none() {
        std::future::pending::<()>().await;
    }
}

/// `250ms`, `10s`, `1m`.
fn duration(text: &str) -> Result<Duration, String> {
    let split = text
        .find(|c: char| !c.is_ascii_digit())
        .ok_or_else(|| format!("{text:?} has no unit (ms, s or m)"))?;
    let (number, unit) = text.split_at(split);
    let number: u64 = number
        .parse()
        .map_err(|_| format!("{text:?} is not a duration"))?;

    match unit {
        "ms" => Ok(Duration::from_millis(number)),
        "s" => Ok(Duration::from_secs(number)),
        "m" => Ok(Duration::from_secs(number.saturating_mul(60))),
        _ => Err(format!("{text:?}: the unit must be ms, s or m")),
    }
}

/// A [`duration`] from 1 ms to 30 days.
fn interval_arg(text: &str) -> Result<interval::Interval, String> {
    let duration = duration(text)?;

    interval::Interval::try_from(duration).map_err(|error| format!("{text:?}: {error}"))
}

/// A [`duration`] from 1 ms to 30 days, or zero for every batch.
fn token_interval(text: &str) -> Result<sequencer::TokenInterval, String> {
    let duration = duration(text)?;
    if duration.is_zero() {
        return Ok(sequencer::TokenInterval::EveryBatch);
    }

    let every =
        interval::Interval::try_from(duration).map_err(|error| format!("{text:?}: {error}"))?;

    Ok(sequencer::TokenInterval::Every(every))
}

/// `0.0.0.0:50051`, `[::1]:50051` or `localhost:50051`: a socket address, or a host name and a port.
#[cfg(feature = "watch-grpc")]
fn listen_address(text: &str) -> Result<ListenAddress, String> {
    if text.parse::<SocketAddr>().is_ok() {
        return Ok(ListenAddress(text.to_owned()));
    }

    let Some((host, port)) = text.rsplit_once(':') else {
        return Err(format!("{text:?} has no port (host:port)"));
    };
    if port.parse::<u16>().is_err() {
        return Err(format!("{text:?}: {port:?} is not a port"));
    }
    let valid_host = !host.is_empty()
        && host
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '.');
    if !valid_host {
        return Err(format!("{text:?}: {host:?} is not a host name"));
    }

    Ok(ListenAddress(text.to_owned()))
}

#[cfg(feature = "source-logical")]
fn slot_name(text: &str) -> Result<pges_replication::name::SlotName, String> {
    pges_replication::name::SlotName::new(text).map_err(|error| error.to_string())
}

/// An error about the options, which exits with status 2 and the usage.
fn usage_error(kind: ErrorKind, message: impl std::fmt::Display) -> clap::Error {
    Cli::command().error(kind, message)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(args: &[&str]) -> Result<Cli, clap::Error> {
        Cli::try_parse_from(std::iter::once("pges-sequencer").chain(args.iter().copied()))
    }

    /// The run's plan for `args` after `--database-url`.
    fn plan(args: &[&str], failover_env: Option<&str>) -> Result<Plan, clap::Error> {
        let mut argv = vec!["--database-url", "postgres://a/b"];
        argv.extend_from_slice(args);
        #[allow(irrefutable_let_patterns)]
        let Cli::Run(run) = parse(&argv)? else {
            panic!("not the run: {argv:?}");
        };
        Plan::new(*run, failover_env)
    }

    #[test]
    fn durations_need_a_unit() {
        assert_eq!(duration("250ms"), Ok(Duration::from_millis(250)));
        assert_eq!(duration("10s"), Ok(Duration::from_secs(10)));
        assert_eq!(duration("2m"), Ok(Duration::from_secs(120)));
        assert_eq!(duration("0s"), Ok(Duration::ZERO));
        for bad in ["", "10", "s", "1h", "-1s", "1.5s"] {
            assert!(duration(bad).is_err(), "{bad:?}");
        }
    }

    #[test]
    fn intervals_are_at_least_a_millisecond() {
        assert_eq!(interval_arg("1ms"), Ok(interval::Interval::MIN));
        assert!(interval_arg("0s").is_err());
        assert!(interval_arg("43201m").is_err());

        for flag in [
            "--lease-ttl",
            "--renew-interval",
            "--acquire-interval",
            "--retry-interval",
            "--recheck-interval",
        ] {
            let error = plan(&[flag, "0ms"], None).unwrap_err();
            assert_eq!(error.kind(), ErrorKind::ValueValidation, "{flag}");
        }
        let error = plan(&["--token-interval", "43201m"], None).unwrap_err();
        assert_eq!(error.kind(), ErrorKind::ValueValidation);
        assert_eq!(
            plan(&["--token-interval", "0s"], None)
                .unwrap()
                .sequencer
                .token_interval,
            sequencer::TokenInterval::EveryBatch
        );
    }

    #[test]
    fn the_lease_is_renewed_before_it_expires() {
        let lease = plan(&[], None).unwrap().sequencer.lease;
        assert_eq!(lease, sequencer::Lease::DEFAULT);

        let lease = plan(&["--lease-ttl", "2s"], None).unwrap().sequencer.lease;
        assert_eq!(lease.renew(), Duration::from_secs(1));

        let lease = plan(&["--lease-ttl", "2s", "--renew-interval", "500ms"], None)
            .unwrap()
            .sequencer
            .lease;
        assert_eq!(lease.renew(), Duration::from_millis(500));

        for renew in ["2s", "3s"] {
            let error = plan(&["--lease-ttl", "2s", "--renew-interval", renew], None).unwrap_err();
            assert_eq!(error.kind(), ErrorKind::ArgumentConflict, "{renew}");
        }
    }

    #[test]
    fn the_pool_needs_two_connections() {
        assert_eq!(plan(&[], None).unwrap().max_connections, 4);
        assert_eq!(
            plan(&["--max-connections", "2"], None)
                .unwrap()
                .max_connections,
            2
        );
        for few in ["0", "1"] {
            let error = plan(&["--max-connections", few], None).unwrap_err();
            assert_eq!(error.kind(), ErrorKind::ValueValidation, "{few}");
        }
    }

    #[test]
    fn options_of_the_other_source_are_refused() {
        let outbox = plan(&["--source", "outbox", "--recheck-interval", "250ms"], None).unwrap();
        // Without `source-logical` the outbox is the only source.
        #[allow(irrefutable_let_patterns)]
        let SourcePlan::Outbox(config) = outbox.source else {
            panic!("not the outbox: {outbox:?}");
        };
        assert_eq!(config.recheck_interval.get(), Duration::from_millis(250));

        #[cfg(feature = "source-logical")]
        {
            let error = plan(&["--recheck-interval", "250ms"], None).unwrap_err();
            assert_eq!(error.kind(), ErrorKind::ArgumentConflict);

            for args in [
                &["--slot", "s1"][..],
                &["--replication-url", "postgres://a/b"][..],
                &["--failover"][..],
                &["--no-failover"][..],
            ] {
                let mut argv = vec!["--source", "outbox"];
                argv.extend_from_slice(args);
                let error = plan(&argv, None).unwrap_err();
                assert_eq!(error.kind(), ErrorKind::ArgumentConflict, "{args:?}");
            }
            let error = plan(&["--source", "outbox"], Some("on")).unwrap_err();
            assert_eq!(error.kind(), ErrorKind::ArgumentConflict);
            assert!(plan(&["--source", "outbox"], Some("")).is_ok());

            let logical = plan(&["--slot", "s1"], None).unwrap();
            let SourcePlan::Logical(logical) = logical.source else {
                panic!("not logical: {logical:?}");
            };
            assert_eq!(
                logical.config.slot.as_ref().map(|slot| slot.as_str()),
                Some("s1")
            );
        }
    }

    #[cfg(feature = "source-logical")]
    #[test]
    fn slot_names_are_checked_while_parsing() {
        for args in [
            &["--database-url", "postgres://a/b", "--slot", "Bad-Slot"][..],
            &[
                "bootstrap",
                "--database-url",
                "postgres://a/b",
                "--slot",
                "",
            ][..],
        ] {
            let error = parse(args).unwrap_err();
            assert_eq!(error.kind(), ErrorKind::ValueValidation, "{args:?}");
        }
    }

    #[cfg(feature = "source-logical")]
    #[test]
    fn failover_is_kept_unless_a_flag_or_the_environment_sets_it() {
        use pges_replication::connection::Failover;

        let parse_bootstrap = |args: &[&str]| -> Result<Logical, clap::Error> {
            let mut argv = vec!["bootstrap", "--database-url", "postgres://a/b"];
            argv.extend_from_slice(args);
            match parse(&argv)? {
                Cli::Command(Command::Bootstrap(bootstrap)) => Ok(bootstrap.logical),
                _ => panic!("not bootstrap: {args:?}"),
            }
        };
        let keep = Ok(logical::SlotFailover::Keep);
        let on = Ok(logical::SlotFailover::Set(Failover::Enabled));
        let off = Ok(logical::SlotFailover::Set(Failover::Disabled));

        for (args, env, expected) in [
            (&[][..], None, &keep),
            (&[][..], Some(""), &keep),
            (&["--failover"][..], None, &on),
            (&["--no-failover"][..], None, &off),
            (&[][..], Some("on"), &on),
            (&[][..], Some("off"), &off),
            (&["--no-failover"][..], Some("on"), &off),
            (&["--failover"][..], Some("off"), &on),
        ] {
            assert_eq!(
                &parse_bootstrap(args).unwrap().failover(env),
                expected,
                "{args:?} {env:?}"
            );
        }
        for env in ["true", "On", "yes"] {
            assert!(
                parse_bootstrap(&[]).unwrap().failover(Some(env)).is_err(),
                "{env}"
            );
            let error = LogicalPlan::new(&parse_bootstrap(&[]).unwrap(), Some(env)).unwrap_err();
            assert_eq!(error.kind(), ErrorKind::ValueValidation, "{env}");
        }

        for args in [
            &["--failover", "--no-failover"][..],
            &["--no-failover", "--failover"][..],
        ] {
            let error = parse_bootstrap(args).unwrap_err();
            assert_eq!(error.kind(), ErrorKind::ArgumentConflict, "{args:?}");
        }
        for args in [&["--failover=on"][..], &["--no-failover=off"][..]] {
            assert!(parse_bootstrap(args).is_err(), "{args:?}");
        }

        // The run takes the same flags, for a slot it creates.
        let SourcePlan::Logical(logical) = plan(&["--failover"], None).unwrap().source else {
            panic!("not logical");
        };
        assert_eq!(Ok(logical.config.failover), on);
    }

    #[cfg(feature = "watch-grpc")]
    #[test]
    fn grpc_options_need_grpc_listen() {
        let listen = ["--source", "outbox", "--grpc-listen", "127.0.0.1:50051"];
        let max_streams = |plan: Plan| plan.grpc.map(|grpc| grpc.config.max_streams.get());

        assert_eq!(max_streams(plan(&listen, None).unwrap()), Some(10_000));
        let limited = [&listen[..], &["--grpc-max-streams", "7"]].concat();
        assert_eq!(max_streams(plan(&limited, None).unwrap()), Some(7));
        assert_eq!(
            max_streams(plan(&["--source", "outbox"], None).unwrap()),
            None
        );

        let error = plan(&["--source", "outbox", "--grpc-max-streams", "7"], None).unwrap_err();
        assert_eq!(error.kind(), ErrorKind::MissingRequiredArgument);
        let zero = [&listen[..], &["--grpc-max-streams", "0"]].concat();
        assert!(plan(&zero, None).is_err());
    }

    #[cfg(feature = "watch-grpc")]
    #[test]
    fn grpc_listen_takes_socket_addresses_and_host_names() {
        for good in [
            "0.0.0.0:50051",
            "[::1]:50051",
            "localhost:50051",
            "sequencer-0.watch.svc:1",
        ] {
            assert_eq!(listen_address(good), Ok(ListenAddress(good.to_owned())));
        }
        for bad in [
            "nowhere",
            "localhost",
            ":50051",
            "localhost:port",
            "localhost:70000",
            "local host:1",
            "::1:50051",
        ] {
            assert!(listen_address(bad).is_err(), "{bad}");
        }
    }

    /// A host name binds every address it resolves to that this host has.
    #[cfg(feature = "watch-grpc")]
    #[tokio::test]
    async fn a_host_name_binds_each_of_its_addresses() {
        let listeners = bind_all(&ListenAddress("localhost:0".to_owned()))
            .await
            .unwrap();

        let addrs: Vec<_> = listeners
            .iter()
            .map(|listener| listener.local_addr().unwrap())
            .collect();
        assert!(
            addrs.iter().all(|addr| addr.ip().is_loopback()),
            "{addrs:?}"
        );
        assert!(addrs.iter().any(SocketAddr::is_ipv4), "{addrs:?}");
    }

    #[test]
    fn the_arguments_are_consistent() {
        Cli::command().debug_assert();
    }

    #[test]
    fn options_go_to_the_run_or_to_bootstrap() {
        let cli = parse(&["--database-url", "postgres://a/b"]).unwrap();
        #[allow(irrefutable_let_patterns)]
        let Cli::Run(run) = cli else {
            panic!("not the run: {cli:?}");
        };
        assert_eq!(run.database.database_url, "postgres://a/b");
        let error = parse(&[]).unwrap_err();
        assert_eq!(error.kind(), ErrorKind::MissingRequiredArgument);

        #[cfg(feature = "watch-grpc")]
        {
            assert_eq!(run.grpc_listen, None);
            let cli = parse(&[
                "--database-url",
                "postgres://a/b",
                "--grpc-listen",
                "0.0.0.0:50051",
            ])
            .unwrap();
            #[allow(irrefutable_let_patterns)]
            let Cli::Run(run) = cli else {
                panic!("not the run: {cli:?}");
            };
            assert_eq!(
                run.grpc_listen,
                Some(ListenAddress("0.0.0.0:50051".to_owned()))
            );
            assert!(
                parse(&[
                    "--database-url",
                    "postgres://a/b",
                    "--grpc-listen",
                    "nowhere",
                ])
                .is_err()
            );
        }

        #[cfg(feature = "source-logical")]
        {
            let cli = parse(&[
                "bootstrap",
                "--database-url",
                "postgres://a/b",
                "--slot",
                "s1",
            ])
            .unwrap();
            let Cli::Command(Command::Bootstrap(bootstrap)) = cli else {
                panic!("not bootstrap: {cli:?}");
            };
            assert_eq!(bootstrap.database.database_url, "postgres://a/b");
            assert!(parse(&["bootstrap"]).is_err());
            assert_eq!(
                bootstrap.logical.slot.as_ref().map(|slot| slot.as_str()),
                Some("s1")
            );
            assert_eq!(
                bootstrap.logical.failover(None),
                Ok(logical::SlotFailover::Keep)
            );
            assert!(parse(&["--lease-ttl", "1s", "bootstrap"]).is_err());

            let cli = parse(&[
                "resequence",
                "--database-url",
                "postgres://a/b",
                "--writer-role",
                "app,jobs",
                "--writer-role",
                "audit",
            ])
            .unwrap();
            let Cli::Command(Command::Resequence(resequence)) = cli else {
                panic!("not resequence: {cli:?}");
            };
            assert_eq!(resequence.writer_roles, ["app", "jobs", "audit"]);
            let config = resequence_config(&resequence).unwrap();
            assert_eq!(
                config.writer_roles.iter().collect::<Vec<_>>(),
                ["app", "jobs", "audit"]
            );
            assert_eq!(config.lease, sequencer::Lease::DEFAULT);
            assert!(parse(&["resequence"]).is_err());
            assert!(
                parse(&["resequence", "--database-url", "postgres://a/b"]).is_err(),
                "resequence needs the writer roles"
            );
            assert!(
                parse(&[
                    "resequence",
                    "--database-url",
                    "postgres://a/b",
                    "--writer-role",
                    "app",
                    "--lease-ttl",
                    "0s",
                ])
                .is_err()
            );
            assert!(parse(&["--database-url", "postgres://a/b", "--writer-role", "app"]).is_err());
        }
    }
}
