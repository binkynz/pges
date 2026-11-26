//! Benchmarks for pges: append throughput and WAL per append, sequencer lag and throughput with each source,
//! the outbox's NOTIFY commit lock, and reads.
//!
//! Run them with `cargo run -p pges-bench --release -- <command>`; `--help` lists the commands and their
//! options. Each result is printed to stdout as one JSON object per line, and as a table to stderr at the
//! end. `tests/smoke.rs` runs every command for about a second under `cargo test`.
//!
//! Every run works in scratch databases that it drops at the end: on the server named by `--database-url`
//! (its role needs `CREATEDB`, and `REPLICATION` for the logical source), or else on the PostgreSQL 18 that
//! `pges-testing` finds or starts (`PGES_TEST_DATABASE_URL`, the local `pg-up` cluster, or Docker).
//!
//! `pges-bench server start` runs a private PostgreSQL 18 in Docker with production-like durability to
//! benchmark against, until `pges-bench server stop` (the README lists its settings).
//!
//! The numbers depend on the machine and on the server's settings, which every run prints first. They are
//! a baseline to compare changes against, not targets.

use std::time::Duration;

use clap::Parser;

pub mod append;
pub mod database;
pub mod event;
pub mod measure;
pub mod outbox_notify;
pub mod partitions;
pub mod read;
pub mod report;
pub mod scale;
pub mod sequencer;
pub mod server;
#[doc(hidden)]
pub mod sql;
pub mod wake;
pub mod wal;

/// The command line.
#[derive(Debug, Parser)]
#[command(name = "pges-bench", about = "Benchmarks for pges", version)]
pub struct Cli {
    /// A database on the server to benchmark. Scratch databases are created next to it, so its role needs
    /// CREATEDB. Without it, the server comes from pges-testing: PGES_TEST_DATABASE_URL, the local pg-up
    /// cluster, or Docker.
    #[arg(long, global = true)]
    pub database_url: Option<String>,
    /// The benchmark to run.
    #[command(subcommand)]
    pub command: Command,
}

/// The benchmarks.
#[derive(Debug, clap::Subcommand)]
pub enum Command {
    /// Append throughput and latency, one hot stream against many streams, single appends against
    /// batches, with WAL bytes per event.
    Append(append::Args),
    /// End-to-end lag and throughput of the sequencer with the logical and the outbox source.
    Sequencer(sequencer::Args),
    /// Append throughput of an outbox store against a logical store as concurrency grows.
    OutboxNotify(outbox_notify::Args),
    /// Catch-up read rate of a subscription, and `load` latency by stream length.
    Read(read::Args),
    /// Reads of the log joined with commits as a category's time leaves grow: catch-up, planning, plan-cache
    /// memory, locks and lock-manager waits.
    Partitions(partitions::Args),
    /// Commit to subscriber: the delivery lag of live subscriptions woken in process or over gRPC, and the
    /// cost of many subscriptions on one store.
    Wake(wake::Args),
    /// The scale targets: fills a lasting database with 100M+ streams or billions of events, then measures
    /// appends, loads, catch-up, the sequencer and retention in it.
    Scale(scale::Args),
    /// Starts or stops a private PostgreSQL 18 container with production-like durability to benchmark.
    Server(server::Args),
}

/// Runs `cli`'s benchmark: prints the environment, then each result as a JSON line as it completes, then
/// the table. Returns the results.
pub async fn run(cli: Cli) -> anyhow::Result<report::Report> {
    let benchmark = match cli.command {
        Command::Server(args) => {
            server::run(&args).await?;
            return Ok(report::Report::new("server", &[]));
        }
        Command::Append(args) => Benchmark::Append(args),
        Command::Sequencer(args) => Benchmark::Sequencer(args),
        Command::OutboxNotify(args) => Benchmark::OutboxNotify(args),
        Command::Read(args) => Benchmark::Read(args),
        Command::Partitions(args) => Benchmark::Partitions(args),
        Command::Wake(args) => Benchmark::Wake(args),
        Command::Scale(args) => Benchmark::Scale(args),
    };
    let server = database::Server::new(cli.database_url);

    let environment = report::environment(&server).await?;
    report::emit(&environment);

    let report = match benchmark {
        Benchmark::Append(args) => append::run(&server, &args).await?,
        Benchmark::Sequencer(args) => sequencer::run(&server, &args).await?,
        Benchmark::OutboxNotify(args) => outbox_notify::run(&server, &args).await?,
        Benchmark::Read(args) => read::run(&server, &args).await?,
        Benchmark::Partitions(args) => partitions::run(&server, &args).await?,
        Benchmark::Wake(args) => wake::run(&server, &args).await?,
        Benchmark::Scale(args) => scale::run(&server, &args).await?,
    };

    report::print_environment(&environment);
    report.table.print();
    Ok(report)
}

/// Parses a duration: a number of seconds (`10`, `1.5`), or a number with the suffix `ms`, `s` or `m`.
pub fn parse_duration(text: &str) -> Result<Duration, String> {
    let (number, scale) = if let Some(number) = text.strip_suffix("ms") {
        (number, 0.001)
    } else if let Some(number) = text.strip_suffix('s') {
        (number, 1.0)
    } else if let Some(number) = text.strip_suffix('m') {
        (number, 60.0)
    } else {
        (text, 1.0)
    };

    let value: f64 = number
        .trim()
        .parse()
        .map_err(|_| format!("not a duration: {text:?} (try 10s or 500ms)"))?;
    Duration::try_from_secs_f64(value * scale).map_err(|error| format!("{text:?}: {error}"))
}

/// The commands that measure a server.
enum Benchmark {
    Append(append::Args),
    Sequencer(sequencer::Args),
    OutboxNotify(outbox_notify::Args),
    Read(read::Args),
    Partitions(partitions::Args),
    Wake(wake::Args),
    Scale(scale::Args),
}

#[cfg(doctest)]
#[doc = include_str!("../README.md")]
struct ReadmeDoctests;

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::parse_duration;

    #[test]
    fn durations_parse() {
        assert_eq!(parse_duration("10"), Ok(Duration::from_secs(10)));
        assert_eq!(parse_duration("1.5s"), Ok(Duration::from_millis(1500)));
        assert_eq!(parse_duration("250ms"), Ok(Duration::from_millis(250)));
        assert_eq!(parse_duration("2m"), Ok(Duration::from_secs(120)));
        assert!(parse_duration("soon").is_err());
        assert!(parse_duration("-1s").is_err());
    }
}
