//! Results: JSON lines on stdout, tables on stderr, and the environment they were measured in.

use anyhow::Result;
use serde_json::{Map, Value, json};

use pges::postgres::sql::typed::Query as _;

use crate::{database, measure, sql};

/// What a benchmark measured: one JSON object per result, and a table of the same results.
#[derive(Debug, Clone)]
pub struct Report {
    /// The results, as printed on stdout.
    pub records: Vec<Value>,
    /// The same results for people.
    pub table: Table,
}

impl Report {
    /// An empty report with a table of these columns.
    pub fn new(title: &str, columns: &[&str]) -> Self {
        Self {
            records: Vec::new(),
            table: Table {
                title: title.to_owned(),
                columns: columns.iter().map(|column| (*column).to_owned()).collect(),
                rows: Vec::new(),
            },
        }
    }

    /// Prints `record` as a JSON line, keeps it, and adds `row` to the table.
    pub fn push(&mut self, record: Value, row: Vec<String>) {
        emit(&record);
        self.records.push(record);
        self.table.rows.push(row);
    }
}

/// A table printed to stderr with aligned columns.
#[derive(Debug, Clone)]
pub struct Table {
    /// Printed above the table.
    pub title: String,
    /// Column headers.
    pub columns: Vec<String>,
    /// Cells, one row per result.
    pub rows: Vec<Vec<String>>,
}

impl Table {
    /// Prints the table to stderr.
    pub fn print(&self) {
        let mut widths: Vec<usize> = self.columns.iter().map(String::len).collect();
        for row in &self.rows {
            for (width, cell) in widths.iter_mut().zip(row) {
                *width = (*width).max(cell.len());
            }
        }

        let line = |cells: &[String]| {
            let cells: Vec<String> = cells
                .iter()
                .zip(&widths)
                .map(|(cell, width)| format!("{cell:>width$}"))
                .collect();
            format!("| {} |", cells.join(" | "))
        };
        eprintln!();
        eprintln!("{}", self.title);
        eprintln!("{}", line(&self.columns));
        let rule: Vec<String> = widths.iter().map(|width| "-".repeat(*width)).collect();
        eprintln!("|-{}-|", rule.join("-|-"));
        for row in &self.rows {
            eprintln!("{}", line(row));
        }
    }
}

/// Prints `record` to stdout as one line of JSON.
pub fn emit(record: &Value) {
    println!("{record}");
}

/// The machine and the server settings that matter for the numbers, as a JSON object with
/// `"bench": "environment"`.
pub async fn environment(server: &database::Server) -> Result<Value> {
    let scratch = server.scratch().await?;
    let settings = read_settings(&scratch).await;
    scratch.cleanup().await?;
    let settings = settings?;

    let mut record = Map::new();
    record.insert("bench".into(), json!("environment"));
    record.insert(
        "cpus".into(),
        json!(std::thread::available_parallelism().map_or(0, usize::from)),
    );
    record.insert("cpu_model".into(), json!(cpu_model()));
    record.insert("memory_gib".into(), json!(memory_gib()));
    record.insert("server".into(), Value::Object(settings));
    Ok(Value::Object(record))
}

/// Prints the environment record to stderr, before the tables.
pub fn print_environment(environment: &Value) {
    eprintln!();
    eprintln!(
        "machine: {} CPUs ({}), {} GiB RAM",
        environment["cpus"],
        environment["cpu_model"].as_str().unwrap_or("?"),
        environment["memory_gib"],
    );
    if let Some(settings) = environment["server"].as_object() {
        let settings: Vec<String> = settings
            .iter()
            .map(|(name, value)| format!("{name}={}", value.as_str().unwrap_or("?")))
            .collect();
        eprintln!("server: {}", settings.join(", "));
    }
}

/// A latency in microseconds as milliseconds with two decimals.
pub fn ms(micros: u64) -> String {
    format!("{:.2}", micros as f64 / 1000.0)
}

/// A rate with no decimals.
pub fn rate(per_second: f64) -> String {
    format!("{per_second:.0}")
}

/// The latency columns of a table row: p50, p99 and p99.9 in milliseconds.
pub fn latency_cells(summary: &measure::Summary) -> [String; 3] {
    [ms(summary.p50), ms(summary.p99), ms(summary.p999)]
}

/// Settings read with `current_setting`.
const SETTINGS: &[&str] = &[
    "server_version",
    "shared_buffers",
    "synchronous_commit",
    "fsync",
    "full_page_writes",
    "wal_level",
    "wal_compression",
    "wal_writer_delay",
    "max_wal_size",
    "checkpoint_timeout",
    "max_connections",
    "io_method",
];

async fn read_settings(scratch: &database::Scratch) -> Result<Map<String, Value>> {
    let pool = scratch.pool(1).await?;
    let mut settings = Map::new();
    for name in SETTINGS {
        let value = sql::statement::setting::Query::fixed()
            .bind(sql::statement::setting::Params { name })
            .fetch_one(&pool)
            .await?
            .value;
        settings.insert((*name).to_owned(), json!(value));
    }
    pool.close().await;

    Ok(settings)
}

fn cpu_model() -> Option<String> {
    let cpuinfo = std::fs::read_to_string("/proc/cpuinfo").ok()?;
    let line = cpuinfo
        .lines()
        .find(|line| line.starts_with("model name"))?;

    Some(line.split_once(':')?.1.trim().to_owned())
}

fn memory_gib() -> Option<u64> {
    let meminfo = std::fs::read_to_string("/proc/meminfo").ok()?;
    let line = meminfo.lines().find(|line| line.starts_with("MemTotal:"))?;
    let kib: u64 = line.split_whitespace().nth(1)?.parse().ok()?;

    Some(kib / (1024 * 1024))
}
