//! WAL written during a measurement: the LSN difference, `pg_stat_wal`, and, where `pg_walinspect` is
//! available, the bytes taken by full-page images.
//!
//! All of these are server-wide: other databases' writes during the measurement count too.

use std::time::Duration;

use anyhow::{Context, Result};
use serde_json::{Value, json};
use sqlx::{Executor, PgPool};
use tokio::time::Instant;

use pges::postgres::sql::{typed, typed::Query as _};

use crate::sql;

/// The WAL position and counters at the start of a measurement.
#[derive(Debug, Clone)]
pub struct Mark {
    lsn: typed::PgLsn,
    records: i64,
    fpi: i64,
}

/// WAL written between a [`Mark`] and [`usage`].
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Usage {
    /// Bytes between the two insert LSNs.
    pub bytes: i64,
    /// WAL records, from `pg_stat_wal`.
    pub records: i64,
    /// Full-page images, from `pg_stat_wal`.
    pub fpi: i64,
    /// Bytes of full-page images, from `pg_walinspect`; `None` when it is not available.
    pub fpi_bytes: Option<i64>,
}

impl Usage {
    /// The share of WAL bytes taken by full-page images, when known.
    pub fn fpi_share(&self) -> Option<f64> {
        let fpi_bytes = self.fpi_bytes?;
        (self.bytes > 0).then(|| fpi_bytes as f64 / self.bytes as f64)
    }

    /// The usage as JSON, with bytes per event when `events` is not zero.
    pub fn json(&self, events: u64) -> Value {
        let per_event = (events > 0).then(|| self.bytes as f64 / events as f64);
        json!({
            "bytes": self.bytes,
            "bytes_per_event": per_event,
            "records": self.records,
            "fpi": self.fpi,
            "fpi_bytes": self.fpi_bytes,
            "fpi_share": self.fpi_share(),
        })
    }
}

/// Whether a checkpoint runs before the measurement starts.
#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
pub enum Checkpoint {
    /// Run `CHECKPOINT` first, so the first change to each page writes a full-page image: the WAL of a
    /// measurement that starts right after a checkpoint.
    Before,
    /// Measure from wherever the server's checkpoint cycle is.
    Skip,
}

/// Creates `pg_walinspect` in the database if the role may, so [`usage`] can count full-page image bytes.
pub async fn prepare(pool: &PgPool) {
    let _created = pool.execute(sql::statement::WALINSPECT.sql()).await;
}

/// Makes every connection of `pool`, up to `connections`, report its pending WAL counters to `pg_stat_wal`
/// now. A backend reports them at most once a second, or after ten idle seconds, so without this the WAL of
/// a prefill or warm-up on the same connections lands in the next measurement's `pg_stat_wal` delta.
pub async fn flush_stats(pool: &PgPool, connections: u32) -> Result<()> {
    let mut held = Vec::new();
    for _ in 0..connections {
        // Held until all are acquired, so each query runs on a different connection.
        held.push(pool.acquire().await.context("acquiring a connection")?);
    }
    for conn in &mut held {
        // pg_stat_force_next_flush makes the backend report before it answers ReadyForQuery.
        conn.execute(sql::statement::FLUSH_STATS.sql())
            .await
            .context("flushing WAL statistics")?;
    }

    Ok(())
}

/// Marks the start of a measurement, after a checkpoint if `checkpoint` says so.
pub async fn mark(pool: &PgPool, checkpoint: Checkpoint) -> Result<Mark> {
    if checkpoint == Checkpoint::Before {
        pool.execute(sql::statement::CHECKPOINT.sql())
            .await
            .context(
                "running a checkpoint (the role needs pg_checkpoint, or use --checkpoint skip)",
            )?;
    }

    let row = sql::statement::wal_mark::Query::fixed()
        .bind(sql::statement::wal_mark::Params {})
        .fetch_one(pool)
        .await
        .context("reading the WAL position")?;

    Ok(Mark {
        lsn: row.lsn.parse()?,
        records: row.wal_records.context(STAT_WAL_NULL)?,
        fpi: row.wal_fpi.context(STAT_WAL_NULL)?,
    })
}

/// The WAL written since `start`. Close the pools that wrote it first: a backend reports its WAL
/// counters to `pg_stat_wal` when it is idle for a while or when it exits.
pub async fn usage(pool: &PgPool, start: &Mark) -> Result<Usage> {
    let end = sql::statement::wal_end::Query::fixed()
        .bind(sql::statement::wal_end::Params { start: start.lsn })
        .fetch_one(pool)
        .await
        .context("reading the WAL position")?;
    let end_lsn: typed::PgLsn = end.lsn.parse()?;

    tokio::time::sleep(Duration::from_millis(200)).await;
    let counters = sql::statement::wal_counters::Query::fixed()
        .bind(sql::statement::wal_counters::Params {})
        .fetch_one(pool)
        .await
        .context("reading pg_stat_wal")?;

    Ok(Usage {
        bytes: end.bytes,
        records: counters.wal_records.context(STAT_WAL_NULL)? - start.records,
        fpi: counters.wal_fpi.context(STAT_WAL_NULL)? - start.fpi,
        fpi_bytes: fpi_bytes(pool, start.lsn, end_lsn).await,
    })
}

/// Full-page image bytes between two LSNs, read from the WAL with `pg_walinspect`. `None` when the
/// extension is missing, the WAL is not flushed in time, or it was already recycled.
async fn fpi_bytes(pool: &PgPool, start: typed::PgLsn, end: typed::PgLsn) -> Option<i64> {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let flushed = sql::statement::wal_flushed::Query::fixed()
            .bind(sql::statement::wal_flushed::Params { lsn: end })
            .fetch_one(pool)
            .await
            .ok()?
            .flushed;
        if flushed {
            break;
        }
        if Instant::now() >= deadline {
            return None;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    sql::statement::fpi_bytes::Query::fixed()
        .bind(sql::statement::fpi_bytes::Params { start, end })
        .fetch_one(pool)
        .await
        .ok()
        .map(|row| row.bytes)
}

/// `pg_stat_wal` has one row, whose counters are never NULL.
const STAT_WAL_NULL: &str = "pg_stat_wal returned a NULL counter";
