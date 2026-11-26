//! Latency histograms and a load driver that runs an operation from many tasks for a fixed time.

use std::{future::Future, time::Duration};

use anyhow::{Context, Result};
use hdrhistogram::Histogram;
use serde_json::{Value, json};
use tokio::time::{Instant, MissedTickBehavior};

use crate::event;

/// The largest latency a [`Latency`] tells apart: ten minutes. Longer ones count as ten minutes.
const MAX_MICROS: u64 = 600_000_000;

/// A latency histogram in microseconds, three significant digits.
#[derive(Debug, Clone)]
pub struct Latency(Histogram<u64>);

impl Latency {
    /// An empty histogram.
    pub fn new() -> Result<Self> {
        Ok(Self(
            Histogram::new_with_bounds(1, MAX_MICROS, 3).context("creating a latency histogram")?,
        ))
    }

    /// Records one latency.
    pub fn record(&mut self, latency: Duration) {
        let micros = u64::try_from(latency.as_micros()).unwrap_or(u64::MAX);
        self.0.saturating_record(micros.max(1));
    }

    /// Adds every value of `other`.
    pub fn add(&mut self, other: &Latency) -> Result<()> {
        self.0.add(&other.0).context("merging latency histograms")
    }

    /// How many values were recorded.
    pub fn count(&self) -> u64 {
        self.0.len()
    }

    /// The percentiles the reports show.
    pub fn summary(&self) -> Summary {
        Summary {
            count: self.0.len(),
            p50: self.0.value_at_quantile(0.5),
            p99: self.0.value_at_quantile(0.99),
            p999: self.0.value_at_quantile(0.999),
            max: self.0.max(),
        }
    }
}

/// Percentiles of a [`Latency`], in microseconds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Summary {
    /// Values recorded.
    pub count: u64,
    /// Median.
    pub p50: u64,
    /// 99th percentile.
    pub p99: u64,
    /// 99.9th percentile.
    pub p999: u64,
    /// Largest value.
    pub max: u64,
}

impl Summary {
    /// The summary as JSON, in microseconds.
    pub fn json(&self) -> Value {
        json!({
            "count": self.count,
            "p50_us": self.p50,
            "p99_us": self.p99,
            "p999_us": self.p999,
            "max_us": self.max,
        })
    }
}

/// One call of a benchmarked operation, run by many workers at once.
pub trait Operation: Clone + Send + Sync + 'static {
    /// Runs the operation once. Returns how many events it wrote or read.
    fn call(&self, rng: &mut event::Rng) -> impl Future<Output = Result<u64>> + Send;
}

/// How often each worker starts a call.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Pacing {
    /// The next call starts when the previous one returns.
    Closed,
    /// Each worker is due to start a call every interval (an open loop). A worker that falls behind starts
    /// its overdue calls back to back, and each call's latency counts from when it was due, not from when
    /// it started, so a stall shows in the latency of the calls it delayed (no coordinated omission).
    Every(Duration),
}

/// What a [`drive`] run did.
#[derive(Debug, Clone)]
pub struct Load {
    /// Calls that succeeded.
    pub calls: u64,
    /// Events the successful calls reported.
    pub events: u64,
    /// Calls that failed.
    pub errors: u64,
    /// The first failure, if any.
    pub first_error: Option<String>,
    /// Latency of the successful calls.
    pub latency: Latency,
    /// From the first call's start to the last call's end.
    pub elapsed: Duration,
}

impl Load {
    /// Successful calls per second.
    pub fn calls_per_second(&self) -> f64 {
        per_second(self.calls, self.elapsed)
    }

    /// Events per second.
    pub fn events_per_second(&self) -> f64 {
        per_second(self.events, self.elapsed)
    }
}

/// Runs `operation` from `workers` tasks until `duration` has passed, and merges what they measured. Worker
/// `n` draws from `Rng::new(seed + n)`.
pub async fn drive<O: Operation>(
    operation: O,
    workers: usize,
    duration: Duration,
    pacing: Pacing,
    seed: u64,
) -> Result<Load> {
    let started = Instant::now();
    let deadline = started + duration;

    let mut tasks = tokio::task::JoinSet::new();
    for worker in 0..workers.max(1) {
        let operation = operation.clone();
        let rng = event::Rng::new(seed.wrapping_add(worker as u64));
        tasks.spawn(work(operation, rng, deadline, pacing));
    }

    let mut load = Load {
        calls: 0,
        events: 0,
        errors: 0,
        first_error: None,
        latency: Latency::new()?,
        elapsed: Duration::ZERO,
    };
    while let Some(worker) = tasks.join_next().await {
        let worker = worker.context("a load worker panicked")??;
        load.calls += worker.calls;
        load.events += worker.events;
        load.errors += worker.errors;
        if load.first_error.is_none() {
            load.first_error = worker.first_error;
        }
        load.latency.add(&worker.latency)?;
    }
    load.elapsed = started.elapsed();

    Ok(load)
}

/// Runs `operation` closed-loop from `workers` tasks for `duration` and discards what it measured, so the
/// pool's connections are open and their statements prepared before a measurement starts.
pub async fn warm_up<O: Operation>(operation: O, workers: usize, duration: Duration) -> Result<()> {
    if duration.is_zero() {
        return Ok(());
    }

    let load = drive(operation, workers, duration, Pacing::Closed, u64::MAX / 2).await?;
    match load.first_error {
        Some(error) => anyhow::bail!("warming up: {error}"),
        None => Ok(()),
    }
}

/// `count` per second over `elapsed`; zero when nothing elapsed.
pub fn per_second(count: u64, elapsed: Duration) -> f64 {
    let seconds = elapsed.as_secs_f64();
    if seconds <= 0.0 {
        return 0.0;
    }

    count as f64 / seconds
}

async fn work<O: Operation>(
    operation: O,
    mut rng: event::Rng,
    deadline: Instant,
    pacing: Pacing,
) -> Result<Load> {
    let mut load = Load {
        calls: 0,
        events: 0,
        errors: 0,
        first_error: None,
        latency: Latency::new()?,
        elapsed: Duration::ZERO,
    };
    let mut ticks = match pacing {
        Pacing::Closed => None,
        Pacing::Every(interval) => {
            // Workers start at different offsets within the interval, so paced calls do not arrive in
            // bursts.
            let offset = interval.mul_f64((rng.next_u64() % 1000) as f64 / 1000.0);
            let mut ticks = tokio::time::interval_at(Instant::now() + offset, interval);
            ticks.set_missed_tick_behavior(MissedTickBehavior::Burst);
            Some(ticks)
        }
    };

    loop {
        let due = match ticks.as_mut() {
            Some(ticks) => ticks.tick().await,
            None => Instant::now(),
        };
        if Instant::now() >= deadline {
            break;
        }

        match operation.call(&mut rng).await {
            Ok(events) => {
                load.latency.record(due.elapsed());
                load.calls += 1;
                load.events += events;
            }
            Err(error) => {
                load.errors += 1;
                if load.first_error.is_none() {
                    load.first_error = Some(format!("{error:#}"));
                }
            }
        }
    }

    Ok(load)
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::Latency;

    #[test]
    fn percentiles_cover_long_latencies() {
        let mut latency = Latency::new().unwrap();
        for _ in 0..999 {
            latency.record(Duration::from_millis(3));
        }
        latency.record(Duration::from_secs(5));

        let summary = latency.summary();
        assert_eq!(summary.count, 1000);
        assert!((2990..=3010).contains(&summary.p50), "{summary:?}");
        assert!((2990..=3010).contains(&summary.p99), "{summary:?}");
        assert!(summary.max >= 4_990_000, "{summary:?}");
    }
}
