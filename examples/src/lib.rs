//! Support for the runnable examples in `examples/examples/`: a scratch database for each run, waiting with
//! a time limit, and logging.
//!
//! Run an example with `cargo run -p pges-examples --example <name>`. `cargo test -p pges-examples` runs
//! them all.

pub mod database;
pub mod wait;

/// Sends `tracing` output to stderr, filtered by `RUST_LOG` (default `warn`). Does nothing if a subscriber is
/// already installed.
pub fn init_logging() {
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("warn"));
    let _ = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_writer(std::io::stderr)
        .try_init();
}
