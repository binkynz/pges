//! The sequencer through the facade only: the `sequencer` feature alone is enough to build a logical source,
//! the connection config included.

#![cfg(feature = "sequencer")]
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use anyhow::Result;

use pges::replication::config;
use pges::sequencer::{logical, outbox, sequencer};

#[tokio::test]
async fn the_sequencer_feature_builds_a_logical_source() -> Result<()> {
    let url = "postgres://sequencer@localhost/app";
    let pool = sqlx::PgPool::connect_lazy(url)?;
    let replication = config::Config::from_url(url)?;

    let source =
        logical::LogicalSource::new(pool.clone(), replication, logical::LogicalConfig::default());
    sequencer::Sequencer::new(pool, source, sequencer::SequencerConfig::default());
    Ok(())
}

#[test]
fn the_outbox_channel_is_exported() {
    assert_eq!(outbox::CHANNEL, "pges__outbox");
}
