use std::time::Duration;

use pges_sequencer::{outbox, sequencer};

fn main() {
    let mut config = sequencer::SequencerConfig::default();
    config.retry_interval = Duration::ZERO;

    let mut outbox = outbox::OutboxConfig::default();
    outbox.recheck_interval = Duration::MAX;
}
