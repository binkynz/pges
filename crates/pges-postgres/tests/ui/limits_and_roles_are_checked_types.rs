// A zero `max_txn_age` and an empty list of writer roles cannot be written down.

use std::time::Duration;

use pges_postgres::{limit, retention, store};

fn zero_txn_age(limits: &mut limit::WriterLimits, definition: &mut store::StoreDefinition) {
    limits.max_txn_age = Duration::ZERO;
    definition.max_txn_age = Duration::ZERO;
}

fn no_writer_roles(config: &mut retention::RetentionConfig) {
    config.writer_roles = Vec::new();
}

fn main() {}
