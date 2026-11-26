// Creation options live in `StoreDefinition`, which only creating a store reads; DCB cannot be chosen.

use std::num::NonZeroU16;

use pges_postgres::store;

fn configure(config: &mut store::StoreConfig) {
    config.stream_partitions = NonZeroU16::MIN;
    config.max_txn_age = std::time::Duration::ZERO;
}

fn dcb() -> store::Dcb {
    store::Dcb::Enabled
}

fn main() {}
