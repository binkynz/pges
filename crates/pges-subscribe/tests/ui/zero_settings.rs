use std::time::Duration;

use pges_postgres::store;
use pges_subscribe::{subscription::StoreExt, waker};

fn configure(store: store::Store) {
    let _ = store
        .subscription(pges_subscribe::subscription_name!("zero"))
        .batch_size(0)
        .sharded(0)
        .lease(Duration::ZERO);
    let _ = waker::Wakers::any(Vec::new());
}

fn main() {}
