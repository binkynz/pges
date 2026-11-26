use pges_postgres::store;
use pges_subscribe::subscription::StoreExt;

fn start(store: store::Store) {
    let _ = store.backfill(pges_subscribe::subscription_name!("no-end"));
}

fn main() {}
