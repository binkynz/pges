use pges_postgres::store;
use pges_subscribe::{backfill, subscription::StoreExt};

fn fill(store: store::Store) {
    let _ = store
        .backfill(
            pges_subscribe::subscription_name!("everything"),
            backfill::Until::Head,
        )
        .all_categories();
}

fn main() {}
