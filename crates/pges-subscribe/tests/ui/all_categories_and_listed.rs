use pges_core::{category, non_empty};
use pges_postgres::store;
use pges_subscribe::subscription::StoreExt;

fn all_then_listed(store: store::Store) {
    let _ = store
        .subscription(pges_subscribe::subscription_name!("both"))
        .all_categories()
        .categories(non_empty![category!("jobs")]);
}

fn listed_then_all(store: store::Store) {
    let _ = store
        .subscription(pges_subscribe::subscription_name!("both"))
        .categories(non_empty![category!("jobs")])
        .all_categories();
}

fn main() {}
