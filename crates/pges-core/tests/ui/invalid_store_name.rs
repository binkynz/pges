use pges_core::store;

const RESERVED: store::StoreName = pges_core::store_name!("pg_catalog");

fn main() {
    let _ = RESERVED;
}
