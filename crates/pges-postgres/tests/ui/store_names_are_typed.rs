// Store names are `StoreName`s, checked where they are written: a string is not accepted.

use pges_postgres::{admin, store};

fn open(pool: sqlx::PgPool) {
    let _config = store::StoreConfig::new("ops_es");
    let _admin = admin::Admin::new(pool, "ops_es");
}

fn main() {}
