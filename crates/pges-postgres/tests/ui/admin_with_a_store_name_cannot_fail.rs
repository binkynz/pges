// `Admin::new` with a `StoreName` returns the `Admin` itself: there is no invalid name left to report.

use pges_postgres::admin;

fn administer(pool: sqlx::PgPool) -> Result<admin::Admin, Box<dyn std::error::Error>> {
    let admin = admin::Admin::new(pool, pges_core::store_name!("ops_es"))?;
    Ok(admin)
}

fn main() {}
