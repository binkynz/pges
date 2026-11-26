// A typed statement's parameters and columns have Rust types: binding or reading the wrong one does not
// compile.

use pges_postgres::sql::{template, typed::Query as _};
use pges_subscribe::sql::statement;

async fn cursor(pool: &sqlx::PgPool, values: &template::Values) -> Result<(), sqlx::Error> {
    let sql = statement::cursor::Query::render(values);
    let _wrong_bind = sql.bind(statement::cursor::Params {
        name: "jobs",
        shard: 0_i64,
        fence: 1,
    });

    let row = sql
        .bind(statement::cursor::Params {
            name: "jobs",
            shard: 0,
            fence: 1,
        })
        .fetch_one(pool)
        .await?;
    let _wrong_read: u64 = row.position;

    Ok(())
}

fn main() {}
