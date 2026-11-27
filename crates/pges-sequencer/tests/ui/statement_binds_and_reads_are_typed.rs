// A typed statement's parameters and columns have Rust types: binding or reading the wrong one does not
// compile.

use pges_postgres::sql::typed::Query as _;
use pges_sequencer::sql::statement;

async fn renew(conn: &mut sqlx::PgConnection, fence: i64) -> Result<(), sqlx::Error> {
    // The lease's time to live is an interval, not a count of microseconds.
    let sql = statement::lease_renew::Query::fixed();
    let _wrong_bind = sql.bind(statement::lease_renew::Params {
        ttl: 30_000_000_i64,
        fence,
        shared_version: 1,
        store_version: 1,
        outbox_version: 1,
    });

    let row = statement::lease_stored_token::Query::fixed()
        .bind(statement::lease_stored_token::Params { fence })
        .fetch_optional(&mut *conn)
        .await?;
    let _wrong_read: Option<Vec<u8>> = row.and_then(|row| row.source);

    Ok(())
}

fn main() {}
