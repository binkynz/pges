// A typed statement's parameters and columns have Rust types: binding or reading the wrong one does not
// compile.

use pges_postgres::sql::{statement, typed::Query as _};

async fn lock(conn: &mut sqlx::PgConnection, stream_id: uuid::Uuid) -> Result<(), sqlx::Error> {
    let sql = statement::lock_stream::Query::fixed();
    let _wrong_bind = sql.bind(statement::lock_stream::Params { stream_id: 7_i64 });

    let row = sql
        .bind(statement::lock_stream::Params { stream_id })
        .fetch_one(&mut *conn)
        .await?;
    let _wrong_read: i64 = row.last_commit_id;

    Ok(())
}

fn main() {}
