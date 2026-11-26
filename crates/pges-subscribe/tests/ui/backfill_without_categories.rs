use sqlx::PgConnection;

use pges_postgres::store;
use pges_subscribe::{backfill, batch, handler, subscription::StoreExt};

#[derive(Clone)]
struct Ignore;

impl handler::PgHandler for Ignore {
    async fn handle(
        &mut self,
        _tx: &mut PgConnection,
        _batch: &batch::Batch,
    ) -> Result<(), handler::HandlerError> {
        Ok(())
    }
}

async fn fill(store: store::Store) {
    let _ = store
        .backfill(
            pges_subscribe::subscription_name!("nothing"),
            backfill::Until::Head,
        )
        .run_pg(Ignore)
        .await;
}

fn main() {}
