use pges_postgres::store;
use pges_subscribe::{batch, handler, subscription::StoreExt};

struct Ignore;

impl handler::Handler for Ignore {
    async fn handle(&mut self, _batch: &batch::Batch) -> Result<(), handler::HandlerError> {
        Ok(())
    }
}

fn start(store: store::Store) {
    let _ = store
        .subscription(pges_subscribe::subscription_name!("nothing"))
        .run(Ignore);
}

fn main() {}
