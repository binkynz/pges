// A stream that does not exist cannot be deleted, so `delete_stream` has no `NoStream` to expect.

use pges_core::{event, stream, version};
use pges_postgres::store;

async fn delete<E: event::Event>(store: &store::Store, id: stream::StreamId) {
    let _ = store
        .delete_stream::<E>(id, version::ExpectedVersion::NoStream)
        .await;
}

fn main() {}
