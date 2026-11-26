// A unit of work inside the caller's transaction ends nothing: the caller commits or rolls back.

use pges_postgres::unit_of_work;

async fn finish(work: unit_of_work::WrappedUnitOfWork<'_>) {
    let _ = work.commit().await;
}

async fn abandon(work: unit_of_work::WrappedUnitOfWork<'_>) {
    let _ = work.rollback().await;
}

fn main() {}
