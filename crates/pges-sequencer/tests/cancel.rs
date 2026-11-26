//! Cancellation: a sequencer whose future is dropped at any await point, up to and past its first batch,
//! leaves no pooled connection inside a transaction and no lock held.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

mod common;

use anyhow::Result;
use sqlx::PgPool;

use pges_core::stream;
use pges_sequencer::{outbox, sequencer};
use pges_testing::cancel;

use common::{Fixture, Target};

#[tokio::test]
async fn a_dropped_sequencer_leaves_no_transaction_open() -> Result<()> {
    let f = Fixture::new().await?;
    let pool = cancel::pool(f.db.url(), 3).await?;
    let id = stream::StreamId::new_v7();

    let mut appended = 0;
    for polls in 1.. {
        f.append(Target::Job(id), 1).await?;
        appended += 1;
        free_the_lease(&f.pool).await?;
        cancel::check_released(&pool).await?;

        let source = outbox::OutboxSource::new(pool.clone(), common::outbox_config());
        let run = sequencer::Sequencer::new(pool.clone(), source, common::fast())
            .run(std::future::pending());
        let ended = cancel::poll_then_drop(run, polls).await?;
        assert!(ended.is_none(), "the sequencer stopped: {ended:?}");
        cancel::check_released(&pool).await?;

        // Every run up to here was dropped before or inside its first batch; this one got past it.
        if sequenced(&f.pool).await? == appended {
            break;
        }
    }

    pool.close().await;
    f.finish().await
}

/// Ends the lease a dropped run left behind, so the next run leads at once.
async fn free_the_lease(pool: &PgPool) -> Result<()> {
    sqlx::query("UPDATE pges.sequencer SET lease_until = NULL")
        .execute(pool)
        .await?;

    Ok(())
}

async fn sequenced(pool: &PgPool) -> Result<i64> {
    Ok(sqlx::query_scalar("SELECT count(*) FROM es.log")
        .fetch_one(pool)
        .await?)
}
