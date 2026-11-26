//! The outbox source: a store created with the outbox option queues each commit in its `pending` table, and
//! a trigger sends a notification when the append commits. The sequencer listens for it, so it wakes on
//! each commit instead of polling. Here the fallback recheck is an hour away, so every commit the example
//! sees sequenced was picked up through a notification.
//!
//! Use the outbox where logical replication is not available. It serialises the commits of outbox appends
//! (each sends a notification), so the logical source is the default and the choice for high write rates.
//!
//! Run it with `cargo run -p pges-examples --example outbox`. It creates a scratch database and drops it at
//! the end: on the server named by `PGES_TEST_DATABASE_URL` or `DATABASE_URL`, or else on PostgreSQL 18 from
//! `pges-testing` (the local `pg-up` cluster, or Docker).

use std::time::Duration;

use anyhow::{Context, Result, ensure};
use sqlx::postgres::PgListener;
use tokio::{sync::oneshot, time::Instant};

use pges::core::{event::Event as _, stream, version};
use pges::postgres::{admin, store};
use pges::sequencer::{outbox, sequencer};
use pges_examples::{database, wait};

#[derive(Debug, Clone, PartialEq, pges::Event)]
#[event(category = "tickets")]
enum TicketEvent {
    #[event(name = "ticket.opened")]
    Opened { subject: String },
}

#[tokio::main]
async fn main() -> Result<()> {
    pges_examples::init_logging();
    run().await
}

/// Runs the example in a scratch database.
pub async fn run() -> Result<()> {
    let db = database::Database::create().await?;
    let result = outbox(&db).await;
    db.cleanup().await?;
    result
}

async fn outbox(db: &database::Database) -> Result<()> {
    let pool = db.pool().await?;
    let mut config = store::StoreConfig::new(pges::core::store_name!("helpdesk"));
    let mut definition = store::StoreDefinition::default();
    definition.outbox = store::Outbox::Enabled;
    config.migrate = store::AutoMigrate::CreateOrMigrate(definition);
    let store = store::Store::open(pool.clone(), config).await?;
    store
        .admin()
        .register_category(TicketEvent::CATEGORY, admin::CategoryConfig::default())
        .await?;
    store.admin().maintain().await?;

    // Listen on the same channel as the sequencer, to see the notifications it wakes on.
    let mut listener = PgListener::connect(db.url()).await?;
    listener.listen(outbox::CHANNEL).await?;

    let mut outbox_config = outbox::OutboxConfig::default();
    outbox_config.recheck_interval = pges::core::interval!(1 h);
    let sequencer_pool = db.pool().await?;
    let source = outbox::OutboxSource::new(sequencer_pool.clone(), outbox_config);
    let sequencer = sequencer::Sequencer::new(
        sequencer_pool.clone(),
        source,
        sequencer::SequencerConfig::default(),
    );
    let watch = sequencer.watch();
    let store_id = store.store_id();
    let mut high_water = watch.high_water(store_id);
    let (stop_sequencer, sequencer_stopped) = oneshot::channel::<()>();
    let sequencing = tokio::spawn(sequencer.run(async {
        let _ = sequencer_stopped.await;
    }));
    wait::leading(&watch).await?;

    for n in 1..=3 {
        // Idle between appends, so the sequencer is waiting when each commit arrives.
        tokio::time::sleep(Duration::from_millis(200)).await;

        let appended_at = Instant::now();
        store
            .append(
                stream::StreamId::new_v7(),
                version::ExpectedVersion::NoStream,
                &[TicketEvent::Opened {
                    subject: format!("ticket {n}"),
                }],
            )
            .await?;

        let notification = tokio::time::timeout(wait::LIMIT, listener.recv())
            .await
            .context("no notification")??;
        let reached = wait::high_water(&mut high_water, wait::position(n)?).await?;
        let latency = appended_at.elapsed();
        println!(
            "commit {n}: notification on {} for store {:?}; sequenced at position {} after {latency:?}",
            notification.channel(),
            notification.payload(),
            reached.store(),
        );
        ensure!(
            latency < Duration::from_secs(10),
            "sequenced after {latency:?}: not woken by the notification"
        );
    }

    let _ = stop_sequencer.send(());
    sequencing.await??;

    sequencer_pool.close().await;
    pool.close().await;
    Ok(())
}
