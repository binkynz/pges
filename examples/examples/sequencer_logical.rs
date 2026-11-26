//! The sequencer with the logical source, embedded in a process: bootstrap the replication slot, run the
//! sequencer, and watch the store's positions advance as commits arrive.
//!
//! In production the sequencer usually runs as its own binary instead, from `pges-sequencer` with the `bin`
//! feature. Run `pges-sequencer bootstrap --database-url <url>` once after the migrations and before writers
//! start, then `pges-sequencer --database-url <url>` on one or more hosts; one leads, the others stand by.
//!
//! Run this example with `cargo run -p pges-examples --example sequencer_logical`. It creates a scratch
//! database and drops it, with its replication slot, at the end: on the server named by
//! `PGES_TEST_DATABASE_URL` or `DATABASE_URL`, or else on PostgreSQL 18 from `pges-testing` (the local `pg-up`
//! cluster, or Docker). The logical source needs `wal_level = logical` and a role with `REPLICATION`.

use anyhow::{Result, ensure};
use tokio::sync::oneshot;

use pges::core::{event::Event as _, stream, version};
use pges::postgres::{admin, store};
use pges::replication::config;
use pges::sequencer::{logical, sequencer};
use pges_examples::{database, wait};

#[derive(Debug, Clone, PartialEq, pges::Event)]
#[event(category = "readings")]
enum ReadingEvent {
    #[event(name = "reading.taken")]
    Taken { celsius: i32 },
}

#[derive(Debug, Clone, PartialEq, pges::Event)]
#[event(category = "alerts")]
enum AlertEvent {
    #[event(name = "alert.raised")]
    Raised { reason: String },
}

#[tokio::main]
async fn main() -> Result<()> {
    pges_examples::init_logging();
    run().await
}

/// Runs the example in a scratch database.
pub async fn run() -> Result<()> {
    let db = database::Database::create().await?;
    let result = sequencer_logical(&db).await;
    db.cleanup().await?;
    result
}

async fn sequencer_logical(db: &database::Database) -> Result<()> {
    let pool = db.pool().await?;
    let mut config = store::StoreConfig::new(pges::core::store_name!("plant"));
    config.migrate = store::AutoMigrate::CreateOrMigrate(store::StoreDefinition::default());
    let store = store::Store::open(pool.clone(), config).await?;
    let admin = store.admin();
    for category in [ReadingEvent::CATEGORY, AlertEvent::CATEGORY] {
        admin
            .register_category(category, admin::CategoryConfig::default())
            .await?;
    }
    admin.maintain().await?;

    // The sequencer gets a pool of its own, so it never waits for a connection the application holds.
    let sequencer_pool = db.pool().await?;
    let replication = config::Config::from_url(db.url())?;
    let mut source = logical::LogicalSource::new(
        sequencer_pool.clone(),
        replication,
        logical::LogicalConfig::default(),
    );
    let bootstrap = source.bootstrap().await?;
    let slot = source
        .slot()
        .map_or_else(|| "?".to_owned(), ToString::to_string);
    println!("bootstrap: {bootstrap:?}, slot {slot}");

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

    let fence = wait::leading(&watch).await?;
    println!("leading with fence {}", fence.get());

    let sensor = stream::StreamId::new_v7();
    let any = version::ExpectedVersion::Any;
    let mut appended = 0;
    for round in 1..=3 {
        let readings: Vec<_> = (0..round)
            .map(|n| ReadingEvent::Taken { celsius: 20 + n })
            .collect();
        store.append(sensor, any, &readings).await?;
        store
            .append(
                stream::StreamId::new_v7(),
                any,
                &[AlertEvent::Raised {
                    reason: format!("round {round}"),
                }],
            )
            .await?;
        appended += readings.len() as u64 + 1;

        let reached = wait::high_water(&mut high_water, wait::position(appended)?).await?;
        let categories: Vec<String> = reached
            .categories()
            .iter()
            .map(|(category, position)| format!("category {} at {position}", category.get()))
            .collect();
        println!(
            "round {round}: store at position {}, {}",
            reached.store(),
            categories.join(", ")
        );
    }

    let _ = stop_sequencer.send(());
    sequencing.await??;
    let stopped = watch.status().borrow().clone();
    println!("after shutdown: {stopped:?}");
    ensure!(
        matches!(stopped, sequencer::Status::Stopped),
        "expected Stopped, got {stopped:?}"
    );

    sequencer_pool.close().await;
    pool.close().await;
    Ok(())
}
