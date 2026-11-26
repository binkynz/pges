//! A projection kept up to date by a subscription with `run_pg`: each batch is written to a read-model
//! table in the same transaction that moves the subscription's cursor, so every event is applied exactly
//! once. The sequencer runs in the same process with the logical source and wakes the subscription through
//! its in-process watch.
//!
//! Run it with `cargo run -p pges-examples --example subscription`. It creates a scratch database and drops
//! it, with its replication slot, at the end: on the server named by `PGES_TEST_DATABASE_URL` or
//! `DATABASE_URL`, or else on PostgreSQL 18 from `pges-testing` (the local `pg-up` cluster, or Docker). The
//! logical source needs `wal_level = logical` and a role with `REPLICATION`.

use anyhow::{Result, ensure};
use sqlx::{Executor, PgConnection};
use tokio::sync::oneshot;

use pges::core::{event::Event as _, stream, version};
use pges::postgres::{admin, store};
use pges::replication::config;
use pges::sequencer::{logical, sequencer};
use pges::subscribe::{
    batch, handler,
    subscription::{self, StoreExt},
    waker,
};
use pges_examples::{database, wait};

#[derive(Debug, Clone, PartialEq, pges::Event)]
#[event(category = "carts")]
enum CartEvent {
    #[event(name = "cart.opened")]
    Opened { customer: String },
    #[event(name = "cart.item_added")]
    ItemAdded { sku: String, quantity: u32 },
}

/// Keeps `cart_summary` up to date. It writes only through `tx`, the transaction that also stores the
/// subscription's cursor.
struct CartSummary;

impl handler::PgHandler for CartSummary {
    async fn handle(
        &mut self,
        tx: &mut PgConnection,
        batch: &batch::Batch,
    ) -> Result<(), handler::HandlerError> {
        for event in batch.decode::<CartEvent>() {
            let event = event?;
            match &event.event {
                CartEvent::Opened { customer } => {
                    sqlx::query("INSERT INTO cart_summary (cart_id, customer) VALUES ($1, $2)")
                        .bind(event.stream_id.as_uuid())
                        .bind(customer)
                        .execute(&mut *tx)
                        .await?;
                }
                CartEvent::ItemAdded { quantity, .. } => {
                    sqlx::query("UPDATE cart_summary SET items = items + $2 WHERE cart_id = $1")
                        .bind(event.stream_id.as_uuid())
                        .bind(i64::from(*quantity))
                        .execute(&mut *tx)
                        .await?;
                }
            }
        }

        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    pges_examples::init_logging();
    run().await
}

/// Runs the example in a scratch database.
pub async fn run() -> Result<()> {
    let db = database::Database::create().await?;
    let result = subscription(&db).await;
    db.cleanup().await?;
    result
}

async fn subscription(db: &database::Database) -> Result<()> {
    let pool = db.pool().await?;
    let mut config = store::StoreConfig::new(pges::core::store_name!("shop"));
    config.migrate = store::AutoMigrate::CreateOrMigrate(store::StoreDefinition::default());
    let store = store::Store::open(pool.clone(), config).await?;
    let admin = store.admin();
    admin
        .register_category(CartEvent::CATEGORY, admin::CategoryConfig::default())
        .await?;
    // Also creates the log partitions the sequencer writes to. Run it on a schedule in production.
    admin.maintain().await?;
    pool.execute(
        "CREATE TABLE cart_summary (
           cart_id  uuid PRIMARY KEY,
           items    int8 NOT NULL DEFAULT 0,
           customer text NOT NULL
         )",
    )
    .await?;

    // The slot must exist before the first append, or it would never deliver that append. In production,
    // `pges-sequencer bootstrap` does this once, after the migrations.
    let sequencer_pool = db.pool().await?;
    let replication = config::Config::from_url(db.url())?;
    let mut source = logical::LogicalSource::new(
        sequencer_pool.clone(),
        replication,
        logical::LogicalConfig::default(),
    );
    source.bootstrap().await?;

    let sequencer = sequencer::Sequencer::new(
        sequencer_pool.clone(),
        source,
        sequencer::SequencerConfig::default(),
    );
    let store_id = store.store_id();
    let watch = sequencer.watch();
    let (stop_sequencer, sequencer_stopped) = oneshot::channel::<()>();
    let sequencing = tokio::spawn(sequencer.run(async {
        let _ = sequencer_stopped.await;
    }));

    let summary = store
        .subscription(pges::subscribe::subscription_name!("cart-summary.v1"))
        .category::<CartEvent>()
        .start(subscription::Start::Beginning)
        .waker(waker::inproc::InprocWaker::new(watch.high_water(store_id)))
        .run_pg(CartSummary)
        .spawn();

    let carts = [("ana", 3), ("ben", 1), ("chen", 2)];
    let mut events = 0;
    for (customer, items) in carts {
        let cart_id = stream::StreamId::new_v7();
        let mut appended = vec![CartEvent::Opened {
            customer: customer.into(),
        }];
        for n in 0..items {
            appended.push(CartEvent::ItemAdded {
                sku: format!("part-{n}"),
                quantity: 1,
            });
        }
        store
            .append(cart_id, version::ExpectedVersion::NoStream, &appended)
            .await?;
        events += appended.len() as u64;
    }
    println!("appended {events} events to {} carts", carts.len());

    // Positions are gap-free and count events, so the last event is at position `events`.
    let status = wait::handled(&summary, wait::position(events)?).await?;
    println!("the projection is at position {}", status.position);

    let rows: Vec<(String, i64)> =
        sqlx::query_as("SELECT customer, items FROM cart_summary ORDER BY customer")
            .fetch_all(&pool)
            .await?;
    for (customer, items) in &rows {
        println!("  {customer}: {items}");
    }
    ensure!(
        rows == [("ana".into(), 3), ("ben".into(), 1), ("chen".into(), 2)],
        "unexpected read model {rows:?}"
    );

    // Each stops cleanly: the subscription checkpoints and releases its lease, the sequencer stores its
    // token and releases its lease.
    summary.shutdown().await?;
    let _ = stop_sequencer.send(());
    sequencing.await??;

    sequencer_pool.close().await;
    pool.close().await;
    Ok(())
}
