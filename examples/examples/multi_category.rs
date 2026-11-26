//! One subscription over two categories. The subscription merges them in log order, so a handler sees
//! orders and payments in the order the sequencer gave them positions, here the order they were appended.
//! The handler uses `run`, which delivers at least once, as for a sink outside the database.
//!
//! Run it with `cargo run -p pges-examples --example multi_category`. It creates a scratch database and drops
//! it, with its replication slot, at the end: on the server named by `PGES_TEST_DATABASE_URL` or
//! `DATABASE_URL`, or else on PostgreSQL 18 from `pges-testing` (the local `pg-up` cluster, or Docker). The
//! logical source needs `wal_level = logical` and a role with `REPLICATION`.

use std::sync::{Arc, Mutex, PoisonError};

use anyhow::{Result, ensure};
use tokio::sync::oneshot;

use pges::core::{event::Event as _, position, stream, version};
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
#[event(category = "orders")]
enum OrderEvent {
    #[event(name = "order.placed")]
    Placed { total_cents: u64 },
    #[event(name = "order.shipped")]
    Shipped,
}

#[derive(Debug, Clone, PartialEq, pges::Event)]
#[event(category = "payments")]
enum PaymentEvent {
    #[event(name = "payment.captured")]
    Captured { amount_cents: u64 },
}

/// What the handler saw: the type names in delivery order. A real sink would be a broker or another
/// service.
#[derive(Debug, Default, Clone)]
struct Timeline {
    seen: Arc<Mutex<Vec<String>>>,
}

impl Timeline {
    fn snapshot(&self) -> Vec<String> {
        self.seen
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }
}

/// Delivery is at least once: after a failure between the handler and the checkpoint, a batch comes again.
/// Skipping positions it has already seen makes a repeated batch harmless within this process. `last` is
/// lost on restart, so a real sink deduplicates by position on its own side.
struct TimelineHandler {
    timeline: Timeline,
    last: position::Position,
}

impl handler::Handler for TimelineHandler {
    async fn handle(&mut self, batch: &batch::Batch) -> Result<(), handler::HandlerError> {
        let mut seen = self
            .timeline
            .seen
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        for record in batch.iter() {
            let batch::RawRecorded::Event(event) = record else {
                continue;
            };
            if event.position <= self.last {
                continue;
            }

            println!(
                "  {:>3} {:<9} {}",
                event.position.get(),
                event.category.to_string(),
                event.type_name
            );
            seen.push(event.type_name.to_string());
            self.last = event.position;
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
    let result = multi_category(&db).await;
    db.cleanup().await?;
    result
}

async fn multi_category(db: &database::Database) -> Result<()> {
    let pool = db.pool().await?;
    let mut config = store::StoreConfig::new(pges::core::store_name!("shop"));
    config.migrate = store::AutoMigrate::CreateOrMigrate(store::StoreDefinition::default());
    let store = store::Store::open(pool.clone(), config).await?;
    let admin = store.admin();
    for category in [OrderEvent::CATEGORY, PaymentEvent::CATEGORY] {
        admin
            .register_category(category, admin::CategoryConfig::default())
            .await?;
    }
    admin.maintain().await?;

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

    let timeline = Timeline::default();
    let handler = TimelineHandler {
        timeline: timeline.clone(),
        last: position::Position::BEGINNING,
    };
    println!("the merged timeline:");
    let follower = store
        .subscription(pges::subscribe::subscription_name!("order-timeline.v1"))
        .categories_of::<(OrderEvent, PaymentEvent)>()
        .start(subscription::Start::Beginning)
        .waker(waker::inproc::InprocWaker::new(watch.high_water(store_id)))
        .run(handler)
        .spawn();

    let mut appended = Vec::new();
    for total_cents in [2_500, 4_200] {
        let order_id = stream::StreamId::new_v7();
        let payment_id = stream::StreamId::new_v7();
        let any = version::ExpectedVersion::Any;

        store
            .append(order_id, any, &[OrderEvent::Placed { total_cents }])
            .await?;
        store
            .append(
                payment_id,
                any,
                &[PaymentEvent::Captured {
                    amount_cents: total_cents,
                }],
            )
            .await?;
        store.append(order_id, any, &[OrderEvent::Shipped]).await?;
        appended.extend(["order.placed", "payment.captured", "order.shipped"]);
    }

    // Positions are gap-free and count events, so the last event is at position `appended.len()`.
    wait::handled(&follower, wait::position(appended.len() as u64)?).await?;

    let seen = timeline.snapshot();
    ensure!(
        seen == appended,
        "delivered {seen:?}, appended {appended:?}"
    );

    follower.shutdown().await?;
    let _ = stop_sequencer.send(());
    sequencing.await??;

    sequencer_pool.close().await;
    pool.close().await;
    Ok(())
}
