//! One store-wide subscription split into four shards by stream, run by two instances that share the shards.
//! Each instance gets two shards; each stream's events arrive in order, while streams of different shards
//! interleave freely. The handler uses `run`, which delivers at least once, as for a sink outside the
//! database.
//!
//! Run it with `cargo run -p pges-examples --example sharded`. It creates a scratch database and drops it,
//! with its replication slot, at the end: on the server named by `PGES_TEST_DATABASE_URL` or
//! `DATABASE_URL`, or else on PostgreSQL 18 from `pges-testing` (the local `pg-up` cluster, or Docker). The
//! logical source needs `wal_level = logical` and a role with `REPLICATION`.

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, Mutex, PoisonError},
    time::Duration,
};

use anyhow::{Result, bail, ensure};
use tokio::{sync::oneshot, time::Instant};

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

/// The subscription's shards, shared by the two instances.
const SHARDS: subscription::Shards = pges::subscribe::shards!(4);

#[derive(Debug, Clone, PartialEq, pges::Event)]
#[event(category = "orders")]
enum OrderEvent {
    #[event(name = "order.placed")]
    Placed,
    #[event(name = "order.shipped")]
    Shipped,
}

#[derive(Debug, Clone, PartialEq, pges::Event)]
#[event(category = "payments")]
enum PaymentEvent {
    #[event(name = "payment.captured")]
    Captured,
}

/// What both instances handled: each event once, as `(stream, version)`, and each stream's versions in the
/// order they first arrived.
#[derive(Debug, Default)]
struct Handled {
    events: BTreeSet<(stream::StreamId, u64)>,
    first_arrivals: BTreeMap<stream::StreamId, Vec<u64>>,
}

type Arrivals = Arc<Mutex<Handled>>;

/// Records every event of any category. `run` delivers at least once: a batch can come again after a
/// failure, or from both instances while a shard moves, so an event already recorded is skipped.
struct Record(Arrivals);

impl handler::Handler for Record {
    async fn handle(&mut self, batch: &batch::Batch) -> Result<(), handler::HandlerError> {
        let mut handled = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        for record in batch.iter() {
            let batch::RawRecorded::Event(event) = record else {
                continue;
            };
            let version = event.version.get();
            if !handled.events.insert((event.stream_id, version)) {
                continue;
            }
            handled
                .first_arrivals
                .entry(event.stream_id)
                .or_default()
                .push(version);
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
    let result = sharded(&db).await;
    db.cleanup().await?;
    result
}

async fn sharded(db: &database::Database) -> Result<()> {
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

    // Two instances of one subscription, as two processes would run it.
    let arrivals = Arrivals::default();
    let instances: Vec<subscription::SubscriptionHandle> = (0..2)
        .map(|_| {
            store
                .subscription(pges::subscribe::subscription_name!("shipping-feed.v1"))
                .all_categories()
                .sharded(SHARDS)
                .acquire_interval(pges::core::interval!(100 ms))
                .waker(waker::inproc::InprocWaker::new(watch.high_water(store_id)))
                .run(Record(arrivals.clone()))
                .spawn()
        })
        .collect();

    let any = version::ExpectedVersion::Any;
    let mut appended = 0_usize;
    for _ in 0..20 {
        let order_id = stream::StreamId::new_v7();
        let payment_id = stream::StreamId::new_v7();
        store.append(order_id, any, &[OrderEvent::Placed]).await?;
        store
            .append(payment_id, any, &[PaymentEvent::Captured])
            .await?;
        store.append(order_id, any, &[OrderEvent::Shipped]).await?;
        appended += 3;
    }

    // Each instance's position is the lowest cursor of the shards it holds. A shard moving to the other
    // instance is held by neither for a moment, so wait until the two hold all four between them.
    let end = wait::position(appended as u64)?;
    let deadline = Instant::now() + wait::LIMIT;
    loop {
        let mut statuses = Vec::new();
        for instance in &instances {
            statuses.push(wait::handled(instance, end).await?);
        }
        let held: BTreeSet<subscription::Shard> = statuses
            .iter()
            .flat_map(|status| status.shards.iter().copied())
            .collect();
        if held.len() == usize::from(SHARDS.get()) {
            for status in &statuses {
                println!("an instance holds shards {:?}", status.shards);
            }
            break;
        }
        if Instant::now() > deadline {
            bail!("the instances hold only shards {held:?}");
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    let (delivered, first_arrivals) = {
        let handled = arrivals.lock().unwrap_or_else(PoisonError::into_inner);
        (handled.events.len(), handled.first_arrivals.clone())
    };
    ensure!(
        delivered == appended,
        "delivered {delivered} distinct events, appended {appended}"
    );
    for (stream_id, versions) in &first_arrivals {
        let expected: Vec<u64> = (1..=versions.len() as u64).collect();
        if *versions != expected {
            bail!("stream {stream_id} arrived as {versions:?}");
        }
    }
    println!(
        "{delivered} events of {} streams, each stream in order",
        first_arrivals.len()
    );

    for instance in instances {
        instance.shutdown().await?;
    }
    let _ = stop_sequencer.send(());
    sequencing.await??;

    sequencer_pool.close().await;
    pool.close().await;
    Ok(())
}
