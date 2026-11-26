//! Adding a field to a read model whose events were written long ago: the live projection writes it from
//! now on, and a backfill replays the older events up to the live projection's cursor. Both write through
//! the same guarded statement, so whichever runs first, each cart ends up with its newest item:
//!
//! ```sql
//! ... DO UPDATE SET last_sku = excluded.last_sku, last_sku_pos = excluded.last_sku_pos
//!     WHERE cart_last_item.last_sku_pos IS NULL OR cart_last_item.last_sku_pos < excluded.last_sku_pos
//! ```
//!
//! Run it with `cargo run -p pges-examples --example backfill`. It creates a scratch database and drops it,
//! with its replication slot, at the end: on the server named by `PGES_TEST_DATABASE_URL` or
//! `DATABASE_URL`, or else on PostgreSQL 18 from `pges-testing` (the local `pg-up` cluster, or Docker). The
//! logical source needs `wal_level = logical` and a role with `REPLICATION`.

use anyhow::{Result, ensure};
use sqlx::{Executor, PgConnection};
use tokio::sync::oneshot;

use pges::core::{event::Event as _, position, stream, version};
use pges::postgres::{admin, store};
use pges::replication::config;
use pges::sequencer::{logical, sequencer};
use pges::subscribe::{
    backfill, batch, handler,
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

/// Writes each cart's newest item into `cart_last_item`, stamping the event's position in `last_sku_pos`.
/// The live projection and the backfill both use it; the stamp keeps an older event from overwriting a newer
/// one.
#[derive(Clone)]
struct LastItem;

impl handler::PgHandler for LastItem {
    async fn handle(
        &mut self,
        tx: &mut PgConnection,
        batch: &batch::Batch,
    ) -> Result<(), handler::HandlerError> {
        for event in batch.decode::<CartEvent>() {
            let event = event?;
            let CartEvent::ItemAdded { sku, .. } = &event.event else {
                continue;
            };
            sqlx::query(
                "INSERT INTO cart_last_item (cart_id, last_sku, last_sku_pos) VALUES ($1, $2, $3)
                 ON CONFLICT (cart_id) DO UPDATE
                 SET last_sku = excluded.last_sku, last_sku_pos = excluded.last_sku_pos
                 WHERE cart_last_item.last_sku_pos IS NULL
                    OR cart_last_item.last_sku_pos < excluded.last_sku_pos",
            )
            .bind(event.stream_id.as_uuid())
            .bind(sku)
            .bind(event.position.to_i64())
            .execute(&mut *tx)
            .await?;
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
    let result = backfill(&db).await;
    db.cleanup().await?;
    result
}

async fn backfill(db: &database::Database) -> Result<()> {
    let pool = db.pool().await?;
    let mut config = store::StoreConfig::new(pges::core::store_name!("shop"));
    config.migrate = store::AutoMigrate::CreateOrMigrate(store::StoreDefinition::default());
    let store = store::Store::open(pool.clone(), config).await?;
    let admin = store.admin();
    admin
        .register_category(CartEvent::CATEGORY, admin::CategoryConfig::default())
        .await?;
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
    let watch = sequencer.watch();
    let (stop_sequencer, sequencer_stopped) = oneshot::channel::<()>();
    let sequencing = tokio::spawn(sequencer.run(async {
        let _ = sequencer_stopped.await;
    }));
    wait::leading(&watch).await?;

    // History: carts filled before the read model had a `last_sku`.
    let carts: Vec<stream::StreamId> = (0..20).map(|_| stream::StreamId::new_v7()).collect();
    for (n, cart_id) in carts.iter().enumerate() {
        let mut events = vec![CartEvent::Opened {
            customer: format!("customer-{n}"),
        }];
        events.extend((0..3).map(|item| CartEvent::ItemAdded {
            sku: format!("old-{n}-{item}"),
            quantity: 1,
        }));
        store
            .append(*cart_id, version::ExpectedVersion::NoStream, &events)
            .await?;
    }
    let history = position!(20 * 4);
    wait::high_water(&mut watch.high_water(store.store_id()), history).await?;

    // Deploy: the table gains the field, and the live projection starts writing it. Here it starts at the
    // log's end, as a projection that had already handled the history in its previous version would.
    pool.execute(
        "CREATE TABLE cart_last_item (
           cart_id      uuid PRIMARY KEY,
           last_sku_pos int8,
           last_sku     text
         )",
    )
    .await?;
    let live = store
        .subscription(pges::subscribe::subscription_name!("cart-last-item.v1"))
        .category::<CartEvent>()
        .start(subscription::Start::Now)
        .waker(waker::inproc::InprocWaker::new(
            watch.high_water(store.store_id()),
        ))
        .run_pg(LastItem)
        .spawn();
    wait::handled(&live, history).await?;

    // New items for the first five carts, handled live.
    for (n, cart_id) in carts.iter().take(5).enumerate() {
        store
            .append(
                *cart_id,
                version::ExpectedVersion::Any,
                &[CartEvent::ItemAdded {
                    sku: format!("new-{n}"),
                    quantity: 1,
                }],
            )
            .await?;
    }
    wait::handled(&live, wait::position(history.get() + 5)?).await?;

    // The backfill replays the history up to the live projection's cursor, four carts at a time. Its writes
    // never replace the newer items the live projection wrote.
    let finished = store
        .backfill(
            pges::subscribe::subscription_name!("cart-last-item.v1.last-sku"),
            backfill::Until::Subscription(pges::subscribe::subscription_name!("cart-last-item.v1")),
        )
        .category::<CartEvent>()
        .parallelism(pges::subscribe::lanes!(4))
        .run_pg(LastItem)
        .await?;
    println!(
        "backfilled {} events up to position {}",
        finished.events, finished.until
    );

    for (n, cart_id) in carts.iter().enumerate() {
        let sku: String =
            sqlx::query_scalar("SELECT last_sku FROM cart_last_item WHERE cart_id = $1")
                .bind(cart_id.as_uuid())
                .fetch_one(&pool)
                .await?;
        let expected = if n < 5 {
            format!("new-{n}")
        } else {
            format!("old-{n}-2")
        };
        ensure!(sku == expected, "cart {n}: {sku}, expected {expected}");
    }
    println!("every cart shows its newest item");

    live.shutdown().await?;
    let _ = stop_sequencer.send(());
    sequencing.await??;

    sequencer_pool.close().await;
    pool.close().await;
    Ok(())
}
