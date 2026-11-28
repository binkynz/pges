//! Quickstart: define events with `#[derive(pges::Event)]`, run the migrations, open a store, register a
//! category, append to a stream, load an aggregate and read the stream back.
//!
//! Run it with `cargo run -p pges-examples --example quickstart`. Like every example, it creates a scratch
//! database and drops it at the end: on the server named by `PGES_TEST_DATABASE_URL` or `DATABASE_URL`, or
//! else on PostgreSQL 18 from `pges-testing` (the local `pg-up` cluster, or Docker).

use anyhow::{Result, ensure};
use futures_util::TryStreamExt;

use pges::core::{aggregate, event::Event as _, stream, version};
use pges::postgres::{admin, migrate, store};
use pges_examples::database;

/// One enum per category. Each variant is one stored event type; `name` is what is stored, so the variant
/// can be renamed freely.
#[derive(Debug, Clone, PartialEq, pges::Event)]
#[event(category = "carts")]
enum CartEvent {
    #[event(name = "cart.opened")]
    Opened { customer: String },
    #[event(name = "cart.item_added")]
    ItemAdded { sku: String, quantity: u32 },
    #[event(name = "cart.checked_out")]
    CheckedOut,
}

/// An aggregate folds a stream's events into state.
#[derive(Debug, Default)]
struct Cart {
    customer: String,
    items: u32,
    stage: Stage,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
enum Stage {
    #[default]
    Open,
    CheckedOut,
}

impl aggregate::Aggregate for Cart {
    type Event = CartEvent;

    fn apply(&mut self, event: &CartEvent) {
        match event {
            CartEvent::Opened { customer } => self.customer = customer.clone(),
            CartEvent::ItemAdded { quantity, .. } => self.items += quantity,
            CartEvent::CheckedOut => self.stage = Stage::CheckedOut,
        }
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
    let result = quickstart(&db).await;
    db.cleanup().await?;
    result
}

async fn quickstart(db: &database::Database) -> Result<()> {
    // The migrations create the shared `pges` schema and the store's schema. `StoreConfig::migrate` set to
    // `AutoMigrate::CreateOrMigrate` would let `Store::open` run them instead.
    let pool = db.pool().await?;
    let config = store::StoreConfig::new(pges::core::store_name!("shop"));
    let mut conn = pool.acquire().await?;
    migrate::run(
        &mut conn,
        &config.name,
        &store::StoreDefinition::default(),
        &migrate::MigrateConfig::default(),
    )
    .await?;
    drop(conn);

    let store = store::Store::open(pool.clone(), config).await?;
    store
        .admin()
        .register_category(CartEvent::CATEGORY, admin::CategoryConfig::default())
        .await?;

    let cart_id = stream::StreamId::new_v7();
    let opened = store
        .append(
            cart_id,
            version::ExpectedVersion::NoStream,
            &[
                CartEvent::Opened {
                    customer: "ana".into(),
                },
                CartEvent::ItemAdded {
                    sku: "brake-pad".into(),
                    quantity: 2,
                },
            ],
        )
        .await?;
    println!(
        "appended versions {}..={} to cart {cart_id}",
        opened.versions.start(),
        opened.versions.end()
    );

    // Command handling: load, decide, then append at the version that was loaded.
    let cart = store.load::<Cart>(cart_id).await?;
    println!(
        "loaded cart of {} with {} items, {:?}",
        cart.state().customer,
        cart.state().items,
        cart.expected()
    );
    if cart.state().stage == Stage::Open && cart.state().items > 0 {
        store
            .append(cart_id, cart.expected(), &[CartEvent::CheckedOut])
            .await?;
    }

    let events: Vec<_> = store
        .read_stream::<CartEvent>(cart_id, version::Version::FIRST)
        .try_collect()
        .await?;
    for recorded in &events {
        println!("  v{} {:?}", recorded.version, recorded.event);
    }
    ensure!(
        events.len() == 3,
        "expected 3 events, read {}",
        events.len()
    );

    let cart = store.load::<Cart>(cart_id).await?;
    ensure!(
        cart.state().stage == Stage::CheckedOut,
        "the cart is not checked out"
    );

    pool.close().await;
    Ok(())
}
