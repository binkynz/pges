//! A unit of work: appends to several streams, of different categories, in one transaction. Then a second
//! unit of work with a stale expected version fails with a version conflict and writes nothing.
//!
//! Run it with `cargo run -p pges-examples --example unit_of_work`. It creates a scratch database and drops
//! it at the end: on the server named by `PGES_TEST_DATABASE_URL` or `DATABASE_URL`, or else on PostgreSQL
//! 18 from `pges-testing` (the local `pg-up` cluster, or Docker).

use anyhow::{Result, bail, ensure};

use pges::core::{aggregate, event::Event as _, stream, version};
use pges::postgres::{admin, append, store};
use pges_examples::database;

#[derive(Debug, Clone, PartialEq, pges::Event)]
#[event(category = "orders")]
enum OrderEvent {
    #[event(name = "order.placed")]
    Placed { sku: String, quantity: u32 },
}

#[derive(Debug, Clone, PartialEq, pges::Event)]
#[event(category = "stock")]
enum StockEvent {
    #[event(name = "stock.received")]
    Received { quantity: u32 },
    #[event(name = "stock.reserved")]
    Reserved { order_id: uuid::Uuid, quantity: u32 },
}

#[derive(Debug, Default)]
struct Stock {
    available: u32,
}

impl aggregate::Aggregate for Stock {
    type Event = StockEvent;

    fn apply(&mut self, event: &StockEvent) {
        match event {
            StockEvent::Received { quantity } => self.available += quantity,
            StockEvent::Reserved { quantity, .. } => {
                self.available = self.available.saturating_sub(*quantity);
            }
        }
    }
}

#[derive(Debug, Default)]
struct Order;

impl aggregate::Aggregate for Order {
    type Event = OrderEvent;

    fn apply(&mut self, _event: &OrderEvent) {}
}

#[tokio::main]
async fn main() -> Result<()> {
    pges_examples::init_logging();
    run().await
}

/// Runs the example in a scratch database.
pub async fn run() -> Result<()> {
    let db = database::Database::create().await?;
    let result = unit_of_work(&db).await;
    db.cleanup().await?;
    result
}

async fn unit_of_work(db: &database::Database) -> Result<()> {
    let pool = db.pool().await?;
    let mut config = store::StoreConfig::new(pges::core::store_name!("shop"));
    config.migrate = store::AutoMigrate::CreateOrMigrate(store::StoreDefinition::default());
    let store = store::Store::open(pool.clone(), config).await?;
    let admin = store.admin();
    admin
        .register_category(OrderEvent::CATEGORY, admin::CategoryConfig::default())
        .await?;
    admin
        .register_category(StockEvent::CATEGORY, admin::CategoryConfig::default())
        .await?;
    // A unit of work registers a new event type on a second connection; registering up front avoids it.
    store.register::<OrderEvent>().await?;
    store.register::<StockEvent>().await?;

    let sku = stream::StreamId::from_key(StockEvent::CATEGORY, "brake-pad");
    store
        .append(
            sku.clone(),
            version::ExpectedVersion::NoStream,
            &[StockEvent::Received { quantity: 10 }],
        )
        .await?;

    // Both commands read the stock at the same version.
    let stock = store.load::<Stock>(sku.id).await?;
    println!(
        "stock of brake-pad: {} available, {:?}",
        stock.state().available,
        stock.expected()
    );

    let first_order = place_order(&store, sku.id, stock.expected(), 3).await?;
    println!("order {first_order} and its reservation committed together");

    // The second command still holds the version it loaded before the first order: a conflict.
    let second_order = stream::StreamId::new_v7();
    let mut uow = store.begin().await?;
    uow.append(
        second_order,
        version::ExpectedVersion::NoStream,
        &[OrderEvent::Placed {
            sku: "brake-pad".into(),
            quantity: 4,
        }],
    )
    .await?;
    let reserved = uow
        .append(
            sku.id,
            stock.expected(),
            &[StockEvent::Reserved {
                order_id: second_order.as_uuid(),
                quantity: 4,
            }],
        )
        .await;
    match reserved {
        Err(append::AppendError::Conflict {
            expected, actual, ..
        }) => {
            println!("conflict: expected {expected:?}, the stock is at {actual:?}");
            uow.rollback().await?;
        }
        Err(error) => return Err(error.into()),
        Ok(_) => bail!("the stale reservation was accepted"),
    }

    // Rolled back as a whole: the second order's stream was never created.
    let order = store.load::<Order>(second_order).await?;
    ensure!(order.version().is_none(), "the rolled-back order exists");

    // Retrying the command loads the current version and succeeds.
    let stock = store.load::<Stock>(sku.id).await?;
    let retried = place_order(&store, sku.id, stock.expected(), 4).await?;
    println!("retried as order {retried}");

    let stock = store.load::<Stock>(sku.id).await?;
    println!("stock of brake-pad: {} available", stock.state().available);
    ensure!(stock.state().available == 3, "expected 3 left");

    pool.close().await;
    Ok(())
}

/// Places an order and reserves its stock in one transaction.
async fn place_order(
    store: &store::Store,
    sku: stream::StreamId,
    stock_version: version::ExpectedVersion,
    quantity: u32,
) -> Result<stream::StreamId> {
    let order_id = stream::StreamId::new_v7();

    let mut uow = store.begin().await?;
    uow.append(
        order_id,
        version::ExpectedVersion::NoStream,
        &[OrderEvent::Placed {
            sku: "brake-pad".into(),
            quantity,
        }],
    )
    .await?;
    uow.append(
        sku,
        stock_version,
        &[StockEvent::Reserved {
            order_id: order_id.as_uuid(),
            quantity,
        }],
    )
    .await?;
    uow.commit().await?;

    Ok(order_id)
}
