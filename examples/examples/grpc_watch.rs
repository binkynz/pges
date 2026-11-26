//! A subscription woken by the sequencer's gRPC watch, as when the sequencer runs in a process of its own:
//! `pges-sequencer --grpc-listen 0.0.0.0:50051` serves the watch, and subscribers give their `GrpcWaker` the
//! address of every sequencer, since only the leader serves. Here both run in one process, talking over
//! a local port.
//!
//! Run it with `cargo run -p pges-examples --example grpc_watch`. It creates a scratch database and drops
//! it, with its replication slot, at the end: on the server named by `PGES_TEST_DATABASE_URL` or
//! `DATABASE_URL`, or else on PostgreSQL 18 from `pges-testing` (the local `pg-up` cluster, or Docker). The
//! logical source needs `wal_level = logical` and a role with `REPLICATION`.

use anyhow::Result;
use tokio::sync::oneshot;

use pges::core::{event::Event as _, position, stream, version};
use pges::postgres::{admin, store};
use pges::replication::config;
use pges::sequencer::{grpc, logical, sequencer};
use pges::subscribe::{batch, handler, subscription::StoreExt, waker};
use pges_examples::{database, wait};

#[derive(Debug, Clone, PartialEq, pges::Event)]
#[event(category = "parcels")]
enum ParcelEvent {
    #[event(name = "parcel.scanned")]
    Scanned { depot: String },
}

/// Prints each scan: at least once, outside any transaction.
struct Printer;

impl handler::Handler for Printer {
    async fn handle(&mut self, batch: &batch::Batch) -> Result<(), handler::HandlerError> {
        for event in batch.decode::<ParcelEvent>() {
            let event = event?;
            let ParcelEvent::Scanned { depot } = &event.event;
            println!("  parcel {} scanned at {depot}", event.stream_id.as_uuid());
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
    let result = grpc_watch(&db).await;
    db.cleanup().await?;
    result
}

async fn grpc_watch(db: &database::Database) -> Result<()> {
    let pool = db.pool().await?;
    let mut config = store::StoreConfig::new(pges::core::store_name!("depot"));
    config.migrate = store::AutoMigrate::CreateOrMigrate(store::StoreDefinition::default());
    let store = store::Store::open(pool.clone(), config).await?;
    store
        .admin()
        .register_category(ParcelEvent::CATEGORY, admin::CategoryConfig::default())
        .await?;
    store.admin().maintain().await?;

    // The sequencer side: bootstrap the slot, run the sequencer, and serve its watch.
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
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let endpoint = format!("http://{}", listener.local_addr()?);
    let (stop_server, server_stopped) = oneshot::channel::<()>();
    let serving = tokio::spawn(
        grpc::WatchService::new(watch.clone()).serve(listener, async {
            let _ = server_stopped.await;
        }),
    );
    let (stop_sequencer, sequencer_stopped) = oneshot::channel::<()>();
    let sequencing = tokio::spawn(sequencer.run(async {
        let _ = sequencer_stopped.await;
    }));
    wait::leading(&watch).await?;
    println!("the sequencer serves its watch at {endpoint}");

    // The subscriber side: it knows the store and the sequencers' addresses, nothing else.
    let waker =
        waker::grpc::GrpcWaker::new(store.store_id(), pges::core::non_empty![endpoint.parse()?]);
    let scans = store
        .subscription(pges::subscribe::subscription_name!("scan-printer.v1"))
        .category::<ParcelEvent>()
        .waker(waker)
        .run(Printer)
        .spawn();

    for depot in ["north", "south", "east"] {
        let scanned = [ParcelEvent::Scanned {
            depot: depot.into(),
        }];
        store
            .append(
                stream::StreamId::new_v7(),
                version::ExpectedVersion::NoStream,
                &scanned,
            )
            .await?;
    }
    let status = wait::handled(&scans, position!(3)).await?;
    println!("the subscription is at position {}", status.position);

    // Stop the sequencer before its server, so open streams end with the sequencer's last values.
    scans.shutdown().await?;
    let _ = stop_sequencer.send(());
    sequencing.await??;
    let _ = stop_server.send(());
    serving.await??;

    sequencer_pool.close().await;
    pool.close().await;
    Ok(())
}
