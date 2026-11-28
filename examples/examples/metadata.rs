//! Metadata: set a request's correlation id and actor once with an ambient scope, record the current
//! span's trace (the `otel` feature), and give the commit an event causes the event's follow-up metadata.
//!
//! Run it with `cargo run -p pges-examples --example metadata`.

use std::future::IntoFuture as _;

use anyhow::{Context, Result, ensure};
use futures_util::TryStreamExt;
use opentelemetry::trace::TracerProvider as _;
use opentelemetry_sdk::trace::SdkTracerProvider;
use tracing::{Instrument as _, instrument::WithSubscriber as _};
use tracing_opentelemetry::OpenTelemetrySpanExt as _;
use tracing_subscriber::layer::SubscriberExt as _;

use pges::core::{event::Event as _, metadata, stream, version};
use pges::postgres::{admin, ambient, migrate, store};
use pges_examples::database;

#[derive(Debug, Clone, PartialEq, pges::Event)]
#[event(category = "orders")]
enum OrderEvent {
    #[event(name = "order.placed")]
    Placed { sku: String },
    #[event(name = "order.shipped")]
    Shipped,
}

#[tokio::main]
async fn main() -> Result<()> {
    run().await
}

/// Runs the example in a scratch database.
pub async fn run() -> Result<()> {
    let db = database::Database::create().await?;
    // Spans get OpenTelemetry contexts from this tracer. An application exports them; here they are only
    // read back.
    let provider = SdkTracerProvider::builder().build();
    let tracing = tracing_subscriber::registry()
        .with(tracing_opentelemetry::layer().with_tracer(provider.tracer("example")));
    let result = example(&db).with_subscriber(tracing).await;
    db.cleanup().await?;
    result
}

async fn example(db: &database::Database) -> Result<()> {
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
        .register_category(OrderEvent::CATEGORY, admin::CategoryConfig::default())
        .await?;

    // At the request's edge: one scope for everything the request appends.
    let mut request = metadata::Metadata::default();
    request.correlation_id = Some(metadata::CorrelationId::new_v7());
    request.actor = Some("customer:ana".into());
    let order = stream::StreamId::new_v7();
    let place_order = tracing::info_span!("place_order");
    ambient::scope(request.clone(), async {
        store
            .append(
                order,
                version::ExpectedVersion::NoStream,
                &[OrderEvent::Placed {
                    sku: "brake-pad".into(),
                }],
            )
            .await
    })
    .instrument(place_order.clone())
    .await?;

    // Later, a worker handles the event: its span continues the request's trace, and the commit it
    // causes takes the event's follow-up metadata and the worker's own actor. The follow-up carries the
    // placed order's trace, and an explicit trace wins over the span's, so the shipment records the
    // request's traceparent. Set `follow_up.trace = None` to record the worker's span instead.
    let placed = first_event(&store, order).await?;
    let ship = tracing::info_span!("ship");
    if let Some(parent) = placed.trace_parent() {
        let _ = ship.set_parent(parent);
    }
    let mut follow_up = placed.follow_up();
    follow_up.actor = Some("shipping-worker".into());
    let shipment = stream::StreamId::new_v7();
    store
        .append(
            shipment,
            version::ExpectedVersion::NoStream,
            &[OrderEvent::Shipped],
        )
        .metadata(follow_up)
        .into_future()
        .instrument(ship)
        .await?;
    let shipped = first_event(&store, shipment).await?;

    for (name, event) in [("placed", &placed), ("shipped", &shipped)] {
        let meta = &event.metadata;
        println!(
            "{name}: correlation {:?}, causation {:?}, actor {:?}, traceparent {:?}",
            meta.correlation_id.map(|id| id.to_string()),
            meta.causation_id.map(|id| id.to_string()),
            meta.actor,
            meta.trace.as_ref().map(|trace| trace.traceparent.as_str()),
        );
    }
    ensure!(placed.metadata.correlation_id == request.correlation_id);
    ensure!(placed.metadata.actor == request.actor);
    let trace_id = placed_trace_id(&place_order);
    ensure!(
        placed
            .metadata
            .trace
            .as_ref()
            .is_some_and(|trace| trace.traceparent.contains(&trace_id)),
        "the placed order did not record the request's trace"
    );
    ensure!(shipped.metadata.correlation_id == request.correlation_id);
    ensure!(shipped.metadata.causation_id == Some(placed.commit_id.into()));
    ensure!(shipped.metadata.actor.as_deref() == Some("shipping-worker"));
    ensure!(shipped.metadata.trace == placed.metadata.trace);

    pool.close().await;
    Ok(())
}

async fn first_event(
    store: &store::Store,
    id: stream::StreamId,
) -> Result<stream::StreamEvent<OrderEvent>> {
    let events: Vec<_> = store
        .read_stream::<OrderEvent>(id, version::Version::FIRST)
        .try_collect()
        .await?;
    events.into_iter().next().context("the stream is empty")
}

fn placed_trace_id(span: &tracing::Span) -> String {
    use opentelemetry::trace::TraceContextExt as _;

    span.context().span().span_context().trace_id().to_string()
}
