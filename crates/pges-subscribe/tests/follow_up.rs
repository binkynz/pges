//! Follow-up metadata across subscriptions: a request's correlation id reaches every commit its events
//! cause, each names its cause, and actors are not inherited. Handlers run outside any ambient scope.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

mod common;

use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

use anyhow::{Context, Result};
use futures_util::TryStreamExt;
use sqlx::PgConnection;

use pges_core::{category, interval, metadata, non_empty, stream, version};
use pges_postgres::{ambient, store};
use pges_subscribe::{
    batch, handler,
    subscription::{self, StoreExt},
    subscription_name,
    waker::recheck,
};

use common::{Fixture, JobEvent, OrderEvent, PartEvent};

/// For each job event, opens an order in the projection's transaction, through an ambient scope built
/// from the decoded event's follow-up.
struct OpenOrders {
    store: store::Store,
    scopes_seen: Arc<Mutex<Vec<metadata::Metadata>>>,
}

impl handler::PgHandler for OpenOrders {
    async fn handle(
        &mut self,
        tx: &mut PgConnection,
        batch: &batch::Batch,
    ) -> Result<(), handler::HandlerError> {
        self.scopes_seen.lock().unwrap().push(ambient::current());
        for event in batch.decode::<JobEvent>() {
            let event = event?;
            let order = stream::StreamId::new_v7();
            ambient::scope(event.follow_up(), async {
                self.store
                    .append_in(
                        &mut *tx,
                        order,
                        version::ExpectedVersion::NoStream,
                        &[OrderEvent::Noted { n: 0 }],
                    )
                    .await
            })
            .await
            .map_err(|error| handler::HandlerError::Fatal(error.into()))?;
        }
        Ok(())
    }
}

/// For each order event, adds a part with the raw event's follow-up and its own actor.
struct AddParts {
    store: store::Store,
}

impl handler::Handler for AddParts {
    async fn handle(&mut self, batch: &batch::Batch) -> Result<(), handler::HandlerError> {
        for record in batch.iter() {
            let batch::RawRecorded::Event(raw) = record else {
                continue;
            };
            let mut meta = raw.follow_up();
            meta.actor = Some("part-worker".into());
            self.store
                .append(
                    stream::StreamId::new_v7(),
                    version::ExpectedVersion::NoStream,
                    &[PartEvent::Noted { n: 0 }],
                )
                .metadata(meta)
                .await
                .map_err(|error| handler::HandlerError::Fatal(error.into()))?;
        }
        Ok(())
    }
}

async fn parts(f: &Fixture) -> Result<i64> {
    Ok(sqlx::query_scalar(
        "SELECT count(*) FROM es.commits c JOIN es.categories k USING (category_id)
         WHERE k.name = 'parts'",
    )
    .fetch_one(&f.pool)
    .await?)
}

async fn only_event<E: pges_core::event::Event>(
    f: &Fixture,
    category: &str,
) -> Result<Vec<stream::StreamEvent<E>>> {
    let ids: Vec<uuid::Uuid> = sqlx::query_scalar(
        "SELECT s.stream_id FROM es.streams s JOIN es.categories k USING (category_id)
         WHERE k.name = $1 ORDER BY s.stream_id",
    )
    .bind(category)
    .fetch_all(&f.pool)
    .await?;
    let mut events = Vec::new();
    for id in ids {
        let mut stream: Vec<stream::StreamEvent<E>> = f
            .store
            .read_stream::<E>(stream::StreamId::from_uuid(id), version::Version::FIRST)
            .try_collect()
            .await?;
        events.append(&mut stream);
    }
    Ok(events)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_request_correlates_every_commit_it_causes() -> Result<()> {
    let f = Fixture::new(store::Outbox::Disabled).await?;
    let mut request = metadata::Metadata::default();
    request.correlation_id = Some(metadata::CorrelationId::new_v7());
    request.actor = Some("user:7".into());
    let jobs = [stream::StreamId::new_v7(), stream::StreamId::new_v7()];
    ambient::scope(request.clone(), async {
        for job in jobs {
            f.store
                .append(
                    job,
                    version::ExpectedVersion::NoStream,
                    &[JobEvent::Noted { n: 0 }],
                )
                .await?;
        }
        anyhow::Ok(())
    })
    .await?;

    let scopes_seen = Arc::default();
    let orders = common::Running::spawn(
        f.store
            .subscription(subscription_name!("open-orders"))
            .categories(non_empty![category!("jobs")])
            .waker(recheck::Recheck::every(interval!(10 ms)))
            .run_pg(OpenOrders {
                store: f.store.clone(),
                scopes_seen: Arc::clone(&scopes_seen),
            }),
    );
    let parts_sub = common::Running::spawn(
        f.store
            .subscription(subscription_name!("add-parts"))
            .categories(non_empty![category!("orders")])
            .start(subscription::Start::Beginning)
            .waker(recheck::Recheck::every(interval!(10 ms)))
            .run(AddParts {
                store: f.store.clone(),
            }),
    );
    tokio::time::timeout(Duration::from_secs(20), async {
        while parts(&f).await? < 2 {
            f.sequence_all().await?;
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        anyhow::Ok(())
    })
    .await
    .context("the parts were not added")??;
    orders.stop().await?;
    parts_sub.stop().await?;

    let job_events = only_event::<JobEvent>(&f, "jobs").await?;
    let order_events = only_event::<OrderEvent>(&f, "orders").await?;
    let part_events = only_event::<PartEvent>(&f, "parts").await?;
    assert_eq!(
        (job_events.len(), order_events.len(), part_events.len()),
        (2, 2, 2)
    );
    let job_commits: Vec<metadata::CausationId> = job_events
        .iter()
        .map(|e| metadata::CausationId::from(e.commit_id))
        .collect();
    let order_commits: Vec<metadata::CausationId> = order_events
        .iter()
        .map(|e| metadata::CausationId::from(e.commit_id))
        .collect();
    for job in &job_events {
        assert_eq!(*job.metadata, request);
    }
    for order in &order_events {
        assert_eq!(order.metadata.correlation_id, request.correlation_id);
        assert!(job_commits.contains(&order.metadata.causation_id.unwrap()));
        assert_eq!(order.metadata.actor, None, "the actor is not inherited");
    }
    for part in &part_events {
        assert_eq!(part.metadata.correlation_id, request.correlation_id);
        assert!(order_commits.contains(&part.metadata.causation_id.unwrap()));
        assert_eq!(part.metadata.actor.as_deref(), Some("part-worker"));
    }
    let scopes_seen = scopes_seen.lock().unwrap().clone();
    assert!(!scopes_seen.is_empty());
    assert!(
        scopes_seen
            .iter()
            .all(|scope| *scope == metadata::Metadata::default()),
        "handlers run outside any ambient scope"
    );
    f.finish().await
}
