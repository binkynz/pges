# pges-subscribe

Subscriptions for pges, an event store on PostgreSQL 18+. A subscription follows one or several categories
of a store, or all of them, in log order, with a named cursor and a lease, so one instance runs at a time
and others stand by. A sharded subscription splits the streams between several instances, each stream in
order.

- `run_pg` applies each batch inside the transaction that also moves the cursor, so a projection in the
  same database sees every event exactly once.
- `run` is for anything else, such as a broker or an API: at least once.

Applications reach it as `pges::subscribe` through the [`pges`](https://docs.rs/pges) facade. Depend on it
directly only when you use `pges-postgres` without the facade.

## Features

A waker tells a caught-up subscription that new events may exist. Without one, it rechecks the log on a
timer (`waker::recheck::Recheck`, every 500 ms).

- `wake-grpc` (default): the leading sequencer's gRPC watch stream (`pges-sequencer --grpc-listen`). Give
  it every sequencer's address; it finds the leader. It adds tonic.
- `wake-grpc-tls`: `https` endpoints for the gRPC waker (rustls), with the system's roots or a given CA, and
  an optional client certificate.
- `wake-poll`: reads the newest log entry of each category on an interval, with backoff.
- `wake-inproc`: the sequencer's in-process watch channel, for a sequencer in the same process.
- `wake-notify`: the sequencer's `NOTIFY pges_<store>` (its `watch-notify` feature).
- `otel`: `RawEvent::trace_parent` (and the decoded event's), the commit's trace as an OpenTelemetry parent
  context for the handler's span; it turns on `pges-postgres`'s `otel` too.

## Follow-up metadata

A batch holds events of many commits, so a subscription does not set an ambient metadata scope around its
handler. Give the commits an event causes its `follow_up()`, on the raw or the decoded event: the cause's
correlation (or its commit id when it had none), the cause's commit id as causation, its trace, and no
actor.

## Examples

```rust,no_run
use sqlx::PgConnection;

use pges_core::{category, non_empty};
use pges_postgres::store;
use pges_subscribe::{batch, handler, subscription::{self, StoreExt}, subscription_name};

struct JobCount;

impl handler::PgHandler for JobCount {
    async fn handle(
        &mut self,
        tx: &mut PgConnection,
        batch: &batch::Batch,
    ) -> Result<(), handler::HandlerError> {
        sqlx::query("UPDATE job_count SET n = n + $1")
            .bind(batch.len() as i64)
            .execute(&mut *tx)
            .await?;
        Ok(())
    }
}

async fn follow(store: store::Store) -> Result<(), Box<dyn std::error::Error>> {
    let handle = store
        .subscription(subscription_name!("job-count.v1"))
        .categories(non_empty![category!("jobs")])
        .start(subscription::Start::Beginning)
        .run_pg(JobCount)
        .spawn();

    // ... until the application stops:
    handle.shutdown().await?;
    Ok(())
}
```

Woken by the sequencers' gRPC watch, so it reads the log as soon as new events are sequenced:

```rust,no_run
# #[cfg(feature = "wake-grpc")]
# mod example {
use pges_core::{category, non_empty};
use pges_postgres::store;
use pges_subscribe::{handler, subscription::StoreExt, subscription_name, waker::grpc};

async fn follow(
    store: store::Store,
    projection: impl handler::PgHandler + 'static,
) -> Result<(), Box<dyn std::error::Error>> {
    let waker = grpc::GrpcWaker::new(
        store.store_id(),
        non_empty![
            "http://sequencer-0:50051".parse()?,
            "http://sequencer-1:50051".parse()?,
        ],
    );
    let handle = store
        .subscription(subscription_name!("job-count.v1"))
        .categories(non_empty![category!("jobs")])
        .waker(waker)
        .run_pg(projection)
        .spawn();

    // ... until the application stops:
    handle.shutdown().await?;
    Ok(())
}
# }
```

Every category of the store, split into 8 shards by stream; run as many instances as needed, and they
share the shards:

```rust,no_run
use pges_postgres::store;
use pges_subscribe::{batch, handler, shards, subscription::StoreExt, subscription_name};

struct Forward;

impl handler::Handler for Forward {
    async fn handle(&mut self, batch: &batch::Batch) -> Result<(), handler::HandlerError> {
        for record in batch.iter() {
            if let batch::RawRecorded::Event(event) = record {
                println!("{} {} {}", event.category, event.stream_id, event.type_name);
            }
        }
        Ok(())
    }
}

async fn forward(store: store::Store) -> Result<(), Box<dyn std::error::Error>> {
    let handle = store
        .subscription(subscription_name!("forward.v1"))
        .all_categories()
        .sharded(shards!(8))
        .run(Forward)
        .spawn();

    // ... until the application stops:
    handle.shutdown().await?;
    Ok(())
}
```

## Replica reads

`.replica(pool)` reads the log from a streaming replica. Before each read the subscription waits until
the replica has replayed the WAL position its waker reported (or the primary's, for a waker that reports
none). Checkpoints, leases and `run_pg` transactions stay on the primary.
`.replica(replica::Replica::new(pool).fallback(after, limit))` reads from the primary while the replica lags
longer than `after`, within `limit` (a `replica::FallbackLimit` the subscriptions of a process share). It
works the same for `.all_categories()` and for each shard of `.sharded(n)`.

## Backfills

A new read-model field whose events were written before the projection knew about it is filled by a
backfill: a one-off replay of some categories up to a position, split by stream into parallel lanes, each
event handled exactly once, resumable after a crash. The live projection stamps the field with the event's
position, and both write only over an older stamp
(`WHERE assignee_pos IS NULL OR assignee_pos < $pos`), so they can run in any order:

```rust,no_run
use pges_core::{category, non_empty};
use pges_postgres::store;
use pges_subscribe::{backfill, handler, lanes, subscription::StoreExt, subscription_name};

async fn fill(
    store: store::Store,
    assignees: impl handler::PgHandler + Clone,
) -> Result<(), Box<dyn std::error::Error>> {
    let finished = store
        .backfill(
            subscription_name!("jobs-board.v3.assignee"),
            backfill::Until::Subscription(subscription_name!("jobs-board.v3")),
        )
        .categories(non_empty![category!("jobs")])
        .parallelism(lanes!(16))
        .run_pg(assignees)
        .await?;
    println!("backfilled up to {}", finished.until);
    Ok(())
}
```

## Pool size

Subscriptions share the store's pool with appends and reads. Size it as at least the connections the
application's appends and reads use at once, plus, for each subscription on the store, 2 for `run_pg` or 1
for `run`, plus 1 for a `NotifyWaker` that listens on the store's pool. An instance of a sharded
subscription reads its shards at the same time, so give it one more per shard it may hold. A subscription
checks only its own share at start (`SubscribeError::PoolTooSmall`), so this sum is yours to keep. A backfill
holds one connection per lane.

Docs: <https://docs.rs/pges-subscribe>, once the crate is published.
