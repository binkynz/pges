# pges-postgres

PostgreSQL storage for pges, an event store on PostgreSQL 18+.

- `store::Store` opens a store (one schema), appends events to streams with optimistic concurrency, and
  reads streams and aggregates back. An append is one statement. Outside a unit of work, a lost connection
  is retried once, and a retry never writes the commit twice.
- `unit_of_work::UnitOfWork` puts appends to several streams and your own writes in one transaction;
  `UnitOfWork::wrap` does the same inside a transaction you own.
- `migrate` holds the embedded, checksummed migrations and their runner.
- `admin::Admin` registers categories, keeps their partitions ahead of time, and applies each category's
  `retention::Retention`: keep everything, drop time leaves past a window, or drop them once every
  long-lived stream has a snapshot past them.
- `Store::delete_stream` deletes a stream and its events, and appends a `$stream-deleted` system event so
  projections can purge it. Prefer crypto-shredding for personal data; deletion is for the exceptions.

Applications reach it as `pges::postgres` through the [`pges`](https://docs.rs/pges) facade. Depend on it
directly for the write path alone, without subscriptions or the derive.

What a store needs on the write path, such as the outbox, is chosen when the store is created
(`store::StoreDefinition`) and read at `Store::open`, not by cargo features.

## Features

- `otel`: an append whose metadata has no trace, from the call or the ambient scope, records the current
  `tracing` span's W3C trace context, through `tracing-opentelemetry`.

## Metadata

`ambient::scope` sets a request's metadata once. Every append inside it, including those of a unit of
work, `append_many` and `delete_stream`, fills the fields it leaves unset from the scope; explicit fields
win, and an inner scope overrides the fields it sets. Tasks spawned inside a scope do not inherit it:
wrap them in `ambient::scope(ambient::current(), ..)`.

```rust,no_run
use pges_core::{event, metadata, stream, version};
use pges_postgres::{ambient, append, store};

async fn create<E: event::Event>(
    store: &store::Store,
    events: &[E],
) -> Result<append::AppendResult, append::AppendError> {
    let mut request = metadata::Metadata::default();
    request.correlation_id = Some(metadata::CorrelationId::new_v7());
    request.actor = Some("user:7".into());

    ambient::scope(request, async {
        store
            .append(stream::StreamId::new_v7(), version::ExpectedVersion::NoStream, events)
            .await
    })
    .await
}
```

## Example

```rust,no_run
use pges_core::store_name;
use pges_postgres::store;

async fn open(pool: sqlx::PgPool) -> Result<store::Store, store::OpenError> {
    let mut config = store::StoreConfig::new(store_name!("ops_es"));
    config.migrate = store::AutoMigrate::CreateOrMigrate(store::StoreDefinition::default());
    store::Store::open(pool, config).await
}
```

Retention runs on a schedule next to `Admin::maintain`. It needs the writer roles, whose
`transaction_timeout` bounds how late a commit can still arrive. It must be at most the store's
`max_txn_age`, recorded with the store like its `max_clock_skew` (`StoreDefinition`, raised with
`Admin::set_writer_limits`, never lowered; defaults 1 hour and 5 s):

```rust,no_run
use std::time::Duration;

use pges_core::category;
use pges_postgres::{admin, limit, retention};

async fn retain(
    admin: &admin::Admin,
    jobs: category::CategoryName,
) -> Result<(), Box<dyn std::error::Error>> {
    let keep = Duration::from_secs(90 * 86_400);
    admin
        .set_retention(jobs, retention::Retention::SnapshotAndTruncate { keep })
        .await?;

    admin.maintain().await?;
    let config = retention::RetentionConfig::new(limit::WriterRoles::one("app_writer"));
    let report = admin.apply_retention(&config).await?;
    for refusal in &report.refused {
        eprintln!("kept {:?} of {}: {:?}", refusal.leaf, refusal.category, refusal.reason);
    }
    Ok(())
}
```

Autovacuum usually freezes time leaves and log partitions while they are written. Once they are closed,
`Admin::freeze_closed_partitions` freezes those it missed, so anti-wraparound vacuum later finds them all
frozen instead of reading and rewriting them; it can read a whole partition, so run it off-peak.
`Admin::partition_ages` reports each partition's transaction id age, to alert on.

Docs: <https://docs.rs/pges-postgres>, once the crate is published.
