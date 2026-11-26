# pges

An event store on PostgreSQL 18+. This is the facade: it gathers the pges crates as modules
(`pges::core`, `pges::derive`, `pges::postgres`, `pges::subscribe`, `pges::sequencer`,
`pges::replication`), so an application depends on `pges` alone. Each module is the crate itself, so
`pges::postgres::store::Store` is the same type as `pges_postgres::store::Store`.

Use the facade unless you want one crate without the others, such as `pges-sequencer` in a service of its
own.

## Features

- `derive` (default): `#[derive(pges::Event)]`.
- `subscribe` (default): `pges::subscribe`, without a waker. `wake-grpc` (default), `wake-poll`,
  `wake-inproc` and `wake-notify` add the wakers of the same names; `wake-grpc-tls` adds `https`
  endpoints to the gRPC waker.
- `sequencer`: `pges::sequencer` with the logical and outbox sources. `sequencer-outbox` is the
  sequencer with the outbox source only, without the replication client. `source-logical` adds the
  logical source and turns on `replication`, for its connection config. `watch-notify` adds the
  sequencer's `NOTIFY`, `watch-grpc` its gRPC watch server, `watch-grpc-tls` that server over TLS.
- `replication`: `pges::replication`; `tls` adds TLS to it.
- `otel`: appends without a trace record the current `tracing` span's W3C trace context
  (through `tracing-opentelemetry`); delivered events give it back with `trace_parent()`.

## Example

```rust,no_run
use pges::core::{event::Event as _, stream, version};
use pges::postgres::{admin, store};

#[derive(Debug, pges::Event)]
#[event(category = "jobs")]
enum JobEvent {
    #[event(name = "job.created")]
    Created { title: String },
}

async fn create_job(pool: sqlx::PgPool) -> Result<(), Box<dyn std::error::Error>> {
    let mut config = store::StoreConfig::new(pges::core::store_name!("ops_es"));
    config.migrate = store::AutoMigrate::CreateOrMigrate(store::StoreDefinition::default());
    let store = store::Store::open(pool, config).await?;
    store
        .admin()
        .register_category(JobEvent::CATEGORY, admin::CategoryConfig::default())
        .await?;

    let created = [JobEvent::Created { title: "Replace brake pads".into() }];
    store
        .append(stream::StreamId::new_v7(), version::ExpectedVersion::NoStream, &created)
        .await?;
    Ok(())
}
```

The sequencer also runs as a binary of its own. Once the crates are published,
`cargo install pges-sequencer --features bin` installs it; until then, from a checkout of the repository,
`cargo install --path crates/pges-sequencer --features bin`.

Docs: <https://docs.rs/pges>, once the crate is published. The crate docs have a full example with an
aggregate and a subscription.
