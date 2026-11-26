# pges

pges is an event store for Rust on PostgreSQL 18+. Applications append events to streams with optimistic
concurrency, load aggregates back, and follow categories of events through subscriptions. A sequencer
orders committed appends into a gap-free log per store, so subscribers keep a plain integer cursor, and a
projection in the same database sees every event exactly once.

## Why

- **Versions as ranges.** One append writes one `commits` row, however many events it holds. The row's
  stream versions, and the positions of its log entry, are PG18 virtual `int8range` columns. An append is
  one statement.
- **Partitioned commits.** `commits` is partitioned by category, then by time (a day or a month per
  leaf). Each category has its own indexes and vacuum settings, and retention drops whole leaves: per
  category, keep everything, keep a time window, or keep a window plus snapshots of long-lived streams.
- **Gap-free positions.** One sequencer per database assigns every store's positions in commit order. A
  position is never skipped and never reused, also across restarts and failover, so a cursor is an integer
  and nothing is missed.
- **No polling on the live path.** The sequencer reads commits from a logical replication slot, or from an
  outbox woken by `LISTEN/NOTIFY`. Subscribers can be woken by the sequencer, in process or by `NOTIFY`.
  The poll waker, the default for now, is for setups that have neither.

## Architecture

```text
writers ──▶ streams + commits           one statement per append (or a unit of work)
                 │
                 ▼  logical source: replication slot (default)
                    outbox source:  pending queue, woken by LISTEN/NOTIFY
            sequencer ──▶ log            per-store positions, gap-free, commit order
                 │
                 ▼  wake-up: in process, NOTIFY, or poll
            subscriptions                run_pg: exactly once, in the projection's transaction
                                         run:    at least once, for anything else
```

One sequencer leads per database; others stand by. It runs as the `pges-sequencer` binary or inside an
application. Install the binary with `cargo install pges-sequencer --features bin`.

## Quick start

```toml
[dependencies]
pges = "0.1"   # default features: derive, subscribe, wake-grpc
sqlx = { version = "0.9.1", default-features = false, features = ["runtime-tokio", "postgres"] }
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

Define events, open a store (creating and migrating it), append, and load an aggregate:

```rust,no_run
use pges::core::{aggregate, event::Event as _, stream, version};
use pges::postgres::{admin, store};

// One enum per category; each variant is one stored event type.
#[derive(Debug, Clone, PartialEq, pges::Event)]
#[event(category = "jobs")]
enum JobEvent {
    #[event(name = "job.created")]
    Created { title: String },
    #[event(name = "job.assigned")]
    Assigned { assignee: String },
}

// An aggregate folds a stream's events into state.
#[derive(Default)]
struct Job {
    assignee: Option<String>,
}

impl aggregate::Aggregate for Job {
    type Event = JobEvent;

    fn apply(&mut self, event: &JobEvent) {
        if let JobEvent::Assigned { assignee } = event {
            self.assignee = Some(assignee.clone());
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let pool = sqlx::PgPool::connect("postgres://app@localhost/app").await?;

    let mut config = store::StoreConfig::new(pges::core::store_name!("ops_es"));
    config.migrate = store::AutoMigrate::CreateOrMigrate(store::StoreDefinition::default());
    let store = store::Store::open(pool, config).await?;
    store
        .admin()
        .register_category(JobEvent::CATEGORY, admin::CategoryConfig::default())
        .await?;

    let job_id = stream::StreamId::new_v7();
    let created = [JobEvent::Created { title: "Replace brake pads".into() }];
    store.append(job_id, version::ExpectedVersion::NoStream, &created).await?;

    let job = store.load::<Job>(job_id).await?;
    if job.state().assignee.is_none() {
        let assigned = [JobEvent::Assigned { assignee: "ana".into() }];
        store.append(job_id, job.expected(), &assigned).await?;
    }

    Ok(())
}
```

`Admin::maintain` creates partitions ahead of time. Run it on a schedule, for example once an hour, with
`Admin::apply_retention` for categories that do not keep everything.
Subscriptions see events once a sequencer has written them to the log; the `pges` crate docs show a
subscription and an embedded sequencer.

## Crates

| Crate | What it holds |
|---|---|
| [`pges`](crates/pges) | The facade: every crate below as a module. Most applications depend on this alone. |
| [`pges-core`](crates/pges-core) | Ids, versions, positions, the `Event` and `Aggregate` traits, codecs, the payload frame. No IO. |
| [`pges-derive`](crates/pges-derive) | `#[derive(Event)]`: stable type names, schema versions, upcasting. |
| [`pges-postgres`](crates/pges-postgres) | `Store`: appends, reads, aggregate loads, snapshots, units of work; migrations; `Admin`. |
| [`pges-sequencer`](crates/pges-sequencer) | The sequencer, with the logical and outbox sources, as a library and a binary. |
| [`pges-subscribe`](crates/pges-subscribe) | Subscriptions: `run_pg` (exactly once) and `run` (at least once); wakers; replica reads; backfills. |
| [`pges-watch-proto`](crates/pges-watch-proto) | The sequencer's gRPC watch service, `pges.watch.v1`: the proto file and generated code. |
| [`pges-replication`](crates/pges-replication) | A minimal logical replication client, used by the logical source. |
| [`pges-testing`](crates/pges-testing) | PostgreSQL 18 for tests, template databases, fixtures, a fault-injection proxy. |

## Features of `pges`

| Feature | Default | Adds |
|---|---|---|
| `derive` | yes | `#[derive(pges::Event)]` |
| `subscribe` | yes | `pges::subscribe`, without a waker: a subscription rechecks the log on a timer |
| `wake-grpc` | yes | the gRPC waker, woken by the sequencer's gRPC watch (adds tonic) |
| `wake-grpc-tls` | | `https` endpoints for the gRPC waker, through rustls (turns on `wake-grpc`) |
| `wake-poll` | | the poll waker |
| `wake-inproc` | | the in-process waker, for a sequencer in the same process |
| `wake-notify` | | the `LISTEN/NOTIFY` waker, woken by the sequencer's `NOTIFY` |
| `sequencer` | | `pges::sequencer` with the logical and outbox sources (turns on `sequencer-outbox` and `source-logical`) |
| `sequencer-outbox` | | `pges::sequencer` with the outbox source only, without the replication client |
| `source-logical` | | the logical replication source, with TLS (turns on `sequencer-outbox` and `replication`) |
| `watch-notify` | | the sequencer sends `NOTIFY` after each batch (turns on `sequencer-outbox`) |
| `watch-grpc` | | the sequencer's gRPC watch server, `pges.watch.v1` (turns on `sequencer-outbox`) |
| `watch-grpc-tls` | | the gRPC watch server over TLS, through rustls (turns on `watch-grpc`) |
| `replication` | | `pges::replication`, the replication client |
| `tls` | | TLS for replication connections, through rustls (turns on `replication`) |
| `otel` | | appends without a trace record the current `tracing` span's W3C trace context; delivered events give it back as an OpenTelemetry parent context |

`pges::core` and `pges::postgres` are always there.

## Metadata

Each commit carries metadata: a correlation id, a causation id, an actor, a W3C trace context and string
pairs. Set it once where a request enters with `ambient::scope`; every append inside fills the fields it
leaves unset from the scope. A handler gives the commits an event causes the event's `follow_up()`: the
same correlation, the event's commit as the cause, the same trace, and no actor.

```rust,no_run
use pges::core::{metadata, stream, version};
use pges::postgres::{ambient, store};

#[derive(Debug, pges::Event)]
#[event(category = "jobs")]
enum JobEvent {
    #[event(name = "job.created")]
    Created { title: String },
}

async fn create_job(store: &store::Store, user: &str) -> Result<(), Box<dyn std::error::Error>> {
    let mut request = metadata::Metadata::default();
    request.correlation_id = Some(metadata::CorrelationId::new_v7());
    request.actor = Some(user.into());

    ambient::scope(request, async {
        let created = [JobEvent::Created { title: "Replace brake pads".into() }];
        store.append(stream::StreamId::new_v7(), version::ExpectedVersion::NoStream, &created).await
    })
    .await?;
    Ok(())
}

async fn next_step(
    store: &store::Store,
    cause: &stream::StreamEvent<JobEvent>,
) -> Result<(), Box<dyn std::error::Error>> {
    let created = [JobEvent::Created { title: "Order pads".into() }];
    store
        .append(stream::StreamId::new_v7(), version::ExpectedVersion::NoStream, &created)
        .metadata(cause.follow_up())
        .await?;
    Ok(())
}
```

With the `otel` feature, an append with no trace from its metadata or scope records the current span's.

## Examples

[`examples/`](examples) has runnable programs: `quickstart`, `unit_of_work`, `subscription`,
`multi_category`, `sharded`, `backfill`, `sequencer_logical`, `outbox`, `grpc_watch` and `metadata`. Run one
with:

```sh
cargo run -p pges-examples --example quickstart
```

## Running the tests

Tests need PostgreSQL 18 with `wal_level = logical`. They find one by themselves:

1. `PGES_TEST_DATABASE_URL`, if set;
2. the local cluster from `nix develop -c pg-up`, if it is running;
3. otherwise a Docker container, `pges-test-pg18`, started on first use and reused.

```sh
cargo test --workspace --all-features
```

Each test gets its own database, cloned from a template. [`docs/testing.md`](docs/testing.md) has the detail.

## Docs

- [`docs/design.md`](docs/design.md): architecture and decisions.
- [`docs/schema.md`](docs/schema.md): tables and the design of each statement (the SQL lives in each crate's `sql/` directory).
- [`docs/api.md`](docs/api.md): crates, features and the Rust API.
- [`docs/testing.md`](docs/testing.md): how the tests run.
- [`CHANGELOG.md`](CHANGELOG.md): what each release changed.

## Stability

pges is pre-1.0. A minor version (0.1 to 0.2) may break the API; a patch version does not. Public enums
and config structs are `#[non_exhaustive]`, so a new variant or field is not a breaking change.

The database schema is versioned apart from the crates. Migration 0001 is frozen as released in 0.1.0: every
later schema change is a new migration that the runner applies on top, never an edit of an old one
([`docs/schema.md`](docs/schema.md), Migrations). Stored payloads, event schema versions and the watch
protocol have their own rules ([`docs/api.md`](docs/api.md), Versioning).

These parts are experimental. They are tested, but have seen less use and fewer measurements than the
rest, and may change more in a minor version:

- the gRPC watch, its server (`watch-grpc`, `watch-grpc-tls`) and the gRPC waker (`wake-grpc`,
  `wake-grpc-tls`), and the `pges.watch.v1` protocol in `pges-watch-proto`;
- the outbox source (`sequencer-outbox`), the alternative to the logical source for setups without
  `wal_level = logical`; its appends commit one at a time;
- sharded subscriptions, replica reads and backfills in `pges-subscribe`;
- W3C trace context (`otel`);
- `pges-testing`, which follows the needs of pges's own tests.

The append path, stream reads, aggregates, migrations, `Admin` (categories, partitions, retention), the
sequencer with its logical source, and unsharded subscriptions (`run_pg`, `run`) are the core of 0.1.

**MSRV.** The minimum supported Rust version follows sqlx (currently 1.94); raising it is a
minor-version change.

## License

Licensed under either of [Apache License, Version 2.0](LICENSE-APACHE) or [MIT license](LICENSE-MIT), at
your option.
