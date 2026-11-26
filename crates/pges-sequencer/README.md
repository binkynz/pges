# pges-sequencer

The pges sequencer. It orders committed appends into each store's log, gap-free, so subscribers follow a
store or a category with a plain integer cursor. Several sequencers can run against one database: a lease
elects one leader, and the others stand by.

It learns about commits from a source:

- the logical source (default) reads them in commit order from a logical replication slot;
- the outbox source drains the `pending` queue of stores created with the outbox option. A trigger
  notifies it when an append commits; a slow recheck is only a fallback.

Run it as the `pges-sequencer` binary, or embed it in an application through the
[`pges`](https://docs.rs/pges) facade's `sequencer` feature. Depend on this crate directly for a service
that only sequences, or to write a source of your own.

## Features

- `source-logical` (default): the logical replication source, through `pges-replication` with TLS.
- `watch-notify`: `NOTIFY pges_<store>` after each batch, for subscribers' `NOTIFY` wakers.
- `watch-grpc`: the gRPC watch server (`pges.watch.v1.Watch`, tonic), for subscribers' gRPC wakers. The
  leader streams each store's high-water mark; a standby answers `UNAVAILABLE`. With `bin`,
  `--grpc-listen <addr>` serves it.
- `watch-grpc-tls`: the gRPC watch over TLS (rustls), optionally requiring client certificates. With `bin`,
  `--grpc-tls-cert`, `--grpc-tls-key` and `--grpc-tls-client-ca`.
- `bin`: the `pges-sequencer` binary. `pges-sequencer bootstrap` creates the logical source's publication
  and slot; `pges-sequencer resequence` recovers from a lost slot.

With the logical source, create the slot before the first append: run the migrations, then
`pges-sequencer bootstrap`, then start the writers. `pges-sequencer bootstrap --failover` creates the slot
with `FAILOVER` (PostgreSQL 17+), so standbys with `sync_replication_slots` on keep a copy of it.

| `bootstrap` flag | `PGES_FAILOVER` | New slot | Existing slot |
|---|---|---|---|
| none | unset | created without `FAILOVER` | keeps its setting |
| `--failover` | `on` | created with `FAILOVER` | `FAILOVER` turned on |
| `--no-failover` | `off` | created without `FAILOVER` | `FAILOVER` turned off |

The two flags exclude each other, and either overrides `PGES_FAILOVER`. Changing an existing slot needs
the sequencers stopped, because `bootstrap` refuses a slot in use. Running the sequencer applies the setting
only to a slot it creates; it never changes an existing one. In the library, this is
`LogicalConfig::failover`, a `logical::SlotFailover`.

## Recovering a lost slot

If the server invalidates the slot (`max_slot_wal_keep_size`, `idle_replication_slot_timeout`), the slot is
dropped, or a store held commits before the first slot existed, the sequencer records the reason in
`pges.sequencer.halted` and stops; every sequencer then refuses to start (`Status::Halted`). To recover, stop
every sequencer and run `pges-sequencer resequence`. It creates a new slot, gives the commits the
sequencers never saw their log rows in commit id order, clears `halted`, and exits. Then start the
sequencers again: they resume from the new slot.

`resequence` searches back from each store's newest sequenced commit by the store's `max_txn_age` plus
twice its `max_clock_skew`, both recorded in `pges.stores` (defaults 1 hour and 5 s; see
`pges_postgres::limit`). It refuses unless every writer role, given with `--writer-role` (or
`PGES_WRITER_ROLES`, comma-separated), caps its transactions at that `max_txn_age` with
`transaction_timeout`.

```rust,no_run
use pges_postgres::limit;
use pges_replication::config;
use pges_sequencer::logical;

async fn recover(pool: sqlx::PgPool) -> Result<(), Box<dyn std::error::Error>> {
    let replication = config::Config::from_url("postgres://sequencer:secret@db:5432/app")?;
    let mut source = logical::LogicalSource::new(pool, replication, logical::LogicalConfig::default());
    let config = logical::ResequenceConfig::new(limit::WriterRoles::one("app_writer"));
    let resequenced = source.resequence(&config).await?;
    for store in &resequenced.stores {
        println!("store {}: {} commits recovered", store.store, store.commits);
    }
    Ok(())
}
```

## Example

```rust,no_run
use pges_replication::config;
use pges_sequencer::{logical, sequencer};

async fn run(pool: sqlx::PgPool) -> Result<(), Box<dyn std::error::Error>> {
    let replication = config::Config::from_url("postgres://sequencer:secret@db:5432/app")?;
    let source = logical::LogicalSource::new(pool.clone(), replication, logical::LogicalConfig::default());
    let sequencer = sequencer::Sequencer::new(pool, source, sequencer::SequencerConfig::default());
    sequencer.run(std::future::pending()).await?;
    Ok(())
}
```

Docs: <https://docs.rs/pges-sequencer>, once the crate is published.
