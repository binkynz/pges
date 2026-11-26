# Changelog

All notable changes to the pges crates. The crates are released together, with one version. The format
follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the crates follow
[semantic versioning](https://semver.org/) as Cargo applies it before 1.0: a minor version may break the API.

## [Unreleased]

### Added

- **Append partition routing** (`pges-postgres`). A single append names its stream's hash partition of
  `streams`, so it locks only that partition whatever the planner's statistics and `plan_cache_mode`, and
  reuses its plan (about 3x the appends per second at concurrency 1). It needs one prepared statement per
  partition on each connection, about 130 KB of server plan-cache memory each (8.6 MB at 64 partitions), and
  runs only while they fit sqlx's statement cache (`statement-cache-capacity >= stream_partitions + 36`; the
  default 100 holds the default 64 partitions) and the role may write every partition directly. Otherwise
  appends go through `streams` as before. `StoreConfig::append_routing` (`AppendRouting::Auto`, the default,
  or `Streams`) turns it off.

## [0.1.0] - Unreleased

The first release: `pges`, `pges-core`, `pges-derive`, `pges-postgres`, `pges-subscribe`, `pges-sequencer`,
`pges-replication`, `pges-watch-proto` and `pges-testing`. It needs PostgreSQL 18 and Rust 1.94.

### Added

- **Stores and appends** (`pges-postgres`). A store is a PostgreSQL schema, created and migrated by
  `Store::open` or the `migrate` functions. Appends to a stream with an expected version, in one statement;
  `append_many` for several streams in one transaction; `UnitOfWork` for appends alongside the caller's own
  statements. Crate-generated, time-ordered commit ids make a retried append idempotent.
- **Reads and aggregates.** Stream reads, `load` of an `Aggregate`, snapshots (`load_snapshotted`,
  `save_snapshot`), stream deletion.
- **Events** (`pges-core`, `pges-derive`). `#[derive(Event)]` gives each variant a stable type name and a
  schema version, with upcasting from older versions. JSON codec by default. A versioned payload frame.
- **Metadata.** Correlation id, causation id, actor, W3C trace context and string pairs on each commit; an
  ambient scope for a request; `follow_up()` for the commits an event causes. The `otel` feature records and
  returns the current span's trace context.
- **Partitions and retention** (`Admin`). Categories partitioned by time (a day or a month per leaf);
  `maintain` creates partitions ahead; retention per category (keep everything, a time window, or a window
  plus snapshots), log pruning, closed-partition freeze, writer limits.
- **The sequencer** (`pges-sequencer`). One leader per database, with a lease and standbys, writes a
  gap-free log of positions per store in commit order. Two sources: a logical replication slot (default) or
  an outbox woken by `LISTEN/NOTIFY`. Slot bootstrap and resequence after a lost slot. In-process watch,
  `NOTIFY` after each batch, and a gRPC watch server with optional TLS. A `pges-sequencer` binary (feature
  `bin`).
- **Subscriptions** (`pges-subscribe`). Ordered reads over listed categories or the whole store, with a
  plain integer cursor. `run_pg` delivers exactly once inside the projection's transaction; `run` at least
  once. Leases and fencing, sharded subscriptions, replica reads with fallback, backfills. Wakers: gRPC
  (default), in-process, `LISTEN/NOTIFY` and poll.
- **Logical replication client** (`pges-replication`): replication connections, the pgoutput subset the
  sequencer needs, standby status, optional TLS through rustls.
- **gRPC watch protocol** (`pges-watch-proto`): `pges.watch.v1`, with generated tonic client and server.
- **Test support** (`pges-testing`): PostgreSQL 18 from a local cluster or Docker, a database per test
  cloned from a template, store fixtures, a fault-injection proxy, a primary with a streaming replica, a
  cancellation harness and throwaway TLS certificates.

### Stability

- Migration 0001 (shared and per store) is frozen as released: later schema changes are new migrations.
- Experimental: the gRPC watch and waker, the outbox source, sharded subscriptions, replica reads,
  backfills, `otel`, and `pges-testing` (README, Stability).
- MSRV 1.94. The MSRV follows sqlx; raising it is a minor-version change.

[Unreleased]: https://github.com/binkynz/pges/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/binkynz/pges/releases/tag/v0.1.0
