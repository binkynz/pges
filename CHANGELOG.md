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
- **Partition headroom** (`pges-postgres`). `Admin::partition_headroom` reports, per category, the upper
  bound of its newest time leaf and how far ahead of now it is (`CategoryHeadroom::below_one_bucket` flags
  less than one bucket), and the positions left in the newest log partition. Alert on it to catch a stopped
  `Admin::maintain` before appends fail.
- **`AppendError::NoPartition`** (`pges-postgres`). An append whose commit id no leaf of its category takes
  (maintenance stopped for about `LEAVES_AHEAD` buckets) fails with `NoPartition { category, commit_id }`
  instead of a raw database error (SQLSTATE 23514). `delete_stream` reports it as `Write(NoPartition)`.

### Changed

- **`append_many` plans for its streams** (`pges-postgres`). The batch statement runs with
  `plan_cache_mode = force_custom_plan`, so it locks only the partitions of `streams` its streams live in
  (4 locks each) whatever the planner's statistics and the role's or session's `plan_cache_mode`. Before, a
  generic plan, which the plan cache kept for a store analyzed nearly empty or under `force_generic_plan`,
  locked every partition, and at 1M streams ran 1.5x to 6x slower. In its own transaction the setting is
  part of `BEGIN` (no extra round trip) and ends with the transaction; inside a unit of work it is set
  before the batch and the transaction's previous value is put back after it (two more round trips, and one
  more prepared statement on that connection). Stores where the plan cache chose a cheap generic plan now
  plan every batch: about 1 ms more per batch of 16.

### Fixed

- **A slot ahead of the token halts sequencing** (`pges-sequencer`). After promoting an asynchronous
  standby with a synchronised failover slot, the slot could be confirmed past the standby's stored token:
  the sequencer's batch with the log rows and the token comes later in WAL than the commits it confirms.
  The server then silently started after those commits, and they were never sequenced. The logical source
  now reads `confirmed_flush_lsn` whenever it opens and halts with `SlotLost::Ahead` when it is past the
  token; `pges-sequencer resequence` recovers the commits. The source no longer confirms idle WAL from
  keepalives directly: it hands the position out as an empty batch's token, which the sequencer stores
  before it is confirmed, so the slot is never ahead of the token on a healthy server. A slot the source
  creates has its consistent point stored as the token at once. design.md now recommends a synchronous
  failover target.
- **Category names no longer collide with other categories' partitions** (`pges-postgres`, `pges-core`,
  `pges-derive`). A category named like another's leaf took that leaf's name: `a_202611`'s partition was
  `a`'s November leaf `commits_a_202611`. Registered first, it left `a` without that leaf, so its appends
  failed all month while maintenance reported success; registered second, it failed. Names like a leaf's
  index (`a_202611_s`) or a key (`a_pkey`, `pkey`) failed to register. Leaves are now
  `commits_<cat>__<suffix>`, the category partition's key `commits_<cat>__pkey` and the key of `commits`
  `commits__pkey`, and category names may not contain `__` (`CategoryError::DoubleUnderscore`). Maintenance
  also checks a partition's parent: a name taken by another relation fails with `NameTaken` (on
  `RegisterCategoryError`, `ChangeBucketError` and `MaintainError`) instead of being skipped or dropped.
  Store migration 0001 changed in place (nothing is published yet).
- **Late retries in a transaction are deduplicated** (`pges-postgres`). A caller-supplied commit id that
  already committed on the stream returns `Outcome::Deduplicated` in a unit of work and in `append_in`
  inside a transaction too, even when the retry comes later than `max_clock_skew`. Before, the skew check
  ran first and the retry failed with `CommitIdSkew`, aborting the caller's transaction; only appends
  outside a transaction looked the id up. Only ids above the stream's last commit id are checked against
  the skew now, so a late id the stream has not stored is `CommitIdOutOfOrder`.
- **The logical source ends only walsenders** (`pges-sequencer`). Before `START_REPLICATION` it ended
  whichever process held the slot, including a session running `pg_drop_replication_slot` or another slot
  function. A drop ended mid-call could remove the slot while its client saw an error. Now only a walsender
  holding the slot is ended; another session is waited for, as before, up to `LogicalConfig::start_timeout`.

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
