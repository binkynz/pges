# pges — design

Architecture and the decision log. pges is a Rust event store on PostgreSQL 18+, built for high write
rates, billions of events and many subscribers. It is published on crates.io, so every dependency must be a
published crate.

Detail: [schema.md](schema.md) (tables, column order, queries, partitions, roles), [api.md](api.md)
(crates, features, Rust API, sources, wake-ups, versioning), DDL in `../crates/pges-postgres/migrations/`.

## Goals

- Append with optimistic concurrency per stream; optional Dynamic Consistency Boundary (DCB) by tags.
- A per-store log in true commit order, gap-free, with a plain integer cursor.
- Subscribers wake without polling and without writer-side `LISTEN/NOTIFY`: only the sequencer notifies
  subscribers. Writers to outbox stores notify only the sequencer's outbox source (`pges__outbox`).
- Partitioned storage with cheap archival.
- Generic over payload encoding, stream keys and deployment (single primary up to primary + replicas).

## Non-goals (v1)

- Multi-cluster sharding. One PG cluster is the unit; shard above the crate if ever needed.
- Querying payloads in SQL. Payloads are opaque bytes; query through projections.

## Deployment

| Component | Required | Notes |
|---|---|---|
| PG18 primary | yes | with the default logical source: `wal_level = logical`, REPLICATION role for the sequencer; the outbox source needs neither |
| Sequencer | yes | one active instance plus standbys; leader elected by a lease row |
| HA standby | no | if present, use failover slots (`failover = true`) so the slot survives promotion, and make the failover target a synchronous standby (Guardrails) |
| Read replicas | no | offload subscriber catch-up reads |

Guardrails:

- Deploy order with the logical source: migrations, then `pges-sequencer bootstrap` (creates the
  publication and the slot), then writers. A sequencer that creates the slot itself while writers already
  run may halt (`CommitsBeforeSlot`, or `Ahead` if interrupted mid-creation) until `pges-sequencer resequence`
  runs (schema.md, Logical source). A bootstrap run after writers started halts the same
  way and records the halt, so `pges-sequencer resequence` can run next, before any sequencer starts.
- Failover slots need a synchronous failover target. The sequencer confirms a source commit to the slot only
  after its token is stored, but that transaction comes later in WAL than the commits it covers. Slot
  synchronisation copies the slot's position to a standby that has the commits, even when it does not yet
  have the token's transaction. An asynchronous standby promoted in that window has a slot ahead of its
  token: the server would skip the commits in between. The sequencer halts there instead
  (`SlotLost::Ahead`), and `pges-sequencer resequence` recovers them (Sequencer, Slot loss). To avoid the
  halt, put the standby you would promote in `synchronous_standby_names` and in `synchronized_standby_slots`:
  the token's transaction (`synchronous_commit = on`) then waits for the standby's flush before the slot is
  confirmed. With a quorum (`ANY k`), only the standbys that confirmed are covered, so promote one of them.
- `max_slot_wal_keep_size` and `idle_replication_slot_timeout` (PG18), so an abandoned or stuck slot cannot
  fill the disk. Either one invalidates the slot when it fires, and commits whose WAL was not yet confirmed
  are then never sequenced until an operator runs `pges-sequencer resequence` (Sequencer, Slot loss). Set
  `idle_replication_slot_timeout` well above the longest expected sequencer outage.
- Alert on `pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)` at a fraction of
  `max_slot_wal_keep_size`, on `pg_replication_slots.wal_status IN ('extended', 'unreserved')` and on low
  `safe_wal_size`, well before invalidation.
- Writer role: `transaction_timeout` and `idle_in_transaction_session_timeout`, at most the store's
  `max_txn_age`. Long writer transactions pin the slot's `restart_lsn`, and retention and resequence rely on
  the cap: both refuse to run unless the role's setting is within it.
- Writer limits: each store records `max_clock_skew` and `max_txn_age` (`pges.stores`; defaults 5 s and
  1 h). Writers' `StoreConfig::max_clock_skew`, when set, must not exceed the store's; `None` (the default)
  uses the store's recorded value, so raising the store's with `set_writer_limits` is enough. `Store::open`
  refuses a larger one. The limits can only be raised, with `Admin::set_writer_limits`: raise them before
  opening wider writers or lengthening `transaction_timeout`, and only when needed, because a raised limit
  stays.
- `logical_decoding_work_mem` set explicitly (e.g. 256MB-1GB); the sequencer subscribes with
  `streaming = off`, so it receives whole committed transactions in commit order and large units of work
  spill to disk.
- `wal_compression = lz4` (or `zstd`). PG18 enables data checksums by default, so hint-bit writes emit
  full-page images.
- Raise `max_locks_per_transaction` when categories carry many live leaves. In PG18 it also sets the
  fast-path lock slots per backend. A category read's two statements take about 11 locks with
  custom plans, whatever the leaves. A statement planned over every leaf takes about 2 locks per live leaf:
  the earlier one-statement category read took 2007 per read at about 1000 leaves, most in the shared
  lock table, and 7–28% of readers waited on the LockManager lock; a generic plan of the commit fetch would
  take 2003 (Spike 3, below).
- Keep `stream_partitions` at 64 (the default) up to about 1B streams. Single appends lock only their
  stream's partition (9 locks), but `append_many` locks each partition it touches (4 locks each) (schema.md,
  Tables). Do not set `plan_cache_mode = force_generic_plan` on writer roles of a store whose appends go
  through `streams`: it brings back the single append's lock on every partition. `append_many` sets custom
  plans for itself.
- Store migration 0001 sets the `streams` partitions' autovacuum scale factor to 0.02 (PostgreSQL's 0.2
  allowed about 312k dead tuples per partition at 100M streams). Existing stores must be recreated.
- Retention per leaf costs O(leaf streams) (schema.md, Partition maintenance, Retention). Its
  per-stream probes are serial, so a cold cache makes a leaf take seconds at 100M streams; time
  `apply_retention` on the real store size, and prefer monthly over daily buckets for categories with
  retention.
- Closed leaves and log partitions should be all-frozen before anti-wraparound vacuum reaches them: at
  20k commits/s, 200M XIDs (`autovacuum_freeze_max_age`) pass about every 2.8 h. Run
  `Admin::freeze_closed_partitions` off-peak; freezing an unfrozen partition writes about half its size in
  WAL. Alert on `Admin::partition_ages` well before `autovacuum_freeze_max_age` for closed partitions not all
  frozen (schema.md, Closed partitions).
- Size `max_wal_size` from the expected WAL rate. With `checkpoint_completion_target` 0.9, a checkpoint
  starts once the WAL since the last one passes about `max_wal_size` / 1.9, so timed checkpoints hold only
  up to about that much WAL per `checkpoint_timeout`: about 30 MB/s at 16GB and 5 minutes. Above it,
  checkpoints come from WAL volume (every 3 minutes at about 50 MB/s in the 2026-11-25 durable runs).
- Replicas with `hot_standby_feedback = on` need a `statement_timeout` for catch-up readers; a long snapshot
  holds back HOT pruning on the primary.
- High-volume stores: keep projection read models in a separate database, so their WAL does not compete
  with the sequencer's decoding.

## Data flow

```
append ──▶ streams (UPDATE, row lock) + commits (INSERT)        one statement, one round trip
                                   │ WAL (publication: commits, column list excludes payload from the output)
                                   ▼
                      sequencer ──▶ log (per-store positions, commit order)
                                   │
                                   └── watch endpoint: high-water mark per store and per category
                                       (u64s, level-triggered)
                                                │
subscribers ◀───────────────────────────────────┘  read log ⋈ commits WHERE last_position > cursor
```

The column list trims the decoder's output only. Postgres decodes every WAL record of the database in one
walsender, and reassembles TOASTed payloads before pgoutput filters them, so the sequencer's ceiling is the
database's WAL MB/s, not commits/s.

## Stores and categories

- A **store** is a Postgres schema holding the table set below. It is the unit of ownership, grants,
  retention defaults and position numbering, typically one per bounded context.
- A **category** (`parts`, `orders`, `jobs`) groups streams of one aggregate type inside a store.
- `commits` is LIST-partitioned by category, then RANGE by time, so a category gets its own indexes, vacuum
  and retention. Its primary key is `(category_id, commit_id)`.
- One sequencer per database: one source, one publication (each store's `commits` with a column list), one
  position counter per store. No ordering across stores.
- No table per aggregate type and no typed payload columns; typed tables are read models.

## Schema, append and DCB

One row per **commit** (one append call), not per event, in `commits` (LIST by category, then RANGE by
uuidv7 commit id). `streams` holds each stream's version and is the concurrency authority; `log` holds the
sequencer's positions. Versions and positions are exposed as PG18 virtual `int8range` columns. Appends are one
statement. DCB maps tags to version-counted tag heads. Full DDL, column-order rationale and statements:
[schema.md](schema.md) and `../crates/pges-postgres/migrations/`.

## Sequencer

1. Acquire the lease and bump `fence` in one statement, then publish the address:
   `UPDATE pges.sequencer SET fence = fence + 1, lease_until = now() + $ttl, leader = $addr
   WHERE lease_until IS NULL OR lease_until < now() RETURNING fence, source, source_token, halted`.
   The seeded row has `lease_until` NULL, hence the `IS NULL`. Renew with
   `UPDATE pges.sequencer SET lease_until = now() + $ttl WHERE fence = $f`; 0 rows means stand down. The
   leader stops sequencing once its local lease deadline passes. A graceful shutdown stores the token and
   releases the lease (`lease_until = NULL`), so a standby takes over at once; after a crash it waits for
   expiry. Before every `START_REPLICATION` the logical source terminates the walsender still holding the
   slot and retries while the slot is active, for up to `start_timeout`, while the sequencer keeps renewing
   its lease (schema.md, Sequencer lease). A leader that stops leading closes its source first
   (`Source::close`), so the walsender releases the slot at once.
2. Read committed appends from its `Source`, resuming from `pges.sequencer.source_token`. Sources (api.md):
   logical replication through our own `pges-replication` crate (default, feature `source-logical`; built),
   a transactional outbox queue (built, no feature), or Kafka (planned). A Supabase ETL source can be built
   on the public `Source` trait outside the published crates.
3. Assign `[last_position+1, last_position+n]` per commit in source order (WAL commit order for logical).
4. Every few ms, in one transaction: fence check (`SELECT … FOR SHARE`, no row update) and log rows. The
   source token commits with them through a replication origin, or is persisted with the high-water marks
   at most once a second (schema.md, Sequencer batch). Then `ack` the source up to the durable token (for
   logical replication, confirm that end LSN to the slot).
5. Publish the new high-water mark and the post-commit WAL insert LSN (`pg_current_wal_insert_lsn()`) on the
   watch endpoint.

Exactly-once effect: on restart the source resumes after the durable token, the logical source drops
transactions whose commit LSN is below it, and anything replayed between the token and the log tail is
deduplicated by commit id against `log`.

Long-running transactions do not delay the built-in sources: a commit is delivered when it commits. They do
pin the slot's `restart_lsn` and so retained WAL; `max_txn_age` caps them.

### Slot loss

1. The sequencer treats `pg_replication_slots.invalidation_reason IS NOT NULL`, a missing slot while
   `source_token` is set, a slot whose `confirmed_flush_lsn` is past the stored token (`Ahead`), a
   `START_REPLICATION` error naming an invalidated slot, or a store with commits before the first slot
   (`CommitsBeforeSlot`) as fatal. It sets
   `pges.sequencer.halted`, stops sequencing, puts the watch into an error state and alerts. It never
   recreates the slot by itself. Retention refuses to run while `halted` is set. Built:
   `SourceError::Halt(logical::SlotLost)`; the sequencer writes `pges.sequencer.halted` under its fence,
   releases the lease, and `run` returns the error. The watch status becomes `Status::Halted(text of run's
   error)`, which is the alert; the binary logs the error and exits 1. Tested end to end with
   `max_slot_wal_keep_size` in a private container (`tests/invalidation.rs`).
2. Recovery is an explicit `LogicalSource::resequence` (CLI `pges-sequencer resequence`), run while every
   sequencer is stopped. It takes the lease only while `halted` is set, drops the old slot, creates a new one
   and reads in the snapshot it exports at its consistent point, which closes the window for new commits.
   It first refuses unless every writer role caps its transactions at the stores' `max_txn_age`. For each
   category it then finds the orphans: commits with
   `commit_id >= pges.uuidv7_floor(ts(last sequenced commit) - max_txn_age - 2 × max_clock_skew)` that have
   no log row, using each store's recorded limits, anti-joined with that category's `log_by_category`
   entries from the matching position forward (`log` has no `commit_id` index). Rows an earlier resequence
   wrote do not count as the matching position. Writers check commit ids against `statement_timestamp()`,
   so the skew counts twice; with one skew a commit could be missed. It sequences them in `commit_id`
   order, stores the consistent point as the token, and clears `halted`; the sequencers then resume from the
   new slot, deduplicating by commit id against the recovered set. It records the positions each category
   got (`categories.resequenced_from`, `resequenced_through`), so retention scans them whole. Built
   (schema.md, Resequence).
3. The recovered range is ordered by `commit_id`, not true commit order; causal order inside it is
   best-effort.
4. A slot ahead of the token (`SlotLost::Ahead`) comes from promoting a standby that had a synchronised
   failover slot but not the sequencer's last transactions. Their log rows and token are lost with the old
   primary; the commits they sequenced are on the new primary, and START_REPLICATION from the old token
   would silently start at the slot's position, past them. The procedure is the same as for any halt: stop
   every sequencer (they have stopped by themselves), run `pges-sequencer resequence` against the new
   primary, then start them. The orphan search finds those commits: they are newer than the last commit the
   new primary has a log row for. Subscribers whose cursor passed positions the old primary handed out see
   those positions again with other commits; treat a promotion like any restore and rebuild or rewind
   projections fed from positions above the promoted log's newest row.

## Subscribers

- Persist a cursor (`subscriptions.position`, keyed by `(name, shard)`) plus a lease and fence for exclusive
  consumers. An unsharded subscription is one shard, row 0.
- Sharded subscriptions (`.sharded(n)`, built): each shard has its own row, cursor, lease and fence, and
  gets the streams whose id hashes to it. Instances record themselves in `subscription_members`; each takes
  at most its fair share, `ceil(n / live instances)`, and releases shards above it.
- Store-wide subscriptions (`.all_categories()`, built) read the log page alone, then each category's
  commits by id, so categories registered after start are included.
- Loop: read `log ⋈ commits WHERE last_position > cursor ORDER BY last_position LIMIT k`; when caught up,
  wait on the watch value. Filter by category with the `(category_id, last_position)` index.
- Treat the watch as a hint only. On reconnect or sequencer failover, catch up from the database first.
- Replica reads (built): every watch message carries the LSN the sequencer read after its batch commit. A
  replica reader waits until `pg_last_wal_replay_lsn()` reaches it; falling back to the primary is opt-in per
  subscription (`Replica::fallback(after, limit)`), with a cap the caller shares, so lag does not send every
  subscriber to the primary at once. They compose with store-wide and sharded subscriptions; each shard
  waits on its own.
- Wake-ups are pluggable (gRPC watch, in-process, LISTEN/NOTIFY from the sequencer only, poll); see api.md.
  Built: the gRPC, in-process, poll and LISTEN/NOTIFY wakers, and the sequencer's NOTIFY and gRPC watch
  (`pges.watch.v1`). Without a waker a subscription re-reads on a plain timer (`waker::recheck::Recheck`,
  500 ms).
- Degraded mode (sequencer unreachable): read the newest `log_by_category` entry per category at a slow
  interval (`categories.last_position` is only written about once a second).

## Read paths

| Need | API | Path |
|---|---|---|
| One aggregate's state | `store.load::<Job>(id)`, `store.load_snapshotted::<Job>(id)` | snapshot, then `commits_<cat>` by `(stream_id, last_version)`, pruned by `snapshots.commit_id` or `streams.first_commit_id` passed as a parameter |
| All events of a category, in order | `store.subscription(name).category::<JobEvent>()` | a page of `log` by `(category_id, last_position)`, then `commits_<cat>` by `commit_id = ANY(page)`; wakes on the category's high-water mark |
| Several categories, interleaved | `store.subscription(name).categories(non_empty![category!("jobs"), category!("orders")])` | one query per category, each bounded by the store's newest position `H`, merged at a cut no category can skip (schema.md) |
| All categories | `store.subscription(name).all_categories()` | a page of `log` alone up to `H`, then the commits of each category in the page by id |
| Sharded | `.sharded(n)` on either of the above | the same reads, filtered in SQL by the stream id's hash |
| A field backfill | `store.backfill(name, P).categories(..)` | the listed-categories read up to `least(H, P)`, split into lanes by stream |
| Queries and search | read-model tables | maintained by projections (below), never by scanning events |

Positions inside one category have gaps (other categories took them); cursors only need to increase.

## Retention

Policy is per category, with a default per store copied into a category when it is registered. It is applied
by `Admin::apply_retention(&RetentionConfig)`, run on the same schedule as `Admin::maintain`:

| Policy | Effect |
|---|---|
| `Forever` | nothing is dropped |
| `Window { keep, then: Drop \| Archive }` | for categories whose streams are short-lived; whole partitions are detached, optionally exported, then dropped; `Archive` is not built |
| `SnapshotAndTruncate { keep }` | for long-lived streams: before a partition goes, every live stream with commits in it must have a snapshot at or after its last version there |

- A leaf is dropped only once it is past `retain_for` plus the clock-skew tolerance, past `max_txn_age`
  plus twice the skew, and fully sequenced (schema.md, Retention). The skew and `max_txn_age` are the
  store's recorded writer limits. Retention refuses to run while the sequencer is halted, and unless every
  writer role's `transaction_timeout` is at most `max_txn_age`.
- Positions a resequence gave are in commit id order, not commit order. The fully-sequenced scan covers that
  range whole, and the next scan start ignores rows in it.
- Both policies raise `streams.floor_version` to each stream's newest version in the leaf, so a read of a
  stream whose commits were dropped fails with `Truncated` instead of returning part of it.
- The retention horizon is recorded per category before the leaf goes; a subscriber whose cursor is behind
  it gets `CursorBeforeHorizon` and must rebuild from snapshots or an existing projection. The check is part
  of the read statement, never a separate step. Store-wide subscriptions compare against the highest
  horizon of any category. A backfill instead reads each category from its horizon, skipping events that
  are gone.
- A log partition is dropped once it is full, not the newest, and every row in it is at or below its own
  category's `retention_scan_from`. A `Forever` category pins every log partition it writes to. Put
  high-volume short-lived categories in their own store, or use `Admin::prune_log`, which deletes a
  category's log rows up to `retention_scan_from`, except rows whose commit still exists.
- Archive export (not built): a crate tool writes detached partitions to object storage (Parquet via
  `arrow`); reading archived streams goes through an optional archive adapter.
- Personal data: prefer crypto-shredding (encrypt sensitive payload fields with a per-subject key; deleting the
  key makes them unreadable everywhere, including archives and replicas). For exceptions,
  `Store::delete_stream` deletes the stream's commits and snapshot at once and keeps a `$stream-deleted`
  tombstone commit, so projections purge their rows (`SystemEvent::StreamDeleted` through
  `Batch::system()`; typed decoding skips it). It does not reach replicas' history or backups. It records
  the deleted commit ids in `deleted_commits`, so store-wide reads skip their log rows. Retention forgets
  those records when it drops the leaf, once its horizon covers their log rows.
- Never drop by time alone for a category with live long-lived streams: `Window` refuses a leaf while a
  stream with commits in it had a commit within `keep` (`LiveStreams`).

## Projections and search

A projection is a subscriber that writes a read model. Two delivery modes:

- **Postgres sink, exactly-once effect**: the read-model writes, the cursor update and the fence check commit
  in one transaction.
- **External sink, at-least-once**: idempotent upserts keyed by document id, skipped when the stored
  `source_version` is newer; cursor kept in the sink where possible (e.g. a Tantivy commit payload).

Changing a read model, cheapest first:

1. **Derived from columns already in the read model** (normalised part number, new `tsvector` weights):
   a SQL migration with a batched backfill. No events read.
2. **In events, not yet projected**: a targeted backfill.
   - Deploy the new handler first; it writes the field for live events and stamps `<field>_pos`.
   - Record the live cursor `P` at deploy, then replay only the relevant categories up to `P`, per stream
     in parallel (order within a stream is enough for a per-aggregate field). Use snapshots where they
     already hold the value.
   - Backfill writes are guarded: `WHERE <field>_pos IS NULL OR <field>_pos < $pos`, so they never overwrite
     a live write.
   - Crate API (built): `store.backfill(name, P).categories(..).run_pg(handler)` (api.md).
3. **Not in any event**: no replay can recover it. Old rows stay null or default; new events carry it,
   and upcasters handle old event versions.
4. **Shape or meaning of the model changes**: full blue/green rebuild. Build `jobs_v2` from position 0
   (or from the retention horizon plus snapshots) while `jobs_v1` serves, then swap a view.

Search-shaped read models, in order of preference:

1. **Postgres, built in** (works on Cloud SQL):
   - Full-text: a stored generated `tsvector` with weights (`setweight`), GIN index, `websearch_to_tsquery`.
   - Fuzzy: `pg_trgm`. GIN for `%`/`ILIKE` filters; GiST for top-k nearest matches (`ORDER BY name <-> $q LIMIT 10`).
   - Identifiers (part numbers): a normalised column (upper-case, separators stripped) with B-tree for exact
     and trigram for near matches.
   - Semantic: `pgvector` column fed by an embedding step in the projection, if needed.
   - Hybrid ranking: combine FTS, trigram and vector ranks with reciprocal rank fusion in one query.
   - Limits: `ts_rank` is not BM25 and ranks every match; GIN writes are expensive on hot read models
     (tune `fastupdate`/`gin_pending_list_limit`).
2. **Embedded Tantivy** when ranking quality or write volume outgrows Postgres: BM25, fuzzy terms, facets,
   in-process in the projection service, cursor stored in the index commit.
3. **External engine** (OpenSearch, Meilisearch, Typesense) only if search must be shared across services.

Cross-aggregate search documents (a job with its order number and part names) come from one projection
subscribing to several categories in the same store, keeping small lookup tables (e.g. `order_id → order_no`)
updated in the same transaction.

## Snapshots, partitions, crates

Snapshot table, partition maintenance and retention mechanics are in [schema.md](schema.md); crate layout and
feature flags are in [api.md](api.md).

## Spikes before building

1. Append throughput: one hot stream vs 100k streams; single vs batched; p50/p99; WAL bytes per append,
   including the full-page-image share. Measured on durable storage (Durable dev-box run, below).
2. Sequencer throughput and lag at sustained load; restart and failover correctness. Kill the sequencer
   between the log commit and the ack, and between the ack and the next batch, then assert that no
   `commit_id` appears twice in `log`. Measure decode throughput at realistic payload sizes (above and below
   the TOAST threshold), with and without a projection writing read models in the same database. Compare
   against `pg_logical_emit_message(true, 'pges', <commit ref>)` with `messages = true` instead of the table
   publication. Restart and failover correctness is done (Crash correctness, below): kills before
   the log commit, between the commit and the ack, and between the ack and the next batch, on both sources,
   with a restart or a standby taking over, plus SIGKILL of the binary. No commit twice, gap-free, every
   commit once, commit order kept per category and per store. Promoting a physical standby with a
   failover slot is covered by `tests/promotion.rs` (Decided, Slot ahead of the token).
   Throughput and lag are measured on durable storage. Open: the payload-size, projection and
   `pg_logical_emit_message` variants.
3. Join read rate `log ⋈ commits` across partitions (runtime pruning) and catch-up with PG18 async I/O;
   plan-cache memory, planning time and lock-manager waits per read as live leaves grow. Measured (Spike 3,
   below).
4. Confirm on PG18: `uuid_extract_timestamp` is immutable and allowed in a virtual generated column;
   virtual columns work on partitioned tables; `RETURNING` from data-modifying CTEs on hash-partitioned
   `streams` behaves as in schema.md; uuidv7 partition routing under clock skew.
5. Outbox source: `pending` churn and sequence contention at target append rates, against logical. Today
   each non-empty batch costs BEGIN, the fence check, one DELETE per outbox store (even empty ones), one
   INSERT per touched store, COMMIT and one LSN read; the fence check and the drain could become one
   statement. Also measure the NOTIFY commit lock, which serialises outbox appends.
6. Cloud SQL: logical decoding flag, failover slots, `idle_replication_slot_timeout` availability,
   grants for the replication origin functions.
7. Slot invalidated under load, then `resequence`: no commit lost, none sequenced twice. Done (below).

## SQL layout (2026-11-25)

SQL lives in `.sql` files per crate, rendered by a typed template, and a statement registry is checked against
PostgreSQL 18 with `describe` in tests. Each query's parameters and row are Rust types, so a wrong bind or
read does not compile, and the registry test checks those Rust types against PostgreSQL. sqlx's compile-time
macros were rejected: most statements interpolate store and category identifiers, which the macros cannot
check, and a published crate would have to ship an offline query cache. Built for
pges-postgres, pges-sequencer, pges-subscribe, pges-testing and the bench, with typed bindings in all of
them. pges-replication sends only replication commands, which are not SQL, and the `pges` facade runs no
SQL.

- **Files.** Each statement is `crates/<crate>/sql/<group>/<name>.sql` (`bench/sql/` for the bench), loaded
  with `include_str!`. A file starts with a `--` header: what the statement does, its `$n` parameters and why
  it is shaped that way. The header is dropped when the statement is rendered, so comments never reach the
  server. A statement with variants is a list of fragment files joined by newlines (`append/head.sql` with
  `append/commit.sql` or `append/commit_outbox.sql`).
- **Template.** `pges_postgres::sql::template` renders `{name}` placeholders from typed values only:
  `StoreName`, `CategoryName`, `CategoryId`, `Ident` (for `IdentPlaceholder`s: leaves, log partitions,
  tables, parents, indexes, bounds, databases, template databases), `Literal` (`{from}`, `{to}`) and
  `SnapshotName`. Every other value is a bind parameter. `Statement::render` is infallible and leaves an
  unfilled placeholder in place; `Statement::sql()` returns the text of a one-file statement without
  placeholders without allocating. Statements are rendered once per store (`StoreSql` at `Store::open`,
  `AdminSql` once per store); those naming a leaf or log partition when it is handled.
- **Registry.** Each crate has a `#[doc(hidden)] pub mod sql` with `sql::statement`: one `Statement` constant
  per statement (name, files, `Kind::Query` or `Kind::Ddl`, parameter types, result columns) and `ALL`. A
  query is declared with `pges_postgres::typed_query!`, which also generates a module with `Query`, `Params`
  and `Row`; the constant's parameter types and columns come from those Rust types, and the query is listed in
  `TYPED`. DDL constants are written by hand and have no parameters or columns. Call sites hold a
  `typed::Rendered<Query>` and run it with `.bind(Params { .. })` and a fetch; a statement whose fragments
  repeat in code (the outbox recheck) registers one composition and is built with `Rendered::compose` from the
  same files. The module supports the pges crates and is not a stable API. Other crates reuse pges-postgres's
  statements (pges-testing's fixtures and the bench render the partition DDL) and get the validated store name
  from `pges_postgres::sql::store_name`.
- **Describe test.** `pges_testing::statement` checks a registry: `check_files` (embedded text matches disk,
  no unused file, headers present, unique names, no placeholder left after rendering with every value set),
  `check_typed` (each query's Rust parameter and column types against what PostgreSQL infers, and again as
  sqlx runs it with the Rust types, plus nullability), `run_ddl`, `check_no_inline_sql` (no literal, `format!`
  or `const` SQL passed to sqlx in `src/`) and `check_typed_calls` (no untyped bind or read in `src/`). Each
  crate's `tests/statement.rs` runs them: every query checked on a real store, every query listed in `TYPED`
  (`every_query_is_typed`), every DDL statement run in a real scenario, and the set of DDL that ran compared
  with the registry's.

## Targets (v1, 2026-11-25)

Per PostgreSQL cluster with one sequencer; beyond that, split stores across clusters. Reasoned from the design
and the dev-box benchmarks; not yet measured on production hardware, and no larger test machine is available,
so they are checked on the dev box with a durable configuration and extrapolated.

| Measure | v1 target | Stretch | Limit | Dev box, durable (2026-11-26) |
|---|---|---|---|---|
| Appends | 20k commits/s sustained, 40k peak | 50k+ | WAL flush, the `streams` update per commit | Miss on this box, flush-bound: many-single, 1M streams, concurrency 64: 12.1k/s, p99 8.8 ms (2026-11-25: 9.8k/s) |
| Events | 100k/s sustained | 250k+ | events per commit; batched events cost about 180 B of WAL against 670 B single | Met with multi-event commits: 10-event commits at concurrency 64, 118.6k ev/s; `append_many` of 10, 75k ev/s |
| One hot stream | about 1k appends/s | none | the stream's row lock and one flush per commit; a property, not a tuning target | Met at 1 writer: 708/s. Much better at 4–16 writers: 4 writers 622/s (2 min) and 552/s (rerun, was 330); 16 writers 247/s (10 min, was 133) and 414/s (rerun, 2 min, was 198) |
| Sequencer | 50k commits/s | 100k | one single-threaded walsender per cluster | Met: drains a 500k backlog at 201k/s |
| Commit to sequenced, p99 | under 50 ms at the sustained rate | under 20 ms | WAL flush stalls | Met at 8000 ev/s offered: p50 8.1 ms, p99 21 ms |
| Commit to subscriber woken, p99 | under 100 ms | under 50 ms | sequencer lag plus one gRPC hop and a read | Met (rerun at load about 5): 1 subscription p99 7.0 ms in-process, 8.6 ms gRPC; 200 subscriptions on 20 connections 74.8 ms and 67.1 ms. On a busier box 97–117 ms at 200 (earlier: 92 ms and 109 ms at 1 subscription, 4000 ev/s) |
| Retained events | 10 billion per cluster | 50 billion+ | storage; retention drops whole leaves | Not measured at 10B; 590M events / 230M commits measured (Scale run); 470 B per single event, about 162 B in 10-event commits; extrapolated 2.6–4.7 TB of data |
| Streams | 100M+ total, 100k+ writing at once | | `streams` is hash-partitioned | Met at 100M on the dev box (60 s runs, Scale run): appends at concurrency 64 indistinguishable from 1M (11–18k against 11–13k/s, interleaved); load 19.8k/s at concurrency 16. Conditions: `stream_partitions` 64; retention SQL since rewritten (O(leaf streams), 1474 s → 42 s for 22 leaves at 10M), not rerun at 100M |
| Subscriptions per store | 200+ | 1000 | read load scales with subscriptions × event rate; replicas take it | Met for throughput: 200 subscriptions kept up, 0 `Recovering`, the 20-connection pool saturated |
| Catch-up per subscription | 100k events/s | 200k | log page, then commits by id | Met at the default `batch_size` 1024: 182k ev/s (63k at 256); category catch-up 111k ev/s at 1002 leaves with batch 256 (rerun) |

The last column is from one shared dev box with a consumer NVMe (fdatasync 1.3–2.4 ms), 10-minute runs unless
noted; details and caveats in the two Durable dev-box runs, below.

Commit to subscriber woken (`pges-bench wake`, 1000 ev/s, 16 writers, 20 s runs, 20 subscriber
connections): the gap between the gRPC and the in-process waker is within run-to-run noise. At 200
subscriptions the lag comes from the subscriber pool (20 of 20 connections in use) and the checkpoint commit
(with its fsync) after each batch, for both wakers, not from gRPC. Profiling found no gRPC delay to remove:
TCP_NODELAY is set on both sides, about 30 B messages never fill the 64 KiB HTTP/2 window, the waker does not
debounce, and one HTTP/2 connection shared per endpoint made no difference. The result depends on the box's
load: a rerun at load about 5 gave the figures above, interleaved runs at load about
8 gave p99 97–117 ms in-process and 53–117 ms gRPC at 200. On 200 subscriber connections, so a pool that is not
saturated, an earlier run gave p99 42.9 ms in-process and 24.8 ms gRPC (the in-process waker wakes all 200 at
once). Size the subscriber pool for the subscriptions it serves.

Likely first bottlenecks: the sequencer's walsender, WAL volume (replicas, backups, full-page images after
checkpoints), vacuum on `streams` and `subscriptions`. On the dev box the first limit hit was the WAL flush.
`append_many`'s cost was unpruned planning, since fixed; the stale cached plan once blamed on store
creation does not show on a fresh store (Durable dev-box run, 2026-11-26).

## Spike results (2026-11-24, PG 18.7)

`crates/pges-testing/tests/spike.rs` runs the migrations and the statements in schema.md against PG18 (Docker and
the Nix shell). All pass. Confirmed: virtual `int8range` and `uuid_extract_timestamp` columns on
LIST → RANGE partitioned tables; detached create, CHECK, attach of category and time partitions;
`pges.uuidv7_floor` bounds; every append outcome in the classification table including idempotent `Any`
retries, wrong category, out-of-order and skewed commit ids (PGE01); stream reads from the category partition;
lease acquisition on a fresh database; the fenced log batch; the horizon guard in the category read; outbox
drain in `seq` order when commits finish out of order; the publication column list (no payload) with
`publish_via_partition_root`.

Found and fixed in the docs:

- Slot and origin names are server-wide. A fixed `pges_sequencer` collides when two pges databases share a
  server; they are now `pges_<database oid>` (schema.md, Slot and origin names).
- PG18 pgoutput sends Relation messages for the leaf partition as well as the root. The Insert is published
  under the root's relation id, so the decoder routes by relation id.
- Decoding only sees flushed WAL. Writers with `synchronous_commit = off` add up to `wal_writer_delay`
  (200 ms by default) to sequencing latency.

Replication client spike (`crates/pges-replication/tests`, PG 18.7):

- pgoutput v4 with `streaming = off` works end to end through `pges-replication`. The leaf partition's
  Relation arrives together with the root's, and the Insert carries the root's oid.
- A restart after `confirm(Commit.end_lsn)` does not resend that transaction. For logical replication,
  XLogData `wal_start` and `wal_end` are the same LSN (the record's), so only `Commit.end_lsn` is a safe
  point to confirm. The sequencer token is the `end_lsn` of the last Commit in a batch.
- A slot's `confirmed_flush` only advances when the client confirms. If the publication sees no traffic but
  the database writes other WAL, the slot holds that WAL back. `Keepalive.wal_end` is safe to confirm once
  everything received before it is durable and no Begin is waiting for its Commit; the logical source does
  that on idle keepalives.
- The walsender disconnects after `wal_sender_timeout` without a status update. `next()` answers keepalives
  that ask for a reply, but a sequencer busy with one batch for longer must call `send_status` in between.
  Built as `Source::keep_alive`: the sequencer runs it next to each batch's database work, and the logical
  source sends a status update every `status_interval` (10 s). Tested with
  `ALTER DATABASE … SET wal_sender_timeout = '1s'`, which logical walsenders honour.
- After `stop()` (CopyDone) the slot is released, and the same connection can run `DROP_REPLICATION_SLOT`.
- A FATAL error (for example a terminated walsender, 57P01) is followed by the server closing the socket,
  not by ReadyForQuery; the client reports the server's error.
- pg_hba matches a logical replication connection (`replication=database`) against the database name, not
  the `replication` keyword.
- Tuple values for `commits` arrive in text format in publication column order (`last_version`,
  `commit_id`, `stream_id`, `category_id`, `event_count`). The sequencer looks columns up by name through
  `pgoutput::Relations`, not by position.
- `postgres-protocol` 0.6.12 does not parse CopyBothResponse ('W'); the crate frames it itself. It also
  brings newer RustCrypto crates next to sqlx's older ones; `cargo deny` only warns about the duplicates.

### Benchmarks (2026-11-25, pges-bench, dev box)

`pges-bench` (`bench/`) measures spikes 1, 2 (throughput and lag), 3 (catch-up and load) and 5 (the NOTIFY
lock). These numbers are a dev-box baseline, not targets. The box was a Ryzen 9 9900X (24 CPUs, 91 GiB RAM)
shared with other workloads, at load average 11–25, so expect noise of ±30% or more.

The pg-up server used for the reruns runs PG 18.7 with `fsync`, `full_page_writes` and server
`synchronous_commit` off, `shared_buffers` 128MB, `wal_writer_delay` 200ms and `io_method` worker. The
closed-loop durable findings came from a private PG 18.7 cluster with the same settings, except `fsync`,
`synchronous_commit` and `full_page_writes` on.

Sequencer on pg-up (exact lag by commit id): writers with `synchronous_commit` on, 2000 ev/s
target, 8 writers, 100k backlog, 15 s.

| source | payload B | drain ev/s | sequenced ev/s | lag p50 ms | lag p99 ms | lag p99.9 ms | lag max ms |
|---|---|---|---|---|---|---|---|
| logical | 100 | 198579 | 1995 | 1.45 | 5.03 | 64.64 | 79.94 |
| outbox | 100 | 189489 | 1996 | 0.24 | 1.37 | 2.68 | 11.70 |
| logical | 4000 | 147830 | 1995 | 1.33 | 4.66 | 58.08 | 72.00 |
| outbox | 4000 | 131924 | 1996 | 2.02 | 5.27 | 6.72 | 9.44 |

Every run appended 1999 ev/s and reported no `Recovering`.

Append on pg-up: concurrency 16, 10 s after a 1 s warm-up, CHECKPOINT first, 100 B payload, batch
10, 100k streams.

| scenario | calls/s | events/s | p50 ms | p99 ms | p99.9 ms | WAL B/event |
|---|---|---|---|---|---|---|
| hot-single | 973 | 973 | 15.25 | 29.81 | 35.04 | 775 |
| hot-batch | 728 | 7278 | 22.03 | 31.76 | 38.17 | 182 |
| many-single | 34390 | 34390 | 0.41 | 1.12 | 2.72 | 670 |
| many-batch | 1111 | 11108 | 14.27 | 20.88 | 25.97 | 670 |

Closed-loop findings on the durable cluster:

- The NOTIFY commit lock is real on fsync'd storage. Outbox appends plateau at about 0.6–1.3k/s whatever the
  concurrency, while logical reaches about 20k/s at concurrency 64 (outbox 4% of logical, p99 104 ms). On
  `fsync=off` servers the lock barely shows. The 20k/s was a short run at 100k streams, with no timed
  checkpoint in it. The 10-minute run at 1M streams (2026-11-25) sustained 9.8k/s at concurrency 64, and a
  15 s rerun at 100k streams gave 14.7k/s (Durable dev-box run, below).
- WAL per event and the full-page-image share, right after a CHECKPOINT (concurrency 16, 100 B payload):

  | scenario | WAL B/event | FPI share |
  |---|---|---|
  | hot-single | 852 | 8% |
  | hot-batch | 272 | 31% |
  | many-single | 1628 | 58% |
  | many-batch | 761 | 12% |

  Without full-page images (pg-up, above) the same scenarios write 775, 182, 670 and 670 B per event. Full-page
  images dominate the WAL for single appends spread over 100k streams right after a checkpoint. Averaged over
  10 minutes the share is much lower (Durable dev-box run, below). FPI shares from runs shorter than a checkpoint cycle
  are not meaningful either way: creating a scratch database forces a checkpoint, and the prefill then writes
  each page's full-page image before measurement starts.

The first durable sequencer runs, and the first pg-up sequencer lag table, used order-based lag matching and
pacing that dropped missed calls. Their lag figures are approximate and are not quoted here; rerun them on a
durable server with the exact-lag bench before quoting sequencer lag on fsync'd storage.

### Durable dev-box run (2026-11-25)

`pges-bench server start` runs a private durable PostgreSQL 18.7 container (127.0.0.1:54330, data in a
Docker volume, `--replica none|async|sync`, `--set name=value`). Settings: `fsync`, `synchronous_commit` and
`full_page_writes` on, `wal_level` logical, `wal_compression` lz4, `shared_buffers` 8GB,
`effective_cache_size` 32GB, `max_wal_size` 16GB, `min_wal_size` 2GB, `checkpoint_timeout` 5min,
`checkpoint_completion_target` 0.9, `wal_buffers` 64MB, `max_locks_per_transaction` 256, `random_page_cost`
1.1, `io_workers` 8, `autovacuum_vacuum_cost_limit` 2000; the full list is in `bench/README.md`. No replica
in the measured runs. Same box as above (Ryzen 9 9900X, 24 threads, 91 GiB), with a consumer NVMe without
power-loss protection under LVM, shared with other workloads at load average 20–55. Short A/B runs varied by up
to 2x; treat every figure as ±30%. `pg_test_fsync` in the container: fdatasync 1.3 ms (about 770 flushes/s);
a later rerun under load measured 2.4 ms (about 408/s). The flush is the key limit on this box.

Against the v1 targets (10-minute runs unless noted):

| Measure | Measured |
|---|---|
| Appends, single event, 1M streams | 9.8k/s at concurrency 64 (p99 17 ms, p99.9 109 ms); 14.7k/s at concurrency 256 (3 min, p99 71 ms) |
| Events | 124k ev/s with 10-event commits to random streams (12.4k commits/s, p99 10 ms); 63k ev/s with `append_many` of 10 streams (p99 43 ms) |
| One hot stream | 961/s with 1 writer; 300/s with 4 writers (1 min); 152/s with 16 writers |
| Sequencer throughput | draining a 500k backlog of single-event commits: 171k/s (decode of already-written WAL) |
| Commit to sequenced | logical source, 8000 ev/s offered (about 80% of what the box sustains), 128 open-loop writers: p50 7.6 ms, p99 26.5 ms, p99.9 64 ms, max 513 ms; no `Recovering`; slot lag at most 25 MB |
| Catch-up per subscription | 2M events, monthly leaves: 54k ev/s at `batch_size` 256 (before the `LATERAL` read), 172k ev/s at 1024 |

- Appends are flush-bound: 50–64 of 64 backends were sampled waiting on WAL write or flush, while WAL
  bandwidth was only 7–58 MB/s.
- The hot stream collapses under concurrent writers because of the retry convoy: at 16 writers, 3306 of 3314
  appends took the retry path (6620 statements, a second `FOR UPDATE` wait of 47 ms on average). The fix
  (Decided, Crate-generated commit ids) raised it from 246 to 508 appends/s in an experiment; it was built
  later (Durable dev-box run, 2026-11-26, below).
- WAL per event and full-page-image share, averaged over 10 minutes including timed checkpoints:

  | scenario | WAL B/event | FPI share |
  |---|---|---|
  | many-single | 706 | 4.9% |
  | many-batch (`append_many`) | 735 | 5.8% |
  | many-events (10-event commits) | 178 | 3.5% |
  | hot-single | 766 | 1% |

- Timed checkpoints wrote 0.15–0.7M buffers over about 270 s; FPIs peaked at 1.5–5k/s just after a
  checkpoint started, and throughput dipped by about 10–25%. `many-batch` (46–58 MB/s of WAL) triggered
  WAL-based checkpoints about every 3 minutes despite `max_wal_size` 16GB (Deployment, guardrails).
- Autovacuum ran insert-driven vacuums of the `commits` leaf and `log_0000` every minute (fast, no index
  scans); `streams` leaves were vacuumed only at the start. No vacuum stalls.
- Reruns (15 s, 100k streams, load average 5–25): many-single 1,048 calls/s at concurrency 1 (p99
  1.3 ms), 7,993 at 16 and 14,724 at 64 (p99 11 ms); `append_many` of 10 at 64: 7,401 calls/s (74k ev/s);
  10-event commits at 64: 168k ev/s (p99 9 ms). WAL per event matched: about 670 B single, 170–183 B for
  multi-event commits.
- Bigger hardware: a server NVMe with power-loss protection flushes in about 20–50 µs instead of 1.3 ms,
  which should lift the commit rate per writer, the hot stream and the 20k commits/s target. It would not
  change WAL bytes per event, the FPI share, the hot-stream retry convoy, per-leaf planning, locking and
  plan-cache costs, the single-threaded walsender, or the flush-plus-batching floor of commit-to-sequenced
  lag.

**`append_many` cost.** The earlier "about 10x a single append" was first put down to a stale cached plan:
the bench's prefill ran `append_many` on the same pooled connections while the tables were empty, and a
generic plan with a sequential scan and hash join over all 64 `streams` partitions was seen (about 6 ms per
batch at 100k streams). The bench now runs `ANALYZE` after the prefill (`--statistics autovacuum` skips
it). A later A/B found no such trap since the `bumped` fix below (Durable dev-box run, 2026-11-26), so the library
does not `ANALYZE` at store creation. The cause that held was in the SQL: the `bumped` update did not prune, so custom plans planned it over every partition (0.8–1.7 ms of planning per
call against 0.4 ms of execution). The extra `stream_id = ANY($1)` filter (schema.md, Several streams) cuts
planning to 0.36 ms. On a `fsync=off` server at 100k streams, median of 3 runs of 10 s:

| concurrency | before (calls/s) | ANALYZE only | both fixes | events/s | p99 |
|---|---|---|---|---|---|
| 1 | 371 | 361 | 760 | 7.6k | 2.5 ms |
| 16 | 3213 | 3381 | 7019 | 70k | 4.9 ms |

A batch of 10 now costs about 7x one single append, so it is cheaper per event. On durable storage the flush
dominates (1.7 ms for a batch of 10 against 1.2 ms single, concurrency 1). An A/B on the durable
server at concurrency 16 (40 s runs) confirmed the cause at a smaller effect: planning 1.39–1.50 ms per call
with the old SQL, 0.52–0.65 ms with the new; 4,586–5,009 against 5,591 calls/s. At concurrency 1 both forms
often settle on a generic plan and the difference is lost in noise.

### Durable dev-box run (2026-11-26)

The same durable server settings as the 2026-11-25 run (above), on private containers that were removed
afterwards. The box was shared: load average 6–34 during the first runs (16–28 in the 10-minute hot-stream
runs, 17–33 in the wake runs) and 8–14 during the reruns. Treat every figure as ±30%; other test suites
overlapped some hot-stream runs, probably including a "before" run. Runs are 10 minutes unless noted. The
Targets table above has the verdicts.

| Measure | Measured |
|---|---|
| Appends, single event, 1M streams | 12.1k/s at concurrency 64, p99 8.8 ms, p99.9 15 ms |
| Events | 118.6k ev/s with 10-event commits at concurrency 64 (p99 10 ms); 75k ev/s with `append_many` of 10 (p99 16 ms) |
| Sequencer throughput | draining a 500k backlog: 201k/s |
| Commit to sequenced | 8000 ev/s offered, 128 writers: p50 8.1 ms, p99 21 ms, p99.9 54 ms, max 229 ms; no `Recovering` |
| Commit to subscriber woken | 1 subscription at 4000 ev/s: in-process p50 7.9 ms, p99 92 ms; gRPC p50 8.5 ms, p99 109 ms, p99.9 489 ms |
| 200 subscriptions | 1000 ev/s, a 20-connection pool, 5 min: every subscription kept up (200k deliveries/s), 0 `Recovering`; in-process p50 57 ms, p99 198 ms; gRPC p50 59 ms, p99 102 ms; the pool saturated (20 of 20 in use) |
| Catch-up per subscription | 2M events, monthly leaves: 63k ev/s at `batch_size` 256, 182k at 1024 (`LATERAL`: 77k and 141k) |

- The wake lag (`pges-bench wake`) runs from an append returning to the handler receiving the event, so it
  includes sequencing, the wake-up and the read. With 200 subscriptions the latency is bound by the
  20-connection pool, not the waker.
- Hot stream (hot-single, 1000 streams), appends/s before and after the retry id:

  | writers | run | before | after | p99 | load |
  |---|---|---|---|---|---|
  | 1 | 2 min | 591 | 708 | | 7–20 |
  | 4 | 2 min | 250 | 622 | 43 → 17 ms | 7–20 |
  | 16 | 2 min | 344 | 443 | | 7–20 |
  | 16 | 10 min | 133 | 247 | 762 → 295 ms | 16–28 |
  | 4 (rerun, 60-bit step) | 2 min | 330 | 552 | 21.0 → 12.6 ms | 9.8–13.8 |
  | 16 (rerun, 60-bit step) | 2 min | 198 | 414 | 282 → 114 ms | 7.9–9.5 |

  The reruns ran the old and the new binary back to back and confirm the direction and size.
- Reads as leaves grow: Spike 3, below. Rerun at 1002 leaves: two-step against `LATERAL`, 3.02M
  against 378k ev/s with 16 readers.
- Plan cache after store creation (many-batch, concurrency 16, 100k streams, 3 rounds of 20 s, load 11–32):

  | variant | calls/s | median |
  |---|---|---|
  | no `ANALYZE` | 5031 / 3164 / 3094 | 3164 |
  | `ANALYZE` after the prefill | 5654 / 2863 / 2683 | 2863 |
  | `ANALYZE` on the empty store before the prefill | 5206 / 3179 / 602 | 3179 |
  | `force_custom_plan` server-wide | 5598 / 2796 / 2140 | 2796 |

  Statement time in `pg_stat_statements` was the same with and without `ANALYZE`, and auto mode used generic
  plans for about 60% of calls in every variant, with no 6 ms plan. A rerun confirmed it on a fresh store
  without `ANALYZE` (20 s, load 9.5): the `append_many` statement averaged 0.591 ms per call (max 9.8 ms)
  over 114,631 calls, at 5,450 calls/s. The 602 calls/s collapse was not rerun.

### Scale run (100M streams, 590M events, 2026-11-26)

`pges-bench scale` (bench/README.md, Scale) on a private server like the 2026-11-25 run's, removed afterwards;
it peaked at 181 GB of disk. Runs were 30–60 s on a shared box (load average 2–47), so every figure is ±30%.
A rerun repeated retention and the partition count at 10M and 1M streams. The Targets table above has the
verdicts: 100M streams met for appends, loads and catch-up, on condition that `stream_partitions` stays at
64 and the retention SQL is fixed (since done, measured at 10M); 10B retained events not measured,
extrapolated from 590M.

**Fill.** `scale fill --streams 100000000` uses the crate's DDL (`Admin::register_category`, the leaf
statements, `Admin::maintain`, then `ANALYZE`) and generates commits, log rows and streams in SQL. It took
3072 s. Unlike real appends: multi-row inserts of 500k rows from 8 workers; `streams` rows inserted once and
never updated; log rows in commit id order rather than by a sequencer; about 57k XIDs for 230M commits;
one identical payload, so dump sizes mean nothing; the commits primary key at about 55% leaf density where
appends would give about 90%.

| Category | Leaves | Streams | Commits | Events |
|---|---|---|---|---|
| orders | 24 monthly with data | 60M | 150M (1 event each) | 150M |
| audit | 24 monthly with data | 20M | 40M (10 events each) | 400M |
| jobs | 60 daily | 20M | 40M (1 event each) | 40M |
| **Total** | | **100M** | **230M** | **590M** |

`stream_partitions` 64, 100 B per event; the log holds 590M positions in 6 partitions. Comparisons: a 1M-stream
store filled the same way (interleaved A/B rounds against it), and streams-only stores at 64 and 256
partitions.

| Measure | 100M streams, 590M events | 1M streams, same tool |
|---|---|---|
| Appends, single event, existing streams | 368 / 2,653 / 10,855 per s at concurrency 1 / 16 / 64 (p99 13.7 ms at 64); interleaved at 64: 17.8k and 11.5k | interleaved at 64: 12.9k and 10.7k |
| Appends, new streams | 315 / 3,108 / 13,122 per s; interleaved at 64: 13.1k and 13.5k | interleaved at 64: 13.0k and 11.1k |
| Load (random stream), concurrency 1 | `orders` 642/s (p50 1.62 ms, p99 2.49 ms), `audit` 1,333/s, `jobs` 1,266/s | 5,556/s, p50 0.17 ms |
| Load, concurrency 16 | `orders` 19.8k/s, `audit` 19.0k/s, `jobs` 17.8k/s | |
| Category catch-up (batch 1024) | `orders` 160k ev/s from half the log, 294k from the start; `audit` 1.30M; `jobs` 263k | `orders` 255k |
| Store-wide catch-up | 573k ev/s from half the log, 531k from the start | 570k |
| Resequence after bootstrap halted (`CommitsBeforeSlot`) | 1.98M commits recovered in 4.8 s | |
| Sequencer drain, 500k backlog | 118k commits/s (not comparable; below) | |

- Appends show no measurable degradation from 1M to 100M; the WAL flush dominates. Buffer hit during
  appends: database 98.2%, `streams` heap 94.4%, `streams` index 97.9%.
- Loads are I/O-bound: `commits_orders` heap hit was 10.8% (shared_buffers 8 GB against a 178 GB
  database). They are a worst case of the fill's shape: every `orders` stream lives about 19 months and
  touches 22–24 leaves. An `orders` read probes the `_s` index of every leaf from the stream's first commit
  on (3 levels each, about 72 cold reads). Custom plan: 2.2 ms planning (2,014 buffer hits) against 0.36 ms
  execution over 27 leaves; generic plan 0.4 ms and 0.14 ms. The head lookup prunes to one hash partition
  (0.04 ms planning, 4 buffers). `read/stream.sql` had no upper commit-id bound; one was added since, which
  helps short-lived streams, not these (schema.md, Read one stream).
- Catch-up shows no degradation from 1M.
- Resequence's search is bounded by the store's window, so it stays fast at 230M commits. The drain figure
  is probably an artifact: the slot was created just before resequence wrote 1.98M log rows, and the 4.2 s
  drain also decoded that WAL. It is not evidence that the sequencer slows at scale; to be re-measured on a
  slot without resequence WAL (Targets keeps the 2026-11-26 run's 201k/s).

**Retention** (`jobs`, keep 7 days, 675k commits per daily leaf at 100M; 67k at 10M in the rerun):

| Statement | 100M streams | 10M streams (rerun) |
|---|---|---|
| `retention/live_streams.sql` | 196 s (merge join over all 64 `streams` primary keys) | 0.15–0.49 s (nested loop with per-probe pruning) |
| `retention/raise_floors.sql` | 36–45 s (hash join over a sequential scan of all streams) | 1.3–2.6 s; 1.37 s with a nested loop forced |
| `retention/scan.sql` | 1.2 s (hash plan) | 65–82 s (nested-loop anti-join), 3 leaves |
| detach and drop | about 0 | |

- About 4 minutes per leaf at 100M with the plans the run got; the run was stopped after 2 of about 23
  eligible leaves (about 1.5 h in all). The refusal path (an `orders` leaf with live streams) took 32 ms.
- All three statements are a problem at scale. `raise_floors` is O(all streams) per leaf. `live_streams`
  and `scan.sql` are planned optimistically: their success case returns no row, so `LIMIT` and EXISTS never
  stop early. If `scan.sql`'s plan flipped at 100M, one leaf would take about 4–5 h (675k × about 730k
  comparisons).
- Measured rewrites: `live_streams` as a `LATERAL` per-stream probe, 196 s → 5.8 s; `raise_floors` as a
  nested loop, 43 s → 11 s; `scan.sql`'s `sequenced` as a full count, 76 s → 15 ms (parallel hash anti-join).
  Schema.md, Partition maintenance, Retention, has the detail; all three have since been rewritten.

**Storage as filled** (bytes per row):

| Table | Heap | Indexes |
|---|---|---|
| `orders` commit | 204.9 | primary key 65.6 (55% density, from the fill; about 40 at 90%); `(stream_id, last_version)` 52.1 (70%) |
| `audit` commit (10 events) | 1,366 (136.6 per event) | 103.5 (10.4 per event) |
| log row | 60.3 | primary key 22.5 (90%); `log_by_category` 90.1 (55%) |
| stream | 143.8 | primary key 42.5 (66%) |

- About 178 GB of relations; the database was 181 GB, with WAL at 16–37 GB during the fill.
- `log_by_category` at 55% is real: the sequencer inserts in the same category-interleaved order.
- B-tree levels: `streams` primary keys 3 (1.56M rows per partition), the `orders` leaf primary key 4, the
  `_s` index 3, `log_by_category` 4, the log primary key 3. The rerun reproduced the sizes at 10M.

**Vacuum and freeze.** After the fill commit leaves were 99.7–100% all-frozen, `streams` 96.8% all-visible
but 58% frozen, log partitions 82–95% frozen and `log_0001` 14%. `VACUUM (FREEZE)` of an all-frozen 1.2 GB
leaf: 7 ms, 1 page, 2 KB of WAL; of the unfrozen 1.98 GB `log_0001`: 4.3 s, 219k pages, 916 MB of WAL. After
4.7M updates `streams` was 67% HOT with 2.24M dead tuples and no autovacuum (default scale factor 0.2).

**Partition count** (streams-only stores, concurrency 64):
- Sizes are the same at 64 and 256 partitions (heap 143.8 against 144.0 B per stream, primary key 42.6
  against 43.1), both 3 levels.
- Relation locks per append, new or existing stream: 134–135 at 64 partitions (all fast-path), 518 at 256
  (256 fast-path). Identical with `force_custom_plan`: the MATERIALIZED input CTE prevents pruning.
  Forcing custom plans made throughput worse (1.3k/s).
- Throughput at 100M streams: 64 partitions 9.9k / 12.0k / 9.4k / 12.6k per s; 256 partitions 5.0k / 3.7k /
  5.9k / 6.0k per s, p50 10–16 ms, 38% of wait samples on `LWLock:LockManager`, execution 0.75 ms against
  0.28 ms per append. The rerun at 1M streams, interleaved: 64 partitions 16.1k and 18.3k/s, 256 partitions
  5.4k (p99 50 ms) and 6.0k/s. So 256 partitions cost about 3x at any store size.
- A higher `max_locks_per_transaction` was not tested.
- The single append now prunes to its stream's partition: 9 locks at 64 and 256 partitions, and 256
  partitions now run as fast as 64 (12.5–14.7k/s at 1M streams; schema.md, Append (one stream)).

**Extrapolation to 10B events.** Assumptions: 100 B payloads, storage linear in rows, the commits primary key
at 90% density (about 40 B), the log as the sequencer writes it.

| Commit shape | Per event | Data at 10B events |
|---|---|---|
| all single-event | about 470 B (heap 205, commit indexes 92, log 173; about 435 B with full log partitions reindexed) | 4.7 TB, of which the log about 1.7 TB |
| this run's mix | about 263 B | 2.6 TB |
| all 10-event | about 162 B | 1.6 TB |

- Plus about 19 GB of `streams` at 100M streams. A base backup is about the same size; WAL archive is about
  700 B per single event written, about 7 TB per 10B single events; a restore at 500 MB/s takes about
  1.5–2.6 h.
- Log: about 100 partitions of 1e8 positions. Leaves: categories × retained buckets; 10B over 24 months in
  one single-event category is about 417M commits per monthly leaf, about 124 GB of heap and indexes, most
  likely still 4 index levels.
- What grows: stream loads (planning and probes grow with the category's leaves since the stream's first
  commit) and retention per leaf (O(all streams) with the SQL of that run; since O(leaf streams)). Catch-up and appends showed no growth.
- Freeze: insert-triggered autovacuum with `autovacuum_freeze_min_age = 0` freezes leaves while they are
  written, so anti-wraparound vacuum of an all-frozen leaf costs milliseconds. An unfrozen leaf costs a full
  read and about half its size in WAL, terabytes at 10B. At 20k commits/s, 200M XIDs pass about every 2.8 h,
  so anti-wraparound passes are frequent; the heavy one is `streams`, about 14 GB scanned per pass at 100M.
  The fill did not reproduce real XID ages, so this part is extrapolated.

**Sizing note: 10B events, 100M streams, one cluster.**
- Disk: 2.6–4.7 TB of data depending on events per commit, plus 30% headroom, WAL (`max_wal_size` 16–64 GB)
  and the slot limit (50 GB): about 5–8 TB of NVMe with power-loss protection, plus archive storage (about
  7 TB of WAL per 10B single events).
- RAM: 128 GB with shared_buffers of 32 GB. The append hot set is `streams` (about 19 GB) plus the current
  leaves' `_s` indexes (up to about 22 GB by month end at 417M commits a month) plus the right edges of the
  log indexes. Loads of old streams read from disk.
- Partitions: `streams` 64 (about 2.9 GB of heap and primary key per partition at 1B streams); not
  128–256 while `append_many` locks each partition it touches. Leaves in monthly buckets, at most about
  100–200 per category. Log about 100 partitions of 1e8 positions.

### Spike 3: reads as leaves grow (2026-11-25)

`pges-bench partitions`: 1M commits on daily leaves, 16 concurrent readers. Rows are filled with SQL, not
through appends and the sequencer. The bench renders the crate's own leaf DDL and read statements; only
the old one-statement LATERAL read, kept for comparison, is a bench-only file.

| measure | 12 leaves | 102 leaves | 1002 leaves |
|---|---|---|---|
| Category catch-up, join form (ev/s) | 29k | 25.5k | 1.8k |
| Category catch-up, `LATERAL` (ev/s) | 87k | 75k | 43k |
| Execution per 256-row page, join | 4 ms | 7–16 ms | 185–205 ms |
| Execution per page, `LATERAL` | 0.44 ms | 0.67 ms | 5.3 ms |
| 16 readers, join (ev/s) | 1.13M | 377k | 9.7k |
| 16 readers, `LATERAL` (ev/s) | 2.56M | 786k | 323k |
| Custom-plan planning | 0.15 ms | 0.74 ms | 20.5 ms |
| Plan-cache memory per statement per connection | 96 KiB | 544 KiB | 4.1 MiB |
| Relation locks per execution (at most 256 fast-path) | 27 | 207 | 2007 |
| Readers waiting on the LockManager lock | 0% | 0.8% | 12.2% |

2026-11-26 rows (the two-step read; 1M events, 16 readers, 10 s concurrent phase, batch 256, load 6–34):

| measure | 12 leaves | 102 leaves | 1002 leaves |
|---|---|---|---|
| Category catch-up, `LATERAL` → two-step (ev/s) | 104k → 73k | 29k → 162k | 27k → 91k |
| Store-wide catch-up, before → after (ev/s) | 126k → 71k | 59k → 146k | 115k → 104k |
| 16 readers, `LATERAL` vs two-step, same run (ev/s) | 2.15M vs 2.06M | 0.79M vs 2.36M | 237k vs 1.71M |
| LockManager waits, `LATERAL` vs two-step | | 0.6% vs 0% | 27.8% vs 0% |
| Plan-cache memory per connection, fetch in auto mode | 72 KiB | 520 KiB | 4116 KiB |

- Per page at 1002 leaves: `LATERAL` took 21–36 ms of custom planning, 5.4–9 ms of execution and 2007–3018
  locks. Two-step: the log page 0.04 ms planning and 0.05 ms execution, then the fetch 0.43 ms planning and
  0.09 ms execution, with 5 + 6 locks in auto mode.
- At 12 leaves two-step catch-up read lower (73k against 104k): some of it noise, some the second round trip
  per page.
- Rerun at 1002 leaves (load 9.2–11.6): category catch-up 110.9k ev/s, store-wide 114.6k ev/s. Log
  page 0.04 ms plan + 0.04 ms exec, 5 locks, 32 KiB of plan cache. Fetch with a custom plan 0.21 ms plan +
  0.04 ms exec, 6 locks. Fetch with a generic plan 9.09 ms exec and 2003 locks. `LATERAL` with a custom plan
  18.9 ms plan + 5.0 ms exec, 3018 locks. 16 readers: 3.02M against 378k ev/s, p99 2.1 against 13.7 ms,
  LockManager waits 0% against 7.9%.
- Plan-cache memory does not go away. In auto mode PostgreSQL still builds and keeps the fetch's generic
  plan to compare costs with: about 4 MiB per connection at 1000 leaves, as `LATERAL` did. The store-wide
  read already had this. Auto mode picks custom plans for the fetch, but if it ever chose the generic plan,
  each page would cost about 9 ms and 2003 locks again. Sending the fetch as an unnamed statement (sqlx
  `persistent(false)`) would always plan custom and cache nothing, for one more round trip per page; not
  done yet.

- The join was pathological: the planner costs each nested-loop probe as a scan of every leaf (it ignores
  run-time pruning), so it picked a parallel hash join over the whole category for every page. The
  `LATERAL` lookup with `LIMIT 1` fixes it (Decided; schema.md, Read a category from a cursor).
- Generic planning stays at 0.26 ms, and the plan cache uses the generic plan. What still grew with leaves:
  executor startup, about 2 locks per leaf and plan-cache memory. The two-step read (a log page, then
  `commit_id = ANY(page)`, as the store-wide read does) removed the first two (above).
- Store-wide catch-up: 102–129k ev/s. Its fetch (`commit_id = ANY($1)`) uses custom plans that prune to the
  touched leaves (0.04–0.08 ms, 6 locks); the generic plan would take 15 ms and 2003 locks at 1000 leaves,
  but auto mode picks custom.
- A rerun (300k events, 8 s, 16 readers) matched within noise: at 102 / 1002 leaves, category
  catch-up 90k / 48k ev/s, store-wide 162k / 170k ev/s, LockManager waits with `LATERAL` 0% / 7.2%.

### Crash correctness (spike 2, 2026-11-25)

`crates/pges-sequencer/tests/crash.rs` kills the sequencer at an exact point of its batch loop under 4
concurrent writers, with the outbox and the logical source: before the log commit, between the commit and
the ack, and between the ack and the next batch. A restart or a standby that was already running takes
over. The log stays gap-free, no commit appears twice, every commit is sequenced once, and the log keeps
commit order per category and per store for appends that did not overlap. After a kill before the log commit
with no standby, the killed batch's commits are absent from the log and, for the outbox, still pending.
`tests/bin.rs` SIGKILLs three binaries in turn mid-load (each after 30 or more new log rows), for each
source, and a fourth drains the rest. No bug was found.

- `crash.rs` takes about 12 s; 60 property cases (`PROPTEST_CASES=30`) passed in 64 s. The SIGKILL tests take
  about 4–5 s.
- Mutations, each reverted: turning off the dedupe window fails the logical before-commit cases and the
  logical property test; acking the source before the log commit fails 3 logical tests with missing commits;
  draining the outbox outside the batch transaction fails the outbox before-commit cases (in 0.9 s, through
  the rolled-back check).
- Not covered: overlapping appends in the order check. Promoting a physical standby (failover slots) is
  covered by `tests/promotion.rs`.

### Slot loss under load (spike 7, 2026-11-25)

`crates/pges-sequencer/tests/invalidation.rs` invalidates a live slot with `max_slot_wal_keep_size = 1MB`
while four writers append through the invalidation, the halt, `resequence` and the restart. The log stays
gap-free, no commit is sequenced twice or missed, versions stay in order per stream, the recovered block is
in commit id order, and each writer's later appends sit after it. A sample run: 17 commits sequenced before
the halt, 43 recovered, 335 after.

## Decided

- Name: `pges` (crates `pges`, `pges-*`; shared SQL schema `pges`).
- `sqlx` for v1 (widely used; appends are one statement, so pipelining matters less). Revisit
  `tokio-postgres` if append benchmarks show round trips dominate.
- DCB is not in v1. The schema (`opt_dcb.sql`) stays reserved, guarded by `pges.stores.dcb`. It is not a
  public option either (Compile-time checks, below).
- Sequencer token default: coarse, written about once a second, with dedupe against the log tail
  (schema.md, option b). Replication origins (option a) stay available where the grants exist.
- Build order: `pges-core`, `pges-postgres`, `pges-sequencer` with the outbox source, `pges-subscribe`; a
  `pges-replication` spike alongside, then the logical source becomes the default.
- Upcast chains are strict: `upcast(..)` must cover versions 1..version-1, so an old version can never
  silently decode as unknown. To stop supporting an old version, its upcaster returns an error. Upcasters
  work on the codec's value representation (`serde_json::Value` for json). Unknown names and versions above
  the current one decode to `Decoded::Unknown`.
- `#[event(codec = ...)]` stays optional and defaults to `"json"`. Per-version codecs
  (`codec(1 = "json")`) are future work, needed once a second codec exists.
- Unit variants are supported and stored as `{}`.
- The derive finds the path to `pges-core` from the calling crate's Cargo.toml (proc-macro-crate): the
  facade first, under any name (`::<name>::core`), then `pges-core` under any name, then `::pges_core`.
  Dev-dependencies count too. `#[event(crate = "...")]` overrides it, as serde's does; the path names
  `pges-core`'s root. Set it by hand for a re-export, or when the facade is a dev-only or optional
  dependency. pges-core's own `Itself` maps to `::pges_core` rather than `crate`, so doc examples work
  (2026-11-25); if pges-core ever uses the derive itself, add `extern crate self as pges_core;`.
  The facade's own tests depend on `pges-core` under another name (`pges-core-dep`), so they prove the
  facade path alone is enough; renaming it back would silently weaken them. (2026-11-25)
- `Lsn` lives in `pges_core::lsn` (it has no IO); `pges-replication`, `HighWater::lsn()` and
  the sequencer token use it.
- TLS in `pges-replication` uses rustls with the `ring` provider, not `aws-lc-rs` (which needs cmake and nasm
  to build) and not OpenSSL. `sslmode` follows libpq: `prefer` by default with the `tls` feature, `disable`
  without it; a mode that needs TLS without the feature is a configuration error. Unix sockets never use TLS.
  (2026-11-24)
- TLS details: `tls` is not a default feature of `pges-replication`; the sequencer's
  `source-logical` feature turns it on. `SslMode` keeps all variants whatever the features. `prefer` and
  `allow` retry the other transport only on a TLS handshake or certificate error, or SQLSTATE 28000, so a
  wrong password is not sent twice. Roots: the `sslrootcert` file, else the platform store through
  rustls-native-certs. A missing `sslrootcert` file is an error in every TLS mode (stricter than libpq).
  Server certificates must have SANs (rustls ignores the CN). `sslnegotiation=direct` is supported. Client
  certificates are not supported yet. (2026-11-24)
- The first `pges-sequencer` cut has the outbox source and publishes `HighWater` in process through a
  `tokio::sync::watch` channel. The logical source and the NOTIFY notifier followed next, and
  the gRPC watch server later, on the same `Source` trait (gRPC watch, below). (2026-11-24)
- `pges-replication` is the replication client; the published `pgwire-replication` alternative is dropped.
- TLS for `pges-replication`, with SCRAM-SHA-256-PLUS channel binding, was required before the logical source
  could become the default. It is done (above).
- `pges-replication`'s `Config` getters use tokio-postgres's `get_*` scheme: `get_host`, `get_port`,
  `get_user`, `get_database`, `get_application_name` and the TLS getters. The password has no public getter.
  (2026-11-24)
- `SnapshotPolicy::Manual`: `snapshot_after` writes a snapshot on every call, because the caller asked for it.
- No `dcb` cargo feature until DCB appends exist; `Store::open` refuses DCB stores.
- The option scripts (outbox, dcb) get their own versioned scopes (`<store>.outbox`, `<store>.dcb`) when they
  first need a migration. Until then the `pges.stores` flags record them.
- `append_many` locks streams in a deterministic order without relying on the planner: a locking step
  `SELECT … FROM streams WHERE stream_id = ANY(…) ORDER BY stream_id FOR UPDATE` inside the statement
  (LockRows above Sort locks in sorted order) (schema.md, Append).
- `Admin::register_category` must not expose a category before its partitions exist: the row and the
  partitions are created in one transaction.
- Store hardening (2026-11-24):
  - `Admin::change_bucket(name, bucket)` changes a category's bucket, from the upper bound of the newest leaf
    (read from the partition bounds); a `Day`-to-`Month` change gets one bridging leaf named `YYYYMMDD`.
    `register_category` never changes an existing bucket, so two services that disagree cannot flip it.
  - Leaf index names end in `_s`, so every generated name fits 63 bytes for a 40-character category with a
    `Day` bucket (schema.md, Naming).
  - `UnitOfWork::commit` checks with `SELECT 1` (25P02 means aborted), because sqlx does not expose the
    transaction status. Owned units of work pay that round trip only after `conn()` was used; wrapped ones
    always do.
  - A unit of work from `Store::begin` on a pool of size 1 that must register an event type fails with
    `AppendError::NoSpareConnection` instead of waiting for the acquire timeout.
- Crate-generated commit ids under contention: on falling behind the stream's last commit id, the crate
  locks the stream row (`SELECT … FOR UPDATE`; an autocommit append opens a transaction for this) and picks
  the new id under the lock, so it cannot fall behind again. At most 5 new ids, and one rerun after a creation
  race, with separate budgets. Caller-supplied ids still fail with `CommitIdOutOfOrder`. A retry loop without
  the lock was tried and failed: appends that arrive later can overtake one waiting for the row lock. The
  new id is not computed in the append statement, because the crate must know the id it sent to deduplicate
  a retry after a lost response. `append_many` in its own transaction takes the retry lock with SKIP LOCKED
  (and a 1 ms `lock_timeout`, see below) and restarts the batch if another transaction holds it, so a retry
  never waits for a lock out of stream id order. (2026-11-25)
  - Hot-stream retry id (2026-11-26): when `last_commit_id` is less than `min(1 s, max_clock_skew / 2)` behind a fresh UUIDv7, or
    ahead of it, the new id is a random step of 1 to 2^60 above it, in its millisecond; otherwise it is the
    greater of a fresh UUIDv7 and that id. A fresh id jumps past every queued waiter's id, so each waiter
    fails its recheck and retries. Half the skew keeps the retry path inside the skew check for a writer in
    sync; a writer more than skew minus the window behind the database (4 s at defaults) can now get
    `CommitIdSkew` on a retry. The step is random because two appends can pick an id above the same last
    id (a SKIP LOCKED retry, an `append_many` batch that started over, a lost commit reply), and the same id
    would report the second as `Deduplicated` without writing it; +1 lost appends in a test every run. It
    takes 64 random bits from two fresh UUIDv7s, since only their low 32 bits are random. The skew check,
    the orphan search window, retention's scan bounds and resequence order rest on the server-side skew
    check and on ids increasing per stream, which still hold (schema.md, Append (one stream)). Measured: 16 writers
    133 → 247 appends/s over 10 minutes; at 16 writers about 60–73% of appends still retry, because queued
    ids generated in the last id's millisecond fall below the new one.
  - Late retries (2026-11-27): the skew check skips an id at or below the stream's `last_commit_id`, so a
    retry of a stored commit returns no row and is classified `Deduplicated` in every mode. Before, a unit
    of work retried later than `max_clock_skew` got `CommitIdSkew` and an aborted transaction. A savepoint
    around each append in a transaction was the alternative; it costs a round trip and sqlx's savepoint
    handling is unsafe (Decided, sqlx 0.9.1). Nothing at or below `last_commit_id` is ever written, so every
    stored id still passed the check. Cost: one more probe of the partition's key, already in cache for the
    update, and two more relation locks (9, 11 with the outbox), the partition and its key in shared mode.
    Checking only in the rows `created` and `bumped` write would read nothing more, but the function would
    then run only for appends that apply, and a test counts the statement's runs by its calls.
- Outbox wake-ups: the outbox source does not poll. Writers NOTIFY `pges__outbox` (payload: the store's
  schema name) from a statement-level trigger on `pending`, only when the statement enqueued rows. The
  source LISTENs on a connection of its own: before its first drain, and again, followed by a drain, after
  losing it. `recheck_interval` (1 s) is only a fallback. The recheck runs on the listening connection, so
  a silently dead listener is replaced within about `recheck_interval` + 1 s. `ready` keeps its in-flight
  connect and probe deadline across the sequencer's lease-renewal cancels. Cost: NOTIFY's server-wide
  commit lock serialises notifying commits. The outbox is the fallback for databases without
  `wal_level = logical`; the logical source stays the default and the scale option. (2026-11-25;
  replaces polling)
- `Source` is `ready()` + `next(tx, limit)` + `ack(token)` + `open(token)` + `kind()`: the outbox's DELETE
  must commit in the batch transaction, so `next` runs inside it and `ready` waits outside it. The logical
  source buffers whole transactions in `ready()` and ignores `tx`. (2026-11-24)
- `HighWater`, `StoreId` and `CategoryId` live in `pges-core`, so subscribers and wakers do not depend on the
  sequencer. `pges-postgres` adopts the newtypes later. (2026-11-24)
- No `source-outbox` cargo feature: the outbox source has no heavy dependency, so it is always built.
  (2026-11-24)
- SQL type conversion happens in the crate that owns the statement (`pges-postgres`, `pges-sequencer`), never
  in `pges-core`. (2026-11-24)
- `Sequencer::run` takes a shutdown future, not tokio-util's `CancellationToken`. A graceful shutdown releases
  the lease; after a crash a standby waits for expiry. (2026-11-24)
- Database errors while leading or standing by are retried every `retry_interval` and surface as
  `Status::Recovering`, on the grounds that a missing log partition or grant can be fixed while it runs. Only
  `Halted`, `SourceMismatch`, fatal or invalid source errors, unknown or invalid stores and position overflow
  end `run()`. (2026-11-24)
- One sequencer per database (`pges.sequencer` is a single row), and one kind of source per database.
  (2026-11-24)
- One source kind per database is enforced in SQL: a trigger on `pges.stores` (SQLSTATE `PGE02`,
  `MigrateError::OutboxMismatch`), so the rule holds for every writer of `pges.stores`, not only the runner.
  The logical source still skips outbox stores defensively. A composite source remains possible later.
  (2026-11-25)
- `HighWater::lsn()` uses `pg_current_wal_insert_lsn()`: `pg_current_wal_lsn()` can lag the batch's own commit
  with `synchronous_commit = off`. (2026-11-24)
- The MSRV build and `cargo deny` run in CI. `cargo deny` also runs locally under `nix develop`.
- Migrations were edited in place until the first publish (0.1.0), because nothing was released.
  `pges/0001_init.sql` gained the one-source-kind trigger this way, and the store's `streams` autovacuum
  settings were folded into store migration 0001 (2026-11-26). Databases built from an older text fail with
  `ChecksumMismatch` and must be recreated. Since 0.1.0 the 0001 migrations are frozen: every schema change
  is a new migration. (2026-11-25)
- Logical source (2026-11-25):
  - Slot `pges_<database oid>` (or `LogicalConfig::slot`), publication `pges`. Both are created if missing,
    and the publication is repaired: `ALTER PUBLICATION` adds missing non-outbox stores, with a warning.
  - Token = the end LSN of the batch's last transaction, 8 bytes big-endian; empty means none.
  - `Source::keep_alive` stays a trait hook whose default never resolves, rather than a background task
    owning the stream.
  - Slot loss is a typed `logical::SlotLost` boxed in `SourceError::Halt`, for now. `SourceError::Halt` is a
    new variant for failures that must persist `pges.sequencer.halted`.
  - `SlotLost::CommitsBeforeSlot` is `Halt`, recorded in `halted`, and resequence recovers it (it was
    `Fatal` at first, which left those commits unsequenceable forever).
  - `LogicalConfig` defaults: `status_interval` 10 s, `read_ahead` 1000 commits, `start_timeout` 30 s,
    `failover` `SlotFailover::Keep` (`Disabled` at first).
  - The token transaction commits with `synchronous_commit = on`, so the slot is never confirmed past log
    rows a server crash could lose.
  - `Status::Leading` is announced after the source opened and the stores were loaded.
- NOTIFY: on by default when `watch-notify` is compiled; channel `pges_<store schema name>`; payload
  `1;store;lsn;id:pos,...`, with `*` for the categories when it would exceed 7999 bytes; sent concurrently
  with sequencing on another connection, merged per store; a notify failure never fails the log write.
  (2026-11-25)
- Binary `pges-sequencer` (feature `bin`): flags with `PGES_*` environment variables; the logical source is
  the default when compiled. (2026-11-25)
- `pges-subscribe` (2026-11-25):
  - The entry point is the `subscription::StoreExt` extension trait (`pges-postgres`'s API was kept apart from
    it); through the facade it is `pges::subscribe::subscription::StoreExt`.
  - Default feature `wake-poll` until `wake-grpc` exists; then the default switches to `wake-grpc`. The
    switch is done (gRPC watch, below).
  - Without a waker, a plain recheck timer (500 ms) is the documented fallback, not an error.
  - Wakers treat `known.categories()` as the watched set; an empty list means the whole store.
  - `Wakers::any` returns the first waker that fires, not the highest value seen.
  - `Batch::decode` returns `batch::DecodeError`, which `OnUnknown::Fail` needs; `?` in a handler makes it
    `HandlerError::Fatal`.
  - A handler that leaves its transaction aborted is `Transient`: the failed statement may have been a
    serialization failure.
  - Fatal errors release the lease, so a standby takes over at once (and probably meets the same error).
  - `Start` applies only when the row is created and on redefine. Empty advances (a cut with no events) are
    checkpointed in autocommit without calling the handler.
- Sequencer polish (2026-11-25):
  - `Status::Halted(Arc<str>)` is set for every error `run()` returns: persisted halts, `RunError::Halted`
    at acquire, `SourceMismatch`, fatal and invalid source errors, unknown or invalid stores, position
    overflow. It carries the error's text, not the error: `RunError` cannot be cloned (it holds boxed source
    errors), and `run` returns it by value. The typed error is `run`'s result. A shutdown still gives
    `Stopped`.
  - Slot bootstrap as built: `LogicalSource::bootstrap` and the subcommand `pges-sequencer bootstrap` (not a
    `--create-slot` flag) create the publication and the slot without a lease. It is idempotent: an existing
    valid slot is kept; an invalidated slot or a missing slot with a stored token halts without recording
    `halted`; a database whose stores all use the outbox is `Invalid`. Run it after the migrations and
    before writers start. The sequencer still creates a missing slot itself on open, with the same check.
  - The slot is created with `SNAPSHOT 'export'`, and the stores are checked for commits inside that
    snapshot, which sees exactly what the slot will not stream. This closes the first-start window: commits
    found there drop the slot and give `Halt(CommitsBeforeSlot)` (`Fatal` at first); none is skipped
    silently. If the drop
    fails too, the error names the slot for an operator. `SNAPSHOT 'use'` was not used: it needs a
    transaction on the replication connection, and a sqlx connection cannot import it.
  - Residual first-start window: a crash, shutdown or lease loss that drops `open` between
    `CREATE_REPLICATION_SLOT` and the end of the snapshot check leaves a persisted slot that was never
    checked; later opens stream from it. Bootstrapping the slot before writers start avoids this. Running
    bootstrap while a sequencer creates the slot can also leave an unchecked slot (schema.md, Slot
    bootstrap). Since 2026-11-27 such a slot has no token, and a later open halts if the stores hold
    commits (Slot ahead of the token, below).
  - `Source::close` is a trait hook with a no-op default. It cannot fail and is awaited before the lease is
    released. It is called whenever a term ends (lease lost, shutdown, fatal error), not on a recoverable
    error inside a term: the next `open` already resets the source and ends the previous walsender.
  - `open` is not bounded by the lease. The sequencer renews the lease while `open` runs, and drops it on
    shutdown or a lost lease; `start_timeout` stays 30 s.
  - `pges-replication` gains `create_slot_with`, `SlotSnapshot`, `CreatedSlot.snapshot` and
    `name::SnapshotName`, as additions; `create_slot` is unchanged.
- Subscriptions, leases and wakers (2026-11-25):
  - The lease is renewed on a side connection (autocommit, the store's pool) while a handler runs and while
    a failed batch backs off, once less than half of it is left.
  - A handler whose side renewal found the fence moved runs to completion; it is not cancelled, because a
    cancelled handler could leave its own state or an external sink half-written. Then `run_pg` rolls back
    and `run` skips its checkpoint. The cost: a deposed `run_pg` handler can hold read-model row locks until
    it returns.
  - A failed side renewal (a database error, not a moved fence) is retried after lease/8 and is not shown as
    `Recovering`: the handler is still working, and a real database problem shows up at the checkpoint.
  - If the lease was lost during a handler call that also failed, `LostLease` wins over the handler error,
    fatal or transient: the deposed instance stands by, and the new holder meets the error itself.
  - A shutdown during a retry backoff returns at once and leaves the batch uncheckpointed, so it is
    delivered again on the next start. Shutdown is checked before a lost lease, because the shutdown future
    is fused.
  - `NotifyWaker` fires on every (re)connect, including the first: one extra log read at start, and no
    wake-up can be missed between the subscriber's read and the LISTEN. It fires once after a payload it
    cannot read.
  - An overflow notice (`*`) raises every watched category to that notice's store position in the waker's
    kept state, so it wakes only a `known` below that position.
  - `NotifyWaker::with_separate_pool` exists so LISTEN can use a pool that bypasses a transaction-mode
    pooler; its connection does not count against the store's pool. `new` counts it.
  - `NotifyWaker` does not detect a half-open connection by itself, and sqlx's `PgListener` (0.8 and
    0.9.1) has no keepalive option.
  - Idle backstop (2026-11-27): every subscription reads the log after `max_idle` (default 30 s) without a
    wake-up, whatever its waker; each shard on its own. A waker that stays quiet without failing (a
    `NotifyWaker` on a half-open connection, or a custom one) otherwise stalled the subscription forever
    while it kept renewing the lease, so no standby took over. Before, only documentation pointed to
    `Wakers::any` with a poll waker. A read per 30 s is negligible beside the lease renewal every 7.5 s.
    Treating each lease renewal as a wake-up was the alternative; it ties the delivery bound to the lease
    and could not be tuned on its own.
- Compatibility after start (2026-11-27): `min_compatible` was checked only by `Store::open` and the
  migration runner, so a subscription or sequencer of a release a later migration stopped supporting kept
  running. Lease renewals now carry the check (a `NOT EXISTS` on `pges.scopes` in the renewal statement),
  and a refused renewal is followed by one read of `pges.scopes` that turns it into a fatal `CrateTooOld`
  and releases the lease. The renewal is the one statement every lease holder runs on a timer, so the
  check needs no extra round trip, and an old holder cannot keep its lease, so a newer standby takes over.
  Alternatives: a separate query per renewal (a round trip each), or recording each holder's crate version
  in its lease row so a contracting migration refuses to run while old holders exist (a schema change, and
  it blocks the migration instead of stopping the old process). Appends and reads hold no lease and stay
  checked at open only; the rolling-upgrade rule (one release at a time) covers them.
  Taking a lease carries the same check (2026-11-27), so an old standby cannot take a released lease and
  work until its first renewal. The acquire statement always returns one row with a `too_old` flag, so a
  held lease and a too-old crate are told apart in the same round trip; a separate `crate_too_old` read
  follows only a refusal, for the error's details. A too-old standby stops even when the lease is held: it
  could never lead. The alternative, an extra read of `pges.scopes` after every acquire that returned no
  row, costs a round trip on each standby poll.
- `pges` facade (2026-11-25):
  - Re-exports only, each crate as an inlined module (`pges::core`, `pges::derive`, `pges::postgres`,
    `pges::subscribe`, `pges::sequencer`, `pges::replication`), plus the derive at the root; the facade is the only crate that
    re-exports. The derive is `pges::Event` (`pges::derive::Event` also works), with no `crate`
    attribute needed. No prelude.
  - Default features `derive`, `subscribe`, `wake-poll`. `bin` and `json` are not mirrored. The sequencer
    features changed later (Small polish, below), and the default waker became `wake-grpc` (gRPC watch,
    below).
  - No top-level `pges::Error` for now: it would be a new type beyond re-exports and would have to track
    every crate's error enums.
- Licence (2026-11-25): `MIT OR Apache-2.0`, the Rust ecosystem's usual pair, copyright "The pges
  Authors", so no single person or company is named. `LICENSE-MIT` and `LICENSE-APACHE` sit at the root;
  each crate links to them so they are packaged.
- The docs state no write-rate limit for the outbox source (2026-11-25). The benchmark numbers stay in
  Spike results; user docs describe the NOTIFY commit lock and recommend the logical source for scale.
- `bootstrap` without a failover flag keeps an existing slot's setting; `--failover` and `--no-failover` set
  it explicitly, and a new slot is created without failover unless asked (2026-11-25). Silently
  turning failover off on a re-run is the worse surprise. Built as
  `logical::SlotFailover` (`Keep`, the default, or `Set(Failover)`).
- READMEs and examples (2026-11-25):
  - The workspace and each crate have a `README.md`, compiled as doctests through a hidden
    `#[cfg(doctest)]` item in `lib.rs`.
  - The examples are a workspace member outside `crates/`: `examples/` (`pges-examples`,
    `publish = false`). `tests/run.rs` includes each example file as a module and calls its `run()`, so
    `cargo test` keeps them working.
- `UnitOfWork::commit` returns `Aborted` in its Failed branch even when the rollback fails, and discards
  the rollback's error: `TransactionError::Aborted` carries no source. (2026-11-25)
- `append_many` in its own transaction cannot deadlock against other batches. Waiting happens in only two
  places: `locked` (row locks, taken in stream id order before the statement writes anything) and
  `created`'s insert. The insert waits only for a transaction that inserted the same stream, or that updated
  it in its `bumped` step; `ON CONFLICT` ignores lock-only row locks, because HeapTupleSatisfiesDirty skips
  a lock-only xmax. A batch runs `created` before `bumped` (`head` is `created UNION ALL bumped`), and
  nothing after `bumped` waits: the `commits` insert conflicts only with the stream's own creator or locker.
  So a transaction that has already bumped a stream never waits on another batch. Inserts run in stream id
  order after every `locked` step, and retry locks do not wait, so no wait cycle can form.
  `batches_creating_the_same_streams_never_deadlock` (16 writers, two plan shapes, single appends making
  streams new for one snapshot and existing for another) passes; the same test deadlocks within seconds
  when the pooled retry waits instead of skipping. Inside a unit of work the retry waits, and the
  documented retryable deadlock remains. (2026-11-25)
  - The retry lock runs with `lock_timeout` at 1 ms (2026-11-27). SKIP LOCKED did not keep it from waiting:
    when the updater of the row version the statement found commits between PostgreSQL's visibility check and
    its conditional wait, `heap_lock_tuple` follows the update chain with `heap_lock_updated_tuple`, which
    waits for the holder of a newer version whatever the wait policy (PostgreSQL 18.7; NOWAIT is the same).
    A batch holding its own streams then waited for a batch locking the retried stream and one of its
    streams in id order: a deadlock, seen once in CI in the test above. In plain SQL, pgbench running
    autocommit updates of one row, transactions locking it and a second row in order, and transactions
    holding the second row while locking the first with SKIP LOCKED (24 clients, 30 s) hit 68 deadlocks in
    about 400,000 transactions.
    `append::tests::the_retry_lock_of_a_batch_never_waits` hammers that shape with the crate's statement; the
    race is narrow, so without the timeout it fails about half its runs. A timeout aborts the transaction,
    which the batch rolls back to start over anyway. Alternatives: three round trips (set, lock, restore),
    or not locking a stream the batch does not hold and always starting over, which needs the batch
    statement to report the streams it locked.
- Migration versions are `pges_postgres::migrate::SchemaVersion`, owned by pges-postgres, not pges-core,
  because only the migration runner uses them (2026-11-25).
- Small polish (2026-11-25):
  - The facade's `sequencer` feature turns on the logical source, matching `pges-sequencer`'s default;
    `sequencer-outbox` is the outbox-only sequencer. `source-logical` turns on `replication` in the facade,
    rather than `pges-sequencer` re-exporting the connection `Config`, because only the facade
    re-exports.
  - Subscriptions check the store's pool at start: `run_pg` needs 2 connections, `run` 1, plus
    `Waker::store_connections` (1 for `NotifyWaker::new`, 0 for `with_separate_pool`; `Wakers` sums). Fewer
    fails with `SubscribeError::PoolTooSmall` (fatal). The check is a lower bound by design. The sizing rule
    is documented on `Builder` and in the README: appends' and reads' concurrency, plus per subscription 2 for
    `run_pg` or 1 for `run`, plus its waker's connections.
  - `NotifyWaker` reconnect backoff: 100 ms doubling to 5 s after a failed listen or a connection lost within
    5 s of opening; reset by a connection that stayed up 5 s; the deadline survives a dropped call.
  - `pges-testing`: a `TestDb` dropped without `cleanup` drops its database and slots on a thread with its
    own runtime; the first `TestDb::new` per process sweeps `t_*` databases older than 1 h. `cleanup` stays
    strict about leftover slots.
  - `pges-sequencer` binary: `--failover` sets `LogicalConfig.failover` to `SlotFailover::Set(Enabled)` and
    `--no-failover` to `Set(Disabled)`; with neither it is `Keep` (at first, `--failover[=on|off]`
    with default off). The flags exclude each other. `PGES_FAILOVER` takes `on` or `off`; the binary reads
    it itself, not through clap, so either flag overrides it. A run applies the setting only to a slot it
    creates. `bootstrap` also sets an existing slot to it with `ALTER_REPLICATION_SLOT` (PostgreSQL 17+). It refuses while the slot is active, checked again once the replication connection is
    open, because the server waits for an active slot instead of failing.
- gRPC watch (2026-11-25):
  - The protocol lives in its own crate, `pges-watch-proto`, not in `pges-core`. `pges-core` does no IO, and
    the generated client and server are tonic transport code. A separate crate lets the sequencer and
    `pges-subscribe` share the types without `pges-subscribe` depending on `pges-sequencer`. Other languages
    use the shipped proto file.
  - The generated code is checked in, with a test that generates it again and fails on any difference
    (`PGES_REGENERATE=1` rewrites it), instead of a `build.rs`. Users' builds need no `protoc` and no extra
    build dependencies (protox, prost-build, prettyplease and petgraph stay dev-only), and build faster.
  - The proto is versioned (`pges.watch.v1`); a shape change is a new package. Adding a field or an enum
    value to `v1` under a new number is compatible, and a number is never reused. An older reader's `state()`
    getter returns `UNSPECIFIED` for a value it does not know. Every value carries the term's fence, and the
    client also fires once on a higher fence, as a defence against other server implementations.
  - A sequencer that is not leading (standing by, recovering outside a term, halted, stopped, abandoned)
    answers `UNAVAILABLE`, so a client with a list of endpoints finds the leader by trying each.
    `Recovering` inside a term keeps open streams.
  - An unknown store streams zeros, as the in-process `Watch` does. `NOT_FOUND` would wrongly refuse a new
    store that has no events yet.
  - `GrpcWaker` retries transient failures silently until `unreachable_after` (30 s) passes with no endpoint
    serving. While no leader is reachable nothing new is sequenced, so routine restarts and handovers should
    not flash `Recovering`. A real outage still surfaces, and the subscription then falls back to re-reading
    the log on its retry interval. Refusals that retrying cannot fix are reported at once (gRPC watch
    hardening, below).
  - `WatchService::serve` ends open streams itself on shutdown: tonic's graceful shutdown would otherwise
    wait for them forever.
  - The facade does not re-export `pges-watch-proto`: no user-facing API needs the proto types by name.
  - A sequencer whose `run` future is dropped (a panic, an aborted task) reports `Status::Abandoned`, so the
    gRPC watch and in-process watchers stop treating it as the leader. Before this the status stayed
    `Leading(fence)` and the gRPC server kept every client on a dead leader. A dropped run does not release
    its lease, so a standby takes over only once it expires. Alert on it. `Stopped` now means only that `run`
    returned after a shutdown. A dropped run gets its own status (2026-11-25). The name says what happens
    to the run (its term and lease are left behind), not how Rust ended the future. The proto value is `SEQUENCER_STATE_ABANDONED = 6`.
  - The binary awaits `run` in `main`, so a dropped run there means a panic: the process exits with Rust's
    status 101, and there is no new exit code. The drop guard that sets `Abandoned` runs only while a panic
    unwinds. The workspace's `panic = "deny"` is a clippy lint, not a profile setting, so panics unwind
    today. A profile with `panic = "abort"` would skip the guard: nothing would report `Abandoned`, and the
    process would die by SIGABRT instead of exiting 101 (not addressed yet).
  - `wake-grpc` replaces `wake-poll` as the default waker of `pges-subscribe` and `pges`
    (2026-11-25). The cost: `pges-subscribe`'s normal dependency tree grows from 132 to 155 crates (tonic,
    hyper, h2, tower and their helpers), and a clean debug build from 7.6 s to 9.4 s (release 11.0 s to
    12.5 s, on 24 cores). Code that uses `PollWaker` must now enable `wake-poll`.
  - TLS was a follow-up (gRPC watch hardening, below).
- gRPC watch hardening (2026-11-26):
  - TLS sits behind separate opt-in features, `watch-grpc-tls` (pges-sequencer) and `wake-grpc-tls`
    (pges-subscribe), mirrored by the facade, not inside `watch-grpc` or the default `wake-grpc`, so the
    default waker's build stays lean: without them the dependency tree is unchanged. Both use rustls with
    ring through tonic's `tls-ring` (no OpenSSL, no aws-lc-rs), like `pges-replication`. Mutual TLS is
    optional (`ServerTls::client_ca`). Certificates and keys are checked when `ServerTls` / `ClientTls` are
    built, because tonic accepts a PEM with no certificate. The client trusts the system's roots or one CA.
  - The watch's per-store channels are bounded by pruning, not by refusing unknown stores. A channel of a
    store no leader announced lasts only while someone holds a receiver; such channels are dropped the next
    time a new unannounced id is asked for. Refusing unknown store ids would break the rule that a new store
    with no events yet streams zeros (above). Channels are thus bounded by announced stores plus live
    receivers, and through gRPC by `max_streams`.
  - `WatchConfig::max_streams` defaults to 10 000, one semaphore per server across every connection. A
    gRPC waker holds one stream per subscription or shard instance, and the v1 targets are 200–1000
    subscriptions per store across several stores. One more stream gets `RESOURCE_EXHAUSTED`, which the
    waker treats as transient. There is no per-connection cap: it would not stop a client that opens many
    connections, and HTTP/2's `max_concurrent_streams` would make an over-limit client hang instead of
    seeing `RESOURCE_EXHAUSTED`. A per-client limit would need client identity; it is not built.
  - A stalled leader (sequencing hung on a database call, status still `Leading`) ends its gRPC streams with
    `UNAVAILABLE` once the term's lease deadline passes without renewal, and refuses new ones while it stays
    expired. The deadline is the local one, `sent + ttl`, taken before each renew query is sent and set at
    acquire (before the status turns `Leading`) and at every renew. It falls before the database lease
    expires, so streams end before a standby can take over. It stays crate-private on `Watch`; only the gRPC
    server uses it. Each stream keeps one timer and moves it on renewal. With defaults a healthy leader
    renews at 5 s against a 10 s deadline.
  - `GrpcWaker` sorts failures into two classes. Transient (`UNAVAILABLE`, `RESOURCE_EXHAUSTED`,
    `DEADLINE_EXCEEDED`, `INTERNAL`, timeouts, broken connections, TLS failures not about a certificate) are
    retried silently until `unreachable_after`. Refusals (`UNIMPLEMENTED`, `INVALID_ARGUMENT`,
    `PERMISSION_DENIED`, `UNAUTHENTICATED`, `FAILED_PRECONDITION`, and certificate failures in the TLS
    handshake) fail the call at once with `Rejected`, but only when no endpoint of the round served; a
    refusal another endpoint makes up for is only logged, so one wrong address in the list does not flash
    `Recovering` on every handover. A `Rejected` does not stop the waker: the next call retries after the
    backoff, so a brief glitch cannot become permanent. Under TLS 1.3 a missing client certificate, and a
    plain client against a TLS server, look like broken connections and cannot be told apart from network
    failures, so they stay transient and end as `Unreachable`.
  - A stream that sends a position above `Position::MAX` is a protocol error: it fails the call that
    received it with `Rejected` (`Rejection::Protocol`) instead of reconnecting in a silent loop. No
    sequencer sends one.
  - `--grpc-listen` takes a host name and port as well as a socket address. The name is resolved at start
    and the watch is served on every resolved address (deduplicated, `WatchService::serve_all`). An address
    the host does not have (`AddrNotAvailable`) is skipped with a warning, so `localhost` works on both
    stacks and in containers without IPv6; any other bind error, or no address bound, fails the start.
  - No shared HTTP/2 connection per endpoint in the waker: measured, it gave no gain worth its state.
- Subscription modes (2026-11-25):
  - Shard rows are keyed by `(name, shard)`, not by a name suffix: the name rule has no character that
    could not collide, and one statement can act on every shard of a name, which the atomic create and
    redefine need.
  - An unsharded subscription is one shard, row 0, and `.sharded(1)` is the same definition. One acquire
    statement (`shard < n`) serves both. Only `n > 1` uses the per-instance coordinator.
  - Shards are claimed automatically with a fair share, not assigned with `.shard(k)`. An instance with no
    shards is invisible in the shard rows, so `subscription_members` lets the others see it and give up
    shards.
  - One handler per instance, called by one shard at a time (the lock is taken before `BEGIN`, so no
    connection sits idle in a transaction). Requiring `Clone` or a factory would split `run` and `run_pg`.
    For parallel handlers, run more instances.
  - The shard hash is SHA-256 of the 16 id bytes, first 8 bytes big-endian, sign bit cleared, modulo `n`,
    computed in SQL. It is stable across PostgreSQL versions and platforms, unlike `hashtext` or
    `uuid_hash`, and mixes all 16 bytes, which matters for v1-style ids with a constant node. The filter
    runs inside the statement, so other shards' payloads are never sent or decoded. It is pinned by a test;
    changing it is a breaking change, since it would move streams between shards across a deploy.
  - Store-wide shards fetch other shards' commits with a NULL payload, so a missing row still means
    retention.
  - Create runs once per name (`NOT EXISTS`, no `ON CONFLICT`); shard 0 is in every definition, so two
    concurrent creates conflict as a whole.
  - The waker is shared through one fan-out task per instance, which keeps the level-triggered contract.
  - Backfill progress lives in a `backfills` table, one row per lane, not in `subscriptions`: lanes need
    several cursors under one fixed definition, and `subscriptions` would share the live subscriptions'
    namespace and lease model. Per-stream rows would mean millions of rows.
  - Backfill lanes use a compare-and-set on the lane cursor instead of a lease. Exactly once holds even if
    two runs overlap; the second stops with `Overtaken`.
  - A backfill reads one page at a time; the slowest lane gates the page, and order within a stream is kept.
    `P` is resolved once and stored; `UntilAhead` is checked before it is stored.
  - Backfills always read unsharded, from the primary. `Until::Subscription` on a sharded subscription is
    the lowest cursor of its live shards.
  - A replica reader with no LSN from its waker asks the primary for its insert LSN, one cheap round trip
    per wake, so a level-triggered waker cannot spin on a stale replica.
  - The replica wait lowers an insert-LSN target that sits right after a WAL page header to the page start
    (sizes read from the replica once), instead of waiting for unrelated WAL. Fixing it on the reading side
    covers both the waker's LSN and the subscription's own read of the primary, and leaves the sequencer
    and the wire format alone; no SQL function exposes a record end for the sequencer to announce.
  - A fallback permit is taken per page read, not per subscription, so it bounds load on the primary directly.
    Replica errors count as lag only with fallback on; otherwise they show as `Recovering`.
  - Every shard of a sharded subscription with a replica waits and falls back on its own, and all shards
    take permits from the same `FallbackLimit`.
- Sharding policy (2026-11-25):
  - Each instance keeps a fair share of at most `ceil(n / m)` shards; extra instances are warm standbys
    (4 shards over 3 instances is 2+2+0). Placement is predictable, with less rebalancing as instances
    come and go. To use every instance, pick `n` as a multiple of the instance count.
  - The pool check (`PoolTooSmall`) stays a lower bound and ignores shards; the docs give the per-shard
    connection rule. A strict check of `n` extra connections would reject instances that only ever hold a
    few shards.
  - A redefine ends the term (every shard row in one statement) and takes the lease again; a standby may
    take over in between. Accepted: harmless, and one code path for sharded and unsharded subscriptions.

- Retention and resequence (2026-11-25):
  - Resequence lives in pges-sequencer, as `LogicalSource::resequence` and `pges-sequencer resequence`, not
    `Admin::resequence` / `pges resequence`. It needs the replication connection, slot creation, the
    exported snapshot and the source's store routes; pges-postgres does not depend on pges-replication, and
    there is no `pges` binary.
  - `SlotLost::CommitsBeforeSlot` is `Halt`, not `Fatal`: as `Fatal`, those commits could never be
    sequenced. Resequence handles it with an empty-log floor, which scans every commit.
  - The recovery window is `max_txn_age + 2 × max_clock_skew`; Slot loss above was corrected from one skew. A
    commit X after the last sequenced commit L has `X_id ≥ X_stmt − skew ≥ L_commit − txn_age − skew ≥ L_id −
    txn_age − 2·skew`. Retention uses 2× for the same reason. `ResequenceConfig::lease` defaults to
    `Lease::DEFAULT` (10 s); the window comes from each store's recorded writer limits (below).
  - Resequence takes the lease with an acquire that succeeds only while `halted` is set and the lease is
    free, so both refusals come from one statement with no side effects, and renews it at half the TTL.
  - The orphans are read in a REPEATABLE READ transaction in the exported snapshot, and written in a
    separate READ COMMITTED transaction: lease renewals update `pges.sequencer` and would fail a write in the
    snapshot. The orphan set cannot change in between, because `halted` is set and the lease held.
  - One final transaction locks the fence row (`FOR UPDATE`), writes the log rows in chunks of 10 000,
    commits with `synchronous_commit = on`, stores the consistent point as the token, clears `halted` and
    releases the lease. Renewal stops just before it, so a late renewal cannot take the lease back.
  - The coarse marks (`pges.stores.last_position`, `categories.last_position`) are not written; the next
    term seeds its dedupe window with every recovered commit and persists them.
  - Any inactive slot of this database is dropped, valid or invalidated, so a failed run can be retried. The
    new slot keeps the old slot's failover unless `LogicalConfig::failover` is `Set`.
  - A failure before the final commit drops the new slot and releases the lease; on `LeaseLost` the slot is
    left alone, because it may belong to the resequence that took the lease over. A failed COMMIT is
    `CommitUnknown` and keeps the slot; running it again is safe.
  - `Admin::apply_retention(&RetentionConfig)` is separate from `maintain()`: it needs the writer roles
    (the margins come from the store), and returns a report of what it dropped and refused.
    `maintain()` keeps no arguments and stays non-destructive. The store default lives in `pges.stores` and
    is copied into a category when it is registered.
  - The writer-role check reads `pg_db_role_setting` in PostgreSQL's order (role in database, role,
    database, every role). A value set only in `postgresql.conf` cannot be read by a non-superuser, so it is
    refused (`TransactionTimeoutNotSet`).
  - Window refuses a leaf while a stream with commits in it is not deleted and had a commit within `keep`
    (Retention, above: "never drop by time alone"). SnapshotAndTruncate needs a snapshot of any state
    version at or after each such stream's last version in the leaf.
  - Window raises `floor_version` too, not only SnapshotAndTruncate, so reads of a stream whose commits were
    dropped fail with `Truncated` instead of returning part of it. It costs one UPDATE per leaf.
  - Leaves go per category, oldest first, stopping at the first refusal: drop leftovers of an interrupted
    run, find the stop position with one index scan, scan, record floors and horizon in one transaction
    (after rechecking `halted`), detach concurrently (or `FINALIZE`), drop, and only then raise
    `retention_scan_from`. Raised before the drop, a run interrupted in between would see the leaf's commits
    as unsequenced and refuse the leaf forever, and `prune_log` could delete log rows of commits that still
    exist.
  - `prune_log` deletes up to `retention_scan_from`, not `horizon_position`: rows between the two can belong
    to commits of the next leaf, which still exists.
  - A log partition is dropped when it is not the newest, is full, and has no row above its own category's
    `retention_scan_from`; a category with no rows in it does not pin it.
  - `delete_stream` deletes the stream's commits and snapshot now and keeps the `$stream-deleted` tombstone
    commit: retention, which may be `Forever`, cannot be relied on to remove personal data. It locks the
    stream row first (a missing stream is `NotFound`), appends through the normal append path, and refuses
    archived streams. It is generic over `E`, like `read_stream::<E>`.
  - The schema.md sentence "`Admin` warns when a `Window` category is registered in a store that has a
    `Forever` one" is not built (the crate has no logging dependency); it is a follow-up.

- Writer limits, retention and resequence together (2026-11-25):
  - Each store records `max_clock_skew` and `max_txn_age` in `pges.stores`, set at creation (from
    `StoreDefinition`). Appends, retention and resequence read the same values, so they cannot
    disagree. The defaults live once, in `pges_core::store`, and a test checks they match the column defaults.
  - A writer wider than the store is refused, not auto-raised: `Store::open` fails with
    `ClockSkewAboveStore`. Raising is an explicit operator action, `Admin::set_writer_limits`, under the
    maintenance lock, so it cannot race a retention run between its scan and its drop. Raising on open would
    race it, and would also need writers to have `UPDATE` on `pges.stores`.
  - Limits only rise; `set_writer_limits` refuses a lower value with `Lowering`. Commits written under a
    larger value stay in the log, out of commit id order by up to that much, and nothing records where they
    are. After a lowering, a leaf could be refused as `NotSequenced` forever, or `retention_scan_from` could
    pass rows of commits that still exist, which a log partition drop would then delete and a later
    resequence sequence twice. Lowering is unsafe even after every wider writer has stopped. The cost: a
    limit raised by mistake, or for a history import, stays raised, and retention keeps the larger margin.
  - The `max_txn_age` and `max_clock_skew` overrides in `RetentionConfig` and `ResequenceConfig` (and the
    binary's `--max-txn-age`, `--max-clock-skew`) are removed. A smaller override is unsafe, and a larger
    one adds nothing a raised store value does not.
  - Resequence refuses when the writer-role check fails (`ResequenceError::WriterRole`), as retention does,
    and does not just warn. Its guarantee that no commit is missed or sequenced twice depends on the cap. The
    check runs before the lease, so a refusal has no side effects. `ResequenceConfig::new(writer_roles)`
    replaces `Default`, and the binary requires `--writer-role`.
  - The recovered range is recorded per category (`categories.resequenced_from`, `resequenced_through`).
    Retention's scan covers it whole and its next scan start ignores rows in it; resequence's search for the
    newest sequenced row ignores it too. The alternative, scanning to the end of the log once after a
    resequence, costs more and has no clear point to stop. A range retention has already passed is
    replaced, not widened.
  - Retention reads the range, the stop row and the scan in one REPEATABLE READ, READ ONLY snapshot, rather
    than rechecking the range in the horizon transaction. Otherwise a resequence committing during the run
    (after `halted` is cleared, so the halted recheck does not catch it) would leave the run with an
    out-of-date range, and the next scan start could land past rows of commits that stay. The snapshot
    closes this with no new refusal reason.
  - `prune_log` keeps a row whose commit still exists (`NOT EXISTS` against the category partition). Every
    row at or below `retention_scan_from` should name a dropped commit; the guard holds even if the limits
    were broken, as defence in depth.

- Deleted streams in store-wide reads (2026-11-25):
  - `delete_stream` records the ids of the commits it deletes in `deleted_commits`, keyed by
    (`category_id`, `commit_id`) like `commits`, in the same statement as the delete. Their log rows stay,
    and commits not yet sequenced are still sequenced after the delete, so the store-wide read needs the
    record to skip them and still report any other missing commit as `Corrupt`.
  - The log cannot identify them: `log` has no `stream_id`. Matching a missing commit to a deleted stream's
    commit id range would let other streams' commits in that range pass, and would scan every `streams`
    partition, which has no category index.
  - Deleting the log rows in `delete_stream` was rejected. It misses commits sequenced after the delete
    (the outbox drain and the logical source never consult `commits`), breaks gap-free positions, and
    deleting the newest row could let the sequencer reuse positions subscribers have passed.
  - The store-wide read looks up only the commits missing from a page, in one indexed statement on the
    primary, also for replica reads. Listed-category reads and backfills skip a missing commit unless the
    category's horizon passed the read (schema.md, Read a category from a cursor).
  - Retention cleans the table up when it drops a commits leaf (Correctness follow-ups, below). It must not happen in
    `prune_log`: a deleted commit may be sequenced later.

- Read paths and append cost (2026-11-25):
  - The category read looks each log row's commit up in a `LATERAL` subquery with `LIMIT 1`, not a join.
    Same rows: inside `commits_<category>` the commit id is unique, a log row without a commit is still
    dropped, and the shard filter and `LIMIT $4` still apply after the lookup. As a join the planner chose a
    hash join over the whole category per page once leaves grew (185 ms against 5 ms per page at 1000
    leaves). Later replaced by the two-step read (Performance follow-ups, below).
  - `append_many`'s `bumped` step repeats `stream_id = ANY($1)`, so plans prune `streams`' hash partitions.
    Semantics, lock order and the SKIP LOCKED restart are unchanged.
  - The default subscription `batch_size` stayed 256 then (54k against 172k ev/s at 1024 on the dev box);
    later raised to 1024 (below).
  - The hot-stream retry id: accepted then, built later (Crate-generated commit ids, above).
  - Crash tests inject faults with a wrapper around the real source plus a test-only trigger on the log,
    not a cfg or feature failpoint in `sequencer.rs`, so the published crate carries no test-only code.

- Performance follow-ups (2026-11-26; Durable dev-box run, 2026-11-26):
  - The category read is two statements: a page of the category's log (`read/category_log.sql`, with the
    horizon check), then its commits by `commit_id = ANY(page)` (`read/commits.sql`, or
    `read/commits_sharded.sql` for a shard), as the store-wide read does. Custom plans prune the fetch to the
    leaves the page touches, so planning, locks and execution no longer grow with leaves (Spike 3). The
    `LATERAL` statement is retired to the bench (`bench/sql/partitions/category_read_lateral.sql`). The single
    snapshot `LATERAL` gave is replaced by a re-check: a missing commit sends the client back to the
    category's horizon, which retention commits before it drops commits; a horizon above the read's floor is
    `CursorBeforeHorizon` (or a re-read in `Skip` mode), otherwise the row is skipped. The cut comes from the
    log rows; the shard filter applies after `LIMIT`, so a shard's page holds about `batch_size / shards`
    events (schema.md, Read a category from a cursor). A property test checks it against the `LATERAL`
    statement (testing.md).
  - Plan-cache memory is not solved: auto mode still keeps the fetch's generic plan, about 4 MiB per
    connection at 1000 leaves, and choosing it would cost about 9 ms and 2003 locks per page. Sending the
    fetch unnamed (sqlx `persistent(false)`) is a follow-up, not decided.
  - The default subscription `batch_size` is 1024 (catch-up 63k ev/s at 256, 182k at 1024). A subscription
    that keeps up gets small batches anyway; `run_pg` handler transactions can hold up to 4x more commits per
    category while catching up. Backfill's default stays 1000.
  - Store creation does not `ANALYZE`. A/B on a fresh store found no stale-plan trap since the
    `bumped` filter, and an `ANALYZE` of the empty tables gave no benefit (one run collapsed to 602 calls/s).
  - The hot-stream retry id is built (Crate-generated commit ids, above).

- SQL files and the statement registry (2026-11-25; SQL layout, above):
  - Variants are fragment files joined by newlines, so the Rust side is one `match` (on `Outbox`, for
    example). A fragment that continues a `WITH` list starts with `,`. SQL repeated per item, the outbox
    recheck over every outbox store, is built in code from fragments with `typed::Rendered::compose`; the
    registry holds the two-item shape so the registry test checks it, and a unit test ties the code to it.
    The sharded commit fetch is its own file, because its change is in the select list. (The fragment-built
    sharded category read and the test that kept its two shard expressions identical went with the two-step
    read; the shard expression is now only in `read/commits_sharded.sql`.)
  - File headers are dropped at render. Reasoning goes in the header, not in comments inside the SQL body.
  - `render` is infallible: a missing value leaves `{name}` in place and the statement fails in PostgreSQL.
    `check_files` renders every statement with every value set and fails on any leftover. This avoids a new
    runtime error path and new public error variants.
  - `PgType` is an open newtype over sqlx's type names with constants; types without one, such as `oid` and
    `pg_lsn`, use `PgType::named` in their `typed::Value` impl.
  - Identifier placeholders take an `IdentPlaceholder`, so passing a non-identifier placeholder to
    `Values::ident` does not compile. `{database}` and `{template}` are identifier placeholders, so
    `pges_testing::drop_database` refuses a name that is not a plain identifier instead of escaping it.
    `{snapshot}` takes a `SnapshotName` (hex digits and dashes); the file supplies the quotes, as it does
    for `{from}` and `{to}`.
  - `describe` gives a type only to a parameter PostgreSQL cannot infer, one at a time (the one its error
    names, or the first not yet given), and retries; the type is the parameter's Rust type's.
    `snapshot/upsert`'s `$1` is the only one today; the parameter it is compared with is checked against it.
  - `Kind::Ddl` covers DDL and utility commands (`SET`, `SET TRANSACTION`, `VACUUM`). They are run in a
    scenario, not described. Registry tests never run `ALTER SYSTEM` (the bench's `NOT_RUN`). The bench
    composes its `PREPARE`, `EXPLAIN … EXECUTE` and `EXECUTE` fragments in Rust with a rendered statement and
    registers them as `Kind::Ddl`; its test runs them composed.
  - One store-name check: `template::StoreName` wraps the store-name rule, and pges-sequencer uses it instead
    of its own. (The type is now `pges_core::store::StoreName`; the alias is gone.)
    `pges_postgres::sql::store_name(&Store)` hands other crates the name `Store::open` already validated, so
    infallible constructors such as `PollWaker::new` need no second check.
  - Replication commands are not SQL and cannot be described; they stay in pges-replication's Rust.
  - `check_no_inline_sql` skips comments and doc comments, and counts a SQL keyword only when whitespace or
    `(` follows it. It covers SQL a crate writes; sqlx's own `LISTEN`/`UNLISTEN` from `PgListener` is
    outside it. The facade runs no SQL and has only this check and `check_typed_calls`.
  - Equivalence was shown once, not kept as a test: the server's received statements were compared
    (`log_statement=all`) for its suite before and after, plus a text comparison of every rendering with the
    old `format!` text. Keeping the old code as a second copy for a permanent test was rejected.
  - Known limits: a call site that forgets a placeholder value is caught only when it runs; `{from}`/`{to}`
    quoting depends on the template (a `Literal::int` in a quoted template, or a timestamp in an unquoted
    one, fails only at run time); `Statement::sql()` does not refuse a statement with placeholders.

- Typed statement bindings (2026-11-25; SQL layout, above):
  - The Rust types are the single source. `typed_query!` derives the registry constant's parameter types and
    columns at compile time through the const trait `typed::Value` (`sqlx::Type::type_info` cannot run in a
    `const`), and `check_typed` asserts that `Value::TYPE` equals `type_info()` for every type in use. No
    query has a hand-written type list.
  - Parameters and columns are compared by compatibility (`sqlx::Type::compatible`), not equality, and checked
    twice: against what PostgreSQL infers, and against a prepare with the Rust parameter types, which is what
    runs (the sqlx statement cache is cleared first, because it keys on the SQL text alone). A `name`
    parameter bound as `&str` is registered as `TEXT`, so `Statement.params` holds the types sqlx sends,
    not the ones PostgreSQL infers; a narrower type (`i32` for `int8`) fails.
  - Nullability fails the test, not a warning: where describe reports it, `Option<T>` on a NOT NULL column
    and `T` on a nullable one both fail. Unknown nullability (expressions, and statements whose parameters
    had to be given types) accepts either. When describe reports nullable a column that never holds NULL,
    the code reads `Option<T>` and turns `None` into the error the untyped read gave; the SQL is not cast, so
    rendered SQL does not change (`pg_replication_slots.failover`/`active` through `not_null` in
    pges-sequencer's `logical.rs`; the `pg_stat_wal` counters in the bench).
  - Columns are decoded by name, so `ColumnDecode` messages are unchanged and the registry test guarantees
    the names. Columns that share a name are read by position (`as "count" at 1`), with the name still
    checked when the row is read; the macro rejects at compile time a column read by name whose name is not
    unique, and an `at N` that is not the field's own position.
  - One module per statement (`statement::append::{Query, Params, Row}`): `macro_rules!` cannot concatenate
    identifiers without the `paste` crate, and the module keeps the constant names.
  - `typed::Rendered<S>` ties the SQL to the statement's types, so one statement's SQL cannot be run with
    another's parameters. A `like` variant must have identical `Params` and `Row` (the `Variant` bound,
    checked by the compiler) and renders into its base's type. A statement composed in code is built with
    `Rendered::compose` from its registry files; that the files belong to the entry is only a
    `debug_assert`, and the composition's shape is tied to the registry by a unit test.
  - No sqlx impls on domain newtypes: the orphan rule forbids them for pges-core's types, SQL conversions
    do not belong in pges-core, and `migrate::SchemaVersion` is public, so impls would put sqlx into the
    public API. Conversions stay explicit at call sites.
  - `typed::PgLsn` is a pges-postgres newtype, because sqlx (0.8 and 0.9.1) has no `pg_lsn`. It is encoded only: LSNs
    are still read as `::text` and parsed, so the SQL stays as it was.
  - A statement without parameters whose rows are ignored keeps running as text (`unit_of_work/check`):
    running it typed would switch it to the extended protocol. It is still in `TYPED` and checked.
  - `check_typed_calls` rejects `sqlx::query*`, `query_as`, `query_scalar`, `try_get`, `sqlx::Row` and
    `use sqlx::` of `query`, `query_with` or `Row` in `src/`. It does not forbid `.bind(`, which typed call
    sites use. Exemptions are per file: pges-postgres's `sql/typed.rs`, pges-testing's `statement.rs` (it
    holds the names it scans for), and bench's `partitions/composed.rs`, the `EXPLAIN … EXECUTE` read
    composed at run time.
  - Where PostgreSQL inferred a different type than a call site bound, the call site changed, not the type:
    the category bucket is bound as `PgInterval` (was text cast by `$n::interval`), `backfill/create`'s lane
    count as int4 (was int2), bench `partitions/fill`'s counts as int4 (was int8; a count above `i32::MAX` is
    now an error), and LSNs as `PgLsn` (were text). Values and rendered SQL are unchanged.
  - The untyped registry check (`check_query`) is removed; `template::Statement` written by hand remains
    only for DDL.

- Compile-time checks (2026-11-25):
  - Principle: misuse that types can describe fails at compile time. Config structs have public fields, so a
    check in a constructor can be bypassed by assigning a field. Validation therefore lives in the field
    types: `StoreName`, `CategoryName`, `SubscriptionName`, `SourceKind`, `SlotName`, `PublicationName`,
    `Interval`, `NonEmpty`, `EventCount`, `Shards`, `Lanes`, `Lease`, `SchemaVersion`, bounded `Position`
    and `Version`. A struct built from valid fields is valid, so `ConfigError`,
    `ResequenceError::InvalidConfig` and the `InvalidName`, `NoCategories` and `InvalidConfig` variants of
    `SubscribeError` and `BackfillError` are gone.
  - Literals go through macros (`store_name!`, `category!`, `interval!`, `non_empty!`, `position!`,
    `version!`, `subscription_name!`, `shards!`, `lanes!`, `source_kind!`, `slot_name!`,
    `publication_name!`). Each expands to a block with a `const` item, `{ const NAME: T = …; NAME }`, so the
    check runs in const evaluation. An invalid literal is a compile error, also under `cargo check` and
    inside generic functions; nothing panics at run time. A plain `const {}` block is not used: `cargo check`
    does not evaluate it in function bodies. A `Cow` payload cannot be matched in const (E0493), so the
    core macros check a `bool` first and the replication macros use the const-stable `Option::expect`.
  - Builder states are type parameters. A subscription or backfill builder carries its category scope
    (`NoScope`, then `Listed` or `AllCategories`); `run` and `run_pg` exist only on a chosen scope, and a
    backfill's `until` is a parameter of `backfill(name, until)`. The marker is a sealed trait with no
    methods, so no crate-private type leaks.
  - What stays a run-time check, and why:
    - Database facts: `UnknownCategory`, `StoreNotFound`, `SchemaBehind`, `CrateTooOld`, `ChecksumMismatch`,
      `OutboxMismatch`, `StoreRequiresFeature`, `ClockSkewAboveStore`, `Lowering`, the writer-role checks,
      `DefinitionChanged`, `ShardsChanged`, `UntilAhead`, `Overtaken`, `CursorBeforeHorizon`, the
      sequencer's `SourceMismatch` and `Halted`. Values read from the database are parsed at the boundary
      into the checked types, and a bad one is `Corrupt` or `SourceError::Invalid`.
    - Data lengths: `NoEvents` and `TooManyEvents`. A slice's length is data, not a type.
    - Text read at run time: `parse`, `FromStr` and `TryFrom` on each checked type, `Config::from_url`,
      `Notice::decode`, endpoint strings.
    - Pool and transaction state: `PoolTooSmall`, `NoSpareConnection`, `Aborted`.
  - `Event::TYPES` is checked by a hidden associated const (`Event::__CHECK`) that register, append and
    decode force. It is evaluated after monomorphisation, so a bad hand-written list fails under
    `cargo build`, not `cargo check`. The derive already rejects these lists.
  - Schema versions are `SchemaVersion(NonZeroU16)`. A stored 0 is a corrupt frame
    (`FrameError::OutOfRange`), not an unknown event: only a hand-written impl could have written it.
  - Positions and versions are bounded at `i64::MAX` by their constructors; nothing clamps. pges-core carries
    their infallible `to_i64` / `from_i64`, and `EventCount::to_i16`: the bound makes them total, so they
    are the one exception to SQL conversions living in the crate that owns the statement.
  - High-water marks and notices keep their fields private, so `categories` stays sorted; they are no longer
    `#[non_exhaustive]`, which private fields make unnecessary. A gRPC watch message with a position above
    the maximum gives `None` from `WatchResponse::high_water()`; `GrpcWaker` reports it as `Rejected`
    (`Protocol`).
  - A store's creation options live in `StoreDefinition`, separate from `StoreConfig` (how a writer opens
    it), so an existing store's options cannot be set and silently ignored. `StoreDefinition` has no name,
    so the two cannot disagree. `max_clock_skew` is in both: the definition's is recorded; the config's is
    an `Option`, `None` (the default) meaning the store's recorded value. A writer
    that sets one larger than the store's is still refused at open. Behaviour change: a writer on a store
    whose recorded skew is not 5 s now uses the store's value, where it used 5 s before.
  - `Loaded` is read-only, so `save_snapshot` never stores a state under the wrong version. A unit of work
    in the caller's transaction is its own type, `WrappedUnitOfWork`, with `finish()` and no `commit` or
    `rollback` that end nothing.
  - `delete_stream` takes `DeleteExpected { Any, Exact(Version) }`: a stream that does not exist cannot be
    deleted, so `NoStream` is not expressible.
  - `check_writer_roles` takes `max_txn_age` as a `Duration`, and `Lowering` reports `Duration`s: these are
    the store's values read from the database, which may lie outside `Interval`'s range.
  - Durations given to timers, leases and retries are `Interval`: 1 ms to 30 days.
    30 days is far above any timer the crates run, and keeps `Instant + d` and `i64` microseconds from
    overflowing. This includes `RetryPolicy::backoff` and `GrpcConfig`'s timeouts and keepalives. Fields where
    zero means something stay `Duration`: `token_interval` (every batch), `unreachable_after` (report at the
    first failed round), `Lease::renew` (half of a 1 ms TTL is below `Interval::MIN`).
  - The sequencer's lease is a `Lease` value whose constructors keep the renew period shorter than the TTL;
    `ResequenceConfig` uses the same type. A source's kind is a `SourceKind`; a commit's event count is an
    `EventCount`, parsed where a source reads it, so the log writer does not check it again.
  - The binary keeps `--source` as an option, not a subcommand, so environment-only deployments keep
    working. It checks all options together after parsing and exits 2 on an option the chosen source would
    ignore, whether it came from a flag or the environment.
  - `NonEmpty<T>` lives in pges-core and is shared; there are no per-use list types. `Wakers` cannot be
    empty, `GrpcWaker` takes `NonEmpty<Endpoint>`, `WriterRoles` wraps `NonEmpty<String>`.
  - `StartOptions` takes a sealed `Publications` (a name, an array of 1 to 8, or `NonEmpty`), so existing
    array call sites keep compiling and an empty list is a compile error. A const-generic impl with
    `const { assert!(N > 0) }` was rejected: it fails only after monomorphisation, so `cargo check` misses
    it.
  - The derive accepts only field serde attributes that round-trip (an allowlist): `#[serde]` on the enum or a variant is rejected, even when the enum also derives
    serde for another purpose, because it never changes the stored data.
  - The replication `Config` TLS setters are unchanged.
    Contradictory settings still fail `Config::check`; URLs are the main input, and `SslMode` keeps all six
    variants whatever the features.
  - No global fallback limit: a replica's fallback is
    `Replica::fallback(after, limit)`, and callers share one `FallbackLimit`. It removes an
    initialisation-order error and a fallback setting that was ignored without a replica. A fallback now
    needs an explicit limit; the process-wide default of 4 is gone.
  - The timer waker is public (`waker::recheck::Recheck`) and is the only way to set the recheck interval,
    so it cannot be set and then ignored next to another waker.
  - `store::Dcb` is removed: DCB is not a public option until DCB appends exist. A
    DCB store made by another build is still refused at open.
  - `SubscriptionName` lives in pges-subscribe: subscriptions and backfills share the rule, each in its own
    namespace.
  - trybuild compile-fail snapshots hold rustc's wording, so they run only on the stable toolchain CI pins,
    1.97.1. CI's lint and test jobs are pinned to it; every other compiler,
    including the MSRV job's 1.94, skips them. Bumping the toolchain bumps the CI pin, the `TOOLCHAIN`
    constant in each crate's `tests/compile_fail.rs` and the `.stderr` files together.
- Correctness follow-ups (2026-11-26):
  - Snapshot replacement: the stored snapshot's version and state version never go down. One of the same
    state version replaces it when at least `$8` versions ahead (unchanged); one of a higher state version
    replaces it at the same version or newer; one of a lower state version never does. Retention's
    `missing_snapshots.sql` accepts a snapshot of any state version at or after the stream's last version in
    the leaf, and a stored version that never falls keeps that check true after it ran, so
    `load_snapshotted` does not fail with `Truncated` after a truncate. Old code in a rolling deploy cannot
    overwrite new code's snapshot, and the two cannot take turns. Rejected: "the current state version always
    wins, but never below the floor": its floor check races retention's transaction, and old and new code
    still take turns. The cost: after rolling back to a lower `SNAPSHOT_VERSION`, that code's saves are
    skipped until it raises the version again (documented on `save_snapshot` and `SNAPSHOT_VERSION`).
  - Bootstrap records `CommitsBeforeSlot` in `pges.sequencer.halted` (`lease/halt_unleased.sql`:
    `coalesce(halted, $1)`, only while no process holds the lease), so `resequence` keeps its one
    precondition and operators see the halt. Rejected: letting resequence also accept "no token and no
    slot". Bootstrap still does not record `Missing` or `Invalidated`: they arise only after a sequencer ran,
    and it records them itself.
  - `deleted_commits` is cleaned up when retention drops a commits leaf, in the transaction that raises the
    horizon (`retention/forget_deleted.sql`). Only rows whose log row lies in the leaf's scan range,
    (`retention_scan_from`, new horizon], are deleted, not every id within the leaf's bounds: a deleted commit
    sequenced after the drop would then reach a store-wide read unrecorded and stop it as `Corrupt`. Such a
    late deletion, with a log row below a later scan start, stays recorded; rare and bounded.
  - Listed reads take a mode, `read::BeforeHorizon` (crate-private): `Fail` for subscriptions, which must not
    skip events they have not handled (`CursorBeforeHorizon`, unchanged), and `Skip` for backfills, which read
    each category after max(cursor, its horizon). Events below a horizon are gone; a projection that needs
    them starts from snapshots. Rejected: starting every lane at the highest horizon, which would skip other
    categories' events. `BackfillError::CursorBeforeHorizon` is removed, since nothing returns it (breaking,
    before 0.1.0). Each backfill page costs one more statement (the horizons).
  - The store-wide read keys fetched commits and the deleted-commit lookup by (category, commit id), like the
    tables, since caller-chosen commit ids may repeat across categories.
  - `$stream-deleted` is defined once, as `pges_core::event::STREAM_DELETED`; pges-postgres's private copy and
    `pges_subscribe::batch::STREAM_DELETED` are removed (breaking, before 0.1.0).

- Metadata propagation (2026-11-25; api.md, Store and Core types):
  - The ambient scope lives in `pges_postgres::ambient` (reached as `pges::postgres::ambient`). pges-core has
    no tokio and does no IO, the appends that read the scope live in pges-postgres, and a module named
    `metadata` would clash with `pges_core::metadata` when a caller imports both.
  - W3C `traceparent` formatting and parsing are our own, on the opentelemetry API types, and match
    `opentelemetry_sdk`'s `TraceContextPropagator` (unit tests compare the two). Libraries should depend only
    on the API; the SDK is a dev-dependency. The global propagator is not used: it is a no-op unless the
    application configures one, and it may not be W3C.
  - Precedence, field by field: explicit `.metadata(..)` > innermost scope > the current span (`otel`, trace
    only) > unset. The trace inside `follow_up()` counts as explicit, so a follow-up commit records the
    cause's traceparent, not the handler span's; set `trace = None` to record the handler span.
  - Metadata is resolved at the append future's first poll, not when it is built or at `into_future`. So
    `ambient::scope(m, store.append(..))` and `.into_future().instrument(span)` work, and
    `tokio::spawn(append.into_future())` does not inherit, as for any task-local.
  - Nested scopes merge: the inner scope's set fields win, and its unset fields come from the enclosing scope,
    read when `scope` is called. This keeps an outer correlation when an inner scope only sets the actor.
  - The subscription runner sets no scope per batch: a batch spans many commits with different correlations
    and causes, and its boundaries depend on `batch_size` and timing, so a per-batch scope would exist only
    sometimes and misattribute handlers that append once per batch. Handlers use the per-event helpers
    (`RawEvent::follow_up`, `StreamEvent::follow_up`, `trace_parent`).
  - `follow_up` drops `extra`, which may hold request-specific pairs, and the actor: the actor is whoever
    makes the new commit.
  - `delete_stream`'s `$stream-deleted` commit takes the scope, so the deleting actor and request are
    recorded.
  - Correlation and causation are separate newtypes, `CorrelationId` and `CausationId`. Both convert from
    `CommitId`; neither converts into the other. Causation is not a `CommitId`, because an outside cause may
    be a message id that is not UUIDv7. The wire format is unchanged.

- Cancel-safe transactions (2026-11-26; superseded by the sqlx 0.9.1 upgrade below):
  sqlx 0.8 counted a transaction only after BEGIN returned, so a future dropped mid-BEGIN left a pooled
  connection in a transaction nothing ended. pges then began every pooled transaction through
  `pges_postgres::sql::transaction` (`Pooled`, `close_if_dropped`, `begin_owned`) and denied sqlx's begin
  methods with clippy's `disallowed-methods`. All of it was removed with the upgrade.

- sqlx 0.9.1 and MSRV 1.94 (2026-11-26):
  - sqlx is 0.9.1 and `rust-version` is 1.94, sqlx 0.9's MSRV; CI's MSRV job runs 1.94. Rendered SQL goes to
    sqlx through `template::text` / `Statement::render_text` and `typed::Rendered` (an `SqlStr` shared by
    every bind), sqlx 0.9's `AssertSqlSafe` in one place: the text is the registry's, with only validated
    placeholder values. `check_no_inline_sql` sees through `AssertSqlSafe(..)` and
    `template::text(..)`. `check_typed` needs `Executor::describe`, which 0.9 puts behind the driver's
    `offline` feature: pges-testing depends on `sqlx-postgres` with `offline` rather than on sqlx's `macros`.
  - 0.9.1 counts the transaction before BEGIN's round trip, so a top-level begin dropped at any point queues
    its ROLLBACK, which the next use of the connection (or the pool's ping on release) sends. Evidence, on
    0.9.1: the old sentinel's sweep of `pool.begin()` found nothing in 20 sweeps; a `pool.begin()` dropped
    with BEGIN's reply held by the proxy leaves the connection clean; with `Pooled::begin` made a plain
    `Connection::begin`, `close_if_dropped` a no-op and `begin_owned` a plain `pool.begin()`, every drop test
    of pges-postgres (append, snapshot, stream lock, append_many, delete, unit of work, snapshot save, open,
    category), pges-sequencer and pges-subscribe passed; under 0.8.6 the same unguarding failed 6, 1 and 2 of
    them. So `sql::transaction` and the begin lint are gone: crates begin with sqlx (`pool.begin()`,
    `Connection::begin` on a `PoolConnection`).
  - Not fixed upstream: a savepoint begin (a begin on a connection already in a transaction) that is dropped
    in flight rolls back the whole outer transaction and leaves sqlx at depth 0, so the outer `commit` does
    nothing. 0.9.1's `Transaction::begin` builds the `Transaction` (open) before the begin, so the begin's
    guard rolls back to the savepoint and the dropped `Transaction` rolls back one level more. The crates
    never begin a savepoint; the migration functions, which begin on a caller's connection, say to run them
    outside a transaction. The sentinel `a_dropped_sqlx_savepoint_rolls_back_the_outer_transaction` fails
    when sqlx fixes it. No lint replaces the old one: clippy cannot tell a savepoint begin from a top-level
    one. A savepoint begin that fails (on an aborted transaction) does the same
    (`a_failed_sqlx_savepoint_rolls_back_the_outer_transaction`).
  - Savepoint guard: users may begin savepoints on the connections we hand out (`PgHandler::handle`'s `tx`,
    `UnitOfWork::conn`, a wrapped connection). Before checkpointing, `run_pg` and the backfill check
    `Connection::is_in_transaction` (sqlx's client-side depth, no round trip); if the transaction ended, the
    batch fails with a transient error and is retried like an aborted one, so no checkpoint moves in
    autocommit. A unit of work records whether its connection was in a transaction at `begin` / `wrap`; once
    it no longer is, appends fail with `AppendError::TransactionEnded` (rather than autocommit) and
    `commit` / `finish` with `Aborted`. The depth itself is behind sqlx's unexported `TransactionManager`,
    so a wrapped connection already inside a savepoint that drops one level is not detected.
  - The drop tests stay; they now guard against regressions in sqlx and in our own code, and
    `the_harness_catches_a_transaction_left_open` checks the harness still sees a transaction left open.
    The harness does not count a poll after which the future has already woken itself (a yield): sqlx 0.9
    yields about 800 times while deriving a SCRAM key per new connection, which made the sequencer's drop
    test, which connects anew per run, take more than 15 minutes.

- Retention at scale (2026-11-26; schema.md, Partition maintenance, Retention and Closed
  partitions):
  - `sequenced` is `NOT EXISTS (leaf EXCEPT seen)`, not a count: a set operation is only ever hashed or
    sorted, whatever the estimates, while a `count(*) = 0` anti-join could still become a nested loop.
  - The policy checks probe each of the leaf's streams with `LATERAL (… LIMIT 1)`. The order is fixed
    whatever the statistics; the price is a serial plan, latency-bound on a cold cache.
  - `raise_floors` keeps one `UPDATE … FROM` under scoped `SET LOCAL enable_hashjoin/enable_mergejoin = off`
    (reset right after), rather than a PL/pgSQL loop, a `ctid` join or `= ANY(array)`, which were slower or
    fragile. Its join order can still invert under skewed statistics (a known limit).
  - Admin sessions run with `jit = off`: JIT never pays off for maintenance statements, and compiled
    retention's per-stream loops for 0.6–0.8 s per statement.
  - The freeze is a separate call, not part of `maintain`, which must stay cheap so leaves are always created
    ahead. It takes the maintenance lock per partition, so `maintain` and retention wait for one vacuum at
    most, and the lock keeps a vacuum from blocking retention's `DETACH CONCURRENTLY`. A partition is a
    candidate when `relallfrozen` (table plus TOAST) is below its file pages, or when it has dead rows,
    because `relallfrozen` is stale after a delete.
  - `partition_ages` covers every partition of the store and takes no lock, so an alert poll never waits on
    a retention run.
  - The `streams` autovacuum settings are part of store migration 0001 (edited in place
    before the first publish, 2026-11-26).
- Append and stream-read pruning (2026-11-26; schema.md, Append (one stream), Read one
  stream):
  - Single appends prune `streams` by a parameter filter (`s.stream_id = $1`) and rely on custom plans,
    because PG18 locks every partition of a generic plan. Superseded for single appends by the routing below
   ; still how `append_many` prunes.
  - Stream reads are bounded above by `last_commit_id`, and a read is a cut at its head: commits appended
    while it pages are not returned, as snapshots are already checked against `streams.version`.
- Single appends name their stream's partition (2026-11-26; schema.md, Append (one
  stream)):
  - The client computes PostgreSQL's hash partition routing for the stream id (`uuid_hash_extended` with
    the partition seed, `hash_combine64`, modulo) and the append statement names `streams_pNNN`. No plan has
    partitions to prune, so the locks (7, 9 with the outbox) are the same with statistics never gathered,
    gathered nearly empty, and under `force_generic_plan`, where the statement through `streams` locked
    70–71 (135 unprepared).
  - Safe fallback: `Store::open` lists the partitions and checks 32 probe ids with
    `satisfies_hash_partition`; any mismatch (another hash, such as a big-endian server's, mixed moduli, a
    missing, renamed or detaching partition) keeps the statement through `streams`. Routing also requires
    the role to select, insert and update every partition directly (a grant on `streams` covers only
    statements through it) and the partitions to be in the store's schema; otherwise the store falls back,
    silently, as for the other mismatches. Routing is fixed at open.
  - Fit or fall back (2026-11-26): one prepared statement per partition while they fit in
    the pool's statement cache with 36 entries to spare (`stream_partitions + 36 <= statement-cache-capacity`;
    sqlx's default 100 holds 64). When they do not fit, the append goes through `streams` with its one
    prepared statement, the path before routing; never unprepared partition statements. The
    numbers at 256 partitions with the default cache, concurrency 1, `synchronous_commit = off`: unprepared
    partition statements 3.0–3.1k appends/s against 4.4–4.9k/s through `streams` (3.4k against 5.9k under
    `force_generic_plan`), and 9.6–10.8k/s prepared with a cache of 400. Prepared, an append is not planned
    after its sixth run on a connection: 3x the appends per second at concurrency 1 at 64 partitions. The
    price is plan-cache memory, about 130 KB per partition statement and connection (8.6 MB at 64
    partitions against 0.6 MB).
  - `StoreConfig::append_routing` (`AppendRouting::Auto`, the default, or `Streams`) turns routing off, for
    a pool shared by many stores or an application that would rather spend neither the cache entries nor the
    memory; appends then always use the prepared statement through `streams`.
  - Rejected: unnamed statements through `streams` (custom plans whatever the statistics, but
    `force_generic_plan` gives 135 locks, plus the round trip); `plan_cache_mode = force_custom_plan` (a
    setting of the role or session, so not the crate's to guarantee; per transaction it costs a statement);
    a SQL function with `SET plan_cache_mode = force_custom_plan` (9 locks in every regime and as fast as
    today, but a store schema change, and the statement would live in a migration instead of `sql/`);
    `satisfies_hash_partition` in the query (the planner does not prune on it).
  - `append_many` stayed as it was at first (one prepared statement through `streams`, 4 locks per touched
    partition, every partition under a generic plan): running it unprepared fixes the statistics regimes
    but cost 20–25% of batches per second at concurrency 1. Fixed by custom plans (below).
  - Single appends through `streams` (routing off, or fallen back) keep the plan cache's choice
    (2026-11-27). Running them in their own transaction with `force_custom_plan`, as `append_many` does, locks
    only the stream's partition in every regime (70–71 down to 9 at 64 partitions, nearly empty or under
    `force_generic_plan`), but an autocommit append goes from one round trip to three and holds its row lock
    until COMMIT: 5.1–5.3k down to 4.3–4.4k appends/s at concurrency 1 and 29–32k down to 22–26k at 16
    (100k streams, `synchronous_commit = off`), in the regime where plans were already custom. Rejected:
    every store on that path pays to protect stores analyzed nearly empty, which barely feel the fan-out;
    routing gives 7 locks in every regime.
- `append_many` plans for its streams (2026-11-26; schema.md, Append (several streams)):
  - The batch statement runs with `plan_cache_mode = force_custom_plan`: in its own transaction through
    `BEGIN; SET LOCAL plan_cache_mode = force_custom_plan`, one simple-query round trip
    (`Connection::begin_with`); inside a unit of work through `set_config(…, true)` before the batch and
    the transaction's previous value after it (two round trips). A custom plan prunes `streams` to the
    batch's partitions, so a batch locks 4 entries per partition it touches whatever the statistics and the
    role's or session's mode, with routing on or off: 8 for 2 streams over 2 partitions where the plan
    cache's generic plan locked 128 (analyzed nearly empty), 16 for 4 over 4 where `force_generic_plan`
    locked 256, at 64 partitions. No new prepared
    statement in the store's own transactions; one (`append/plan_mode.sql`) on connections that run batches
    in units of work. It also covers stores whose appends go through `streams` (256 partitions with sqlx's
    default cache).
  - Batches per second, batches of 16 single-event appends to random existing streams of 1M
    (`pges-bench scale append --batch 16`, 10 s runs, two rounds interleaved, before | after), on a dev box
    shared with other work (load 12–50). `synchronous_commit = off`: with the plan cache's own choice
    (custom plans) 64 partitions 592–659 | 574–650 at concurrency 1 and 5.9–6.5k | 6.1–6.5k at 16, 256
    partitions 381–663 | 531–649 and 5.1–5.7k | 5.9–6.0k (no change: the plans were custom already);
    under `force_generic_plan` 64 partitions 457–476 | 689–702 and 3.2–3.3k | 5.2–5.3k, 256 partitions
    104–111 | 641 and 0.8–0.9k | 4.6–4.9k; a store of 3 streams analyzed, appending new streams (four
    rounds) 1057–1134 | 1054–1146 and 7.2–9.0k | 8.8–8.9k. Durable commits are bound by the WAL flush and
    the box's I/O (round-to-round spread up to 2x): 64 partitions 222–335 | 210–326 at 1 and 1.7–3.9k |
    1.4–3.7k at 16; `force_generic_plan` 56–128 | 174–176, 256 partitions 46 | 332.
  - The cost: where the plan cache chose a generic plan that was cheap to run, every batch is now planned.
    `pges-bench append` (100k streams created by batches, then analyzed, durable): the plan cache kept a
    generic plan (48 plans for 3380 batches, 0.77 ms of execution, every partition locked); forced custom
    plans take about 1 ms of planning and 0.7 ms of execution. At concurrency 1 that was 439–571 → 422–534
    batches/s over six rounds on a quieter box, and 282–456 → 231–251 over four under heavier load; at 16,
    3.0–3.8k → 3.3–4.1k.
  - Rejected, measured: appending a batch stream by stream with the partition statements of routed single
    appends, in stream id order (2 locks per partition touched, no new statement, deadlock-free among such
    batches). It was faster at concurrency 1 (64 partitions, `synchronous_commit = off`, 419–665 → 757–920
    batches/s; 1.2 ms of planning and 2.1 ms of execution for the batch statement against 0.06 ms of
    execution for each partition statement) but slower at 16, where the 16 round trips per batch cost more
    than the planning: 4.4–5.1k → 3.9–4.6k (four rounds), durable 2.4–3.0k → 2.0–2.7k, and a nearly empty
    store's new streams 8.3–8.9k → 5.9–6.6k. It would also have made two lock orders (stream by stream on
    stores that route, the batch statement on those that do not) that can deadlock each other, and needed a
    store that routes. One statement per partition over arrays, grouped in Rust: a `many_head.sql` per
    partition doubles the partition statements per connection, so 64 partitions no longer fit sqlx's
    default cache with the reserve, and it still costs a round trip per partition touched (about 14 of 16
    streams at 64 partitions). The batch unprepared: 20–25% slower at concurrency 1 (earlier).
  - The single append keeps the plan cache's choice: a routed append names its partition, and the fallback
    through `streams` runs in autocommit, with no `BEGIN` to carry the setting.
- Partition names and headroom (2026-11-27):
  - Category names may not contain `__`, and every name built from a category other than its partition
    puts `__` right after the category: leaves `commits_<cat>__<suffix>`, their indexes
    `commits_<cat>__<suffix>_s`, the category partition's key `commits_<cat>__pkey` (named at creation, so
    PostgreSQL does not derive `commits_<cat>_pkey`), and the key of `commits` is `commits__pkey`. Before,
    category `a_202611`'s partition was category `a`'s November leaf: registered first, it left `a` without
    that leaf, and every append to `a` that month failed; registered second, it failed. Leaf, index and key
    names of one category now never equal another category's partition or each other (schema.md, Naming).
    Migration 0001 was edited in place for the CHECK on `categories.name` and the `commits` key name:
    nothing is published yet.
  - Rejected: naming partitions by category id (`commits_<id>`). It is unambiguous without a validation
    rule, but `commits_<cat>` is the name operators, read statements and tests use, and the other generated
    names only need a separator. Rejected: refusing category names that end like a leaf suffix
    (`_\d{6}(\d{2})?$`, `_s`, `_pkey`): it patches the cases known today and leaves the scheme ambiguous.
  - Maintenance checks a partition's parent, not only its name: a name that is a partition of another
    parent, or a relation that is not a table, fails with `NameTaken` instead of being skipped, dropped or
    attached under.
  - A commit id no leaf takes fails with `AppendError::NoPartition { category, commit_id }`, recognized from
    PostgreSQL's routing error (23514 with no constraint, naming `commits` or the category's partition), not
    a database error. No default partition and no leaf created on demand: an append must not run DDL, and a
    default partition would take rows that later block attaching the leaf for their range.
    `Admin::partition_headroom` reports each category's newest leaf bound and how far ahead it is, and the
    positions left in the newest log partition; that is what to alert on. `Store::open` does not warn about
    low headroom: the crate has no logging dependency.

- Slot ahead of the token (2026-11-27; schema.md, Logical source):
  - The logical source confirms the slot only through `Source::ack`, so the slot is never confirmed past a
    token stored in `pges.sequencer`. Idle WAL is no longer confirmed directly from a keepalive: the source
    hands its position out as the token of an empty batch, at most once per `status_interval`, and the
    sequencer stores and acks it like any other. Each such batch writes a little WAL (the fence row lock and,
    about once a `token_interval`, the token), which the next keepalive reports, so they are spaced out.
  - Every `open` (each term and each reconnect) reads `confirmed_flush_lsn` and halts with
    `SlotLost::Ahead { slot, token, confirmed }` when it is past the stored token, before
    `START_REPLICATION`. A slot at or behind the token is normal: a restart after a crash finds the slot
    behind, because its position is saved lazily.
  - A slot the source creates (in `open` or `bootstrap`) has its consistent point stored as the token at
    once, if none is stored, so the check has something to compare with from the start. A slot with no
    token at all halts only when the stores hold commits; with none, nothing can be lost.
  - Recovery is the existing `resequence`, not an automatic one: it needs the writer roles' transaction cap
    and a stopped fleet, which only the operator can vouch for. Rejected: storing the timeline with the token
    and halting on any timeline change (it halts on every switchover, including a clean one where nothing
    was lost), and confirming the batch transaction's own end LSN (it would skip source commits that end
    between the last delivered one and the batch).
  - Tested with a real promotion: a replica pair (`replica::Pair::start_failover`) whose replica
    synchronises the failover slot, has its WAL stream cut before the batch that sequences the last commit,
    and is promoted; and with the state built directly by `pg_replication_slot_advance`.

## Open decisions

- Multi-tenancy: a tenant column only, or LIST sub-partitioning.
- Archive tier.
- Whether a failed `START_REPLICATION` should hand the connection back inside the error; today `start()`
  consumes it.
- Whether `pges-postgres`'s `DbError` and `pges-replication`'s `SqlState` share a SQLSTATE type.
- Temporary slots and `origin 'none'`: added when the sequencer needs them. (`SNAPSHOT 'export'` exists
  now: `create_slot_with`.)
- Whether persistent SQLSTATEs (for example 42501, permission denied) should end `Sequencer::run` instead of
  being retried as `Status::Recovering` forever.
- Whether the `HighWater::lsn()` read after every batch (one round trip) should be optional when no replica
  readers exist.
- Whether the `SELECT 1` check of a wrapped `UnitOfWork::commit` should be opt-in, if projections commit
  through `wrap` often.
- Whether `append_in` on a connection outside a transaction should open its own transaction for the locked
  rerun, instead of failing with `CommitIdOutOfOrder` after 5 new ids.
- Whether the NOTIFY payload budget should be smaller than PostgreSQL's limit, to keep the notify queue
  light.
- Whether a `keep_alive` failure should let the batch's database work finish and report afterwards, instead
  of dropping it (today safe: recovery re-reads the token and the dedupe window covers a COMMIT that landed).
- Whether to build a top-level `pges::Error` with `From` impls.
