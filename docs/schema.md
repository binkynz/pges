# pges — schema

The DDL lives in `crates/pges-postgres/migrations/` (`pges/` for the shared schema, `store/` for each store,
with the `opt_outbox.sql` and `opt_dcb.sql` option scripts). This document explains the names, the column
order, the queries the crate runs and how partitions are maintained.

## Naming

| Thing | Rule | Example |
|---|---|---|
| Shared schema | `pges` | `pges.stores`, `pges.sequencer` |
| Store schema | chosen by the user, one per bounded context | `ops_es` |
| Tables | plural nouns, no prefix inside a store schema | `streams`, `commits`, `log` |
| Ids | `<thing>_id` | `stream_id`, `commit_id`, `category_id` |
| Stream versions | `version`, `last_version`, `floor_version` | |
| Store positions | `position`, `last_position`, `horizon_position` | |
| Ranges | plural of the thing, always virtual | `versions`, `positions` |
| Small enums | `int2` with a CHECK and a comment, not Postgres enums (enums need DDL to extend) | `state`, `retention` |
| Partitions | `<table>_<category>` for LIST, `<table>_<category>__<yyyymm[dd]>` for RANGE leaves | `commits_jobs__202611` |
| Log partitions | `log_<first position / 10^8>` | `log_0042` |
| Hash partitions | `<table>_p<remainder>`, zero-padded to 3 digits, or to the digits of the highest remainder above 1000 partitions | `streams_p007`, `streams_p0007` |
| Indexes | `<table>_<purpose>` | `log_by_category` |
| Leaf indexes | `<leaf>_s` for the unique `(stream_id, last_version)` index | `commits_jobs__202611_s` |
| Category partition keys | `commits_<category>__pkey`, named when the partition is created | `commits_jobs__pkey` |
| Objects on `commits` itself | `commits__<name>`: `commits_<name>` is category `<name>`'s partition | |
| Constraints over several columns | `<table>_<purpose>` | `categories_retain_for`, `scopes_window` |

Category names are limited to `^[a-z][a-z0-9_]{0,39}$` so every generated name stays within PostgreSQL's
63-byte limit, which it would otherwise silently truncate. For a 40-character category: `commits_<cat>` is 48
bytes, its key `commits_<cat>__pkey` 54, a leaf `commits_<cat>__<YYYYMMDD>` 58, its index `<leaf>_s` 60, and
the `<leaf>_pkey` PostgreSQL derives when the leaf is attached 63. The attach constraint is `bound`
(constraint names are per table); log partitions are `log_<n, at least 4 digits>`. Store names only qualify
these names. They follow the same rule and also exclude `pges`, `public`, `information_schema`, `pg_*` and
the SQL keywords that cannot name a schema unquoted (`select`, `user`, ...).
`crates/pges-postgres/src/ident.rs` builds every name and tests the longest case.

Category names also exclude `__`, which separates the category from the rest of every name built from it. All
these names share one namespace (tables and indexes are both in `pg_class`), so without it a category named
like another's leaf would take that leaf's name: `a_202611`'s partition would be `a`'s November leaf
`commits_a_202611`. With the separator, every name built from a category other than its partition contains
`__` right after the category and continues with something that does not start with `_`, so it is never
another category's partition `commits_<cat>`, and it names its category unambiguously. The category
partition's primary key is named `commits_<cat>__pkey` when the partition is created
(`crates/pges-postgres/sql/category/create_partition.sql`), because the name PostgreSQL would derive,
`commits_<cat>_pkey`, is category `<cat>_pkey`'s partition. For the same reason anything ever created on
`commits` itself (an index, a constraint) is named `commits__<name>`, never `commits_<name>`; `commits` has
none today. `ident::tests` checks that the names of categories named like each other's leaves, indexes and
keys never collide.

Hash partitions of `streams` and `snapshots` are named in store migration 0001. Up to 1000 partitions the
remainder has 3 digits (`streams_p000` to `streams_p999`); above, every name takes the digits of the highest
remainder (`streams_p0000` to `streams_p1000` for 1001), so no two collide. Before 0.1.0 they were cut to 3
digits, and a store of 1001 partitions failed to create `streams_p100` twice. `stream_partitions` is a `u16`,
so the longest name is `snapshots_p65534` (16 bytes).

Leaves created before the `_s` suffix keep their old `_stream` index name: maintenance runs no DDL on existing
leaves.

## Column order

Postgres stores each row as a 23-byte header (padded to 24) followed by the columns in declared order, each
aligned to its type: `int8`, `timestamptz`, `interval` and `xid8` to 8 bytes, `int4` and longer varlenas to
4, `int2` to 2, `uuid` and `bool` to 1. The rules:

1. NOT NULL fixed-width columns first, widest alignment first: 8-byte, then `uuid` (16 bytes, lands on an
   8-byte boundary anyway), then `int4`, then `int2`/`bool`.
2. Nullable fixed-width columns next.
3. Variable-width columns (`text`, `bytea`, arrays) last.
4. Virtual generated columns are not stored, so their position does not matter. They go at the end for
   readability.

This removes padding, and Postgres can cache the byte offset of every column up to the first nullable or
variable-width one, which speeds up decoding on hot paths. The saving per row is a few bytes; what matters
is billions of rows on `commits`, `log` and `streams`.

| Table | Fixed part | Row size before variable columns |
|---|---|---|
| `commits` | `last_version` 8, `commit_id` 16, `stream_id` 16, `category_id` 4, `event_count` 2, `format` 2 | 24 + 48 = 72 bytes, then `payload` |
| `log` | `last_position` 8, `commit_id` 16, `category_id` 4, `event_count` 2 | 24 + 30 → 56 bytes with tail padding |
| `streams` | `version` 8, `floor_version` 8, `stream_id` 16, `first_commit_id` 16, `last_commit_id` 16, `category_id` 4, `state` 2 | 24 + 70 → 96 bytes, then `stream_key` if set |

Add 4 bytes per row for the line pointer.

Event type ids are not stored as a column. An `int4[]` costs about 25 bytes of array header per row; the
types travel inside the payload frame instead, and subscribers filter after decoding.

## Tables

| Table | Written by | Shape | Notes |
|---|---|---|---|
| `pges.migrations` | migration runner | one row per applied migration per scope | checksums detect edited migrations |
| `pges.scopes` | migration runner | one row per scope | `schema_version`, `min_compatible` |
| `pges.stores` | runner, sequencer, admin | one row per store | coarse store high-water mark, `outbox` and `dcb` flags, default retention for new categories (`retention`, `retain_for`), writer limits `max_clock_skew` and `max_txn_age` (recorded at creation, only raised, by `Admin::set_writer_limits`; Writer limits); all stores of a database share one `outbox` value (Migrations) |
| `pges.sequencer` | sequencer, resequence | single row | lease, fence, `source_token`, `halted` (cleared by `pges-sequencer resequence`) |
| `categories` | admin, sequencer, resequence | one row per category | coarse category high-water mark, retention policy and horizon, partition bucket, the positions resequence gave the category (`resequenced_from`, `resequenced_through`; 0, 0: none) |
| `event_types` | crate, outside the caller's transaction | one row per type name | ids cached by the client after commit |
| `streams` | append path | one row per stream, HASH(`stream_partitions`) | version, first and last commit id, retention floor; autovacuum scale factor 0.02, insert scale factor 0.05 |
| `commits` | append path | one row per append, LIST(category) → RANGE(commit_id) | the events |
| `log` | sequencer | one row per commit, RANGE(last_position) | store order |
| `pending` | append path, sequencer | queue, outbox stores only | drained by the sequencer |
| `deleted_commits` | `delete_stream`, retention | one row per deleted commit, key (`category_id`, `commit_id`) | lets the store-wide read tell a deleted commit's log row from corruption; retention forgets the rows whose log rows its horizon covers when it drops a leaf (Partition maintenance, retention step 2) |
| `snapshots` | append path (`snapshot_after`), `save_snapshot` | latest per stream, HASH(`stream_partitions`) | upserted |
| `subscriptions` | subscribers | one row per shard (one, shard 0, for an unsharded subscription) | cursor, lease, fence, definition (categories, number of shards); rows with `shard >= shards` are retired |
| `subscription_members` | sharded subscriptions | one row per running instance | liveness, for each instance's fair share of the shards |
| `backfills` | backfills (`pges-subscribe`) | one row per lane of a backfill | lane cursor, end, lane count, categories |
| `tag_heads`, `commit_tags` | append path | DCB stores only | |

`stream_partitions` is a store-creation parameter (`StoreDefinition::stream_partitions`, default 64). It is
fixed for the life of the store. 64 suits up to about 1B streams (about 2.9 GB of heap and primary key per
partition there, still 3–4 B-tree levels). More partitions do not slow single appends, which name their
stream's partition (Append (one stream)) whatever the statistics, but `append_many` locks 4 relations per
partition it touches (Append (several streams)). Single appends keep one prepared statement per partition on each connection when the pool's
statement cache holds them with 36 entries to spare (sqlx's default of 100 holds 64); with more partitions,
or with `StoreConfig::append_routing` set to `AppendRouting::Streams`, they go through `streams` with one
prepared statement (Append (one stream)).

`streams` partitions are created with `fillfactor = 70`, `autovacuum_vacuum_scale_factor` 0.02, as
`snapshots` has, and `autovacuum_vacuum_insert_scale_factor` 0.05, as leaves and log partitions have. With PostgreSQL's 0.2, a partition of 1.56M rows (100M streams)
collected about 312k dead tuples before autovacuum started; the scale run left 2.24M dead tuples and no
autovacuum after 4.7M updates, 33% of them non-HOT.

`snapshots.format` 1 means the state is encoded with `serde_json`.

## Migrations

The migrations are embedded in `pges-postgres` (`migrate::SHARED`, `migrate::STORE`, `migrate::OUTBOX`) and
applied by its runner
(`migrate::run_shared`, `migrate::run_store`, `migrate::run`, or `StoreConfig::migrate` with
`AutoMigrate::CreateOrMigrate`). The migration texts are in `crates/pges-postgres/migrations/`, the runner's
own statements in `crates/pges-postgres/sql/migrate/`.

- Each migration runs in its own transaction holding the migration lock, so concurrent runners apply it
  once. The lock is the `pges` row of `pges.scopes`, locked `FOR UPDATE SKIP LOCKED`
  (`crates/pges-postgres/sql/migrate/lock.sql`), which needs `UPDATE` on `pges.scopes`: the owner and store
  creators have it, readers, writers and the sequencer do not. Before the first shared migration created the
  table, the advisory lock `hashtext('pges.migrate')` stands for it
  (`crates/pges-postgres/sql/migrate/bootstrap_lock.sql`); a runner that takes it and then finds the table
  locks the row too, so no runner holds only the advisory lock while another holds the row.
- The runner tries the lock and does not wait in the server: between tries it sleeps 10 ms, doubling up to
  250 ms. One run waits at most `MigrateConfig::lock_wait` in all (60 s by default), then fails with
  `MigrateError::LockTimeout`, naming the session holding the lock: for the row, the transaction in its
  `xmax` looked up in `pg_locks` (`crates/pges-postgres/sql/migrate/lock_holder.sql`); for the advisory lock,
  its key in `pg_locks` (`crates/pges-postgres/sql/migrate/bootstrap_holder.sql`); the role from
  `pg_stat_activity`. Both views show these columns to every role. A run that finds every migration applied
  takes no lock. Store migrations run with `SET LOCAL search_path` set to the store, so
  their SQL uses unqualified names.
- Every `run_shared` (so every `migrate::run` and auto-migrating `Store::open`) first refuses a schema `pges` that belongs to another role than `current_user`
  (`crates/pges-postgres/sql/migrate/shared_schema_owner.sql`, `MigrateError::SharedSchemaNotOwned`), a
  superuser or a member of the owner's role included: a schema's owner may drop any table in it and put its
  own in its place, whose triggers then run as each role that writes to it. The check reads only the
  catalog and runs before anything in the schema is read, so it does not depend on what the schema holds:
  a squatter that fills in `pges.migrations` and `pges.scopes` with the right rows and checksums is refused
  as well. A schema created ahead by its future owner (`CREATE SCHEMA pges AUTHORIZATION <owner>`) is used
  when that owner migrates. To migrate a schema owned by another role, migrate as that role: a login role
  that inherits from a NOLOGIN owner group runs `SET ROLE <owner>` on the connection first (Roles).
  `run_store` alone does not check: it trusts a schema the owner migrated, so a store creator granted
  privileges in `pges` creates stores without owning it (Roles, The publication). It is called alone only
  after the owner ran `run_shared`.
- An applied migration is compared with the embedded text by its SHA-256 checksum; an edited one fails with
  `ChecksumMismatch`. A runner whose newest migration is below the scope's `min_compatible` fails with
  `CrateTooOld`.
- After each migration the runner sets `pges.scopes.schema_version` and `min_compatible` from the embedded
  migration.
- Store creation is a single transaction: `CREATE SCHEMA`, the `pges.stores` row
  (`crates/pges-postgres/sql/migrate/insert_store.sql`, with the final outbox flag and
  `StoreDefinition::max_clock_skew` and `max_txn_age`), store migration 0001, and `opt_outbox.sql` when
  requested.
- `opt_outbox.sql` is version 1 (`opt_outbox`) of the store's outbox scope `<store>.outbox`
  (store names contain no `.`): it inserts its `pges.scopes` row, as 0001 does for
  the store, and the runner records it in `pges.migrations` with its checksum. `migrate::run_store` checks
  and completes the outbox scope of a store that has one, as it does the store's own; a store created
  without the outbox has none and never gets one. A later outbox migration is `migrate::OUTBOX`'s next entry.
- `opt_dcb.sql` is applied by no public path, only by a test: DCB is not a public option. It is not recorded
  in `pges.migrations`, and it is not frozen (below); `pges.stores.dcb` records it, and `Store::open` refuses
  such a store.
- `Store::open` checks the version, `min_compatible` and checksums of the shared scope, the store's and,
  for a store with the outbox, its outbox scope. A scope that is behind fails with `SchemaBehind` unless
  auto-migrate is enabled, an edited migration with `ChecksumMismatch` (naming the scope, `<store>.outbox`
  for an edited outbox script), and a scope whose `min_compatible` is above the crate's with `CrateTooOld`.
- Lease renewals check `min_compatible` again (2026-11-27): `subscription/renew.sql` and
  `subscription/checkpoint_renew.sql` in pges-subscribe (the shared scope, the subscription's store and its
  outbox scope) and `lease/renew.sql` in pges-sequencer (the shared scope, every store and every outbox
  scope) update no row while a scope's `min_compatible` is above the crate's newest migration for it, passed
  as parameters. The holder then reads the scopes with `crates/pges-postgres/sql/migrate/too_old.sql`
  (`migrate::crate_too_old`) to tell this from a lease another instance took, and stops with `CrateTooOld`
  (api.md, Rolling upgrades). An outbox scope is compared with the crate's newest outbox migration, so a
  later release can retire a crate that drains or fills `pending` the old way. Scopes of options this crate
  does not know (`<store>.<option>`) are not compared.
- Taking a lease checks the same scopes: `subscription/acquire.sql` and `lease/acquire.sql` compute the
  check once in a CTE and always return one row, whose `too_old` column says whether the check failed and
  whose other columns are NULL when no lease was taken. A failed check takes no lease and locks no lease
  row, so "too old" is never mistaken for a lease another instance holds. The standby then reads the scopes
  with `migrate::crate_too_old` for the error's details and stops with `CrateTooOld`, whether or not the
  lease was free. The resequence acquire (`lease/acquire_halted.sql`) is not checked; its renewals are,
  including the one just before it writes.
- **Migration 0001 is frozen from 0.1.0 on.** The shared and store migrations `0001_init.sql`, and
  `opt_outbox.sql` (outbox migration 1), are never edited again: every schema change after 0.1.0 is a new
  migration (`0002_...`, or the next outbox migration), applied by the runner on top of the existing one. An
  edited migration would fail every existing database with `ChecksumMismatch`. `opt_dcb.sql` is not frozen:
  no release applies it, and DCB will get its own versioned scope (`<store>.dcb`) when it ships.
  Before 0.1.0, migrations were edited in place, so a database created by an unreleased
  build fails with `ChecksumMismatch` and must be recreated. Test templates are keyed by the checksums, so
  they rebuild on their own.

**One source kind per database.** A database holds stores of one kind: all with the outbox or none. A trigger,
`pges.check_one_source_kind` in `crates/pges-postgres/migrations/pges/0001_init.sql`, right after `CREATE
TABLE pges.stores`, enforces it for every writer of `pges.stores`, not only the runner. After each insert, or
update of `outbox`, it takes the migration lock and raises `PGE02` if another store has a different `outbox`.

- The check locks the `pges` row of `pges.scopes` first, the migration lock (the runner already holds it in
  the same transaction, so it does not wait for itself). It waits for the lock in the server, under the
  caller's `lock_timeout`. So of two READ COMMITTED
  transactions adding stores of different kinds, the second waits and then sees the first's row. Under
  REPEATABLE READ the snapshot would not see it; the runner uses the default level.
- SQLSTATE `PGE02` maps to `MigrateError::OutboxMismatch`, and nothing is created.
- `opt_outbox.sql` still runs `UPDATE pges.stores SET outbox = true`. The trigger fires again and passes,
  because the row was inserted with `outbox` already true. The sequencer's `UPDATE pges.stores SET
  last_position` does not fire it (`UPDATE OF outbox`). Changing the flag by hand is refused too.

**Outbox wake-ups.** `crates/pges-postgres/migrations/store/opt_outbox.sql` adds a statement-level trigger on
`pending`, `notify_pending`, that wakes the outbox source (Sequencer batch): after an insert that enqueued at
least one row, it runs `pg_catalog.pg_notify('pges__outbox', <the store's schema name>)`.

- Channel `pges__outbox` has two underscores, so it cannot be any store's `pges_<store>` channel: store
  names start with a letter.
- The payload is the store's schema name. Identical notifications in one transaction are sent once.
- A statement that enqueued nothing (a failed expected-version check) does not notify.
- `pg_notify` is schema-qualified, so a function of the same name on the `search_path` cannot shadow it.
- Cost: NOTIFY makes each committing transaction take a server-wide lock held until its commit is done, so
  outbox appends commit one at a time. At high write rates use the logical source.

## Queries

Every statement lives in a file `crates/<crate>/sql/<group>/<name>.sql`; this section points at the files and
explains the design. Each file's header says what the statement does, its `$n` parameters and why it is shaped
that way. Each crate lists its statements in a registry (`pges_postgres::sql::statement::ALL`,
`pges_sequencer::sql::statement::ALL`, `pges_subscribe::sql::statement::ALL`; hidden modules, not part of the
API), and its `tests/statement.rs` checks the files against the registry and each query's Rust parameter and
column types against PostgreSQL 18. Statements run with fully qualified names. The store's statements are
rendered once per store at `Store::open`; those naming a leaf or log partition are rendered when it is
handled. Stream and category reads target `<store>.commits_<cat>` directly, so plans contain only that
category's leaves.

### Event type registration

The first time a store sees an `Event` type (`store.register::<E>()`, called implicitly before the first
append, read or subscribe that uses `E`), the crate runs, in autocommit on a pooled connection and never on
the caller's connection, `crates/pges-postgres/sql/event_type/register.sql` then
`crates/pges-postgres/sql/event_type/select.sql`.

Both take all of `E::TYPES` as `$2`. Ids enter the client cache only from that `SELECT`. The append statement
never writes `event_types`. On read, an unknown `event_type_id` triggers one reload of `event_types`
(`crates/pges-postgres/sql/event_type/by_category.sql`) before a decode error is returned.

### Append (one stream)

`crates/pges-postgres/sql/append/head.sql` (the `input`, `created` and `bumped` steps), completed by
`crates/pges-postgres/sql/append/commit.sql`, or `crates/pges-postgres/sql/append/commit_outbox.sql` for
outbox stores. `input` holds the stream, category, expected version (-1 `NoStream`, -2 `Any`, n ≥ 1
`Exact(n)`), event count, format, payload, stream key and the commit id, checked by `pges.check_commit_id`.
`created` inserts a new stream, `bumped` updates an existing one, and the commit is inserted for whichever
returned a row.

- The append names the hash partition of `streams` that holds the stream (`{stream_table}`, such as
  `streams_p017`), so no plan has partitions to prune and the append's locks do not depend on statistics or
  `plan_cache_mode`: 9 relation locks (11 with the outbox) in every regime measured (`streams` never
  analyzed, analyzed at 30 streams per partition, analyzed with 3 streams in all, `force_generic_plan`); 7 and 9 before the skew check read
  `last_commit_id` (2026-11-27), which takes the partition and its key in a second, shared mode. The insert into a partition also takes a lock on `streams` itself, to check the
  partition's bound. Through `streams`, a custom plan prunes to the stream's partition (9 locks, measured before the skew check's read), but the
  plan cache keeps a generic plan when the partitions were analyzed nearly empty, and PG18 locks every
  partition of a generic plan before run-time pruning: 70 locks at 64 partitions, 71 with
  `force_generic_plan` (135 unprepared), and every sixth execution planned a generic plan once (again after
  each autoanalyze invalidated it), locking every partition in that transaction.
- Routing (`crates/pges-postgres/src/route.rs`): PostgreSQL routes a row to remainder
  `hash_combine64(0, uuid_hash_extended(id, HASH_PARTITION_SEED)) % modulus` (`partbounds.c`); the crate
  computes the same, a port of lookup3 for 16 bytes on a little-endian server. `Store::open` lists the
  partitions with `crates/pges-postgres/sql/append/stream_leaves.sql`, with 32 fixed probe ids, and routes
  only when every partition has one modulus, each remainder below it has one plain partition in the store's
  schema whose name needs no quoting and that the current role may select, insert and update directly
  (privileges granted on `streams` cover only statements through it), and `satisfies_hash_partition` puts
  every probe where the crate does. Otherwise (a big-endian server, a partition detached, being detached or
  renamed, a role granted only `streams`) the append goes through `streams` as above, and so it does when
  the partition statements do not fit the statement cache (below) or `StoreConfig::append_routing` is
  `AppendRouting::Streams`. The routing is read
  once at open: DDL that moves partitions afterwards (which the crate never runs) makes appends fail (a new
  stream's with a partition constraint violation, 23514), or, for a detached partition, write to the
  detached table, until the store is opened again. A property
  test checks the hash against `satisfies_hash_partition` for random ids at every modulus from 1 to 256,
  and against the `tableoid` of inserted rows.
- One statement text per partition, so they cost statement cache entries and server memory: about 130 KB of
  plan cache per prepared partition statement and connection (8.6 MB for 64, against 0.6 MB for the one
  statement through `streams`, most of it the generic plan of 64 partitions it planned once). The store
  routes appends only when the partition statements fit in the pool's statement cache
  (`statement-cache-capacity`, read at open from the pool's options) with 36 entries to spare for its other
  statements and the application's (`stream_partitions + 36 <= capacity`); otherwise the append goes through
  `streams` with one prepared statement, as before routing, so they do not evict each other. sqlx's default of
  100 holds the default 64 partitions. Running the partition statements unprepared instead was measured and
  rejected: a Parse round trip per append made 256 partitions with the default cache 30–35% slower than the
  prepared statement through `streams` (below). A pool that several stores share, or that runs many
  statements of its own, should raise the capacity by `stream_partitions` per store, or turn routing off.
- A prepared partition statement is planned only for its first five runs on a connection; the plan cache
  then keeps its generic plan, which is as good as a custom one. Through `streams` every append was planned
  (custom plans). Concurrency 1, 1M streams, `synchronous_commit = off` (so the WAL flush does not hide the
  CPU): 64 partitions 5.0k → 15.0k appends/s (p50 0.2 → 0.1 ms); 256 partitions with
  `statement-cache-capacity=400` 14.5k/s (with the default cache they go through `streams`, 5.0k/s as
  before; unprepared partition statements measured 3.5k/s). With 3 streams analyzed (new streams): 5.6–7.2k
  → 17.8k/s. Under `force_generic_plan` the statement through `streams` was not planned per append either:
  64 partitions 13.3k → 14.5k/s; 256 partitions 10.3k/s through `streams` (unprepared partition statements
  3.3k/s, 14.4k/s prepared with a cache of 400). At concurrency 64 with durable commits the
  runs are bound by the WAL flush and within the box's noise (12–28k/s before and after, every regime).
- `bumped` also filters on `s.stream_id = $1` besides the join with the materialized `input`: through
  `streams`, the join alone would not prune even a custom plan (134 locks at 64 partitions, 518 at 256).
  `created` locks only its partition: an insert routes its row when it runs.
- The client always generates the commit id (uuidv7 from the `uuid` crate), so the crate can retry after a
  lost response. `pges.check_commit_id` raises SQLSTATE `PGE01` when the id's timestamp is more than the
  writer's skew (`$9`: `StoreConfig::max_clock_skew`, or the store's recorded value when `None`) from
  `statement_timestamp()`; the crate maps it to `AppendError::CommitIdSkew`. `Store::open` refuses a
  `max_clock_skew` larger than the store's recorded one (Writer limits).
- An id at or below the stream's `last_commit_id` (one scalar subquery on the row `bumped` updates, in the
  same partition) is not checked: the function gets an infinite tolerance, so it still runs once per
  statement. Such an append cannot apply, so it returns no row and the classification query below reports
  a retry of a stored commit as `Deduplicated`, however late it comes and in every mode. Before
  (2026-11-27), the skew check ran first, and a unit of work or `append_in` inside a transaction got
  `CommitIdSkew` and an aborted transaction for a retry more than the skew after the first attempt; only
  autocommit looked the id up after the error. `last_commit_id` only grows, so reading it in the
  statement's snapshot is safe: an id the snapshot sees at or below it stays at or below it. A late id the
  stream has not stored is then `CommitIdOutOfOrder` instead of `CommitIdSkew`; a new id (above
  `last_commit_id`, or for a new stream) is still checked. `append_many` does the same with the
  `last_commit_id` its `locked` step read under the row lock: its ids are crate-generated, but the retry
  after a lost connection sends them again, possibly more than the skew after the first attempt committed
  (before 2026-11-28 that retry failed with `CommitIdSkew` for a batch that was written).
- `pges.check_commit_id` refuses an id that is not a UUIDv7 with SQLSTATE `22023`, before the skew check and
  whatever the tolerance: `uuid_extract_timestamp` returns NULL for most other versions, which the skew check
  alone would let through, and a v1 id's timestamp is not a v7 one's. The crate only sends v7 `CommitId`s
  (`CommitId::from_uuid` refuses others), so it maps the code to no error of its own.
- `i.commit_id > s.last_commit_id` keeps commit ids increasing within a stream, so `first_commit_id` is a
  true minimum and `snapshots.commit_id` is a safe lower bound for reads. `least()` keeps the minimum even
  if the check is ever relaxed; `first_commit_id` is not indexed, so the update stays HOT.
- The same check makes every retry of an append that already succeeded return no row, for every
  `ExpectedVersion` including `Any`, at no extra cost: its id is at or below `last_commit_id`.
- `event_count` is at most 32767 (`int2`); the client rejects larger commits before encoding.

**No row returned.** The crate classifies the outcome with one follow-up query, in the same transaction when
there is one (`crates/pges-postgres/sql/append/classify.sql`; `$1` stream id, `$2` category id, `$3` commit
id). Its `done` column filters on the stream too: without it, a commit id reused on another stream of the same
category would be reported as a deduplicated retry of this append.

Before a rerun with a new crate-generated commit id, the crate locks the stream's row until the transaction
ends (`crates/pges-postgres/sql/append/lock_stream.sql`). `append_many`, in its own transaction, uses
`crates/pges-postgres/sql/append/try_lock_stream.sql` instead, which does not wait for a lock another
transaction holds. `SKIP LOCKED` alone still waits in one case: when the updater of the row version the
statement found commits while it locks the row, PostgreSQL 18 goes on to lock the newer versions and waits for
a transaction holding one, whatever the wait policy (`heap_lock_updated_tuple`). So the statement lowers
`lock_timeout` to 1 ms around its lock (set, lock, and restore are ordered by one-time filters) and the crate
takes a timeout (SQLSTATE 55P03) as a busy row; the batch then rolls back as for any busy row.

| Result | Outcome |
|---|---|
| `done` is set | duplicate: return the original result with `Outcome::Deduplicated` |
| no stream, `expected = Any` or `NoStream` | the append raced the stream's creation (the update's snapshot cannot see the other insert); retry once |
| no stream, `Exact` | `Conflict { actual: None }` |
| `category_id <> $2` | `CategoryMismatch` |
| `state = 1` / `state = 2` | `StreamArchived` / `StreamDeleted` |
| version differs from `Exact` / stream exists for `NoStream` | `Conflict { actual }` |
| commit id `<= last_commit_id` | `CommitIdOutOfOrder` for a caller-supplied id. For a crate-generated id, the crate locks the stream (above) and runs the append again with a new id. When `last_commit_id` is less than `min(1 s, max_clock_skew / 2)` behind a fresh UUIDv7, or ahead of it, the new id is a random step of 1 to 2^60 above `last_commit_id`, in its millisecond; otherwise it is the greater of a fresh UUIDv7 and that id. Under the lock the new id cannot fall behind. Outside a transaction the crate opens one for this rerun (BEGIN, lock, append, COMMIT). At most 5 new ids per append; only `append_in` on a connection outside a transaction, where the lock ends with the statement, can use them up and then fail with `CommitIdOutOfOrder` |
| stream exists, active, same category, version matches, commit id `> last_commit_id` | another append changed the stream while the statement ran (for example `Exact` racing the creation); retry once, then `Conflict { actual }` |

The rerun after a race and the new commit ids have separate budgets: an `Any` append that raced the stream's
creation and then finds its id behind still gets a new id.

Why the lock: an `UPDATE` that arrives after the row's holder commits can take the newest row version before
an `UPDATE` already waiting for the lock. Without the lock, appends that got their ids later keep
overtaking a waiter; with 16 writers on one stream, some appends needed more than 5 new ids. The new id is
not computed in the append statement, because the crate must know the id it sent to deduplicate a retry
after a lost response. If the COMMIT of the locked autocommit rerun loses its reply, the retry on a new
connection classifies by the new id, so deduplication still works.

Why the new id stays just above `last_commit_id` (design.md, Decided, Crate-generated commit ids;
`crates/pges-postgres/src/append.rs`, `commit_id_above`):

- A fresh UUIDv7 jumps past every queued waiter's id, so each waiter fails its recheck and retries in turn:
  on one hot stream with 16 writers, 3306 of 3314 appends took the retry path. Staying just above
  `last_commit_id` keeps the queued ids above it.
- The window is `min(1 s, max_clock_skew / 2)`. Half the skew means the retry path alone never trips the
  skew check for a writer whose clock is in sync. A writer whose clock is behind the database by more than
  skew minus the window (4 s at the defaults) can get `CommitIdSkew` on the retry path.
- The step is random, not +1. Two appends can pick an id above the same `last_commit_id`: one that chose
  its id under a lock its transaction then gave up (an `append_many` batch that starts over, a commit whose
  reply was lost), or one that did not get the lock (SKIP LOCKED). The same id for both makes the second
  look like a retry of the first, and it is reported as `Deduplicated` without being written. With +1, an
  `append_many` deadlock test lost appends every run.
- The step takes 64 random bits from two fresh UUIDv7s, because only the low 32 bits of one are random (the
  42 bits above them are a counter). A step of up to 2^60 stays inside the 74 counter and random bits of
  one millisecond; on overflow it moves to the next millisecond.
- Measured: in `a_retried_append_does_not_make_the_queued_ones_retry` (16 writers, 401 appends), about 390
  appends ran twice with fresh ids and about 240–290 with the new rule. Appends that overtake a queued one
  still force retries. On the durable dev box, 16 writers went from 133 to 247 appends/s over 10 minutes
  (design.md, Durable dev-box run, 2026-11-26).

A unique violation (23505) on the commits key means two different appends used the same commit id in one
category. `commits` itself has no key: each category partition has `PRIMARY KEY (commit_id)`, which each leaf
copies as `<leaf>_pkey`, so a commit id is unique within a category and may repeat in another. Only a 23505
on a constraint named `*_pkey` (the leaf's copy) counts; one on the per-leaf
`(stream_id, last_version)` index is returned as a database error. Outside a transaction, after either
`PGE01` or that 23505, the crate runs the classification query: if `done` is set it returns the original
result as deduplicated, otherwise `CommitIdSkew` or `DuplicateCommit`. Inside a transaction it returns the
error directly, and the transaction is aborted. Appends do not run under a savepoint, which would cost a
round trip each. A retry of a stored id never gets here: it is at or below `last_commit_id`, so the
statement skips its skew check and returns no row.

A commit id that no leaf of its category takes (maintenance stopped for about `LEAVES_AHEAD` buckets, or a
leaf dropped by hand) fails the insert into `commits` with PostgreSQL's routing error: SQLSTATE 23514, no
constraint, and the table where routing stopped, `commits_<cat>` (or `commits` when the category partition
itself is missing). Only that shape, in the store's schema, naming `commits` or the category partition of one
of the append's commits, becomes `AppendError::NoPartition { category, commit_id }`; a CHECK violation names
its constraint and stays a database error. When `commits` is named and the append's commits are in several
categories, the category is the one whose id the error's detail gives (`Partition key of the failing row
contains (category_id) = (3).`; only the untranslated `(category_id) = (3)` part is read). PostgreSQL omits
the detail when the role may not read `commits.category_id` or `commits` has row-level security; the error
then stays a database error. `Admin::partition_headroom` reports how far each category's
leaves reach (`crates/pges-postgres/sql/partition/headroom.sql`) and how many positions the newest log
partition has left, so the condition can be alerted on before appends fail.

- Outbox stores (`pges.stores.outbox`, read at `Store::open`) add one more CTE step, the `pending` insert
  from the commits insert's `RETURNING` (`crates/pges-postgres/sql/append/commit_outbox.sql`).
- DCB stores (`pges.stores.dcb`) add the `tag_heads` and `commit_tags` steps.

**Several streams (`append_many`).** The same shape over arrays, with the existing streams locked first and
the input sorted by `stream_id`: `crates/pges-postgres/sql/append/many_head.sql`, completed by
`crates/pges-postgres/sql/append/many_commit.sql`, or `crates/pges-postgres/sql/append/many_commit_outbox.sql`
for outbox stores. Its `locked` step locks the existing streams in stream id order before `input` yields a
row.

- `head` joins `input` on `stream_id`; there is no ordinal, because the client rejects a batch naming a
  stream twice (`AppendError::DuplicateStream`).
- Outbox stores add the `pending` insert as for one stream, ordered by `commit_id`.
- Outside a unit of work the statement runs in its own transaction (BEGIN, statement, COMMIT), because one
  statement can partly apply. All succeed or the crate rolls the transaction back.
- Custom plans, whatever the statistics and the role's or session's `plan_cache_mode`. The statement goes
  through `streams`, and only a custom plan prunes its hash partitions: PostgreSQL 18 locks every partition
  of a generic plan before run-time pruning. So the transaction begins with
  `crates/pges-postgres/sql/append/begin_many.sql`, `BEGIN; SET LOCAL plan_cache_mode = force_custom_plan`
  in one simple-query round trip (`Connection::begin_with`), and its classification and retry statements
  are custom-planned too. The setting ends with the transaction. Inside a unit of work the caller owns the
  transaction, so the batch sets the mode with `crates/pges-postgres/sql/append/plan_mode.sql`, which
  returns the previous value, and sets that back after the batch: two more round trips, and one more
  prepared statement on connections that run it. If the batch fails, its error is returned and the
  transaction must be rolled back anyway; a unit of work dropped between the two keeps `force_custom_plan`
  until its transaction ends.
- Streams missing from `RETURNING` are classified one by one with the single-stream query. Before a
  retryable stream is sent again (alone, in the same transaction), the crate locks its row, and for an
  out-of-order generated id picks the new id under that lock. `locked` locks only the streams that existed
  when the statement started, so a stream another transaction created later is not locked yet, and waiting
  for its lock now could deadlock against a batch that locks in stream id order. In its own transaction the
  crate therefore locks with `SKIP LOCKED`. If another transaction holds the row, it rolls the whole batch
  back and starts over in a new transaction, whose `locked` step now sees the stream. Every restart counts
  against that stream's retries (one rerun, five new ids), so restarts are bounded. Inside a unit of work the
  crate cannot roll back and waits for the lock; as with separate `UnitOfWork::append` calls, the loser of a
  deadlock fails with a retryable database error. A `PGE01` maps to `CommitIdSkew` with the batch's first
  commit id.
- Overlapping batches that create the same new streams do not deadlock in their own transaction. `locked`
  takes every row lock before the statement writes. `created` then inserts in stream id order. The insert
  waits only for a transaction that inserted the same stream, or that updated it in its `bumped` step; never
  for a lock-only row lock, because `ON CONFLICT` ignores a lock-only xmax. A batch runs `created` before
  `bumped` (`head` is `created UNION ALL bumped`), and nothing after `bumped` waits: the `commits` insert
  conflicts only with the stream's own creator or locker. So a transaction that has already bumped a stream
  never waits on another batch. Retry locks use `SKIP LOCKED` under a 1 ms `lock_timeout`, since `SKIP
  LOCKED` alone can wait for the holder of a newer row version (above). `crates/pges-postgres/tests/append_many.rs`
  races such batches. Inside a unit of work the retry waits for the lock, so the retryable deadlock above
  remains there.
- `locked` takes the row locks of the existing streams in stream id order (LockRows above Sort) before
  `input` yields a row: `input` reads it in a one-time filter (an InitPlan). `bumped` then updates rows the
  statement already holds, so the lock order does not depend on the join the planner picks. New streams are
  inserted by `created` in stream id order after that. A test races batches on a pool with nested loops and
  merge joins disabled (hash join, physical scan order) against a normal pool; before `locked` existed, it
  deadlocked within 2 s.
- `bumped` repeats `stream_id = ANY($1)`: the join alone does not prune `streams`' hash partitions, so custom
  plans would plan the update over all of them (0.8–1.7 ms of planning per call at 64 partitions; 0.36 ms
  with it). Semantics are unchanged: every `i.stream_id` comes from `unnest($1)`.
- After a store is created, connections that ran `append_many` on empty tables could keep a generic plan
  with a sequential scan and hash join over every `streams` partition (about 6 ms per batch at 100k
  streams) until autovacuum analysed a `streams` leaf. Custom plans (above) end that.
- Locks (64 partitions; pinned by `a_batch_locks_the_streams_partitions_it_touches` and
  `a_batch_in_its_own_transaction_plans_for_its_streams`): 4 per partition the batch touches (`locked` and
  `bumped`, each on the partition and its key; 13 relation locks in all for one stream, 169 for 64 streams
  over 40 partitions), with statistics never gathered, gathered nearly empty, and under
  `force_generic_plan`, with routing on or off. With the plan cache's own choice a generic plan locked every
  partition: 128 locks on partitions when they were analyzed with 3 streams in all, 256 with
  `force_generic_plan`; the plan cache also kept the generic plan for batches of 16 on a store of 2,000
  streams (about 30 per partition) created by one batch and analyzed. A batch's 4 locks per partition are
  two lock modes on two relations, inherent to locking rows before updating them.
- Cost: a populated store's batches were custom-planned already (about 1.2 ms of planning and 2.1 ms of
  execution per batch of 16 at 1M streams and 64 partitions), so forcing custom plans costs nothing there;
  where the plan cache chose a generic plan it was slower than the custom one at 1M streams (it reads
  every partition). Where the plan cache chose a generic plan that was cheap to run (100k streams created
  by batches, then analyzed), every batch is now planned: about 1 ms more per batch of 16. Batches per
  second: design.md, Decided, `append_many` plans for its streams.

### Read one stream

Read the bound first (`crates/pges-postgres/sql/read/stream_head.sql`), and the snapshot if the aggregate is
`Snapshotted`. A stream in another category fails with `CategoryMismatch`, a deleted one with `StreamDeleted`,
and a read from a version at or below `floor_version` with `Truncated`. Then, against the category's own
partition, one page at a time (page size 256): `crates/pges-postgres/sql/read/stream.sql` (`$1` the stream id,
`$2` the first version wanted, `$3` a lower and `$4` an upper bound on commit ids, `$5` the page size).

`stream_head.sql` also returns `last_commit_id`. `$3` is `snapshots.commit_id` when loading from a snapshot,
otherwise `streams.first_commit_id`; `$4` is `streams.last_commit_id` at the head read. Both bounds prune
leaves, so a stream probes only the leaves of its lifetime: a 1-hour `jobs` stream 56 days old went from 57
leaves to 1 (planning 0.93 → 0.12 ms, execution 0.30 → 0.015 ms, 125 → 6 locks; 10M streams). A long-lived
stream still probes every leaf of its lifetime (an `orders` stream of 19 months, 24 → 20); snapshots are the
lever there. The upper bound also ends the read at the head's version: commits appended during a read are not
returned, consistent with the snapshot's `version <= streams.version` check. The first commit returned can
start below `$2`; the client skips those events. The next page uses `$2 = previous last_version + 1`. The
read ends once that is above `streams.version` from the head read, which belongs to the commit `$4` names, so
a stream whose length is a multiple of the page size, or a snapshot at the head, asks for no empty page.

Retention can raise the floor and drop the oldest commits, or `delete_stream` delete them, after the head
read. So the client checks that the pages are contiguous: each commit starts at or below the next version it
wants, and the last page reaches the head's version. Otherwise it reads the head again and fails with
`Truncated` when the floor now covers the missing versions, `StreamDeleted` when the stream was deleted, and
`Corrupt` otherwise.

### Snapshots

A load reads the stream's snapshot (`crates/pges-postgres/sql/snapshot/read.sql`) and ignores it when its
`state_version`, `format` or category differ, or its version is outside `[floor_version, streams.version]`.
A snapshot above `streams.version` from the head read was saved after that read. Its version was committed
before it, so `load_snapshotted` reads the head again, then the snapshot, up to 8 times; each retry needs
another save in between. Ignoring it would replay from version 1 or fail with `Truncated`, while retention
may have dropped the start of the stream because this very snapshot covers it.
Saving is an upsert: `crates/pges-postgres/sql/snapshot/insert.sql` completed by
`crates/pges-postgres/sql/snapshot/replace.sql` (`snapshot/upsert`, for `snapshot_after`), with
`crates/pges-postgres/sql/snapshot/live_stream.sql` between them for `save_snapshot` and the save of
`load_snapshotted` (`snapshot/save`).

The upsert's `$8` is the number of versions the new snapshot must be ahead of the stored one: 1 for manual
saves (and `snapshot_after` with `Manual`), `n` for `EveryN(n)`. The stored version and state version never
go down:

- same state version: replaced when the new snapshot is at least `$8` versions ahead;
- higher state version: replaced when the new snapshot is at the same version or newer;
- lower state version: never replaced.

So old code in a rolling deploy cannot overwrite new code's snapshot, and a snapshot that retention's
`missing_snapshots.sql` accepted before raising a floor stays at or above that floor. After a rollback to a
lower `SNAPSHOT_VERSION`, that code's saves are skipped.

A save for a stream that was deleted, or does not exist, stores nothing: a save from a load made before
`delete_stream` would otherwise bring the deleted state back. `snapshot/save` locks the stream's row `FOR KEY
SHARE`, which conflicts with the `FOR UPDATE` lock `delete_stream` takes first, so a save and a deletion that
overlap are ordered: the save waits and then sees the stream deleted, or the deletion waits and then deletes
the saved snapshot. Appends lock the row `FOR NO KEY UPDATE`, which does not conflict with it.

- The save names the stream's partition of `streams` (`{stream_table}`), as a routed append does, whenever
  the store knows its partitions, also when the appends fall back to `streams` because of the statement
  cache: it runs unprepared (sqlx `persistent(false)`), so one statement per partition costs no cache
  entries, only a parse and a plan per save, and two round trips instead of one (sqlx sends Parse and
  Describe, then Bind and Execute). It takes 6 relation locks (`snapshots`, its partition, their
  keys, the stream's partition of `streams` and its key) under `force_generic_plan` too. A store that does
  not route goes through `streams`: an unprepared statement gets a custom plan, which prunes to the stream's
  partition, except under `force_generic_plan`, where it locks every partition (64 at 64).
- `snapshot_after` uses `snapshot/upsert`, which does not read `streams`: it adds 4 relation locks to the
  append's (`snapshots`, the stream's partition of it and their keys) and no row lock
  (`an_append_with_a_snapshot_locks_one_streams_partition`). With the deleted check through `streams`, a
  generic plan locked every partition (about 69 relation locks per append at 64 partitions, against 4 to 5
  without it) and wrote a row-lock WAL record per append. The append needs no check: its snapshot is written
  only after the append updated or created the stream's row in the same transaction, which a deleted stream
  refuses (`bumped` requires `state = 0`, `created` conflicts with the existing row), and the append's `FOR NO
  KEY UPDATE` lock makes `delete_stream`'s `FOR UPDATE` wait until the transaction ends, after which the
  deletion deletes the snapshot.

### Writer limits

Each store records two writer limits in `pges.stores`: `max_clock_skew`, how far a commit id may be from the
database clock, and `max_txn_age`, the longest a writer transaction stays open. Creation records
`StoreDefinition::max_clock_skew` and `max_txn_age` (defaults `pges_core::store::DEFAULT_MAX_CLOCK_SKEW`, 5 s,
and `DEFAULT_MAX_TXN_AGE`, 1 h, the same as the column defaults). Appends, retention and resequence all use
them, so they cannot disagree.

`Store::open` reads the store's row with its recorded skew compared with the writer's own
(`crates/pges-postgres/sql/open/store.sql`, `coalesce($2, max_clock_skew)`). The writer's skew is
`StoreConfig::max_clock_skew`, or the store's recorded value when `None`. It refuses a writer whose skew is
larger: `OpenError::ClockSkewAboveStore { configured, store }`. A wider writer could put a commit into a leaf
retention is dropping, or one resequence does not search.

`Admin::set_writer_limits(WriterLimits)` raises them, under the maintenance lock (Partition maintenance), so a
retention run in progress finishes with the old values first. `max_txn_age` is an `Interval`, so it is never
zero. The update (`crates/pges-postgres/sql/limit/raise.sql`) changes the row only when neither limit goes
down. With no row, `crates/pges-postgres/sql/limit/recorded.sql` tells the errors apart: no row is
`StoreNotFound`, otherwise `Lowering { max_clock_skew, max_txn_age }`.

- Limits only rise. Commits written under a larger value stay in the log, out of commit id order by up to
  that much, and nothing records where they are. Retention and resequence would assume the smaller bound.
- A failed call changes nothing. Raise the skew before opening wider writers, and `max_txn_age` before
  lengthening the writer roles' `transaction_timeout`.

**Writer roles.** `limit::check_writer_roles(conn, &WriterRoles, max_txn_age)` checks that each writer role
caps its transactions. Retention and resequence run it first. It reads the role-in-database, role, database,
then every-role `transaction_timeout` (`crates/pges-postgres/sql/limit/writer_timeout.sql`).

The value is parsed in Rust (`us`, `ms`, `s`, `min`, `h`, `d`; fractions allowed). `WriterRoles` is never
empty. An unknown role is `UnknownRole`; unset or 0 is `TransactionTimeoutNotSet`, and so is a value set only
in `postgresql.conf`, which non-superusers cannot read; a value above `max_txn_age` is
`TransactionTimeoutTooLong`. A session that overrides the value with `SET` is not seen.

### Delete a stream

`Store::delete_stream::<E>(id, expected)`, in one transaction:

1. Lock the stream's row (`crates/pges-postgres/sql/append/lock_stream.sql`; none: `NotFound`).
2. Run the Append (one stream) statement with one `$stream-deleted` event (data `{}`, schema version 1).
3. Mark the stream deleted and release its key, returning its first commit id
   (`crates/pges-postgres/sql/delete/mark_deleted.sql`).
4. Delete its commits from that id on, except the `$stream-deleted` commit, and record their ids in
   `deleted_commits`, in one statement (`crates/pges-postgres/sql/delete/commits.sql`).
5. Delete its snapshot (`crates/pges-postgres/sql/delete/snapshot.sql`).

- The row is locked first, so an `Any` expectation cannot create the stream. The append's expected-version
  check and outbox `pending` insert apply as for any append; an archived stream fails (`StreamArchived`),
  because the append requires `state = 0`.
- The `$stream-deleted` commit stays so subscribers can read it. It is registered in `event_types` per
  category like any type.
- The publication publishes inserts only, so the deletes never reach the logical source. The publication holds
  only each store's `commits`, so inserts into `deleted_commits` are not published either.
- Log rows of the deleted commits stay, and commits not yet sequenced still get one: the outbox drain and the
  logical source never consult `commits`. Category reads skip a log row whose commit their fetch does not find (Read a category from a
  cursor). The
  store-wide read skips the commits recorded in `deleted_commits` (All categories, below).
- `deleted_commits` is keyed like `commits`, by category and commit id, so caller-chosen commit ids reused in
  two categories cannot collide. Its rows are about 44 bytes and hold no payload. Retention deletes them when
  it drops the leaf that held the commits, once its horizon covers their log rows (Partition maintenance,
  retention step 2). A deleted commit sequenced after that, with a log row below a later scan start, stays
  recorded (not yet addressed).

### Read a category from a cursor

In two steps:

1. A page of the category's log, with the horizon checked in the same statement:
   `crates/pges-subscribe/sql/read/category_log.sql` (`$1` the category id, `$2` the cursor, `$3` `H`, `$4`
   the page size). `log_by_category` covers it as an index-only scan.
2. The commits of the page's rows at or below the merge cut (below), by id from the category's own
   partition: `crates/pges-subscribe/sql/read/commits.sql` (`commit_id = ANY($1)`), or
   `crates/pges-subscribe/sql/read/commits_sharded.sql` for a shard. A custom plan prunes the time leaves to
   those holding the ids, so planning, locks and execution do not grow with the leaves. Every category's log
   page is read before any fetch, so the cut is known: rows above it are read again by the next page, and
   fetching their payloads would be wasted (with category rates of 100:1 and a full page of the busy
   category, the sparse one would fetch up to `$4` payloads to deliver about 1% of them). A category with no
   row at or below the cut runs no fetch.

- The one-statement form looked each commit up in a `LATERAL` subquery with `LIMIT 1`. Its plan covered every
  time leaf: at 1000 daily leaves about 20 ms of custom planning, 5 ms of execution and 2000–3000 locks per
  page, against well under 1 ms and 11 locks in two steps (design.md, Spike 3). It is kept for comparison in
  the bench, `bench/sql/partitions/category_read_lateral.sql`. As a plain join the planner had chosen a hash
  join over the whole category per page (185 ms per page at 1000 leaves).
- The horizon check is part of the log statement, never a separate step. Retention commits the new horizon
  before `DETACH … CONCURRENTLY` starts, and the detach waits for older snapshots, so a statement that passes
  the check still sees the leaf. On zero rows the client reads `horizon_position` once more, in one statement
  for every category that returned none: a cursor below it raises `CursorBeforeHorizon`, otherwise the
  category is caught up (`crates/pges-subscribe/sql/read/horizons.sql`).
- The fetch is a second statement, so retention can raise the horizon and drop commits between the two.
  Retention commits the horizon raise before it drops commits, so when a page's commit is missing the client
  reads the category's horizon again (`horizons.sql`). If it is now above the read's floor, `Fail` mode
  raises `CursorBeforeHorizon` and `Skip` mode (backfills) reads again from the new horizon. Otherwise the
  log row is skipped, as for a stream `delete_stream` deleted; listed-category reads do not consult
  `deleted_commits`, so they cannot tell such a commit from a corrupt one.
- The cut comes from the log rows (`$4` rows read means a full page), not from the commits fetched, so
  skipped rows still move the cursor.
- `$3` is `H`, below. A single-category subscription uses it too, so one rule covers one or more categories.
- `$4` is the subscription's `batch_size` (default 1024): log rows per category per page.
- The same code serves replica reads (both statements on the replica, the horizon re-read on the primary),
  backfill lanes and both `BeforeHorizon` modes.
- `crates/pges-subscribe/tests/two_step.rs` checks that the two-step read delivers exactly what the
  `LATERAL` statement gave, whole and sharded (testing.md).

**Several categories: merge rule.** One query per category cannot return a consistent page on its own:
each has its own snapshot and its own `LIMIT`. The client:

1. Reads `H`, the store's newest position, 0 for an empty log (`crates/pges-subscribe/sql/read/head.sql`). The
   sequencer writes log rows in order, one transaction at a time, so every position at or below `H` is already
   visible. If `H` is at or below the cursor, no category is read.
2. Runs each category's log query with `$3 = H`, each against that category's horizon.
3. Lets `cut` be the smallest last row position over the categories that returned exactly `LIMIT` rows, or
   `H` if none did.
4. Fetches the commits of the rows at or below `cut` only, emits them and sets
   `Batch::last_position = cut`. When `cut = H` this can advance
   the cursor with no rows.

A single query with `category_id = ANY(…)` cannot return rows in position order from the index and would
sort. A property test interleaves sequencer batches between the per-category queries. Without the `$3 = H`
bound, events are lost when a category read before a sequencer batch commits comes back short and a
category read after it returns a full page past `H`. A random property test catches that only sometimes, so
`pges-subscribe/tests/merge.rs` also has a paced race test for it.

**Subscription start.** `pges-subscribe` resolves category names
(`crates/pges-subscribe/sql/read/categories.sql`), then loads the event type names of the watched categories
(`crates/pges-subscribe/sql/read/event_types.sql`). The second statement runs again, once, when a frame names
a type id not yet loaded.

**Poll waker.** `waker::poll::PollWaker` reads the newest log entry of each watched category through
`log_by_category` (`crates/pges-subscribe/sql/read/newest_by_category.sql`). With no watched category it reads
`H` (above) instead.

**All categories** (`.all_categories()`). Read a page of `log` alone, then fetch the commits per category.
`subscriptions.category_ids` is `'{}'`.

- The log page, checked against the highest horizon of any category: `crates/pges-subscribe/sql/read/log.sql`.
- For each category in the page, its commits by id: `crates/pges-subscribe/sql/read/commits.sql`.
- For the commits missing from the fetch, on the primary, those `deleted_commits` records:
  `crates/pges-subscribe/sql/read/deleted_commits.sql`.
- On an empty page, or a missing commit `deleted_commits` does not record, the category with the highest
  horizon: `crates/pges-subscribe/sql/read/highest_horizon.sql`.
- For a category id the subscription has not seen yet (registered after start), its name
  (`crates/pges-subscribe/sql/read/categories_by_id.sql`), then its event types.

- The log page reads up to `H`. The cut is the last row's position for a full page, otherwise `H`. One
  statement saw every row up to `H`, so no merge is needed.
- `coalesce` lets a store with no categories pass the check.
- A cursor behind the highest horizon of any category raises `CursorBeforeHorizon`.
- A commit missing from the fetch that `deleted_commits` records was deleted by `delete_stream`, and its log
  row is skipped. The page's cut still comes from the log rows, so skipped rows move the cursor.
- The client keys fetched commits and the `deleted_commits` records by (category, commit id), like the
  tables: caller-chosen commit ids may repeat across categories, and two such commits in one page keep their
  own data.
- Any other missing commit means retention ran in between: the client checks the highest horizon, which is
  at or above the dropped commit's own category horizon. If that horizon is not past the cursor, the
  missing commit is `Corrupt`.
- The lookup runs on the primary, also for replica reads: a commit missing from a replica's fetch was
  deleted by a transaction the primary already shows. It runs after the fetch, and the delete and its
  `deleted_commits` insert are one statement, so a delete landing between the log read and the fetch is
  already visible to it.

**Sharded reads** (`.sharded(n)`). The shard of a stream (`crates/pges-subscribe/sql/read/commits_sharded.sql`)
is the first 8 bytes of the SHA-256 of the id's 16 bytes, as a big-endian integer without its sign bit, modulo
the number of shards. It needs PostgreSQL 18's `bytea` to `int8` cast.

- The category read and the store-wide read both fetch with `crates/pges-subscribe/sql/read/commits_sharded.sql`
  (`$2` shards, `$3` the shard). It keeps other shards' rows, without their payload, so a missing row still
  means that retention removed it; the client skips the rows with no payload. The shard expression is
  written only in this file.
- The filter applies after `LIMIT`: the log page is the same for every shard, and the cut comes from its
  rows. A shard's page therefore holds about `batch_size / shards` events, so more shards mean more round
  trips per event.
- Each shard still scans every log row of its categories and fetches each commit's heap tuple, plus one
  SHA-256 per row. Other shards' payloads are never sent, detoasted or decoded.
- `pges-subscribe/tests/shard.rs` pins the hash: fixed ids map to fixed shards, and a test checks the SQL
  filter against the rule computed in Rust.

**Replica reads** (`.replica(pool)`). Before each page read from the replica, the subscriber waits until the
replica has replayed a target WAL position: the LSN the waker reported, or, when the waker gave none (a poll
waker, the recheck timer), at start and after a retried error, the primary's insert position
(`crates/pges-subscribe/sql/replica/insert_lsn.sql`). A waker's LSN is a hint (any role can send a
`NOTIFY`): if the replica has not replayed it at the first check, the subscriber reads the primary's insert
position once and lowers the target to it, so a forged LSN cannot hold it back. It polls the replica's
replay position (`crates/pges-subscribe/sql/replica/replay_lsn.sql`) until it reaches the target, and reads
the replica's WAL page and segment sizes once (`crates/pges-subscribe/sql/replica/wal_layout.sql`).

- On the replica: `H`, the category read (with the sharded filter `$5`/`$6`), and for all categories the log
  page and the per-category commits fetch (with the sharded `CASE … payload`). A replica replays commits in
  order, so it shows a prefix of the log and the merge cut holds although the statements are separate.
- On the primary: the horizon checks, the highest-horizon check, the `deleted_commits` lookup,
  `categories_by_id`, event-type loads, checkpoints, leases and `run_pg` transactions.
- The replica's position is polled every 2 ms, doubling to 100 ms. A replica not in recovery (for example a
  promoted one) is read without waiting.
- The insert position points past a page header when the last record ended at a page boundary, while the
  replay position stops at the boundary. A target exactly 24 bytes into a page, or 40 into a segment's first
  page, is therefore compared as the page start; no record ends inside a page header.
- With `Replica::fallback(after, limit)`, once the replica has lagged longer than `after`, one page is read
  from the primary while a permit of `limit` is free. Errors reading the replica's position then count
  as lag.
- Each shard of a sharded subscription waits for its own LSN target. A shard that was busy through more than
  one wake sees only the latest, so it takes the primary's insert position as its target, not that wake's
  LSN.

### Slot and origin names

Replication slot and origin names are global to the server, not per database. The logical source names the
slot `pges_<oid of the database>` by default, so several pges databases can share one server;
`LogicalConfig::slot` overrides it. Replication origins (token option (a), not built) will use the same name.
The name is computed at start, never stored in a migration: databases cloned from a template (as the tests
do) would otherwise inherit the template's name. The source checks that an existing slot belongs to the
current database and uses pgoutput (otherwise `SourceError::Invalid`), and that the replication connection
reaches the same database as the pool (`IDENTIFY_SYSTEM`).

### Sequencer lease

One sequencer leads per database and serves every store in it, with one kind of source. Acquire with
`crates/pges-sequencer/sql/lease/acquire.sql` (`SequencerConfig::lease`'s TTL, default 10 s, and
`SequencerConfig::leader`).

A sequencer never cancels the acquire statement once it is sent, even on shutdown: if it took the lease, it
releases it (below). If `halted` is set, it releases the lease and stops with `RunError::Halted`.

Renew at the lease's renew period (`Lease::renew`, default 5 s, half the TTL) with
`crates/pges-sequencer/sql/lease/renew.sql`. Zero rows means stand down. The leader also stops sequencing once
its own local deadline (taken before the renewal was sent) passes, whether or not the renewal answered.

Whenever a term starts, or restarts after an error, the leader reads the durable token again: a COMMIT whose
answer was lost may have stored a newer one than the leader knows
(`crates/pges-sequencer/sql/lease/stored_token.sql`).

No row means the fence moved on: stand down. A token whose `source` differs from the configured source's kind
is used only if it is empty (the outbox stores an empty token); otherwise the sequencer stops with
`RunError::SourceMismatch`.

On graceful shutdown the leader stores its token (Sequencer batch), closes the source, then releases the lease
so a standby takes over within its `acquire_interval` (default 1 s). The release is
`crates/pges-sequencer/sql/lease/release.sql`.

It also releases the lease, without storing the token, when it stops on a fatal error or shuts down while
recovering or during `source.open()`. After a crash, a standby takes over once the lease expires. The
release runs on the term's connection, or on a fresh pool connection if that one fails.

If the source fails with `SourceError::Halt`, the leader records the error text under its fence
(`crates/pges-sequencer/sql/lease/halt.sql`, on a fresh pool connection if the term's connection fails),
closes the source, then releases the lease and stops (`RunError::Source`). Every later acquire sees `halted`
and stops with `RunError::Halted`.

A slot bootstrap that meets commits from before the first slot records the halt outside a term
(`crates/pges-sequencer/sql/lease/halt_unleased.sql`): `halted = coalesce(halted, $1)`, only while no process
holds the lease. A leader that meets the same condition records it under its fence instead.

The logical source, before every `START_REPLICATION`, ends the walsender still holding the slot (an old
leader's, or its own previous connection's), which would otherwise hold it until `wal_sender_timeout`
(`crates/pges-sequencer/sql/logical/terminate_holder.sql`).

- It ends only a walsender (`pg_stat_replication`). Another session holds the slot only while it runs a
  slot function such as `pg_drop_replication_slot`. Ending it mid-call can complete the drop while its
  client sees `terminating connection` (57P01), so the operator retries and finds the slot gone.
- It retries `START_REPLICATION` while the slot is still active (55006), ending the holder again before each
  try, with a pause doubling from 10 ms to 1 s, for up to `LogicalConfig::start_timeout` (30 s), then fails
  transiently. Every retry opens a new replication connection (`START_REPLICATION` consumes it).
- A `START_REPLICATION` that fails otherwise, or still on 55006 at the deadline, is checked against
  `pg_replication_slots`: an invalidated slot is `SourceError::Halt(SlotLost::Invalidated)`, and a missing
  slot with a stored token is `Halt(SlotLost::Missing)`; otherwise the failure is transient.
- The sequencer renews the lease at its renew period while `source.open()` runs, and drops the open on
  shutdown or when a renewal finds the fence moved. A START retry of up to `start_timeout` therefore neither
  loses the lease nor delays a shutdown.
- Whenever a term ends (lease lost, shutdown, or a fatal error), the sequencer calls `Source::close` before
  it releases the lease. The logical source then ends its stream (Logical source), so its walsender lets go
  of the slot at once.

The fence check in each batch stops the old leader from writing.

### Logical source

On every `open` the logical source runs, through the sequencer's pool: the database's oid and name
(`crates/pges-sequencer/sql/logical/database.sql`), the stores
(`crates/pges-sequencer/sql/logical/stores.sql`), the publication check
(`crates/pges-sequencer/sql/logical/publication_exists.sql`) and, when it is missing, its creation
(`crates/pges-sequencer/sql/logical/create_publication.sql`), the stores it publishes
(`crates/pges-sequencer/sql/logical/published.sql`), the addition of each missing non-outbox store's `commits`
(`crates/pges-sequencer/sql/logical/publish_store.sql`), and the slot
(`crates/pges-sequencer/sql/logical/slot.sql`). Only after this source created the slot, on a pool connection,
in the snapshot the slot exported: `BEGIN`, `crates/pges-sequencer/sql/transaction/repeatable_read.sql`,
`crates/pges-sequencer/sql/transaction/snapshot.sql`,
`crates/pges-sequencer/sql/logical/store_has_commits.sql` per non-outbox store, `ROLLBACK`, and then the
consistent point stored as the token (`crates/pges-sequencer/sql/logical/store_initial_token.sql`).

- The publication is repaired, not refused: a missing one is created with the migration's definition, and
  missing non-outbox stores' `commits` are added with the migration's column list, each with a warning. That
  needs ownership of the publication (Roles).
- `pges.stores` is read again when an insert names a schema the source has not seen, so a store created
  while streaming is sequenced. Outbox stores and schemas that are not stores are skipped.
- Then on the replication connection: `IDENTIFY_SYSTEM`; when there is no slot and no token,
  `CREATE_REPLICATION_SLOT pges_<oid> LOGICAL pgoutput (SNAPSHOT 'export'[, FAILOVER true])` (42710 from a
  concurrent creator is accepted, and that creator does the check); then
  `START_REPLICATION SLOT pges_<oid> LOGICAL <token LSN or 0/0> (proto_version '4', publication_names 'pges', streaming 'off', messages 'false')`.
- The commits check imports the exported snapshot: a plain `BEGIN` (sqlx's `Connection::begin`), then the two `SET
  TRANSACTION` statements. The snapshot name is validated (hex digits and dashes) before it is interpolated.
  The replication connection runs no other command until the check is done, because the export ends with
  its next command. The `ROLLBACK` runs only when no store has a commit; otherwise the transaction is dropped
  and sqlx rolls it back.
- If the check finds a commit, or fails, the source drops the slot again with
  `DROP_REPLICATION_SLOT pges_<oid>` on the same connection, after the check. It then fails:
  `Halt(SlotLost::CommitsBeforeSlot)`, which the sequencer records in `halted`, or the check's error. If the
  drop also fails, it fails `Fatal` with a message naming the slot, which an operator must drop.
- The new slot's consistent point is stored as the token right after the check, without the lease, and only
  while no token is stored (none, or the outbox's empty one): `UPDATE pges.sequencer SET source = 'logical',
  source_token = <consistent point>`. A failure drops the slot like a failed check. From then on the slot
  always has a token to be compared with.
- An existing slot's `confirmed_flush_lsn` (read by `slot.sql` as a byte offset) is compared with the stored
  token before `START_REPLICATION`. The source confirms only stored tokens, so on a healthy server the slot
  is at or behind the token. Past it means the log rows and token of the commits in between were lost, as on
  an asynchronous standby promoted with a synchronised failover slot (design.md, Deployment), and the server
  would silently start streaming at the slot's position: `Halt(SlotLost::Ahead { slot, token, confirmed })`.
  Without a token the stored one is read again after the slot
  (`crates/pges-sequencer/sql/logical/stored_token.sql`), since a bootstrap may have stored a new slot's
  consistent point meanwhile; a token is stored before the slot is confirmed to it, so a token read after the
  slot is never behind it. If there is still none, the slot counts as ahead when it is past `0/0` and any
  non-outbox store holds a commit (`store_has_commits.sql` per store, outside a snapshot); with no commit
  there is nothing to lose.
- Outcomes: a slot with an `invalidation_reason` is `SourceError::Halt(SlotLost::Invalidated)`; no slot but
  a token is `Halt(SlotLost::Missing)`; a slot confirmed past the token is `Halt(SlotLost::Ahead)`; a new slot whose exported snapshot already shows a commit in a store
  is dropped again and gives `Halt(SlotLost::CommitsBeforeSlot)`, because it would never deliver those
  commits. Each is recovered with `pges-sequencer resequence` (Resequence, below). A new slot whose exported
  snapshot shows a store without the outbox that the source had not read is dropped again with a transient
  error, and the next attempt reads the stores, publishes the new one and creates the slot again: the store
  was created while the slot was, so its commits were not checked, and one created while the publication was
  missing is not in it.
- The check runs in the snapshot at the slot's consistent point, so it sees exactly the commits the slot
  will not stream: no append is skipped silently. An append that commits while the slot is created can
  still make the first start fail. Create the slot before writers start, with `LogicalSource::bootstrap` /
  `pges-sequencer bootstrap` (Slot bootstrap, below).
- A crash, or a shutdown or lost lease that drops `open`, between `CREATE_REPLICATION_SLOT` and the stored
  consistent point leaves a slot that was never checked and no token. A later open then halts with
  `SlotLost::Ahead` (token `0/0`) if the stores hold any commit, and `resequence` recovers; with no commit it
  streams from the slot. Until 2026-11-27 it streamed in either case, and commits from that creation window
  were skipped.
- Token: `source = 'logical'`; `source_token` is the end LSN of the last transaction of the batch, 8 bytes
  big-endian; empty means none. `open` drops every transaction that ends at or below it.
- `ack` confirms only persisted tokens, and nothing else confirms the slot. While idle (nothing buffered, no
  transaction open), a keepalive's `wal_end`, or the end LSN of a transaction with nothing to sequence,
  becomes the idle position. Once `status_interval` has passed since the last one, `ready` returns and
  `next` hands it out as the token of an empty batch; the sequencer stores it with the next coarse token and
  acks it, and only then is the slot confirmed past the idle WAL. Every buffered transaction ends after the
  idle position, so a batch with both carries the transaction's end. On PostgreSQL 15+ pgoutput skips
  transactions with no published changes, so unrelated WAL shows up only as keepalives. The empty batches
  write WAL of their own (the fence lock, and the token about once a `token_interval`), which later
  keepalives report: hence the `status_interval` spacing.
- A status update goes out at least every `status_interval` (10 s): in `ready`, `next` and `ack`, and in
  `keep_alive` while a batch's database work runs.
- `close` (when a term ends) sends CopyDone and Terminate, with a 1 s timeout after which it just drops the
  socket, so the walsender lets go of the slot at once.

### Slot bootstrap

`LogicalSource::bootstrap` (`pges-sequencer bootstrap`) runs the same statements as `open` up to the slot:
the database, the stores, the publication repair and `pg_replication_slots`. It takes no lease.

- A database whose stores all use the outbox is `SourceError::Invalid`, so no slot holds WAL for nothing. A
  database with no stores yet gets a slot.
- An existing valid slot is kept (`Bootstrap::Exists`). An invalidated one is `Halt(SlotLost::Invalidated)`,
  one confirmed past the stored token (`crates/pges-sequencer/sql/logical/stored_token.sql`) is
  `Halt(SlotLost::Ahead)`, neither recorded in `halted`, and a foreign one `Invalid`.
- An existing valid slot keeps its `failover` unless `LogicalConfig.failover` is `SlotFailover::Set` to
  another value. Then bootstrap connects for
  replication and checks `IDENTIFY_SYSTEM`, then reads the slot again. When it is `active`, bootstrap fails
  `Transient` (stop the sequencers), because `ALTER_REPLICATION_SLOT` waits for an active slot. Otherwise it
  runs `ALTER_REPLICATION_SLOT pges_<oid> (FAILOVER true|false)` and returns `Bootstrap::FailoverChanged`.
  A sequencer that takes the slot between that read and the ALTER still makes bootstrap wait until it stops.
- If the slot is missing, it reads the stored token (`crates/pges-sequencer/sql/logical/stored_token.sql`).
  A non-empty token with `source = 'logical'` is `Halt(SlotLost::Missing)`. Bootstrap does not record it
  in `halted`: a sequencer already ran and records it itself, as it does `Invalidated`.
- Otherwise it connects for replication, checks `IDENTIFY_SYSTEM`, creates the slot with the
  exported-snapshot check and the stored consistent point above (`Bootstrap::Created`, or `Exists` on
  42710), and closes the connection.
  Commits found by the check give `Halt(SlotLost::CommitsBeforeSlot)`, which bootstrap records in `halted`
  through `crates/pges-sequencer/sql/lease/halt_unleased.sql` (Sequencer lease, above), so resequence can
  run at once.

It is safe to run again, and while a sequencer streams from an existing slot, unless the failover setting
must change. Do not run it while a
sequencer creates the slot on its first start: the sequencer's START ends the walsender holding the slot
(the bootstrap's, during its check). The bootstrap then fails with a message that the slot could not be
checked, and no token is stored: the sequencer halts with `SlotLost::Ahead` if the stores hold commits, and
streams from the unchecked slot otherwise.

### Resequence

`LogicalSource::resequence` (`pges-sequencer resequence`) recovers a halted database. First it checks the
writer roles against the shortest `max_txn_age` of the logical stores
(`crates/pges-sequencer/sql/resequence/shortest_max_txn_age.sql`), with `limit::check_writer_roles` and
`ResequenceConfig::writer_roles` (Writer limits); a failure is `ResequenceError::WriterRole`. With no such
store (NULL) the check is skipped. It refuses rather than warns, as retention does: the search window below
holds only if writer transactions are capped. The check comes before the lease, so a refusal has no side
effects.

It takes the lease only while sequencing is halted (`crates/pges-sequencer/sql/lease/acquire_halted.sql`, for
`ResequenceConfig::lease`, default `Lease::DEFAULT`, 10 s). With no row,
`crates/pges-sequencer/sql/lease/halted.sql` tells the refusals apart: `ResequenceError::NotHalted` if
`halted` is null, otherwise `LeaseHeld`.

A refusal changes nothing. It renews the lease at the lease's renew period (half the TTL by default)
(`crates/pges-sequencer/sql/lease/renew.sql`) while it replaces the slot and searches, then once more before
the final transaction, and not after. A renewal that finds the fence moved stops the run with `LeaseLost`.

**Slot.** It runs the statements of `open` up to the slot (database, stores, publication repair,
`pg_replication_slots`). A slot of another database or plugin is `Invalid`; an active one is `SlotInUse`.
Otherwise, on a replication connection checked with `IDENTIFY_SYSTEM`: `DROP_REPLICATION_SLOT pges_<oid>` if
the slot exists (42704 is ignored, 55006 is `SlotInUse`), then `CREATE_REPLICATION_SLOT pges_<oid> LOGICAL
pgoutput (SNAPSHOT 'export'[, FAILOVER true])`, with the old slot's failover unless `LogicalConfig::failover`
is `Set`. Any inactive slot of this database is dropped, valid or invalidated, so a failed run can be retried.

**Search**, in the exported snapshot (the replication connection runs nothing until it ends): `BEGIN`,
`crates/pges-sequencer/sql/transaction/repeatable_read.sql` and
`crates/pges-sequencer/sql/transaction/snapshot.sql`. Then, per non-outbox store: its window from its writer
limits, `max_txn_age + 2 × max_clock_skew` (`crates/pges-sequencer/sql/resequence/window.sql`); the floor (one
window before its newest log row's commit id) and the stop (two windows before)
(`crates/pges-sequencer/sql/resequence/bounds.sql`); and its categories with their resequenced ranges
(`crates/pges-sequencer/sql/resequence/categories.sql`). Per category, only when the log has a row, the
position after which to look: its newest log row with an id below the stop, outside its resequenced range, or
0 (`crates/pges-sequencer/sql/resequence/after.sql`). Then its commits from the floor (the nil uuid for an
empty log) with no log row after that position, in commit id order
(`crates/pges-sequencer/sql/resequence/orphans.sql`). Then `ROLLBACK`.

The commits found are merged per store in commit id order. If the search fails, the new slot is dropped on
the same replication connection afterwards.

**Write**, in one READ COMMITTED transaction: the fence check
(`crates/pges-sequencer/sql/resequence/hold_fence.sql`; 0 rows: `LeaseLost`); per store, its newest position
(`crates/pges-sequencer/sql/log/newest.sql`, at least `pges.stores.last_position` from
`crates/pges-sequencer/sql/log/store.sql`) and the new log rows in chunks of 10 000
(`crates/pges-sequencer/sql/log/insert.sql`); per store, the first and last position each category got
(`crates/pges-sequencer/sql/resequence/record_ranges.sql`); then
`crates/pges-sequencer/sql/transaction/synchronous_commit.sql` and
`crates/pges-sequencer/sql/resequence/finish.sql`, which stores the token (`source = 'logical'`, the
consistent point as 8 bytes big-endian), clears `halted` and releases the lease; then COMMIT.

- The snapshot splits commits exactly: everything committed before the consistent point is visible to the
  search, and everything after it is streamed by the new slot. While `halted` is set and the lease is held,
  nothing else writes `log`. The search runs apart from the write because lease renewals update
  `pges.sequencer`, which would fail a REPEATABLE READ write with serialization errors.
- Bound: every commit after the store's newest sequenced commit L has an id at most
  `max_txn_age + 2 × max_clock_skew` older than L's (the store's recorded limits), and every log row of such
  an id lies after the newest row with an id one more such window older. With one `max_clock_skew` a commit
  could be missed.
- Rows an earlier resequence wrote are in commit id order, not commit order, so they do not count as that
  newest row: on a second resequence a recovered row with an old id could otherwise stand for it, and a
  commit after it would be sequenced again.
- The recorded range lets retention scan the recovered rows whole (Retention, Partition maintenance). A range
  retention has already passed (`resequenced_through <= retention_scan_from`) is replaced, not widened.
- The coarse high-water marks (`pges.stores.last_position`, `categories.last_position`) are not written. The
  next term seeds its dedupe window with every log row above `pges.stores.last_position`, which includes
  every recovered commit, and moves the store's mark up once its source delivers a new commit (Sequencer
  batch, Dedupe window).
- A failure before COMMIT drops the new slot (new replication connection) and releases the lease
  (`crates/pges-sequencer/sql/lease/release.sql`), except on `LeaseLost`, where the slot may already belong to
  the resequence that took the lease over and is left for the next run to drop. A failed COMMIT is
  `CommitUnknown` and keeps the slot, because the token may already point at it. Running resequence again is
  safe either way: it answers `NotHalted` if the commit landed.

### Sequencer batch

**Positions at term start.** Whenever a term starts or restarts after an error, the leader reads every store
(`crates/pges-sequencer/sql/log/stores.sql`) and, per store, its newest position
(`crates/pges-sequencer/sql/log/newest.sql`) and each category's newest position
(`crates/pges-sequencer/sql/log/category_newest.sql`) from the log.

`schema_name` is checked against the store-name rule before it is interpolated (`RunError::InvalidStoreName`
otherwise). A store a source first names during a term is read with `crates/pges-sequencer/sql/log/store.sql`;
no row is `RunError::UnknownStore`.

**The batch.** One transaction per batch, fence first so a deposed leader fails before writing anything
(`crates/pges-sequencer/sql/lease/hold_fence.sql`; 0 rows: the lease was lost, roll back). The fence is
checked with a row lock, not an update, so a batch writes no new version of the hot row. Then come the
source's reads and writes (the outbox drain, below), then the log rows of each store with new rows
(`crates/pges-sequencer/sql/log/insert.sql`).

Positions continue from the store's newest log row, one per event, in source order; never from below
`pges.stores.last_position`, because `prune_log` can delete the newest rows (those of commits retention
dropped) and their positions must not be handed out again. Resequence starts from the same floor. The mark is
not written with the rows: a batch writes it with the token, when `token_interval` is due or at shutdown, and
resequence does not write it (Resequence, above). So a leader that stops uncleanly leaves rows above it, and
`prune_log` and the log partition drop keep every row above the mark (Retention, Log partitions of dropped
commits): the newest row then survives, and the next term continues after it. A commit already in the dedupe
window (below) is skipped. A takeover's `UPDATE … SET fence = fence + 1` waits for the
in-flight batch and fences out every later one.

A log row needs a log partition. Without one the `INSERT` fails, the batch rolls back (the outbox rows stay
in `pending`), and the sequencer retries every `retry_interval`, reported as `Status::Recovering`, until
`Admin::maintain` creates it. `maintain` must run regularly: it creates the next log partition when the store
passes half of the newest.

After each committed batch that wrote rows, and at term start, the leader reads the WAL position it announces
as `HighWater::lsn()`: `crates/pges-sequencer/sql/log/wal_insert_lsn.sql`.

This is the insert position, not `pg_current_wal_lsn()` (the write position): with `synchronous_commit = off`
the write position can be behind the batch's own commit record.

The resume token and the high-water marks are not written per batch:

- **Token, option (a): replication origins** (not built; for the logical source, which uses option (b)).
  `pg_replication_origin_session_setup($slot)` once per connection, `pg_replication_origin_xact_setup(end_lsn,
  commit_ts)` in each batch transaction; on restart, read `pg_replication_origin_progress($slot, true)`. The
  token then commits atomically with the log rows. Needs the origin functions granted to the sequencer role,
  which some managed services do not allow.
- **Token, option (b): coarse** (built; the default). Once `token_interval` (default 1 s;
  `TokenInterval::EveryBatch` stores it with every batch) has passed since the last stored token, in the batch transaction:
  `crates/pges-sequencer/sql/transaction/synchronous_commit.sql` (the source confirms the slot once this
  commits; flushing it also flushes every earlier batch), `crates/pges-sequencer/sql/lease/store_token.sql` (0
  rows: fenced, roll back), then, per store whose marks moved,
  `crates/pges-sequencer/sql/log/set_category_positions.sql` and
  `crates/pges-sequencer/sql/log/set_store_position.sql`. When the leader is idle with progress not yet
  stored, the same statements run in a transaction of their own once `token_interval` has passed, and again on
  graceful shutdown. The source is acknowledged (`ack`) only after that transaction commits, so the slot is
  confirmed only up to the persisted token, and a restart replays at most about `token_interval` of commits.
  The outbox stores `source = 'outbox'` with an empty token. `SET LOCAL synchronous_commit = on` matters when
  the session runs with it off: an asynchronous token commit could be lost in a server crash after the slot
  was confirmed past it, together with the log rows it covers. It costs one statement per stored token. It
  also lowers a session set to `remote_apply` to `on`; durability holds either way.
- **Dedupe window.** Per store, the leader keeps the ids of the commits it sequenced since the last durable
  token, plus `SequencerConfig::dedupe_tail` (default 1024) older ones. It is seeded whenever a term starts or
  restarts after an error, with the newest `dedupe_tail` ids (`crates/pges-sequencer/sql/log/tail.sql`) and
  the ids above `pges.stores.last_position` (`crates/pges-sequencer/sql/log/above.sql`). After each stored
  token it is trimmed back to the newest `dedupe_tail` ids of each store, except the rows above the mark:
  an earlier term sequenced them after the durable token (it crashed before storing one), so the source
  delivers them again first. They stay until the source has replayed them: a replayed one means every row
  before it was replayed too (the earlier terms sequenced them in the order the source delivers them), and a
  new commit means the replay is over. Until then the store's mark is written just below the oldest of them,
  not at the newest position, so a term that stores a token part-way through the replay leaves the rest
  above the mark for the next term. Without this, a token stored during a replay longer than `dedupe_tail`
  let the rest of the replay be sequenced a second time.
- **High-water marks.** `categories.last_position` and `pges.stores.last_position` are written with the
  coarse token (`pges.stores.last_position` held below a replay, above). The watch carries the live values, which the leader derives at term start from `log` (above)
  and keeps in memory.

**NOTIFY (`watch-notify`).** After a batch that wrote rows committed and the WAL insert LSN was read, the
notifier sends, concurrently with sequencing (a future polled by `Sequencer::run`), on another pool
connection, in an implicit transaction of its own: `crates/pges-sequencer/sql/notify/send.sql`.

- One row per store the batch wrote to. The channel is `pges_<store schema name>` (at most 45 bytes). The
  payload is `high_water::Notice::encode` (api.md, Wake-ups): `1;<store position>;<lsn>;<id>:<position>,…`
  for the categories the batch touched, or `*` instead of the list when the payload would exceed 7999 bytes.
- Notifications still waiting are merged per store: the newer high-water mark, and the union of the touched
  categories. Quick successive batches may therefore produce one notification.
- A failure is logged with a warning and dropped; it never fails or delays the log write. Notifications
  still queued when `run` returns are dropped.

**Outbox stores.** The outbox source lists them on open and every `OutboxConfig::store_refresh` (default
10 s): `crates/pges-sequencer/sql/outbox/stores.sql`.

In the batch transaction, after the fence check, it drains each outbox store with the batch budget that is
left, starting at a store that rotates from batch to batch, so one busy store cannot starve the others
(`crates/pges-sequencer/sql/outbox/drain.sql`).

`DELETE … RETURNING` has no order guarantee; positions are assigned in `seq` order. A test commits appends
out of `seq` order inside one drain batch and checks the positions. The rows disappear in the same
transaction as their log rows are written, so a rolled-back batch leaves them in place.

Between batches the source waits for a notification. A trigger on each outbox store's `pending` notifies
channel `pges__outbox` with the store's schema name as payload, once per transaction that enqueued rows
(Migrations, Outbox wake-ups). The source listens on a connection of its own, opened from its pool's connect
options. It listens before its first drain after opening, and again, followed by an immediate drain, after
losing that connection. On a notification it drains until a drain returns less than its budget. A notification
from an unknown store reads the store list at once. As a fallback for lost notifications, after
`OutboxConfig::recheck_interval` (default 1 s) without one it probes the queue heads on the listening
connection itself. The probe is built in code from fragments:
`crates/pges-sequencer/sql/outbox/probe_head.sql`, then `crates/pges-sequencer/sql/outbox/probe_store.sql` per
outbox store, joined by `crates/pges-sequencer/sql/outbox/probe_union.sql`, then
`crates/pges-sequencer/sql/outbox/probe_tail.sql`. With no outbox store it is
`crates/pges-sequencer/sql/outbox/probe_none.sql`. The registry holds the two-store shape, which the registry
test checks, and a unit test checks that the code (`Rendered::compose`) builds exactly that.

If that probe fails or gets no answer within the recheck interval (at least 1 s), the connection is treated
as lost: the source listens on a new one and drains. This finds a connection that died without a reset. It
stops listening when its term ends (`Source::close`).

Stores without the outbox
option are not drained: a database is sequenced by one kind of source, and store creation refuses to mix
outbox and other stores in one database (`PGE02`, Migrations). The logical source still skips outbox stores
defensively.

### Checkpoint (Postgres projections)

A subscription has one row per shard, keyed by `(name, shard)`; an unsharded one is one shard, row 0, and
`.sharded(1)` is the same definition. `pges-subscribe` creates every row on start, unless the subscription
exists (`crates/pges-subscribe/sql/subscription/create.sql`), with the start position, the number of shards
and the requested category ids, sorted (`'{}'` for all categories). `Start` applies only here and on a
redefine; an existing subscription resumes from its stored cursors. A 23505 means another instance created it
at the same moment, and is ignored. Shard 0 is in every definition, so two concurrent creates conflict as a
whole instead of leaving a mix of definitions.

Take over the lapsed lease of the lowest free shard below the requested count (1 when not sharded), with
the same predicate as the sequencer, under a UUIDv7 lease owner chosen per run
(`crates/pges-subscribe/sql/subscription/acquire.sql`).

- `SKIP LOCKED` hands different rows to concurrent instances. It also skips a row locked by an in-flight
  checkpoint; that row's lease is held anyway. The outer `UPDATE` matches one row by primary key, so it never
  waits on another shard's checkpoint.
- The definition is checked on the acquired row: a changed category set fails with `DefinitionChanged`,
  then a changed number of shards with `ShardsChanged`, unless `.redefine(start)` is given.

A redefine replaces the definition and cursor of every row in one statement, bumps the fences, clears the
leases and inserts missing shards (`crates/pges-subscribe/sql/subscription/redefine.sql`). It changes nothing
when the stored definition already matches, so two instances redefining at the same moment do not move the
cursors twice.

- The redefine is not fenced and returns no rows. It ends the instance's term, and the instance takes a
  lease again; a standby may take it in between.
- Rows are never deleted. After a shrink, rows with `shard >= shards` stay as retired rows, so fences only
  grow.

`run_pg` runs the checkpoint in the handler's transaction, after the handler; `run` runs it in autocommit
after the handler. It is fenced and moves the cursor only from where the instance holds it: 0 rows means the
lease was lost or the stored cursor moved, `run_pg` rolls the batch back, and the instance reads the stored
cursor again to tell which (`crates/pges-subscribe/sql/subscription/checkpoint.sql`). When less than half the
lease is left, the checkpoint also renews it (`crates/pges-subscribe/sql/subscription/checkpoint_renew.sql`),
which is also refused once a migration dropped support for the crate (Migrations). The stored cursor then has
not moved and the fence is in place, so a refused checkpoint that finds both where the instance holds them
ends the term as a lost lease, which checks the scopes; reading on from the same cursor would read the same
page and be refused again, forever. This holds for an empty page that only moves the cursor as well.

While caught up and idle, while a handler runs and while a failed batch waits for its retry (in autocommit, on
another pool connection), the instance renews its lease (`crates/pges-subscribe/sql/subscription/renew.sql`),
and on shutdown or a fatal error it releases it (`crates/pges-subscribe/sql/subscription/release.sql`). Both
are fenced.

After any retried error (database or waker) the subscriber reads the stored cursor before its next read
(`crates/pges-subscribe/sql/subscription/cursor.sql`), because a COMMIT whose reply was lost may have moved
it. No row means the fence moved on, and the instance stands down. Such a COMMIT can also become visible only
after that read (the client saw the error while the server still finished it); the next checkpoint then finds
the cursor moved, waiting on the COMMIT's row lock if it is still in flight, and the batch rolls back.

An instance of a sharded subscription records itself in `subscription_members` every `min(acquire_interval,
lease / 2)` and counts the live instances, itself included
(`crates/pges-subscribe/sql/subscription/heartbeat.sql`, which also deletes lapsed members). Its share is the
number of shards divided by that count, rounded up. It releases its highest shards above the share, after
their current batch, and takes the lowest free ones below it. On stop it leaves
(`crates/pges-subscribe/sql/subscription/leave.sql`).

A dead instance's member row and leases lapse after the lease, and the others take its shards.

- Leases are renewed only when `lease_until - now() < lease / 2`, piggybacked on a checkpoint where possible.
  In `run_pg` the renewing checkpoint runs in the handler's transaction, so `now()` is the transaction's
  start time; the client counts the new deadline from before `BEGIN`, not from the checkpoint.
- While a handler runs and while transient retries back off, the lease is renewed with the renewal statement
  above in autocommit, on another pool connection, once less than half is left. 0 rows means the fence
  moved: the batch is rolled back (`run_pg`) or not checkpointed (`run`) once the handler returns. A failed
  side renewal is tried again after a lease/8; a lapsed lease is kept safe by the fenced checkpoint. The
  checkpoint uses the plain statement instead of the renewing one when counting from the transaction's
  start would not move the deadline, so it never shortens a lease that a side renewal extended.
- A 25P02 from the checkpoint means the handler left the transaction aborted (a statement failed and the
  handler ignored the error). PostgreSQL would turn the COMMIT into a ROLLBACK, so the batch is rolled back
  and counted as a transient handler failure, never as handled. No extra `SELECT 1` is needed, unlike
  `UnitOfWork::commit`.
- A batch whose cut moved the cursor without events (other categories took the positions) is checkpointed
  in autocommit, without calling the handler.

### Backfill

`store.backfill(name)` keeps one `backfills` row per lane. A stream's lane is `stream_id` (as a 128-bit
integer) modulo the lane count; persisted progress depends on it, so it never changes.

- The first run creates the lanes (`crates/pges-subscribe/sql/backfill/create.sql`). Every run reads the
  definition and the lane cursors (`crates/pges-subscribe/sql/backfill/lanes.sql`).
- For `Until::Subscription`, `P` is the subscription's cursor, the lowest of its live shards
  (`crates/pges-subscribe/sql/backfill/subscription.sql`).
- A lane's checkpoint runs in the handler's transaction (`crates/pges-subscribe/sql/backfill/checkpoint.sql`);
  0 rows means another run moved it (`Overtaken`), and the transaction rolls back. Lanes with no events in the
  page are moved in autocommit (`crates/pges-subscribe/sql/backfill/advance.sql`).

- Pages use "Read a category from a cursor" with `$3 = P`. `P` is at most `H`, which the run read once at its
  start (`UntilAhead` otherwise), and positions only become visible, never invisible, so pages do not read
  `H` again. Each category is read after its own
  floor, `$2` = max(the lowest lane cursor, the category's retention horizon): before every page the
  backfill reads its categories' horizons (`crates/pges-subscribe/sql/read/horizons.sql`). Events below a
  horizon are gone, so it skips them instead of failing. The read statement still checks its `$2` against
  the horizon; if retention raised one between the two statements, the page is read again. Nothing exists
  below a floor, so the merge cut stays valid. Backfills always read unsharded, from the primary. Each lane
  skips events at or below its own cursor.
- For `Until::Subscription`, retired shard rows are ignored; no row means `UnknownSubscription`.
- `P` is fixed at creation. A run with other categories, another lane count or another explicit `P` is
  refused (`DefinitionChanged`). `UntilAhead` (`P` past `H`) is checked before the lanes are created, so a
  refused end is never stored.
- A `P` inside a commit stops before that commit.

## Partition maintenance

A crate task (`Admin::maintain`) runs on a schedule; it is idempotent. It, `Admin::register_category`,
`Admin::change_bucket`, `Admin::set_retention`, `Admin::set_default_retention`, `Admin::apply_retention`,
`Admin::prune_log`, `Admin::set_writer_limits` and `Admin::freeze_closed_partitions` (once per partition)
take `pg_try_advisory_lock(hashtext('pges.maintain.' || $1))` (`$1` the store) on their own connection, then run
`SET TimeZone = 'UTC'`, `SET lock_timeout = '1s'` and `SET jit = off` (the planner's cost for retention's
per-stream loops made it JIT-compile them for 0.6–0.8 s per statement), and retry DDL that times out (55P03)
with backoff. The statements: `crates/pges-postgres/sql/maintain/lock.sql`,
`crates/pges-postgres/sql/maintain/session.sql` and `crates/pges-postgres/sql/maintain/unlock.sql`.

The lock is tried, not waited for in the server, before `SET lock_timeout` (a second maintainer must wait for
the first, not fail after a second). A busy try returns the connection to the pool and sleeps 10 ms, doubling
up to 250 ms, so operations waiting for another maintainer hold a pooled connection only for each try and
cannot drain the store's pool (`Store::admin` uses it). A try's connection is closed instead if its future
is dropped before the try returns, since it may hold the lock. After the operation's lock wait
(`Admin::with_lock_wait`, 60 s by default) it fails with its error's `LockTimeout` variant, naming the holder
from `pg_locks` and `pg_stat_activity` (`crates/pges-postgres/sql/maintain/holder.sql`). A dedicated
connection outside the pool was rejected: it would skip the pool's `after_connect`, which may `SET ROLE`.

Leaves that already exist get no DDL. The `bound` constraint is dropped only from attached leaves that still
have it (an interrupted creation), found with one catalog query over the category's leaves
(`crates/pges-postgres/sql/leaf/unfinished.sql`), because `ALTER TABLE` takes ACCESS EXCLUSIVE on the leaf
even when there is nothing to drop. A leaf left detached by an interrupted creation
(`crates/pges-postgres/sql/partition/state.sql`) is dropped and created again. The same statement reads the
parent of a name that is a partition: a partition counts as present only under the parent maintenance means
(`commits` for a category partition, `commits_<cat>` for a leaf, `log` for a log partition). A name taken by
anything else (a partition of another parent, an index, a sequence) fails registration, `change_bucket` and
`maintain` with `NameTaken`, and nothing is dropped, skipped or attached under it. The names of the indexes
created with a partition, the category partition's primary key `commits_<cat>__pkey` and a leaf's
`<leaf>_s`, are checked the same way before the partition is created (the leaf's after a detached leaf is
dropped, which drops its own index), so a taken one fails with `NameTaken` naming it instead of SQLSTATE
42P07, and no leaf is left detached.

`Admin::register_category` runs in one transaction: the `categories` row, then the category partition and its
leaves. No store sees the category before its partitions exist; an append in between fails with
`UnknownCategory`. The row: the store's default retention, read only when `CategoryConfig::retention` is
`None` (`crates/pges-postgres/sql/retention/store_default.sql`); the insert, which leaves an existing category
unchanged (`crates/pges-postgres/sql/category/insert.sql`); then its id and bucket
(`crates/pges-postgres/sql/category/select.sql`).

The retention comes from `CategoryConfig::retention`, or from the store's default when it is `None` (`Forever`
if the store has no row). An existing category keeps its retention; `Admin::set_retention`
(`crates/pges-postgres/sql/retention/set_category.sql`; no row: `UnknownCategory`) changes it, and
`Admin::set_default_retention` (`crates/pges-postgres/sql/retention/set_default.sql`; no row: `StoreNotFound`)
changes the default for categories registered later.

Inside the transaction a DDL lock timeout (55P03) aborts the transaction, so it cannot be retried per
statement. The whole transaction is rolled back and retried with the same backoff (8 attempts).
`Admin::maintain` and `Admin::change_bucket` run in autocommit and retry each statement.

New partitions are created detached and then attached. `CREATE TABLE … PARTITION OF` takes ACCESS EXCLUSIVE
on the parent, and a request queued behind a long catch-up read would block every append to the category
(or to the store, for the root and for `log`). `ATTACH PARTITION` takes SHARE UPDATE EXCLUSIVE, and a
matching CHECK constraint lets it skip the validation scan. Each attach still invalidates cached plans.

- **Category registration** attaches an empty partitioned `commits_<cat>`: created detached and partitioned by
  commit id, with its primary key `(commit_id)` named `commits_<cat>__pkey` (the category's: `commits` has no
  key, and `category_id` is constant in the partition; each leaf gets a copy when it is attached)
  (`crates/pges-postgres/sql/category/create_partition.sql`), then attached under the category's id
  (`crates/pges-postgres/sql/category/attach_partition.sql`).
- **Time leaves**, from the later of the newest leaf's upper bound and the bucket holding `now() - 1 hour`, up
  to three buckets after the current one (`admin::LEAVES_AHEAD`), so retries have slack
  (`crates/pges-postgres/sql/leaf/to_create.sql`). The newest upper bound is read from the catalog. Leaves are
  named `commits_<cat>__<suffix>` with the bucket's suffix format (`YYYYMMDD`, `YYYYMM`). A first leaf whose
  lower bound is not a bound of the bucket (after a bucket change) ends at the next bound and is named by its
  lower bound as `YYYYMMDD`. Each returned leaf that is not attached yet is created detached
  (`crates/pges-postgres/sql/leaf/create.sql`, with `autovacuum_freeze_min_age = 0` and
  `autovacuum_vacuum_insert_scale_factor = 0.05`), gets lz4 payload compression
  (`crates/pges-postgres/sql/leaf/compress.sql`), its unique `(stream_id, last_version)` index `<leaf>_s`
  (`crates/pges-postgres/sql/leaf/create_index.sql`) and a `bound` CHECK matching its partition bounds
  (`crates/pges-postgres/sql/leaf/add_bound.sql`), is attached (`crates/pges-postgres/sql/leaf/attach.sql`),
  and loses its `bound` again (`crates/pges-postgres/sql/partition/drop_bound.sql`). Bounds are derived as
  `date_trunc('month', ts AT TIME ZONE 'UTC') AT TIME ZONE 'UTC'` (or the bucket's unit) and always written
  with an explicit offset, as in `pges.uuidv7_floor('2026-12-01 00:00:00+00')`. A change to
  `categories.bucket` goes through `Admin::change_bucket`
  (`crates/pges-postgres/sql/category/set_bucket.sql`), which then creates the leaves the new bucket needs. It
  applies from the upper bound of the newest leaf, so leaves never overlap or leave gaps while maintenance
  runs. With leaves kept three buckets ahead, it takes effect only after the existing ones run out (up to
  about four months for a change away from `Month`). From `Day` to `Month` in mid-month, one bridging leaf
  runs to the first of the next month. `register_category` never changes an existing bucket.

  The unique index is per leaf because a partitioned unique index must include the partition key. It backs
  up the `streams` row lock within a time bucket, and serves stream reads. Aim for at most about 100-200 live
  leaves per category and choose `bucket` accordingly.
- **Log partitions**, 10^8 positions each, created the same way (CHECK on the position range, same
  reloptions): `log_0000` first, then the next one when the store passes half of the newest one. The
  statements: `crates/pges-postgres/sql/log/newest_partition.sql`,
  `crates/pges-postgres/sql/log/last_position.sql`, `crates/pges-postgres/sql/log/create_partition.sql`,
  `crates/pges-postgres/sql/log/add_bound.sql`, `crates/pges-postgres/sql/log/attach_partition.sql` and
  `crates/pges-postgres/sql/partition/drop_bound.sql`.

  `log_by_category` leaves sit at about 55% density (about 90 B per row) under the sequencer's insert
  pattern, which interleaves categories; the primary key on `last_position` stays at about 90% (22.5 B).
  That index is about 18–19% of the storage of a single-event commit (design.md, Scale run).
- **Closed partitions**: `Admin::freeze_closed_partitions`, separate from `maintain`. Closed means a time leaf
  whose upper bound is more than `max_txn_age + 2 × max_clock_skew` in the past
  (`crates/pges-postgres/sql/freeze/closed_leaves.sql`, leaving out leaves being detached), or a full log
  partition other than the newest. Such a partition is frozen with `VACUUM (FREEZE, ANALYZE)`
  (`crates/pges-postgres/sql/freeze/vacuum.sql`) only when its all-frozen pages are fewer than its pages, or
  it holds dead rows (`pg_stat_get_dead_tuples`, table plus TOAST;
  `crates/pges-postgres/sql/freeze/partitions.sql`): a `delete_stream` in a frozen leaf clears the page's
  all-frozen bit, which `relallfrozen` does not show until a vacuum. All-frozen pages are `relallfrozen` of
  the table and its TOAST table; pages come from `pg_relation_size`, so a table never vacuumed counts in full.
  Vacuum skips the pages already all frozen. Each partition takes the maintenance lock in its own session, and
  a vacuum that keeps timing out on its lock is reported in `FreezeReport::busy`. Measured at 10M streams: a
  never-frozen 125 MB leaf took 0.8–1.9 s and wrote 31.7 MB of WAL; a run with nothing to freeze takes about
  15 ms. `Admin::partition_ages` lists every partition of the store with its pages, all-frozen pages, dead
  rows, `age(relfrozenxid)` and `mxid_age(relminmxid)` (the greater of the table and its TOAST table) for
  alerting. It reads only the catalog and takes no maintenance lock, so it answers while maintenance,
  retention or a freeze runs.

  Leaves and log partitions are created with `autovacuum_freeze_min_age = 0`, which in the scale run froze
  commit leaves while they were written (99.7–100% all-frozen) but left log partitions at 82–95% and one at
  14%. Measured: `VACUUM (FREEZE)` of an
  all-frozen 1.2 GB leaf took 7 ms (1 page scanned, 2 KB of WAL); of an unfrozen 1.98 GB log partition 4.3 s,
  219k pages and 916 MB of WAL (one full-page image per page, lz4). An unconditional freeze of every closed
  partition would cost terabytes of WAL at 10B events.
- **Retention**: `Admin::apply_retention(&RetentionConfig)`, outside a transaction, for categories with
  `retention` 1 (Window then Drop) or 3 (SnapshotAndTruncate); code 2 (Window then Archive) is refused as
  `UnsupportedPolicy`. It is separate from `maintain`, which takes no arguments and never drops data; run both
  on the same schedule. First it fails with `SequencerHalted` while `pges.sequencer.halted` is set (no slot
  loss awaiting `resequence`). Then it reads the store's writer limits; every `max_txn_age` and
  `max_clock_skew` below is the store's recorded value, not configuration
  (`crates/pges-postgres/sql/limit/recorded.sql`; no row: `StoreNotFound`), and checks each of
  `RetentionConfig::writer_roles` against that `max_txn_age` (Writer limits, Writer roles); a failure is
  `RetentionError::WriterRole`.

  Then, per policy category (`crates/pges-postgres/sql/retention/categories.sql`), it drops the leaves left by
  an interrupted run: detached tables named like the category's leaves (`^commits_<cat>__[0-9]{6}([0-9]{2})?$`)
  and older than its oldest attached leaf (`crates/pges-postgres/sql/retention/detached_leaves.sql`), each
  with `crates/pges-postgres/sql/partition/drop.sql`.

  The eligible leaves, oldest first (`crates/pges-postgres/sql/retention/leaves_past.sql`): leaves whose upper
  bound is older than `retain_for + max_clock_skew`, and more than `max_txn_age + 2 × max_clock_skew` in the
  past. The append-side skew check means no new row can route into such a leaf. A leaf already "detach
  pending" was recorded by an earlier run and is only detached and dropped. For the others, oldest first,
  stopping at the first refusal:

  1. Is the leaf fully sequenced? The range read, the stop position and the scan run in one REPEATABLE READ,
     READ ONLY transaction (`crates/pges-postgres/sql/retention/snapshot.sql`), starting with the range
     resequence wrote (`crates/pges-postgres/sql/retention/resequenced.sql`). The stop position
     (`crates/pges-postgres/sql/retention/scan_stop.sql`) is the first log row of a commit that started after
     every commit of the leaf ended; with none, the scan runs to the end of the log. If `resequenced_through`
     is above `retention_scan_from`, the stop position becomes the greater of the two. Then one statement
     (`crates/pges-postgres/sql/retention/scan.sql`) reads the category's log rows from `retention_scan_from`
     to the stop position and returns the leaf's horizon (the newest position of a commit below its upper
     bound), `sequenced` (every commit of the leaf has a log row), and `next_scan_from`: the newest position,
     outside the resequenced range, of a commit older than the leaf's upper bound minus `max_txn_age + 2 ×
     max_clock_skew`.

     `sequenced` is `NOT EXISTS (leaf EXCEPT seen)`, not an anti-join: a droppable leaf gives the check no
     row to stop at, and an anti-join planned for its first row became a nested loop over the leaf and the
     scanned rows (148 s against 50 ms on a 67k-commit leaf at 10M streams). The `next_scan_from` bound, and
     `scan_stop.sql`'s bound, are scalar subqueries, computed once.

     The scan covers about one leaf of the category's log rows plus `2 × (max_txn_age + 2 ×
     max_clock_skew)`. It is bounded because every transaction is capped by `max_txn_age`
     (`transaction_timeout` on the writer role) and every commit id is within `max_clock_skew` of its
     statement: a commit whose id is past the stop bound started after every commit of the leaf committed, so
     it was sequenced after them. Every commit of the next leaf is positioned after the last row below the
     `next_scan_from` bound. If `sequenced` is false the leaf is refused (`NotSequenced`) and tried again on
     the next run.

     Positions follow commit order everywhere except inside the range resequence wrote, which is in commit
     id order and may hold commits older than rows before it. So the scan covers that range whole, and
     `next_scan_from` ignores rows inside it: a row there with an old id can sit after rows of commits in
     later leaves. Until retention passes the range, each run scans from the scan start through it again.

     The range is read in the same snapshot as the log. A resequence that committed between reading the
     range and the scan would add rows the scan treats as being in commit order, and `next_scan_from` could
     land past rows of commits that stay. One that commits after the snapshot only adds rows past the log
     tail, which this scan does not see and the next run covers.
  2. In one transaction: `crates/pges-postgres/sql/retention/halted.sql` again (`SequencerHalted`), the policy
     check, then the floors and the horizon. The policy check reports at most `REPORTED_STREAMS` (100)
     streams. Window looks for long-lived streams (`crates/pges-postgres/sql/retention/live_streams.sql`,
     `LiveStreams`); SnapshotAndTruncate for streams without a snapshot at or above their last version in the
     leaf (`crates/pges-postgres/sql/retention/missing_snapshots.sql`, `MissingSnapshots`). Any row refuses
     the leaf. Otherwise, for both policies, each stream's floor is raised to its last version in the leaf
     (`crates/pges-postgres/sql/retention/raise_floors.sql`), and the category's horizon to the scan's horizon
     (`crates/pges-postgres/sql/retention/raise_horizon.sql`; NULL leaves it).

     Both checks look each distinct stream of the leaf up on its own (`CROSS JOIN LATERAL (… LIMIT 1)`), so
     they cost O(leaf streams) and never rely on their LIMIT stopping early: a droppable leaf has no such
     stream, and as joins they merged against every `streams` (or `snapshots`) partition (196 s per leaf at
     100M). `missing_snapshots.sql` probes `snapshots` first and `streams` only for streams without one.
     `raise_floors.sql` runs between `crates/pges-postgres/sql/retention/probe_joins.sql` and
     `crates/pges-postgres/sql/retention/plan_joins.sql` (`SET LOCAL enable_hashjoin`/`enable_mergejoin` off,
     then back to `DEFAULT`), so it updates each stream through its key; a hash join read every `streams`
     partition (43 s per leaf at 100M, 4.2 s against 0.5 s at 10M). The plan relies on realistic `streams`
     statistics: if `streams` is estimated at only a few thousand rows, the planner puts it on the outer side
     of the nested loop (O(streams × leaf streams)). In the same transaction,
     `crates/pges-postgres/sql/retention/forget_deleted.sql` deletes the category's `deleted_commits` rows
     with a commit id below the leaf's upper bound whose log row lies in (`retention_scan_from`, the new
     horizon]. Once the horizon commits, no store-wide read reaches those log rows. A deleted commit whose log
     row is not written yet stays recorded, because that row may land above the horizon.
  3. Detach without blocking writers, then drop, then raise the scan start (only when `next_scan_from` is not
     null): `crates/pges-postgres/sql/partition/detach_pending.sql` (no row: already detached),
     `crates/pges-postgres/sql/partition/detach_concurrently.sql`
     (`crates/pges-postgres/sql/partition/detach_finalize.sql` if the detach is pending),
     `crates/pges-postgres/sql/partition/drop.sql`, then
     `crates/pges-postgres/sql/retention/raise_scan_from.sql`.

     The detach is retried on 55P03 (8 attempts,
     backoff from 50 ms to 2 s), reading `inhdetachpending` before each attempt: a detach that failed in its
     second phase must be finished with `FINALIZE`. A detach that fails in either phase leaves the horizon
     recorded and the scan start unmoved, so the next run finishes it.

  - The horizon and the floors are recorded before anything disappears, so a subscription behind the horizon
    fails with `CursorBeforeHorizon` and a read below a stream's floor with `Truncated`. Window raises the
    floors too: otherwise a stream whose commits were dropped would read as empty or partial.
  - `retention_scan_from` moves only after the drop. A run interrupted before the drop scans the leaf again
    from the old start. If the start had already moved, the leaf's commits would look unsequenced and the
    leaf would be refused forever, and `prune_log` (which deletes up to `retention_scan_from`) could delete
    log rows of commits that still exist.
  - Window refuses a leaf while any stream with commits in it is not deleted and had a commit within `keep`.
    A stream that only crosses a bucket boundary delays the drop until it has been quiet for `keep`; one
    that keeps getting appended to blocks it. SnapshotAndTruncate needs, for every such stream, a snapshot
    of any state version in the same category at or above its last version in the leaf.

  Cost at scale (found and fixed before 0.1.0; design.md, Scale run). Before the rewrites above, three
  statements made one leaf cost minutes to hours on a large store:

  - `retention/scan.sql`: `sequenced` is `NOT EXISTS (leaf rows WHERE NOT EXISTS (log rows ...))`. EXISTS is
    planned for its first row, and the planner expects unsequenced commits, so it can pick a nested-loop
    anti-join with zero startup cost. In the success case (fully sequenced) that loop runs to the end, at a
    cost of leaf rows × log rows scanned: 65–82 s per leaf of 67k commits at 10M streams, and an estimated
    4–5 h per leaf of 675k commits if the plan flips at 100M (it got a hash plan there, 1.2 s). A full count
    (`(SELECT count(*) … WHERE NOT EXISTS (…)) = 0`) got a parallel hash anti-join: 15 ms against 76 s on
    the same leaf.
  - `retention/live_streams.sql`: the planner expects live rows and plans for `LIMIT` to stop early, but a
    leaf that is safe to drop has none, so the scan runs to the end. At 100M streams it chose a merge join
    against all 64 `streams` primary keys and walked every stream: 196 s per leaf (10M streams: a nested
    loop, 0.15–0.49 s). A per-stream `LATERAL` probe over the leaf's distinct stream ids took 5.8 s.
  - `retention/raise_floors.sql`: a hash join over a sequential scan of every `streams` row, so O(all
    streams) per leaf: 36–45 s at 100M, 1.3–2.6 s at 10M. A nested loop (a per-stream update) took 11 s
    against 43 s at 100M.

  In all, about 4 minutes per daily leaf at 100M streams with the plans the run got; the refusal path (a leaf
  with live streams) took 32 ms. After the rewrites, a full run at 10M streams (`jobs`, keep 7 days, 22
  leaves dropped and 1 refused) went from 1474 s to 42 s (12.7 s on a quieter box): `scan` 62 s → 27 ms per
  leaf, `raise_floors` 1.3 s → 0.3–0.6 s, `scan_stop` 99 → 4 ms. `live_streams` is serial and latency-bound
  on a cold cache (0.2–0.8 s per leaf, 11 s on the first leaf right after the fill).
- **Log partitions of dropped commits.** Log partitions are ranged by position across all categories. Both
  `apply_retention` and `prune_log` end by dropping, oldest first, every log partition that is not the newest,
  is full (the store's last position, `crates/pges-postgres/sql/log/last_position.sql`, at or above its upper
  bound − 1) and has no row above its own category's `retention_scan_from`, nor above the store's coarse
  high-water mark (`crates/pges-postgres/sql/log/pinned.sql`; a partition already "detach pending" is dropped
  without the check). The partitions are listed by `crates/pges-postgres/sql/log/partitions.sql`, then
  detached and dropped as leaves are.

  A category with no rows in a partition does not pin it, but a `Forever` (or `SnapshotAndTruncate`) category
  with rows in it does, so such a category pins every log partition it writes to. Use a separate store for
  high-volume short-lived categories. Stores that must mix them can run `Admin::prune_log(category)`: it reads
  the category's `retention_scan_from` (`crates/pges-postgres/sql/retention/prune_category.sql`; none:
  `UnknownCategory`), then, per log partition whose lower bound is at or below that position (skipping one
  "detach pending"), deletes in batches of 10 000 (`crates/pges-postgres/sql/log/prune.sql`) and runs `VACUUM
  (ANALYZE)` on a partition that lost rows (`crates/pges-postgres/sql/log/vacuum.sql`).

  The delete goes up to `retention_scan_from`, not `horizon_position`: rows between the two can belong to
  commits of the next leaf, which still exists, and deleting them would make that leaf look unsequenced
  forever. Rows at or below `retention_scan_from` only name commits retention already dropped, as long as
  writers keep to the store's limits. The delete also keeps any row whose commit is still in the category
  partition, even if writers did not keep to them: without its log row, resequence would take that commit for
  an unsequenced one and sequence it again. Pruned partitions are no longer insert-only.

  Neither the delete nor the partition drop removes a row above `pges.stores.last_position`. The sequencer
  continues from the newest log row, but at least from that mark, and writes the mark with its token, not
  with the rows: about once a second, at shutdown, and not at all after a crash or in resequence. Without the
  guard, a leader that sequenced old backlogged commits and stopped before writing the mark could leave their
  rows as the store's newest; retention drops their leaf, the scan reaches those rows, and `prune_log` deletes
  them; the next term then starts again from the mark and hands their positions to new commits (the outbox
  source never delivers a drained commit again). Rows above the mark stay until the sequencer writes the mark
  above them, which waits until the source has replayed them (Dedupe window, above), and are pruned then
  (`prune_log_keeps_the_rows_above_the_coarse_mark`, `a_log_partition_with_rows_above_the_coarse_mark_stays`,
  `positions_continue_after_a_crash_before_the_mark_and_prune_log`).

  Dropping a whole log partition relies on `retention_scan_from` alone, with no such existence check, which
  would scan the whole partition (not yet addressed).

## Roles

Role names are up to the deployment; the crate needs these capabilities. Integration tests run as a
superuser. `crates/pges-subscribe/tests/roles.rs` runs a non-superuser owner, a writer, a sequencer (each
source) and a reader with exactly the grants below; a few other tests run statements under a narrower role
(a writer denied one `streams` partition, a store creator that does not own the publication). Operations the
roles test does not run (retention, resequence, snapshots) are not checked statement by statement.

**Every role that can connect is trusted for availability.** Any role that can connect to the database may
take any advisory lock, and hold it for as long as its session lasts. pges reserves these keys (each an int8
key from `hashtext`, in this database):

| Key | Held by | Effect of a squatter |
|---|---|---|
| `hashtext('pges.migrate')` | the first shared migration, before `pges.scopes` exists | `run_shared` on an empty database fails with `MigrateError::LockTimeout` |
| `hashtext('pges.maintain.' \|\| <store>)` | every `Admin` operation but `partition_headroom`, per store | the operation fails with its `LockTimeout` variant: `maintain` stops running, and partitions run out |

The migration lock itself is a row of `pges.scopes`, which only roles with `UPDATE` on the table can lock, so
a role that can only connect cannot block migrations or store creation. No lock wait is unbounded: a squatter
causes errors that name its pid and login role (`lock::LockTimeout`), never a hang, and never wrong data. (pges-testing also takes key `726637` while it builds a template
database; tests only.) Still, keep untrusted roles out of the database: `REVOKE CONNECT ON DATABASE <db> FROM PUBLIC`, grant
`CONNECT` to the pges roles only, and give pges a database of its own.

**Every role needs `USAGE` on the schema `pges` and on the store's schema.** No table grant works without it,
and the migrations grant nothing:

```sql
GRANT USAGE ON SCHEMA pges, <store> TO <writer>, <reader>, <sequencer>;
```

**Category partitions need their own grants.** Stream reads (`crates/pges-postgres/sql/read/stream.sql`) and
subscription reads (`crates/pges-subscribe/sql/read/commits.sql`, `commits_sharded.sql`) name
`<store>.commits_<cat>` directly, and PostgreSQL checks the privileges of the table a statement names, not
of its parent. A grant on `commits` does not cover them, and `Admin::register_category` creates a new
partition, as the owner, every time a category is registered. Grant `SELECT` on every table the owner creates
in the store schema, before the first category is registered, and on the tables that already exist:

```sql
ALTER DEFAULT PRIVILEGES FOR ROLE <owner> IN SCHEMA <store> GRANT SELECT ON TABLES TO <writer>, <reader>;
GRANT SELECT ON ALL TABLES IN SCHEMA <store> TO <writer>, <reader>;
```

The writer's other privileges go through `commits` (inserts route to the leaf; `delete_stream` deletes
through `commits`), which PostgreSQL checks on `commits` alone. A writer that appends straight into its
stream's `streams_p<n>` (`AppendRouting::Auto`) needs `INSERT` and `UPDATE` on every `streams` partition;
without them appends go through `streams` (Append (one stream)).

**The publication.** Store creation adds the store's `commits` to the publication `pges`
(`crates/pges-postgres/migrations/store/0001_init.sql`, last statement), which needs the privileges of the
publication's owner: the owner, a role that inherits from it, or a superuser. The publication is created by
the shared migration, so its owner is the role that first migrated the database. Without those privileges,
an outbox store is created outside the publication (the logical source never reads it), and a store for the
logical source fails with SQLSTATE `42501`: added later, its commits made before then would never reach a
slot that already exists. Without the publication (dropped, or a restore with `--no-publications`), store
creation skips it, and the logical source creates it and adds every store before it streams (Logical
source). A store creator that does not own `pges` creates stores with `migrate::run_store` after the owner
ran `migrate::run_shared`: `run_shared`, and so `migrate::run`, refuses a `pges` schema whose owner is not
`current_user` (Migrations).

| Role | Needs |
|---|---|
| owner | owns `pges` and store schemas (migrations refuse a `pges` schema whose owner is not `current_user`: a login role that inherits from a NOLOGIN owner runs `SET ROLE <owner>` before migrating) and, for the logical source, the publication `pges` (or inherits from its owner); runs migrations, maintenance, `apply_retention`, `prune_log` and `set_writer_limits` (DDL, `VACUUM`, `UPDATE` on `pges.stores`; `pg_db_role_setting` is readable by PUBLIC). The one-source-kind trigger runs as the inserting role and locks the `pges` row of `pges.scopes`, so a store creator that does not own `pges` needs `UPDATE` on `pges.scopes`, which recording its migrations needs anyway |
| writer | `INSERT`, `UPDATE` on `streams` (`UPDATE` also covers the `SELECT … FOR UPDATE` taken before a rerun with a new commit id); `INSERT` on `commits`, `pending`, `event_types`, `commit_tags`, `tag_heads`; `UPDATE` on `tag_heads`, `snapshots`; `INSERT` on `snapshots`; `DELETE` on `commits` and `snapshots` and `INSERT` on `deleted_commits` (`delete_stream`); `SELECT` on all, and on `pges.stores`, `pges.scopes` and `pges.migrations` (`Store::open` reads the writer limits and checks the schema versions and migration checksums); `EXECUTE` on `pges.check_commit_id`. The outbox trigger's `pg_notify` needs nothing extra (PUBLIC). Set `transaction_timeout` and `idle_in_transaction_session_timeout` on the role, at most the store's `max_txn_age` |
| reader | `SELECT` on store tables and on `pges.stores`, `pges.scopes` and `pges.migrations` (`Store::open` checks the schema versions and migration checksums; taking and renewing leases checks `min_compatible`); `SELECT`, `INSERT`, `UPDATE` on `subscriptions` and `backfills`; `SELECT`, `INSERT`, `UPDATE`, `DELETE` on `subscription_members` (the heartbeat deletes lapsed members, and leaving deletes the instance's own row). Replica reads need nothing more: `pg_current_wal_insert_lsn()`, `pg_last_wal_replay_lsn()`, `pg_is_in_recovery()`, `current_setting` and `pg_settings` are available to PUBLIC; the replica pool's role needs `SELECT` on the store tables |
| sequencer | `REPLICATION` (logical source only); `SELECT` on `pges.sequencer`, `pges.scopes` (taking and renewing the lease checks `min_compatible`), `pges.stores`, `categories`, `pending`, `log`; `INSERT` on `log`; `UPDATE` on `categories`, `pges.stores`, `pges.sequencer`; `DELETE` on `pending`; `EXECUTE` on the `pg_replication_origin_*` functions for token option (a). `pg_current_wal_insert_lsn()` is executable by PUBLIC. Terminating the previous leader's walsender needs no extra grant when every sequencer instance uses the same role; otherwise grant `pg_signal_backend`. With the logical source: `SELECT` on each store's `commits`, which the source reads in a new slot's snapshot every time it creates the slot (`bootstrap`, a first start, `resequence`). With the logical source and a missing publication: `CREATE` on the database (`CREATE PUBLICATION`), and for `ALTER PUBLICATION … ADD TABLE` the privileges of the owner of each store's `commits` (membership in the owner's role), besides owning the publication it created. The publication then belongs to the sequencer's role, and an owner that is not a member of it can no longer create a store for the logical source (42501, above): give the publication back with `ALTER PUBLICATION pges OWNER TO <owner>`. With `watch-notify`: nothing extra (`pg_notify` is PUBLIC). `resequence`: `SELECT` on `pges.stores`, `commits` and `categories` (in the exported snapshot), `INSERT` on `log`, `UPDATE` on `categories` (the resequenced range) and `pges.sequencer`, `REPLICATION`; the writer-role check reads `pg_db_role_setting`, `pg_database` and `pg_roles` (PUBLIC) |
