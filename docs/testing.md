# pges — testing

Tests need PostgreSQL 18 with `wal_level = logical`. `pges-testing` picks a server once per process:

1. `PGES_TEST_DATABASE_URL`, if set.
2. `PGES_TEST_BACKEND=docker` or `=local` forces that backend.
3. The local cluster from `pg-up`, if its socket exists in `./.pg` (or `PGES_PG_DIR`).
4. A Docker container named `pges-test-pg18`, started through testcontainers and reused across runs.

Docker and the Nix shell are both first-class: `cargo test` works with either and needs no configuration. The
chosen backend is printed once (`pges-testing: using …`, visible with `--nocapture`).

## Running

With Docker only:

```sh
cargo test
```

The first run pulls `postgres:18` and starts the container; later runs reuse it. Remove it with
`docker rm -f pges-test-pg18`.

With Nix:

```sh
nix develop        # pinned Rust toolchain, PostgreSQL 18, cargo-nextest, cargo-deny
pg-up              # throwaway cluster in ./.pg on a unix socket; tests find it automatically
cargo nextest run  # or cargo test
pg-down            # stop; pg-down --clean also deletes ./.pg
```

`cargo nextest` runs one process per test. Use it with `pg-up` or another long-lived server; with the
Docker fallback, every process would look up the container on its own.

`cargo test` also runs the README examples as doctests (`cargo test --doc` runs only those and the rustdoc
examples). `cargo nextest` does not run doctests. The workspace README is included by `pges-examples`
(not published: the README is outside every published package), which builds it with `pges`'s `derive`
feature; the `pges-sequencer` README builds with `source-logical`.

`cargo test` also runs the ten examples in `examples/` (`pges-examples`): `examples/tests/run.rs` includes
each example file as a module and calls its `run()`. Each example makes its own database:

1. With `PGES_TEST_DATABASE_URL` set, a `t_*` test database on that server, as the tests do.
2. Otherwise, with `DATABASE_URL` set, `pges_example_<uuid>` on that server. The role needs `CREATEDB`; the
   logical examples also need `wal_level = logical` and a role with `REPLICATION`.
3. Otherwise a `t_*` test database on the server `pges-testing` picks (`pg-up` or Docker).

`run()` drops the database's slots, then the database, even when the example returns an error. A `t_*`
database of an example that panics is dropped like a test's (Isolation). An example that panics, hangs or
is killed leaks its `pges_example_*` database and its `pges_<oid>` slot, and a hung or killed one its `t_*`
database until the next sweep; clean them up as for tests (Isolation).

## Isolation

- `pges-testing` depends on `pges-postgres` and builds everything with its migration runner. The shared
  `pges` migrations are applied once to a template database whose name includes a hash of their checksums,
  so editing one builds a new template.
- Each test gets its own database cloned from the template (`TestDb::new`), creates its stores (through
  `migrate::run_store`) and categories with `create_store(conn, &StoreName, &StoreDefinition)` and
  `register_category(conn, &StoreName, CategoryName, months_back, months_ahead)`, and drops the database with
  `TestDb::cleanup`. `pges_testing::definition(Outbox)` is the default `StoreDefinition` with 4 stream
  partitions instead of 64, so tests stay fast. `pges_testing::position(n)` and `version(n)` build a
  `Position` or `Version` from a number and panic out of range; a literal uses `pges_core::position!` or
  `version!`. `TestDb::pool` gives a pool on the test's database, `TestDb::connect` one connection.
  `TestDb::cleanup` fails if a replication slot is left, so a test notices it; `TestDb::cleanup_with_slots`
  drops the slots first. `TestDb::existing(name, url)` wraps a database the test does not own, which is never
  dropped. `pges_testing::drop_database(admin_url, name)` ends the walsenders of the database's slots, drops
  the slots, then drops the database.
- `TestDb::name()` and `TestDb::url()` read its name and URL. The fields are private, so a test cannot make
  `Drop` drop another database.
- Replication slot names are server-wide. Tests that create slots derive the name from their database and drop
  the slot before cleanup. Sequencer tests with the logical source create slot `pges_<oid>` for their test
  database and drop it in `Fixture::finish`, once every sequencer has stopped.
- `application_name` is not unique either: concurrent test binaries, and other runs on the same server, use
  the same names (`inv_seq_1`). A test that finds or ends sessions by name also filters on
  `datname = current_database()` (the sequencer fixture's `Running::crash`, `term.rs`, `crash.rs`, the
  invariants harness's `Seq::crash`). Without it, one run's simulated crash ended another run's sequencer,
  which then failed with "terminating connection due to administrator command".
- The server runs with `fsync`, `synchronous_commit` and `full_page_writes` off. Tests that read logical
  decoding output set `synchronous_commit = on` in their session, because decoding sees only flushed WAL.
  The sequencer fixture's writers pool sets it through connect options, for both source kinds.
- A `TestDb` dropped without `cleanup` (a failed test, a `?` return, a panic) drops its database: it ends
  the walsenders of the database's slots, drops the slots, then the database, on a thread with its own
  runtime. A killed test process runs no `Drop`; the first `TestDb::new` of each process drops `t_*`
  databases more than an hour old (by the timestamp in the name), with their slots
  (`tests::the_sweep_drops_only_stale_test_databases` in pges-testing covers it). Manual cleanup (the SQL
  below) is needed only for databases newer than that from killed processes. On the shared `pg-up` cluster,
  once no tests run, drop inactive `pges_*` slots first, then the `t_*` test databases:
  ```sql
  SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots
  WHERE slot_name LIKE 'pges\_%' AND NOT active;
  SELECT format('DROP DATABASE %I WITH (FORCE)', datname) FROM pg_database WHERE datname LIKE 't\_%' \gexec
  ```
  Other test runs may be using the same cluster; clean up only when none are.
- `pges_testing::proxy::Proxy::start(server)` starts a TCP proxy on 127.0.0.1 in front of the test server
  (which it reaches over TCP or its unix socket); `proxy.url(&db)` connects through it. `drop_connections()`
  closes every open connection; `hold(Direction::ToServer | ToClient | Both)` stops forwarding one way until
  `release()`. `refuse()` closes each new connection as soon as it is accepted, until `admit()`, so the client
  sees an end of stream; `accepted()` counts the connections accepted. A connection's drop generation is fixed
  when the proxy accepts it. sqlx talks to the server around a statement, so tests account for it before
  holding responses: a pool pings a connection when it returns to the pool (always) and before handing it out
  (unless `test_before_acquire(false)`), so tests wait for `pool.num_idle()`; a connection prepares a
  statement before its first execution, so tests run the statement once first.
  `Proxy::to(addr)` puts the same proxy in front of any TCP server, such as a sequencer's gRPC watch; connect
  to `proxy.addr()`.
- `pges_testing::tls::Pki::generate()` (feature `tls`, rcgen) makes throwaway PEM certificates per test, so
  no key is checked in: a CA (`ca`), a server certificate it signed for `localhost` and `127.0.0.1`
  (`server_cert`, `server_key`), one it signed for `elsewhere.invalid` only (`elsewhere_cert`,
  `elsewhere_key`, which a client must refuse on `localhost`), a client certificate (`client_cert`,
  `client_key`) and an unrelated CA (`other_ca`). pges-sequencer and pges-subscribe turn the feature on in
  their dev-dependencies.
- Tests that run a sequencer fail if it reported `Status::Recovering`, unless they expect it
  (`Running::stop_after_errors` in `crates/pges-sequencer/tests/common`). Recovery loops can hide bugs: an
  invalid probe statement once passed every test because the retry path drained anyway. Subscription tests
  fail on an unexpected `subscription::State::Recovering` the same way (`common::Running::stop` in
  `crates/pges-subscribe/tests/common`, and the error lists in the merge proptest). Tests that expect a fatal
  stop use `common::Running::stopped()`, which fails if the subscription recovered from an error before the
  fatal one. The subscribe tests' outbox `common::Sequencer` (moved from `category.rs`) fails on an
  unexpected sequencer `Status::Recovering`; `common::wait_sequenced` waits for the log.
  `common::Sequencer::kill` plays a crash: it drops the run without releasing the lease.
- In the sequencer tests, `Running::result()` also checks that the final status matches: `Halted` with the
  error's text, or `Stopped`. `Fixture::on(TestDb, Kind)` builds the fixture in a database that already has
  the shared migrations (used for the private container).
- A test of a timing-sensitive property must be able to fail: check it by breaking the code once (for
  example, confirming a keepalive's `wal_end` straight to the slot, as the logical source did before
  2026-11-27, fails the idle test's restart check, and skipping the `SlotLost::Ahead` check fails all three
  tests in `promotion.rs`). A mutation run that is killed or hangs leaks its databases and slots (above); clean them up
afterwards.

## Suites

| Suite | Where | Runs |
|---|---|---|
| Unit | each crate's `src` | always |
| Spike: the migrations and statement shapes against PG18, appends and classification through pges-postgres's registry (`APPEND`, `APPEND_OUTBOX`, `CLASSIFY`); the sequencer's lease, log insert and outbox drain are still inline copies | `crates/pges-testing/tests/spike.rs` | always |
| Statement registries: the `.sql` files match the registry (embedded text equals disk, no unused file, headers, unique names, no placeholder left with every value set, `sql()` equals `render()`); no SQL literal, `format!` or `const` SQL passed to sqlx in `src/`; every query checked against the Rust types of its parameters and columns (`check_typed` over `TYPED`); every query listed in `TYPED`; no untyped bind or read in `src/` (`check_typed_calls`; exempt: pges-postgres `sql/typed.rs`, pges-testing `statement.rs`, bench `partitions/composed.rs`); every DDL statement run in a real scenario, and the set that ran equals the registry's DDL | `tests/statement.rs` in pges-postgres, pges-sequencer, pges-subscribe, pges-testing and bench (`files_match_the_registry`, `all_sql_comes_from_the_registry`, `queries_match_their_registry_types`, `every_query_is_typed`, `binds_and_reads_are_typed`, `ddl_statements_run`) | always |
| Statement registry details: pges-subscribe's statements are queries except the two `BEGIN`s of a handler's transaction, which run and set their isolation level (`ddl_statements_run`); the shard expression is only in `read/commits_sharded.sql`; the facade runs no SQL and binds nothing untyped (`crates/pges/tests/statement.rs`, `no_sql_outside_the_registries`, `binds_and_reads_are_typed`); bench skips `server/synchronous_standby` (`ALTER SYSTEM`, its `NOT_RUN`), runs its `PREPARE`/`EXPLAIN`/`EXECUTE` fragments composed, and creates the `scale` id functions and dataset table (`scale/commit_id_function`, `scale/stream_id_function`, `scale/dataset_create`) before checking the statements that use them | `crates/pges-subscribe/tests/statement.rs`, `crates/pges/tests/statement.rs`, `bench/tests/statement.rs` | always |
| The checker rejects wrong Rust types: a narrower parameter or column type, a wrong column name, a missing parameter, `Option` on a NOT NULL column, a non-`Option` nullable column, an unfilled `{store}`; correct statements pass (`check_typed_rejects_wrong_rust_types`). pges-testing's template life cycle (create, rename, mark, clone, drop) runs on throwaway `t_<uuid>` databases | `crates/pges-testing/tests/statement.rs` | always |
| Template units: placeholder names, typed values (`StoreName`, `Ident`, `Literal`, `SnapshotName`), rendering, `File::render`; `PgLsn` reads and writes as PostgreSQL does (`pg_lsn_reads_and_writes_as_postgres_does`); the outbox probe code builds exactly the registered two-store statement with `Rendered::compose`, and `outbox/probe_none` with no store; `check_no_inline_sql`'s and `check_typed_calls`'s own cases (`finds_untyped_binds_and_reads`) | `crates/pges-postgres/src/sql/template.rs`, `crates/pges-postgres/src/sql/typed.rs`, `crates/pges-sequencer/src/outbox.rs`, `crates/pges-testing/src/statement.rs` | always |
| Test database cleanup: a dropped `TestDb` with a slot and an open connection leaves nothing, so does a panicking task; an `existing` database is left alone; `cleanup_with_slots` | `crates/pges-testing/tests/cleanup.rs` | always |
| Derive: generated code, upcaster chains, variant shapes | `crates/pges-derive/tests/event.rs` | always |
| Derive compile errors (`trybuild` snapshots), including `#[event(crate = ...)]` that is not a path, has generic arguments, is given twice, or does not resolve; a missing category, duplicate keys, serde field attributes that do not round-trip, two fields under one stored name, `#[serde]` on the enum or a variant | `crates/pges-derive/tests/compile_fail.rs`, `tests/ui/` (`invalid_crate`, `unresolved_crate`, `missing_category`, `duplicate_keys`, `serde_unreadable_field`, `serde_name_collision`, `serde_container`, and the older cases); every accepted serde field attribute round-trips (`tests/event.rs`, `accepted_serde_field_attributes_round_trip`) | on the pinned toolchain (below) |
| Core compile errors: `CausationId` given as a correlation id, `CorrelationId` as a causation id, a bare `Uuid` as a correlation id (`correlation_as_causation`, `causation_as_correlation`, `uuid_as_correlation`); an invalid category, store name or interval literal, each in a `const` item and in a function body (the category also in a generic function); `non_empty![]`; a position or version above `i64::MAX`. A hand-written `Event` whose `TYPES` repeats a name fails when forced (`compile_fail` doctest on `Event`) | `crates/pges-core/tests/compile_fail.rs`, `tests/ui/` (`invalid_category`, `invalid_category_in_fn`, `invalid_category_in_generic_fn`, `invalid_store_name`, `invalid_store_name_in_fn`, `interval_out_of_range`, `interval_out_of_range_in_fn`, `empty_non_empty`, `position_out_of_range`, `version_out_of_range`); `crates/pges-core/src/event.rs` | on the pinned toolchain; the doctest always |
| Store compile errors: `Loaded` is read-only, a wrapped unit of work has no `commit` or `rollback`, creation options need a `StoreDefinition` (and `store::Dcb` is gone), limits and writer roles take checked types, `Admin::new` with a `StoreName` cannot fail, `delete_stream` refuses `ExpectedVersion::NoStream`, store names are typed; a statement's wrong bind or read type, a column read by a shared name, and an `at N` off the field's position | `crates/pges-postgres/tests/compile_fail.rs`, `tests/ui/` (`loaded_is_read_only`, `wrapped_unit_of_work_cannot_commit`, `creation_options_need_a_definition`, `limits_and_roles_are_checked_types`, `admin_with_a_store_name_cannot_fail`, `delete_expects_no_no_stream`, `store_names_are_typed`, `statement_binds_and_reads_are_typed`, `statement_columns_of_one_name`) | on the pinned toolchain |
| Subscribe compile errors: no categories, `all_categories` combined with listed categories (either order), bad name, shards and lanes literals, a backfill without `until` or categories, `all_categories` on a backfill, zero settings (`batch_size(0)`, `sharded(0)`, `lease(Duration::ZERO)`, `Wakers::any(Vec::new())`); a statement's wrong bind or read type | `crates/pges-subscribe/tests/compile_fail.rs`, `tests/ui/` (`subscription_without_categories`, `all_categories_and_listed`, `invalid_subscription_name`, `shards_out_of_range`, `lanes_out_of_range`, `backfill_without_until`, `backfill_without_categories`, `backfill_all_categories`, `zero_settings`, `statement_binds_and_reads_are_typed`) | on the pinned toolchain |
| Sequencer compile errors: an invalid `source_kind!` literal in a `const` item and in a function body, a `Duration` given where the config takes an `Interval`, `CommitRef::new` with a `u16` count; a statement's wrong bind or read type | `crates/pges-sequencer/tests/compile_fail.rs`, `tests/ui/` (`invalid_source_kind`, `invalid_source_kind_in_fn`, `config_takes_intervals`, `commit_ref_takes_an_event_count`, `statement_binds_and_reads_are_typed`) | on the pinned toolchain |
| Replication compile errors: an empty publication list (`[PublicationName; 0]`, a `Vec`), invalid `slot_name!` / `publication_name!` literals | `crates/pges-replication/tests/compile_fail.rs`, `tests/ui/` (`no_publications`, `invalid_names`) | on the pinned toolchain |
| Checked values at the read boundary: a stored schema version 0 is `FrameError::OutOfRange` (`frame.rs`, `a_stored_schema_version_of_zero_is_out_of_range`), and so is a varint that does not fit a u64, while one cut short is `Truncated` (`a_varint_that_overflows_is_out_of_range`, `a_varint_cut_short_is_truncated`); a watch message with a position above `Position::MAX` gives `None` (`pges-watch-proto`, `positions_above_the_maximum_are_refused`) and the gRPC waker records nothing from it (`waker/grpc.rs`, `keeps_the_highest_positions_across_leaders`); `PartitionName`'s `Debug` is the quoted name (`retention.rs`, `partition_names_print_as_their_text`) | `crates/pges-core/src/frame.rs`, `crates/pges-watch-proto/src/lib.rs`, `crates/pges-subscribe/src/waker/grpc.rs`, `crates/pges-postgres/src/retention.rs` | always |
| Untrusted input never panics or hangs (property tests, the default 256 cases each; raise `PROPTEST_CASES` for a longer search; each run fails after 120 s rather than hang): payload frames and the metadata inside them, arbitrary and mutated; W3C trace contexts (`otel`); store, category, slot, publication, snapshot and subscription names, which also agree with their documented rules; `pgoutput` messages, arbitrary, after each valid tag and mutated from a valid message of every kind; CopyData around the XLogData and keepalive lengths (`stream.rs`, `parse_never_panics`) | `crates/pges-core/tests/fuzz.rs`, `crates/pges-replication/tests/fuzz.rs`, `crates/pges-replication/src/stream.rs`, `crates/pges-subscribe/tests/fuzz.rs` | always; trace contexts with `otel` |
| Derive path lookup: the facade under any name wins, then `pges-core` under any name, then `::pges_core` | `crates/pges-derive/src/core_path.rs` (unit tests) | always |
| Facade: the derive as `pges::Event` with no `crate` attribute (every variant shape, serde field attributes, an upcaster, an encode/decode round trip), `pges::derive::Event` with an explicit `crate = "pges::core"`, and a store used through `pges` only. The facade depends on `pges-core` under another name (`pges-core-dep`), so `::pges_core` does not resolve there | `crates/pges/tests/derive.rs`, `crates/pges/tests/store.rs` | always |
| Facade sequencer: the `sequencer` feature alone builds a `LogicalSource` and a `Sequencer` (lazy pool, no database); `outbox::CHANNEL` | `crates/pges/tests/sequencer.rs` (`sequencer`) | with `--all-features` |
| Facade compile error: an explicit `crate = "::pges_core"` overrides the found path and fails in a crate that depends only on `pges` | `crates/pges/tests/compile_fail.rs`, `tests/ui/explicit_crate_path_wins.rs` (`trybuild`) | on the pinned toolchain |
| Store: migrations, open, append, reads, loads, snapshots, admin | `crates/pges-postgres/tests/` | always |
| Snapshot replacement: a higher state version at a lower version does not replace the stored snapshot, and at the same version it does; afterwards a lower state version, or an older snapshot of either, never replaces it; after a truncate to floor 4 `load_snapshotted` still loads from the snapshot | `crates/pges-postgres/tests/read.rs` (`a_snapshot_is_replaced_only_by_a_newer_or_a_higher_state_version`) | always |
| Contended `Any` appends: 16 writers on a multi-thread runtime through `append`, `append_many` and units of work; commit ids increase with versions | `crates/pges-postgres/tests/append.rs` (`contended_any_*`) | always |
| Hot-stream retry id: 16 writers × 25 appends on one stream; the append statement's runs are counted through `pg_stat_user_functions` for `pges.check_commit_id` (`track_functions = 'pl'` on the test database), and fewer than 7/8 of the 401 appends may run twice. Fresh ids made about 390 run twice; the current rule about 240–290 on a loaded box, so the bound tells the two apart without flaking (`a_retried_append_does_not_make_the_queued_ones_retry`). Unit tests bound the random step at 2^60 and check that ids above the same last id differ; `new_commit_ids_are_just_above_a_recent_last_one_or_fresh` takes the fresh id as an argument, with fixed random bits so no step carries into rand_a (a carry also crosses the variant bits, so a raw-id difference check failed about 1 step in 8), and `ids_above_carry_into_rand_a_and_the_timestamp` covers the carry | `crates/pges-postgres/tests/append.rs`, `crates/pges-postgres/src/append.rs` | always |
| One source kind per database: mixed kinds refused through every way of creating a store, and concurrent inserts serialised by the trigger's lock | `crates/pges-postgres/tests/migrate.rs` | always |
| Migration lock: a role that can only connect, holding the old advisory key `hashtext('pges.migrate')`, blocks neither store creation nor the one-source-kind trigger, and a role that reads `pges.scopes` cannot lock its `pges` row (42501) (`a_role_that_can_only_connect_cannot_block_migrations`; before, store creation waited forever); a runner waits for the row while another session holds it and, once its `MigrateConfig::lock_wait` runs out, fails with `LockTimeout` naming that session's pid and login role, creating nothing, and so does an auto-migrating `Store::open` given a short `StoreConfig::migrate_config`; a runner with a longer wait creates the store once the row is free (`a_runner_gives_up_waiting_for_the_migration_lock_and_names_the_holder`); the wait counts only time between a failed try and the try that takes the lock, not time spent holding it (`lock.rs`, `only_time_spent_waiting_counts`); before the shared schema exists, a run times out on the bootstrap advisory key naming its holder, and four concurrent first runs apply the shared migration once (`the_first_shared_migration_is_serialised_by_the_bootstrap_lock`) | `crates/pges-postgres/tests/migrate.rs`, `src/lock.rs` | always |
| Pinned `search_path`: another role's `public.uuid_extract_version`, with a caller's `search_path = public, pg_catalog`, does not reach `pges.check_commit_id`, which still refuses a v4 id with 22023 (`a_function_in_public_does_not_hijack_the_commit_id_check`; before, it called the impostor and accepted the id); every function in `pges` and in an outbox store's schema has exactly `search_path=pg_catalog, pg_temp` in `proconfig` (`every_pges_function_pins_its_search_path`) | `crates/pges-postgres/tests/search_path.rs` | always |
| Maintenance lock: a second maintainer waits for the first (`maintenance_waits_for_another_maintainer`); one whose `Admin::with_lock_wait` runs out fails with `MaintainError::LockTimeout` naming the holder's pid and login role (`maintenance_gives_up_waiting_and_names_the_holder`; before, it waited forever); three operations waiting for the lock on a pool of two leave a connection free, then all complete (`operations_waiting_for_the_maintenance_lock_leave_the_pool_free`; before, the waiters held the pool and an acquire timed out) | `crates/pges-postgres/tests/store.rs` | always |
| Shared schema ownership: the first shared migration refuses a schema `pges` another role created beforehand, even run by a superuser, and creates nothing; the schema's owner migrates into it. A squatter whose `pges` already records every shared migration is refused by `run`, `run_shared` and `run_store` with nothing left to apply, and no store is created (`a_schema_of_another_role_is_refused_with_its_migrations_applied`; before, the check ran only before the first migration and was skipped); a login member of a NOLOGIN owner is refused as itself and migrates, twice, after `SET ROLE` to it | `crates/pges-postgres/tests/migrate.rs` | always |
| Roles: a non-superuser owner migrates, registers a category and maintains; a writer opens the store, appends, reads and deletes a stream; a sequencer sequences (outbox source, and the logical source from a slot it bootstraps, as a `REPLICATION` login); a reader runs a subscription, each with exactly the grants of schema.md (Roles); a logical sequencer recreates a dropped publication as a member of the owner's role, after which the owner is refused a new logical store with 42501 | `crates/pges-subscribe/tests/roles.rs` | always |
| Partition names and headroom: categories named like another's leaf (`jobs_<month>`), its index (`jobs_<month>_s`) or a key (`jobs_pkey`, `pkey`) register before or after it and every leaf has its own parent (`a_category_named_like_a_leaf_registered_first_leaves_no_hole`, `..._after_gets_its_own_partition`; on the old names both fail: the first with SQLSTATE 23514 on an append to `jobs`, the second at registration with 42809 or 42P17); a name taken by a sequence fails registration with `NameTaken` and the sequence stays, and so does the name of a category partition's primary key (`commits_<cat>__pkey`) or a leaf's index (`<leaf>_s`), with no leaf left detached (`an_index_name_taken_by_another_relation_is_refused`; before, SQLSTATE 42P07); an append no leaf takes fails with `NoPartition` through `append`, `append_many` and a unit of work, and works after `maintain`, as does one whose category partition is detached, also in an `append_many` and a unit of work's batch over several categories with that category first or last (`a_batch_over_categories_without_one_category_partition_fails_with_no_partition`; before, a database error); `NoPartition` is the same through `append_in` on a caller's connection in autocommit and in its transaction, with the outbox on, and as `Write(NoPartition)` from `delete_stream` (`no_partition_is_reported_in_every_transaction_mode`); a day category named like another's day leaf leaves no hole, and a bucket change's leaves do not collide (`day_leaves_and_bucket_changes_do_not_collide`); `partition_headroom` reports the leaves' bound, `below_one_bucket` for a day category left with today's leaf only and for one with no leaf, and the log's end and remaining positions before and after `maintain`. Unit: `ident::tests::names_of_different_categories_never_collide` | `crates/pges-postgres/tests/partition.rs`, `crates/pges-postgres/src/ident.rs` | always |
| `register_category` keeps its error when the rollback fails (terminates the admin backend while it waits on a lock) | `crates/pges-postgres/tests/store.rs` (`a_failed_rollback_keeps_the_registration_error`) | always |
| `StoreName` refuses exactly the server's keywords that cannot name a schema (`pg_get_keywords()` `catcode` `R` or `T`, 101 in PostgreSQL 18); a release that changes them fails it | `crates/pges-postgres/tests/store.rs` (`store_names_refuse_the_servers_reserved_keywords`) | always |
| Futures are `Send`: every public operation inside `tokio::spawn`, including retention, `prune_log` and `delete_stream`; an ambient scope, and a nested `scope(current(), append)`, inside `tokio::spawn` | `crates/pges-postgres/tests/spawn.rs` | always |
| Metadata units: `follow_up` (correlation carried over or the cause's id, causation the cause, trace carried over, no actor or extra; a chain keeps its first correlation), `fill_unset` keeps set fields, the wire format round trip; with `otel`, no span context without a layer, the current span's traceparent byte-equal to `opentelemetry_sdk`'s `TraceContextPropagator`, parsing that accepts and rejects as the propagator does (13 inputs), a stored context parents a span | `crates/pges-core/src/metadata.rs` | always; the otel part with `--all-features` |
| Ambient scope units: nothing filled outside a scope, explicit fields beat the scope, nested scopes override the fields they set, a spawned task inherits only when the caller passes the scope on | `crates/pges-postgres/src/ambient.rs` | always |
| Metadata propagation: without a scope, only explicit metadata is stored; explicit fields win and the scope fills the rest; a scope takes an append directly; nested scopes; `append_many`, units of work (plain and wrapped) and `delete_stream`'s tombstone take the scope; follow-ups chain correlation and causation over 3 commits. With `otel`: trace precedence explicit > scope > span > none (also under `.into_future().instrument(span)`, and a scope with an actor and correlation but no trace takes the trace from the span), the stored traceparent equals the span's, a delivered event parents the handler span | `crates/pges-postgres/tests/metadata.rs` | always; the `otel` module with `--all-features`; the default-features CI step runs the rest without `otel` |
| Follow-ups across subscriptions: a request scope appends jobs, a `run_pg` handler opens orders under `scope(event.follow_up(), append_in)`, a `run` handler adds parts with `raw.follow_up()` and its own actor; correlation across 2 hops, causation per hop, the actor not inherited, and handlers see no ambient scope | `crates/pges-subscribe/tests/follow_up.rs` | always |
| Retention: Window drops only old, fully sequenced leaves (an unsequenced leaf is refused, a halted sequencer stops the run, horizon and scan start recorded, a truncated stream fails reads and loads); Window refuses a leaf with a long-lived stream; SnapshotAndTruncate refuses without snapshots and drops after one (`load` fails `Truncated`, `load_snapshotted` works), and the drop forgets a sequenced deletion's `deleted_commits` row but keeps an unsequenced one; writer-role checks against the store's recorded `max_txn_age`, including a database-level `transaction_timeout`; retention keeps the store's recorded skew, not a 5 s default (`retention_allows_for_the_stores_recorded_skew`); `prune_log` keeps the rows of commits that still exist (`prune_log_keeps_the_rows_of_commits_that_still_exist`) and the rows above the store's coarse high-water mark (`prune_log_keeps_the_rows_above_the_coarse_mark`), and a full log partition with such a row stays (`a_log_partition_with_rows_above_the_coarse_mark_stays`); the store default and the CHECK parity; a `keep` above `Retention::MAX_KEEP` refused by `set_retention`, `set_default_retention` and category registration, while `MAX_KEEP` itself runs (`a_keep_beyond_the_maximum_is_refused`); the archive code refused; `prune_log` where another category pins the log; a log partition every category has passed is dropped; a run interrupted before the drop (every detach times out) records the horizon but not the scan start, and the next run drops the leaf (`a_run_interrupted_before_the_drop_is_finished_by_the_next`, about 14 s); a load and a stream read truncated between their head read and their first page (held there by a `LOCK` on `commits_jobs` while the floor is raised and the leaf dropped) fail with `Truncated` instead of returning the stream without its start (`a_read_that_retention_truncates_midway_fails_as_truncated`); a `load_snapshotted` held between its head read and its snapshot read (by a `LOCK` on `snapshots`) while another writer appends and saves a snapshot above the head's version uses that snapshot, both for a stream already truncated and for one truncated meanwhile, instead of failing with `Truncated` (`a_snapshot_saved_during_a_load_of_a_truncated_stream_is_used`; fails without the head reread) | `crates/pges-postgres/tests/retention.rs` | always |
| Retention plans: with hashing, sorting or nested loops disabled, `scan`'s `sequenced` check is a set difference, the policy checks probe `streams` per stream (`Index Cond: (stream_id = …)`, no `Seq Scan on streams`), and `raise_floors` updates stream by stream. `the_statements_answer_as_before_under_every_plan` runs the old (inlined from the earlier version) and new `scan`, `scan_stop`, `live_streams`, `missing_snapshots` and `raise_floors` on a 900-stream leaf (deleted, archived, recent and floored streams, streams with no row, foreign and stale snapshots, unsequenced commits, other leaves' log rows) under 7 planner settings, and requires identical answers and floors, before and after every commit is sequenced | `crates/pges-postgres/tests/retention_plan.rs` | always |
| Closed-partition freeze: only closed, not-all-frozen partitions are frozen, a rerun freezes nothing, a delete in a frozen leaf makes the next run freeze it again (`closed_partitions_are_frozen_once`); `partition_ages` lists every partition and answers while the maintenance lock is held | `crates/pges-postgres/tests/freeze.rs` | always |
| Append lock fan-out: on 64 partitions, after 10 appends on one connection, an 11th append locks only its stream's `streams_pNNN` (and its `_pkey` when the plan reads through it) and at most 7 relations (9 with the outbox), in every regime: `streams` never analyzed (with and without the outbox), analyzed at about 30 streams per partition, analyzed with 3 streams in all (with and without the outbox), `plan_cache_mode = force_generic_plan` (with and without the outbox). The same test pins the fallback: on a pool whose statement cache (99) is one short of 64 partitions and the reserve of 36, nearly empty and forced generic, and with `AppendRouting::Streams` (outbox, forced generic), the connection prepares the one append through `streams` and no partition statement, and the forced generic plan locks every partition (`an_append_locks_only_the_streams_partition_it_writes`, 10 cases). Through `streams` the nearly empty and forced-generic cases locked 70–73 relations (135 unprepared and forced). At 256 partitions the default cache (100) appends through `streams` and a cache of 292 routes (`appends_go_through_streams_when_the_partition_statements_do_not_fit_the_cache`); routing turned off appends through `streams` on a pool where the partitions would fit (`appends_go_through_streams_when_routing_is_off`); the choice at each capacity, 64 and 256 partitions, without a database (`sql::tests::appends_name_partitions_only_while_their_statements_fit_the_statement_cache`). A connection prepares one append per partition it wrote and none through `streams` (`appends_name_their_streams_partition`); a store with a partition renamed to a name that needs quoting appends through `streams` (`appends_go_through_streams_when_its_partitions_cannot_be_named`), and so does a store opened by a role granted every table but one partition (`appends_go_through_streams_when_the_role_may_not_write_a_partition`). `append_many` locks 4 entries per partition a batch touches in every regime: never analyzed (batches of 1, 4 and 16), 3 streams analyzed (with and without the outbox, and with `AppendRouting::Streams`) and under `force_generic_plan` (batches of 4 and 16, with the outbox, and with `AppendRouting::Streams`), in a wrapped unit of work whose plan mode is put back after each batch (`a_batch_locks_the_streams_partitions_it_touches`, 10 cases); with the plan cache's own choice the nearly empty and forced generic cases locked 128 and 256. In its own transaction, on a one-connection pool, ten batches plan the batch statement as ten custom plans and no generic one, on a store analyzed with 3 streams in all (batches of 2) and on sessions that force generic plans (batches of 4), and the session keeps its mode (`a_batch_in_its_own_transaction_plans_for_its_streams`; with the plan cache's own choice 5 and 10 generic plans) | `crates/pges-postgres/tests/append.rs`, `crates/pges-postgres/tests/append_many.rs` | always |
| Partition routing: the crate's port of PostgreSQL's hash partition routing for uuids against PG18, as a property test (`proptest`, 64 cases of up to 47 random ids, each checked with `satisfies_hash_partition` at every modulus from 1 to 256: `remainders_match_postgres`) and through the `tableoid` of 2,032 inserted rows at moduli 1, 2, 3, 7, 64, 100 and 256 (`inserted_rows_land_in_the_computed_partition`); known remainders; the open-time check refuses a missing partition, mixed moduli, a non-hash bound, two partitions for one remainder, a name that needs quoting, and a probe the server places elsewhere, nowhere or twice (`other_partitionings_do_not_route`). A changed hash constant fails four of them | `crates/pges-postgres/src/route.rs` | always |
| Read cut: a 300-commit read that has a commit appended after its first page ends at 300 (`a_read_ends_at_the_version_it_began_at`); a read that reaches the head's version asks for no more pages: a 256-commit stream is one page query for `load` and `read_stream`, and a load from a snapshot at the head runs none, counted as scans of the category's leaves in `pg_stat_user_tables` after the reading pool closes, with autovacuum off on the leaves since an analyzed leaf adds the planner's primary-key probes for the `commit_id` bounds (2 in all, 5 before; 6 once autoanalyze had run; `a_read_that_reaches_the_head_asks_for_no_more_pages`) | `crates/pges-postgres/tests/read.rs` | always |
| `streams` autovacuum: a new store's `streams` partitions all get the autovacuum settings from migration 0001, and the store scope is at schema version 1 with `min_compatible` 1 | `crates/pges-postgres/tests/migrate.rs` | always |
| Migration 0001 review: a store of 1001 stream partitions names `streams` and `snapshots` partitions `_p0000` to `_p1000`, each by its hash remainder, and a 4-partition store keeps `_p000` to `_p003` (with 3-digit names it failed on `streams_p100`: `a_store_with_over_1000_stream_partitions_names_them_by_the_widest_remainder`); the outbox script is recorded as `<store>.outbox` version 1 with its checksum and scope row, and an edited checksum or a raised `min_compatible` fails `migrate::run` and `Store::open` (`the_outbox_script_is_recorded_and_checked`), a store without the outbox has no such scope; `crate_too_old` reports an outbox scope past the crate and ignores unknown option scopes; store creation publishes `commits`, skips a missing publication, and a role without the publication owner's privileges creates an outbox store outside it and is refused a logical store with 42501 (`a_role_that_does_not_own_the_publication_creates_only_outbox_stores`); the logical source publishes a store created without the publication and sequences it (`a_store_created_without_the_publication_is_published_by_the_source`); `commits` has no index, each category partition and leaf has `PRIMARY KEY (commit_id)`, a commit id repeats across categories but not within one, also after maintenance reattaches a detached category partition, and a lookup by category and commit id scans one leaf's key (`commit_ids_are_unique_per_category`); `pges.check_commit_id` refuses nil, v1 and v4 ids with 22023 even with an infinite tolerance (`the_commit_id_check_refuses_ids_that_are_not_uuidv7`; the old function accepted them) | `crates/pges-postgres/tests/migrate.rs`, `partition.rs`, `append.rs`, `crates/pges-sequencer/tests/logical.rs` | always |
| `delete_stream`: only the tombstone stays (commits, snapshot and key deleted), `deleted_commits` holds exactly the removed (category, commit id) pairs, a missing stream is not created, an outbox store enqueues the tombstone, a snapshot saved from a load made before the deletion is not stored, nor one saved while the deletion holds the row (`a_snapshot_saved_after_the_deletion_is_not_stored`, `a_snapshot_saved_during_the_deletion_is_not_stored`); a deletion waits for a save holding the row, and for an append with `snapshot_after`, then deletes the snapshot (`a_deletion_waits_for_a_save_in_progress_then_deletes_its_snapshot`, `a_deletion_waits_for_an_append_with_a_snapshot_then_deletes_it`); a save locks only its stream's partition of `streams` under `force_generic_plan`, 6 relation locks (`a_save_locks_one_streams_partition_under_a_generic_plan`), and an append with `snapshot_after` adds no `streams` partition (`append.rs`, `an_append_with_a_snapshot_locks_one_streams_partition`) | `crates/pges-postgres/tests/delete.rs` | always |
| Writer limits: creation records `StoreDefinition::max_clock_skew` and `max_txn_age`; a wider writer is refused (`ClockSkewAboveStore`) until the store's skew is raised; a writer without its own skew uses the store's (`append.rs`, `a_writer_without_its_own_skew_uses_the_stores`); `set_writer_limits` refuses an unknown store; limits are only raised (`writer_limits_are_only_raised`: each lowering refused with the values unchanged, equal and higher accepted); the `pges_core::store` defaults agree with the column defaults | `crates/pges-postgres/tests/limit.rs`, `crates/pges-postgres/tests/append.rs` | always |
| Retention through the facade: a subscription behind a horizon recorded by real retention stops with `CursorBeforeHorizon`, one started at the horizon reads the rest; a commit past a dropped leaf's bound that committed before the leaf's last commit (positioned below the horizon) is kept, the report's start is before it, and both a subscription at `Start::Retained` and a backfill deliver it (`a_subscription_started_at_retained_reads_every_commit_retention_kept`); after `prune_log` deleted the dropped commit's log row, the store's newest, so the head is below the horizon, listed and store-wide subscriptions at `Start::Retained` still deliver the kept commit instead of failing with `CursorBeforeHorizon` (`a_subscription_started_at_retained_after_prune_log_removed_the_head`; fails when `skip_through` is the head alone); a subscriber sees `SystemEvent::StreamDeleted` and never the deleted events | `crates/pges/tests/retention.rs` | always |
| Replication client: slots, streaming, pgoutput, keepalives, errors, an exported snapshot at the consistent point that sees exactly what the slot skips | `crates/pges-replication/tests/stream.rs` | always |
| Replication: `alter_slot_failover` turns FAILOVER on and off, waits for an active slot, 42704 when missing | `crates/pges-replication/tests/stream.rs` | always |
| Replication authentication methods | `crates/pges-replication/tests/auth.rs` | always; parts skip (below) |
| Replication TLS: sslmode, roots, host names, direct negotiation, channel binding, prefer/allow fallbacks, SCRAM-PLUS message order (fake TLS servers) | `crates/pges-replication/tests/tls.rs` (feature `tls`) | with `--all-features`; the container test skips without Docker, the fake-server tests always run |
| Store faults through the proxy: lost connections (before and after the server committed), lost responses, `Exact` racing a stream's creation, a lost COMMIT of the locked autocommit append (deduplicated on retry), a lost COMMIT of an `append_many` retried after its commit ids left the skew (deduplicated, `a_batch_whose_reply_is_lost_is_deduplicated_after_the_skew`), `UnitOfWork::commit` keeping `Aborted` when its rollback fails | `crates/pges-postgres/tests/fault.rs` | always |
| `append_many`: the SKIP LOCKED restart, deterministically (a new `xact_start` in `pg_stat_activity`, and the first transaction's lock released); overlapping batches creating the same new streams never deadlock (16 writers, two plan shapes); the retry lock leaves the transaction's `lock_timeout` as it was, and never waits even when the row's updater commits while it locks (`the_retry_lock_of_a_batch_never_waits`: 4 s of updates, in-order lockers and batches holding the second row; the race is narrow, so without the 1 ms timeout about half the runs deadlock) | `crates/pges-postgres/tests/append_many.rs`, `crates/pges-postgres/src/append.rs` | always |
| Sequencer: outbox end to end, gap-free positions, restarts, high-water marks, a failing batch retried; wake-ups: an idle source does not scan `pending` (counted through `pg_stat_user_tables`) and wakes on each commit, a terminated listener is replaced, a commit made while nobody listens is drained after listening again (proxy), a listener that stops answering is replaced (proxy), a new store is sequenced on its first notification, one notification per transaction and none for a failed append; positions past the end stop the sequencer with `RunError::PositionOverflow`, leaving the commit in `pending`, `halted` unset and the lease released | `crates/pges-sequencer/tests/outbox.rs` | always |
| Sequencer logical source: commit order with units of work committing in reverse; restart and crash between log commit and token (dedupe); the slot confirmed up to the token and past idle WAL, never past the stored token, and a restart after idle confirms resumes; takeover; a fenced leader lets go of the slot (`Source::close`); a dropped slot halts (dropped while a proxy refuses the replication connection, so the sequencer cannot take the slot back first); commits before the slot halt (`CommitsBeforeSlot`); a commit racing the slot creation halts, not skipped; bootstrap creates the slot before writers, halts on commits before the slot (and records it in `halted`) and on a lost slot with a token, and refuses an outbox database; a store created while streaming; a slow batch under `wal_sender_timeout = 1s`, with a control run that fails without status updates | `crates/pges-sequencer/tests/logical.rs` (`source-logical`, default) | always; needs `wal_level = logical` |
| Logical bootstrap keeps an existing slot's failover without a request, changes it both ways when asked, a new slot gets failover only when asked, opening a sequencer never changes it, and bootstrap refuses (under a timeout) while a sequencer leads | `crates/pges-sequencer/tests/logical.rs` (`source-logical`, default) | always; needs `wal_level = logical` |
| Sequencer terms: lease renewed while the source opens, shutdown during an open that never finishes, close on lease loss, shutdown and fatal errors, `Status::Halted` (fake source); when the term's connection has died, the lease release and the halt record fall back to a fresh connection | `crates/pges-sequencer/tests/term.rs` | always |
| Sequencer crash correctness (spike 2): a sequencer killed before the log commit, between the commit and the ack, and between the ack and the next batch, with 4 concurrent writers, the outbox and the logical source; a restart or a running standby takes over; a logical case whose token is an hour old replays and dedupes; the log is gap-free with no commit twice, every commit is sequenced once, and an append that started after another returned comes later in its category and in the store (`check_commit_order`). After a kill before the log commit with no standby, the killed batch's commits are absent from the log and (outbox) still pending. Property tests vary the point, its occurrence (1–4), restart or standby, the token interval (0, 30 ms, 1 h) and the batch size (1–7) | `crates/pges-sequencer/tests/crash.rs` | always; logical cases need `wal_level = logical` (`source-logical`, default) |
| Slot invalidation under load (spike 7): four writers append while a live slot is invalidated by `max_slot_wal_keep_size = 1MB` (`wal_removed`), the sequencer halts (`Status::Halted`, later starts refuse), `resequence` recovers and sequencing resumes; the log is gap-free with no commit twice and versions in order per stream, every commit is sequenced once, the recovered block is in commit id order and later appends sit after it (`an_invalidated_slot_halts_sequencing_until_resequenced`) | `crates/pges-sequencer/tests/invalidation.rs` (`source-logical`), private container `pges-test-slot-loss-<uuid>` | always; skips without Docker |
| Slot ahead of the token (`SlotLost::Ahead`): a slot advanced past the token with `pg_replication_slot_advance` halts the sequencer, records `halted`, and `resequence` sequences the skipped commit (`a_slot_confirmed_past_the_token_halts_until_resequenced`); a slot the source creates stores its consistent point as the token, and a slot without a token starts when the stores are empty and halts when they hold a commit (`a_new_slot_stores_its_consistent_point_and_a_missing_token_halts_only_with_commits`); a real promotion (`a_promoted_asynchronous_standby_halts_until_resequenced`, below) | `crates/pges-sequencer/tests/promotion.rs` (`source-logical`, default); the promotion test on a private `replica::Pair::start_failover` | always; needs `wal_level = logical` |
| Resequence: refuses with `NotHalted` before and while a sequencer leads, and with `LeaseHeld` leaving the fence and slot untouched; once the sequencer stops, a valid slot is replaced and nothing recovered; a dropped slot's commits (made while no sequencer ran and while halted) recovered in commit id order, including a leftover slot from an interrupted run and a commit after it, then a second run refuses with `NotHalted`; commits from before the first slot; a bootstrap that found commits before the slot, then resequence, then a sequencer, with commit order checked (`resequence_recovers_after_a_bootstrap_found_commits_before_the_slot`); writer roles refused with `WriterRole` (`TransactionTimeoutNotSet`, and `TransactionTimeoutTooLong` for a role `transaction_timeout` of 2 h against the store's 1 h); `SlotInUse` while a plain replication stream holds the slot (slot, `halted`, lease and log unchanged; succeeds once the stream stops); `LeaseLost` when the fence moves while the slot is being created (nothing written, `halted` kept, the new owner's lease untouched, the next run recovers the orphan); `PositionOverflow` (nothing written, `halted` kept, lease released, new slot dropped); each success checks `halted` cleared, the lease released, the token at the consistent point and the slot valid | `crates/pges-sequencer/tests/resequence.rs` (`source-logical`, default) | always; needs `wal_level = logical` |
| Retention with resequence: a leaf whose commits were resequenced past its stop row is dropped (`retention_drops_a_leaf_whose_commits_were_resequenced`); `prune_log` then resequence sequences no commit twice; a second resequence ignores the commit id order of the first's rows; a resequence that lands during a retention run (the run waits on a held `SHARE UPDATE EXCLUSIVE` lock on `commits_jobs`) leaves the orders scan start below the kept commit (`retention_sees_a_resequence_that_lands_during_its_run`); positions continue above the coarse high-water mark after `prune_log` removed the newest rows, for a sequencer and for a resequence (`positions_continue_after_prune_log_removed_the_newest_rows`, `resequence_continues_after_prune_log_removed_the_newest_rows`), and after a leader crashed before writing the mark, for the outbox and the logical source (`positions_continue_after_a_crash_before_the_mark_and_prune_log`) | `crates/pges-sequencer/tests/retention.rs` (`source-logical`) | always; needs `wal_level = logical` |
| NOTIFY: each batch notifies its store with a decodable `Notice`; off with `Notify::Disabled`; a failing NOTIFY (a shadowing `public.pg_notify` on the database's `search_path`) keeps the log moving without `Recovering`, and notifications resume once it is dropped | `crates/pges-sequencer/tests/notify.rs` (`watch-notify`) | with `--all-features` |
| Sequencer binary: sequences a test database with each source, stops on SIGTERM with exit 0, releases the lease; a second signal ends a shutdown hung on the lease row lock with 128 plus its number (`a_second_signal_forces_a_hung_shutdown`); `bootstrap` creates the slot and exits; `bootstrap` creates a failover slot only with `--failover` or `PGES_FAILOVER=on`; it keeps an existing slot's failover without a flag and changes it with `--failover`/`--no-failover`, the flags overriding `PGES_FAILOVER`; `--failover --no-failover` exits 2 and leaves the slot unchanged; a halted database exits 1; `resequence` exits 1 when not halted and 0 on a halted database (`resequence_recovers_a_halted_database_and_exits`); `--grpc-listen` serves the watch, here on `localhost:<port>`, and SIGTERM ends the stream (`watch-grpc`); `--grpc-tls-*` serve over mutual TLS and refuse a client without a certificate, and unusable files or TLS without `--grpc-listen` exit 2 (`the_binary_serves_the_grpc_watch_over_tls`, `watch-grpc-tls`); SIGKILL mid-load: three binaries killed in turn after 30 or more new log rows each, a fourth drains the rest, for each source, with no commit lost or repeated and no "recovering" logged. The binary's unit tests check option planning: intervals out of range, the lease and renew, `--max-connections` below 2, slot names, an option of the other source and a bad `PGES_FAILOVER` are usage errors (exit 2); the gRPC options need `--grpc-listen` (`grpc_options_need_grpc_listen`); `--grpc-listen` takes socket addresses and host names (`grpc_listen_takes_socket_addresses_and_host_names`); `localhost:0` binds each loopback address, IPv4 among them (`a_host_name_binds_each_of_its_addresses`) | `crates/pges-sequencer/tests/bin.rs` (`bin`, Unix), `crates/pges-sequencer/src/bin/pges-sequencer.rs` | with `--all-features` |
| gRPC watch server: the leader streams the current value, then each change; `Status`; an unknown store reads as zero; the stream ends with `UNAVAILABLE` ("stopped") when the sequencer shuts down, then `STOPPED`; a standby answers `UNAVAILABLE`, then takes over with a higher fence and positions that do not go down; a crashed (dropped) leader reports `ABANDONED`, ends its stream and refuses `Watch` with "abandoned", and a standby takes over; shutting the server down ends open streams while the sequencer still leads; a stream on a healthy leader (800 ms lease) stays open for 2.5 s, over three lease lengths, and still delivers; a stalled leader behind a held proxy ends its stream and refuses new ones once its lease expires, the standby leads with a higher fence, and after release the old leader goes to `Standby` (`a_stalled_leaders_streams_end_once_its_lease_expires`); one stream over `max_streams` gets `RESOURCE_EXHAUSTED` over two connections, and an ended stream frees its place (`streams_beyond_the_limit_are_refused`). Units: channels of unannounced stores last only while watched (`watch.rs`); unusable PEMs are refused when `ServerTls` is built (`grpc.rs`) | `crates/pges-sequencer/tests/grpc.rs` (`watch-grpc`), `crates/pges-sequencer/src/watch.rs`, `crates/pges-sequencer/src/grpc.rs` | with `--all-features` |
| Sequencer leadership: takeover after expiry or release, fencing, `halted`, source mismatch; every lease configuration is valid and renews before it expires (`every_configuration_is_valid`, `a_lease_is_renewed_before_it_expires`); a shutdown gives `Stopped` and releases the lease, a dropped run gives `Abandoned` and keeps it (`a_shutdown_reports_stopped_and_a_dropped_run_abandoned`); a migration past the crate stops the leader at renewal with `CrateTooOld` and releases the lease (`a_migration_past_the_crate_stops_the_leader_at_renewal`), and a sequencer past its crate does not take a free lease, which stays untouched (`a_sequencer_past_its_crate_does_not_take_a_free_lease`) | `crates/pges-sequencer/tests/lease.rs` | always |
| Sequencer coarse token: replays after the stored token are dropped (fake `DynSource`); a token stored part-way through replaying a crashed term's untokened commits, with `dedupe_tail` 0, leaves the rest deduplicated in that term and in the next, and the mark reaches the head once the replay ends (`a_token_stored_while_replaying_keeps_the_rest_of_the_replay_deduplicated`) | `crates/pges-sequencer/tests/dedupe.rs` | always |
| Sequencer futures are `Send` (including the logical source, generic and boxed, with `close`, `bootstrap` and `resequence`, and `WatchService::serve` with `watch-grpc`) | `crates/pges-sequencer/tests/spawn.rs` | always |
| Ordering properties: random appends across categories with sequencer restarts and crashes give a gap-free, duplicate-free log ordered per stream; with the outbox and the logical source | `crates/pges-sequencer/tests/property.rs` (`proptest`) | always |
| Subscription merge cut: the pure merge model | `crates/pges-subscribe/src/read.rs` (`proptest`) | always |
| Subscriptions over one category: catch-up and live tail, unknown events skipped or fatal | `crates/pges-subscribe/tests/category.rs` (`wake-inproc`) | with `--all-features` |
| Multi-category merge: position order, cuts at full pages, a paced race of sequencer batches against the reads, and a property test of random interleavings | `crates/pges-subscribe/tests/merge.rs` | always |
| `run_pg`: rollback of failing handlers, transient retries applied once, an aborted transaction never checkpointed, a refused COMMIT stopping the run through the retry policy, a serialization failure at the COMMIT (a SERIALIZABLE handler, the 40001 raised by a deferred trigger) stopping the run through the retry policy (`a_serialization_failure_at_the_commit_follows_the_retry_policy`), a COMMIT lock timeout (a deferred trigger waiting on a row the test locks) retried until the batch commits (`a_commit_lock_timeout_is_retried`; it stopped with `HandlerFailed`), nor one a savepoint begin ended (failed, or dropped through the proxy), nor one the handler ended with a raw `ROLLBACK` (nothing delivered) or `COMMIT` (its writes committed alone), which stops the run without a retry (`a_handler_that_ends_its_transaction_is_not_checkpointed`; before the guard the cursor moved to the end in both cases), also when the handler then returns a transient error or begins another transaction and leaves it aborted (`a_handler_that_commits_then_fails_is_not_retried`; both were retried and applied twice); a savepoint begin of the handler's that fails stops the run without a retry (`a_handler_whose_savepoint_begin_fails_is_not_checkpointed`; it was retried); `SET TRANSACTION READ ONLY` failing the checkpoint with 25006 through the retry policy, after setting the transaction's own level passed (`a_read_only_handler_transaction_fails_at_the_checkpoint`); the session holding the row has TCP keepalives from the lease and the server's `tcp_user_timeout`, unless on a Unix socket (`the_holding_session_has_keepalives_from_the_lease`; the server's defaults before); a handler that streams 20,000 8 KB rows and pauses 8 s after the first, with a 2 s lease, completes (`a_handler_reading_slowly_past_the_lease_completes`; with `tcp_user_timeout` set to the lease every attempt failed with `UnexpectedEof`), a panicking handler leaving the status `Stopped`; READ COMMITTED (asked for), REPEATABLE READ and SERIALIZABLE handlers (`PgHandler::isolation`) whose batches outlast the lease checkpointed with a standby waiting, and checked to run at that level (fails with 40001 at the checkpoint without the row lock); `SET TRANSACTION` in a handler stopping the run; standby takeover; fencing: a takeover while the holder is idle found by the batch's row lock, no handler run (`a_deposed_instance_is_fenced_out`), and a batch past its lease holding the row (the stored lease lapsed, a standby not taking over, a fence bump with `lock_timeout` failing with 55P03, then the batch checkpointed: `a_batch_holds_the_subscription_row_until_it_commits`); redefine, start positions, invalid definitions, the stored cursor winning after a database error (a lost COMMIT played with the instance's own fence), a lost COMMIT landing after the re-read while the next batch waits at its row lock, rolled back by the checkpoint's cursor compare-and-set (`a_lost_commit_landing_late_is_not_applied_twice`; delivered positions 3 and 4 twice without it), and `checkpoint_held.sql` moving no cursor it does not hold (`the_held_checkpoint_moves_only_a_cursor_it_holds`; it moved it before), a slow batch keeping its lease while idle (the checkpoint's renewal counts from the checkpoint); a handler slower than the lease and a retry backoff longer than it (each with a standby), shutdown during a 30 s backoff; for contrast, a side renewal beside a `run` handler that finds the fence moved; a migration past the crate stops a subscription at renewal with `CrateTooOld`, sharded or not (`a_migration_past_the_crate_stops_subscriptions_at_renewal`), also when the renewal is the checkpoint's, of a delivered batch or of an empty page, for `run` and `run_pg`, without handling the refused batch again; `run_pg`'s row lock refuses the batch before its handler runs (`a_migration_past_the_crate_stops_a_subscription_at_a_renewing_checkpoint`; it read and handled the same page forever), and a subscription past its crate does not take a released lease, sharded or not, without recovering first and leaving every fence and lease as it was (`a_subscription_past_its_crate_does_not_take_a_released_lease`) | `crates/pges-subscribe/tests/run_pg.rs` | always |
| Retention horizon: a cursor behind it, a horizon passing a running subscription, and retention between a category's log page and its commit fetch (held there by a `LOCK` on `commits_jobs` while the horizon is raised and the commits deleted) stopping the subscription instead of skipping the commits (`retention_between_the_log_page_and_its_commits_stops_the_subscription`; fails without the missing-commit check); a commit kept below the horizon is delivered by listed and store-wide subscriptions at `Start::Retained`, which skip the dropped one, while one from the beginning still stops (`a_subscription_started_at_retained_reads_commits_kept_below_the_horizon`); a store-wide subscription at `Start::Retained` over 10 unpruned dead rows at batch size 2 reads the 2 live rows in one page and stores one cursor (`a_store_wide_retained_subscription_skips_dead_rows_in_one_page`; with `read/log.sql` it stores 2, 4, 6, 8, 10, 12); `redefine(Start::Retained)` stores `skip_through` and the redefined subscription delivers the kept commit below the horizon (`a_subscription_redefined_at_retained_reads_commits_kept_below_the_horizon`; fails with `CursorBeforeHorizon` when the redefine stores `skip_through` 0) | `crates/pges-subscribe/tests/horizon.rs` | always |
| Two-step category read against the one-statement `LATERAL` read: random appends, sequencer batches and `delete_stream`s (sequenced and unsequenced) across jobs, orders and parts; a whole subscription and a 3-shard one at a random batch size (small, or 1024) deliver exactly the positions, events and `StreamDeleted` events the `LATERAL` statement gives, with nothing twice, and the shards of the `LATERAL` read add up to the whole. Property test, 10 cases; cutting on the fetched commits instead of the log rows fails it | `crates/pges-subscribe/tests/two_step.rs` (`proptest`) | always |
| Wakers: in-process and poll wake only for watched categories; `Wakers::any` keeps waking after one failed | `crates/pges-subscribe/tests/waker.rs` (`wake-inproc`, `wake-poll`) | with `--all-features` |
| LISTEN/NOTIFY waker: hand-sent `pg_notify` payloads (watched categories, store-wide, level-triggered, `*`, unknown version, malformed); a lost connection through the proxy (fires once on the new connection, then listens); the real outbox sequencer with `watch-notify`, including a run with notify disabled where only the reconnect wakes the subscription; a refused connection retried with backoff, not in a tight loop | `crates/pges-subscribe/tests/notify.rs` (`wake-notify`) | with `--all-features` |
| gRPC waker against real sequencers: a logical sequencer wakes a `run_pg` subscription with no recheck timer; per-category firing, level triggering, a server restart on the same port (pending while down, fires once after); a dropped connection and a silent one found by keepalive (`Proxy::to`); a leader handover with no `Recovering`; the waker follows a crashed leader's successor (the crashed leader reports `Abandoned`); a standby is skipped, and with only a standby it stays pending, then returns `Unreachable`; the waker leaves a stalled leader (held proxy) and fires on its successor (`the_waker_leaves_a_stalled_leader_for_its_successor`); a subscription woken over mutual TLS by host name and by IP, and refused for a waker trusting another CA (`Rejection::Tls`), a server certificate for another host (`Rejection::Tls`), a waker with no client certificate, and a plain `http` client (retried, then `Unreachable`) (`a_subscription_is_woken_over_tls`, `wake-grpc-tls`) | `crates/pges-subscribe/tests/grpc.rs` (`wake-grpc`, default; the TLS test with `wake-grpc-tls`) | always; the TLS test with `--all-features` |
| gRPC waker units: the max-merge never goes backwards (proptest `never_goes_backwards`), the backoff, endpoint validation, slow endpoints reported as `Unreachable` even when every call is dropped (silent local listeners, no database); against a fake tonic watch server: each non-transient code reported as `Rejected` twice in a row (`refusals_retrying_cannot_fix_are_reported_at_once`), four transient codes retried (`transient_refusals_are_retried`), a refusal another endpoint of the round makes up for not reported (`a_refusal_another_endpoint_makes_up_for_is_not_reported`), a position above `Position::MAX` reported as `Protocol` three times in a row (`a_position_no_sequencer_sends_is_reported`); unusable PEMs refused when `ClientTls` is built (`wake-grpc-tls`) | `crates/pges-subscribe/src/waker/grpc.rs` | always |
| Watch proto freshness: the checked-in code matches what protox and tonic-prost-build generate from the proto | `crates/pges-watch-proto/tests/generated.rs` | always |
| Pool size: a `NotifyWaker` on too small a store pool fails with `PoolTooSmall`, for `run` and `run_pg`; `run` on 1, `with_separate_pool` and large enough pools start; `run_pg` on a pool of 1 delivers a batch slower than the lease while a standby on a pool of 1 waits (`run_pg_works_on_one_connection`); a separate pool's listening connection does not count | `crates/pges-subscribe/tests/pool.rs` | always; the `NotifyWaker` part with `--all-features` |
| Store-wide subscriptions: every category in position order, a category registered after start, `all_categories` as a changed definition, a cursor behind any category's horizon, and a property test of interleavings (4 categories, one registered midway, restarts and crashes); a deleted stream (deleted after its commits were sequenced, and before) reaches store-wide subscriptions at batch sizes 1, 2 and 64, a listed-category `run` and a backfill as its `StreamDeleted` events only, also sharded (3 shards over 2 instances); a log row whose commit was deleted by hand, with no `deleted_commits` row, still stops the subscription with `Corrupt` (outbox store, real sequencer); two categories that share an explicit commit id keep their own commits (`categories_that_share_a_commit_id_keep_their_own_commits`) | `crates/pges-subscribe/tests/store_wide.rs` | always |
| Sharded subscriptions: two instances share 4 shards, every event once with `run_pg` and each stream in order; a dead instance's shards move to the survivor; a changed shard count needs a redefine (`ShardsChanged`; to 3 and back to 1, retired rows checked); each stream goes to the shard its id hashes to, checked against the rule computed in Rust; fixed ids map to fixed shards; store-wide shards with `run`; a property test with two instances on 3 shards with restarts and crashes; two instances of a 4-shard subscription at `Start::Retained`, listed and store-wide, each deliver part of the commits and together every commit retention kept once, also those below the horizon in every shard, skipping the dropped ones at or below the start and between the start and the horizon (`a_sharded_retained_subscription_reads_every_kept_commit_once`; fails when the reader does not skip) | `crates/pges-subscribe/tests/shard.rs` | always |
| End-to-end invariants under randomized concurrent workloads: four writers append, append in batches, run units of work (committed, dropped part-way, commits cut short by a timeout), delete streams (some cut short) and save snapshots over overlapping streams of three categories; history written two months back is dropped by window and snapshot-and-truncate retention while `maintain`, `apply_retention` and `prune_log` run; outbox or logical sequencers are crashed (sessions terminated), stopped and replaced; a listed `run_pg` subscription that appends a follow-up per jobs event in its transaction, a store-wide 3-shard `run_pg` subscription over two instances and a store-wide `run` subscription are crashed and restarted; a backfill is cut short and resumed. Checked against a ledger of what each call returned: every acknowledged commit in the log once, the log gap-free; nothing a writer was told failed exists, an unknown outcome is all or nothing; per stream, contiguous versions holding the appended events, an append started after another returned later in the stream and the log, `load` and `load_snapshotted` equal the fold of `read_stream` (and of the dropped history), snapshots equal the fold at their version; deleted streams keep only their tombstone and no snapshot; retention dropped only history of its categories; each `run_pg` subscription and the backfill deliver every surviving event once (the follow-ups match the handled events once), `run` at least once, at their positions and in order; no sequencer or subscription recovers from an error. Each case is seeded: the seed fixes each task's operations, not the interleaving. Two cases of 2.5 s by default (outbox, logical); `PGES_INVARIANTS_SEEDS` (a count, or a comma-separated list) and `PGES_INVARIANTS_MILLIS` run more and longer | `crates/pges-subscribe/tests/invariants.rs` | always |
| Replica reads against a real streaming replica (`replica::Pair`): reads wait until the replica has replayed, for an in-process waker and for none; fallback within a limit of 1 against a limit of 0; store-wide and sharded subscriptions read every page from the replica; store-wide reads, unsharded and sharded, from the replica skip a deleted stream's rows (`a_deleted_stream_is_skipped_by_store_wide_reads_from_the_replica`). The page-header offset is a unit test in `src/replica.rs` | `crates/pges-subscribe/tests/replica.rs` | always; starts a private pair |
| Backfills: every event up to the end once and in stream order with 4 lanes; resume after a fatal handler error and after a dropped future; the end from a subscription and from a sharded one (its lowest shard), `Head`, a fixed definition (`DefinitionChanged`), `UntilAhead` and a corrected rerun, `UnknownSubscription`; an end inside a commit stops before it; `PoolTooSmall` and `UnknownCategory` before reading; `Overtaken` (a second run's move waits for the batch holding the lane, and the next batch's lane lock finds it before its handler runs); a handler whose savepoint begin fails moves no lane and stops the backfill without a retry (it was retried); a handler that ends its transaction with a raw `ROLLBACK` or `COMMIT` moves no lane and stops the backfill without a retry (`a_handler_that_ends_its_transaction_moves_no_lane`), also when it then returns a transient error or leaves another transaction aborted (`a_handler_that_commits_then_fails_is_not_retried`; both were retried and applied twice); a REPEATABLE READ lane whose lane lock waited for another run that moved the lane is `Overtaken` (`a_repeatable_read_lane_overtaken_while_it_waits_is_overtaken`; it failed with a database error, 40001); a backfill reads each category from its retention start position (one category with a horizon and start, one without: `a_backfill_reads_each_category_from_its_retention_start`); with category rates of 100:1 the sparse category's commits are fetched only up to the merge cut, once each, counted as scans of its leaves in `pg_stat_user_tables` after the reading pool closes (3 scans for 3 commits over 30 pages, 26 before; `a_sparse_category_fetches_only_the_commits_up_to_the_cut`); a backfill reads `H` once, at its start, not per page, counted as scans of the log partitions' primary keys in `pg_stat_user_indexes` after the reading pool closes (1 over 10 pages, 11 before; `a_backfill_reads_the_head_once`) | `crates/pges-subscribe/tests/backfill.rs` | always |
| Subscription futures are `Send`, including `GrpcWaker::changed`, a replica subscription and a backfill | `crates/pges-subscribe/tests/spawn.rs` | always; the waker part with `--all-features` |
| Cancellation (`pges_testing::cancel`): each transactional future dropped at every await point leaves no connection in a transaction and no lock: append (autocommit, with `snapshot_after`, under the stream lock), `append_many`, `delete_stream` (warmed up first, since the first delete registers the tombstone type), a unit of work (begin, append, commit), `save_snapshot`, `Store::open` creating a store, category registration. Also: the harness catches a connection returned to the pool inside a transaction begun as text (`the_harness_catches_a_transaction_left_open`); sqlx's own `pool.begin()` dropped at every await point, and with a proxy holding BEGIN's reply so the begin is in flight when dropped, leaves no transaction open (`a_dropped_sqlx_begin_leaves_no_transaction_open`, `a_sqlx_begin_dropped_with_its_reply_held_leaves_no_transaction_open`); and a sentinel for the hazard sqlx 0.9.1 keeps, a savepoint begin dropped in flight rolling back the outer transaction (`a_dropped_sqlx_savepoint_rolls_back_the_outer_transaction`; it fails once sqlx is fixed upstream), and its twin for a savepoint begin that fails on an aborted transaction (`a_failed_sqlx_savepoint_rolls_back_the_outer_transaction`); a unit of work after a dropped savepoint begin refuses appends and fails to commit (`a_unit_of_work_fails_after_a_dropped_savepoint_begin`); a unit of work whose `append_many` is dropped at every await point, with one stream retried after the batch statement, commits the whole batch or none of it (`a_dropped_append_many_in_a_unit_of_work_commits_all_or_nothing`); a unit of work whose append is dropped while its statement waits on a lock, and which then raises a duplicate commit id, fails to commit with `Aborted`, not with that error (`a_unit_of_work_append_dropped_before_it_raised_fails_with_aborted`) | `crates/pges-postgres/tests/cancel.rs` | always |
| Sequencer cancellation: `run` dropped at every await point up to its first batch commit; the lease is freed between runs (`UPDATE pges.sequencer SET lease_until = NULL`) | `crates/pges-sequencer/tests/cancel.rs` | always |
| Subscription cancellation: a `run_pg` subscription and a backfill dropped at every await point, each run under a new name so every run takes the same path | `crates/pges-subscribe/tests/cancel.rs` | always |
| `replica::Pair`: the replica follows the primary, and paused replay holds it back | `crates/pges-testing/tests/replica.rs` | always; starts a private pair |
| Faults: network partitions between the sequencer and the server (killing the sequencer mid-batch is covered by `crash.rs` and `bin.rs`, slot invalidation by `invalidation.rs`, promotion by `promotion.rs`) | containers (toxiproxy); planned | CI only, nightly |
| README code: the workspace README and each crate's, as doctests | `README.md`, `crates/*/README.md`, included by each `lib.rs` (the workspace README by `examples/src/lib.rs`) | always; the workspace README with `pges`'s `derive`, the `pges-sequencer` README with `source-logical` |
| Examples: each of the ten examples runs to the end, including `metadata` (a request scope, a follow-up from a handler, otel trace context through the SDK tracer provider and tracing-opentelemetry) | `examples/tests/run.rs` | always |
| Benchmark smoke: each `pges-bench` command runs for about a second with small settings (`append_runs`, `sequencer_runs`, `outbox_notify_runs`, `read_runs`, `partitions_runs`, `wake_runs`; `scale_runs` fills 1000 streams and runs every `scale` action once, retention dropping a `jobs` leaf) | `bench/tests/smoke.rs` | always |
| Benchmarks: append rate (`--statistics autovacuum` skips the `ANALYZE` after the prefill), sequencer lag, outbox NOTIFY lock, catch-up and load, reads as leaves grow (two-step against `LATERAL`, plan-cache memory in auto mode), commit to subscriber woken with the in-process and gRPC wakers for 1 to N subscriptions (`wake`, run at 1 and 200; design.md, Spike results and Durable dev-box run); the scale targets in a database that outlives the command (`scale fill`, then `append`, `load`, `catch-up`, `drain`, `retention`; a 100M-stream fill takes about an hour and 180 GB; design.md, Scale run); `pges-bench server start` gives a durable server | `cargo run -p pges-bench --release -- <command>` | on demand; on a durable server |

`pges-replication/tests/auth.rs` starts a private `initdb` cluster with one auth method per role when
`initdb` and `pg_ctl` are on the PATH (the Nix shell has them), and skips that test otherwise. SCRAM over the
shared server needs the Docker container: run with `PGES_TEST_BACKEND=docker` and `PGES_TEST_DATABASE_URL`
unset.

**Compile-fail tests.** Seven crates have a trybuild `tests/compile_fail.rs`: pges-core, pges-derive,
pges-postgres, pges-replication, pges-sequencer, pges-subscribe and the `pges` facade. Their `.stderr`
snapshots hold rustc's own wording, which changes between releases. Every crate's `tests/compile_fail.rs`
runs only when `rustc --version` is the `TOOLCHAIN` it names (1.97.1, the stable CI pins in ci.yml); other
compilers, the MSRV job's 1.94 included, skip it with only a message on stderr, so the test passes without
checking anything. Run them locally on 1.97.1 (`cargo +1.97.1 test --test compile_fail`). To move to a new
stable, bump the ci.yml pin of the lint and test jobs, the 7 `TOOLCHAIN` constants and the `.stderr` files
together, and re-bless with `TRYBUILD=overwrite cargo test --test compile_fail`. trybuild runs `cargo check`,
so a check that runs only after monomorphisation, such as `Event::__CHECK`, is tested by a `compile_fail`
doctest instead, which builds. trybuild keeps renamed dependencies, which is why the facade's case proves
`::pges_core` is missing there.

`pges-replication/tests/tls.rs` starts its own PostgreSQL 18 container named `pges-test-tls-<uuid>` and
removes it at the end. It generates a throwaway CA and server certificate with rcgen. The CA key is P-384,
so channel binding hashes with SHA-384, not SHA-256: the SCRAM-SHA-256-PLUS hash follows the certificate's
signature algorithm. PostgreSQL needs a server key it can read that is owned by its own user with mode 0600
(or by root with 0640), and testcontainers copies files in as root. So the test copies the certificate and
key to `/pges-tls`, and a `/docker-entrypoint-initdb.d` script, which runs as `postgres`, copies them into
`$PGDATA` with mode 600,
turns `ssl` on and writes a `pg_hba.conf` with `tls_only` and `plain_only` roles. The test skips when
`docker info` fails. The same file runs in-process fake servers (plain TCP, and rustls with an rcgen
certificate) that need no Docker. They check the `N` fallback of `prefer`, the GS2 flags
(`p=tls-server-end-point`, `y`), that `channel_binding=require` refuses a server that skips SCRAM, and that an
`AuthenticationOk` before the server's final SCRAM message is a protocol error. CI runs it with
`--all-features`; run `cargo test -p pges-replication` without features too, which checks the
`TlsFeatureRequired` errors. The TLS container helper stays in this file until another crate needs it; then
it moves into `pges-testing` behind a feature, so rcgen stays out of its normal dependencies.

`crates/pges-postgres/tests/fault.rs` covers the append retry after a lost connection and `Exact` racing a
stream's creation, through `pges_testing::proxy`. It also plays a lost COMMIT of the locked autocommit
append: a deferred constraint trigger on `es.commits` waits on an advisory lock, so the proxy holds the
reply exactly at COMMIT, then drops the connection; the retry returns `Deduplicated` with the new commit
id. `a_failed_rollback_keeps_the_aborted_error` checks that `UnitOfWork::commit` returns `Aborted` when its
rollback fails on a dropped connection.

The promotion test (`crates/pges-sequencer/tests/promotion.rs`) starts a primary and a replica with
`replica::Pair::start_failover`: the replica streams through the physical slot `pges_standby` as the role
`pges_standby`, with `sync_replication_slots = on`, `hot_standby_feedback = on` and `dbname` in
`primary_conninfo`. A sequencer with a failover slot runs on the primary until the replica holds a persistent
synchronised copy of the slot at the stored token (`pg_log_standby_snapshot()` helps the copy become
persistent; slot synchronisation naps up to 30 s, so the waits allow 90 s). A commit is then appended and
replayed on the replica, and `Pair::cut_off` takes `REPLICATION` from the replica's role and ends its
walsender. Slot synchronisation uses an ordinary connection, so it goes on while the WAL stream is cut. A
sequencer on the primary with a one-hour `status_interval` (so no idle position moves the slot further)
sequences the commit, stores the token and confirms it; the replica copies that position, because it has the
WAL up to it, but not the batch. After `Pair::promote`, a sequencer on the promoted replica halts with
`SlotLost::Ahead`, `resequence` sequences the commit, and a restarted sequencer sequences a new append. The
whole test takes about 90 s. The local backend runs the pair on fixed ports (54401, 54402), so two runs on one
machine collide; Docker does not.

`crates/pges-postgres/tests/append_many.rs` runs the SKIP LOCKED restart deterministically: the batch waits
on one stream's lock while another stream is created and locked elsewhere, and must roll back and start
over. Its stress test, `batches_creating_the_same_streams_never_deadlock`, deadlocks within seconds when
the pooled retry is changed to wait instead of skipping. `crates/pges-postgres/tests/append.rs` races
`append_many` batches on a pool with `enable_nestloop` and `enable_mergejoin` off (hash joins) against a
normal pool, to check the lock order does not depend on the plan.

`a_failed_rollback_keeps_the_registration_error` polls `pg_stat_activity` until the registration waits on
the lock. If the registration ends first, the test fails instead of hanging.

The logical restart test (`crates/pges-sequencer/tests/logical.rs`) stores no token for an hour, writes
flushed unrelated WAL so keepalives arrive, and checks that `confirmed_flush_lsn` stays below the end of the
first commit whose token is not stored. The bound is that commit's end, not the WAL position before it: a
keepalive's `wal_end` may fall inside a transaction that commits later. The slow-batch test sets
`ALTER DATABASE … SET wal_sender_timeout = '1s'`, which logical walsenders honour. It waits for states, not
for times: until the batch waits on the lock of the log (`pg_locks`), then, in the control run without status
updates, until the slot's walsender is gone and the sequencer has reported the error. The sequencer notices
the lost connection only when it next uses it, after its batch commits, so the batch's log rows can be
visible before the error; reading the errors as soon as the rows appeared failed under load. The run with
status updates checks that the slot keeps the same walsender while the batch waits three times
`wal_sender_timeout`.

`a_commit_racing_the_slot_creation_is_not_skipped` holds a unit of work open, starts the sequencer, waits
until the walsender waits on a lock (`CREATE_REPLICATION_SLOT` waits for open transactions), then commits.
Without the exported-snapshot check the commit was skipped silently. The binary's logical smoke test runs
`pges-sequencer bootstrap` first, the documented deploy order: appends made after the slot appears but
before `Status::Leading` race its creation.

`pges_testing::replica::Pair::start()` starts a private primary and a streaming hot standby, on the backend
`pges-testing` would use: two Docker containers on a network of their own (the replica runs
`pg_basebackup -R`), or, with the `pg-up` backend, two clusters in a temporary directory started with
`initdb`, `pg_ctl` and `pg_basebackup`, on unix sockets only. A pair takes about 3 s on Docker and 2 s
locally; every replica test starts its own. `pause_replay` and `resume_replay` make the replica lag, so tests
use real lag, not a simulated one. The shared server is never touched, and a pair dropped without `stop` is
removed too. `Pair::start_failover()` starts a replica that synchronises failover slots and can be cut off
(`Pair::cut_off`) and promoted (`Pair::promote`); the promotion test above uses it. `Pair::caught_up` compares the insert LSN with the replay LSN without the page-header
allowance, so on a page boundary it can wait until unrelated WAL is written (up to about 15 s; its timeout is
30 s).

`crates/pges-sequencer/tests/invalidation.rs` starts its own PostgreSQL 18 container
(`pges-test-slot-loss-<uuid>`, removed at the end) with `max_slot_wal_keep_size = 1MB` and
`wal_keep_size = 0`, which must never be set on the shared server. With no stored token (token interval
1 h), the slot never moves past the first commit while the sequencer streams. A few rounds of a 2 MB
insert, `pg_switch_wal()` and `CHECKPOINT` invalidate it (`wal_removed`) and end the walsender. Four
writers keep appending through the halt, `resequence` and the restart; `check_log` and
`check_every_commit_once` (`tests/common`) check the result. A sample run sequenced 17 commits before the
halt, recovered 43 and sequenced 335 after. It skips when Docker is not available.

Retention tests write commits months in the past. They raise the store's skew to append them, then lower
`pges.stores.max_clock_skew` and `max_txn_age` again with direct SQL before retention runs, because
`Admin::set_writer_limits` only raises (`Fixture::limits` in `pges-postgres/tests/retention.rs`,
`default_skew` and `set_skew` in `pges-sequencer/tests/retention.rs`, and `pges/tests/retention.rs`). Most of
these histories are ones honest writers produce, compressed into seconds. The resequence-reorder tests are
not: a recovered commit sits a month before a kept one, which honest limits never allow. They exercise the
recorded-range path; the `CommitsBeforeSlot` path reaches the same reordering with honest limits, but would
need hours of real time. `retention_sees_a_resequence_that_lands_during_its_run` relies on lock timing: once
retention waits on the lock, the test must halt and resequence within the detach retry budget (8 attempts,
about 13 s); locally it takes under a second.

The NOTIFY failure test needs no special role: it creates `public.pg_notify(text, text)` that raises, and
sets `ALTER DATABASE … SET search_path = public, pg_catalog`, so the sequencer's unqualified `pg_notify`
resolves to it on new connections. After `DROP FUNCTION` the cached plan resolves to `pg_catalog` again. A
sequence (`nextval` is not transactional) counts the calls despite the RAISE.

The sequencer fixture's outbox source rechecks only hourly, so a missed notification stalls the test rather
than being covered by the fallback. Waits inside a cancel-safe `Source::ready` must survive being dropped at
every lease renewal (200 ms in `fast()`). Keep deadlines and in-flight connects in the source, not in the
future.

`crates/pges-sequencer/tests/crash.rs` stops the leader at an exact point with `Faulty`, a wrapper around the
real source (a fake of `Source`, so the published crate carries no failpoint). For the kill before the log
commit, the wrapper runs `SET LOCAL pges_test.fault = 'hang'` in the batch transaction, and a test-only
statement trigger on `es.log` (`AFTER INSERT … FOR EACH STATEMENT`) calls `pg_sleep(3600)` when the setting
is on; the test waits for `wait_event = 'PgSleep'` before the kill. The batch transaction holds the fence
row, so no other leader slips in meanwhile. The other two points stop inside `ack` (before the inner source's
`ack`) and in the next `ready`. Only the kill point is deterministic: which commits land in the stopped batch
depends on timing. `Running::crash` drops the task and then ends every session of the sequencer's named pool
with `pg_terminate_backend` (matched by `application_name`); dropping the task alone lets sqlx roll back
politely, which is not what a crash looks like to the server. The logical source's replication connection has
no `application_name`; its walsender ends when the dropped task closes the socket, the same EOF as a process
death. `check_rolled_back` runs only in restart cases: with a standby, the batch may already be sequenced
again. `check_commit_order` (in `tests/common`) uses a tick clock shared by the writers: an append that
started after another returned must come later in its category and in the store. It does not cover
overlapping appends. The SIGKILL tests in `bin.rs` kill after a count of new log rows, so they are not
guaranteed to land both before and after a token store.

**Crashed subscription instances use their own pool.** `Crashable` (`crates/pges-subscribe/tests/common`,
used by `shard.rs`, `merge.rs` and `store_wide.rs`) opens its store on a pool of its own, and `crash()` aborts
the task and then closes that pool. A real crash closes the process's connections; aborting a task does not.
Before sqlx 0.9.1 this was also the only defence against the sqlx begin hazard: sqlx 0.8 (0.9.0 too) counted a
transaction only once BEGIN returned, so a task aborted inside `begin()` after BEGIN was sent left its pooled
connection in a transaction sqlx did not know about, and every later statement on it ran in that
transaction, never committed and kept its locks. In the shard interleavings proptest this showed as one
connection idle in transaction for 18 minutes, blocking renewals, a heartbeat and an append. sqlx 0.9.1
fixes it (design.md, Decided, sqlx 0.9.1); `Crashable` keeps its own pool because a crash closes
connections. Both failing seeds
are saved in `crates/pges-subscribe/tests/shard.proptest-regressions`, so every run replays them first.
A third seed there caught a crash race: aborting an instance dropped its waker fan-out while a shard's task
was still being polled, and that shard reported `the waker's source closed` as a recovered error (about one
run in 20 replayed it). A shard's waker now waits once the fan-out is gone
(`fanned_wakers_wait_once_the_fan_out_is_gone`).

**Cancellation.** `pges_testing::cancel` drops a future at each of its await points and checks what it left
behind. `drop_at_every_await(pool, make)` builds a fresh future with `make` and runs it for N = 1, 2, 3, …
polls until a run completes. `poll_then_drop(fut, n)` waits for the waker between polls, then 2 ms of quiet,
so the next poll runs on to the next round trip and a given N stops every run at the same await point.
`check_released(pool)` then checks every connection of the pool's application (`application_name =
pges-cancel`): none is in a transaction or holds a lock (it waits up to 10 s for closing connections to end);
the connection it checks from is itself outside a transaction (two `transaction_timestamp()` reads differ)
and holds no advisory lock. It then refills the pool and waits until every connection is idle.
`statement_timestamp()` cannot serve for the self-check: over the extended protocol it differs from
`transaction_timestamp()` even in autocommit.

A poll after which the future has already woken itself is a yield, not a wait for the server:
`poll_then_drop` yields once itself (tokio delivers a yield's wake only at the end of the scheduler tick),
then polls again at once without counting it. sqlx 0.9 yields about 800 times per new connection while it
derives the SCRAM key; counted, they made the sequencer's drop test, which connects anew per run, sweep
hundreds of polls per run and take more than 15 minutes. A server reply that lands within that one yield is
taken for a yield too, so its await point is not counted in that run.

`cancel::pool(url, n)` turns off the statement cache (`statement_cache_capacity(0)`) and the ping on acquire
(`test_before_acquire(false)`). A run dropped before a statement was cached, or during the ping (which closes
the connection), changes the path the next run takes, so the same N would stop it somewhere else; together
with the settled pool between runs, this is what makes the sweep reach the BEGIN window. Mutation testing
showed the sweep skipping it without them. A completed run's output must not hold a connection while the
check runs: `a_dropped_sqlx_begin_leaves_no_transaction_open`'s closure drops its transaction (`pool.begin().await.map(drop)`). The sweep is not
fully deterministic: under heavy load a BEGIN can complete within one poll, and that run passes, so the
harness can miss the window but does not flake. A test that must catch the window holds the reply instead:
`a_sqlx_begin_dropped_with_its_reply_held_leaves_no_transaction_open` and the savepoint tests connect
through `pges_testing::proxy`, hold server-to-client bytes (`hold(Direction::ToClient)`) before the one poll
that sends BEGIN (or SAVEPOINT), and release them after the drop. `cancel::poll_check_drop` awaits
`cancel::wait_until_waiting` before the drop, which checks in `pg_stat_activity` that the statement ran on
the server, so the test cannot pass with the statement still unsent. `the_harness_catches_a_transaction_left_open`
checks the sweep still sees a connection returned inside a transaction (one begun as text, which sqlx does
not track).

**A listed subscription only wakes for its own categories.** Wakers fire for a listed subscription only
when one of the categories it reads moves. An append to another category does not make it read again, so its
cursor can stay below the store's newest position. A test that waits for a position must make its last
append in a watched category. `replica.rs`'s
`store_wide_and_sharded_subscriptions_read_every_page_from_the_replica` failed intermittently for this reason
("waiting for position 3 … high_water: Position(2)"): its last append went to `parts`, which the listed
subscription does not read. It now appends `parts` first.

**At-least-once tests count distinct events.** `examples/tests/run.rs`'s `sharded_runs` runs the `sharded`
example, whose handler under `run` records each (stream, version) once and checks that each stream's first
arrivals come in order. Comparing `delivered == appended` exactly failed when a lease handover redelivered
events. The example also waits until the instances' `Status.shards` together cover every shard and each
reached the end: while a shard moves, neither instance holds it.

**Statement registries.** Each crate with SQL has `tests/statement.rs`, built on `pges_testing::statement`:
`check_files`, `check_no_inline_sql`, `check_typed_calls`, `check_typed` and `run_ddl`. `check_typed` checks a
query's Rust types three ways: the PostgreSQL type each Rust type's `typed::Value` registers equals the one
sqlx sends; `describe` (for a parameter PostgreSQL cannot infer, 42P18, 42P08 or 42804, it gives that one
parameter its Rust type's and retries) must report parameters the Rust types accept, the registered column
names, and columns the Rust types decode; and a prepare with the Rust parameter types, as sqlx runs it, must
agree too (the connection's statement cache is cleared first, since sqlx keys it on the SQL alone). Where
`describe` reports nullability, `Option<T>` on a NOT NULL column and `T` on a nullable one fail; where it
cannot tell, either passes. A failure lists the parameter types PostgreSQL infers and each column's type and
nullability, also when PostgreSQL rejects the Rust parameter types, so to choose a new statement's types,
declare a guess and run the test. Queries are checked on a store made with `pges_testing::create_store` (with
`pges_testing::definition`) and a category registered through `Admin`; pges-postgres and pges-sequencer use an
outbox store, so every table exists. pges-testing's fixtures (`register_category`, `create_log_partition`)
render pges-postgres's partition statements, not copies. These tests cannot see a call site that forgets a
placeholder value: some other test must run each call site.

To check that a refactor sends the same SQL, compare what the server receives from two builds. Start a
private cluster (`PGES_PG_DIR=<dir> nix develop -c pg-up`, or `initdb` by hand), run
`ALTER SYSTEM SET log_statement = 'all'` (and `log_line_prefix = '%a|'` to tell sessions apart), point the
tests at it (`PGES_PG_DIR`, or `PGES_TEST_DATABASE_URL=postgres://postgres@localhost/postgres?host=<dir>&port=<p>`),
run the suite on each build, and compare the distinct statements after masking `t_*` database names, UUIDs
and snapshot names. The shared `./.pg` log mixes in other sessions. With `PGES_TEST_DATABASE_URL` set, the
replica tests use Docker.

Not covered by a test yet: redefine's database-error retry; resequence's `LeaseLost` through the fence check
in `write()` (the test reaches it through the lease renewal while the slot is created) and the
`OBJECT_IN_USE` branch of `SlotInUse` (another process takes the slot between the activity check and the
drop; not deterministic); the drop of a new slot failing after a failed check; and that a side renewal's own
fence check (not only the fenced checkpoint) stops the checkpoint: the outcome is the same either way; a
renamed `pges` dependency end to end (only the lookup's unit tests cover it); a deleted stream with the
logical source (its late log rows behave like the outbox case, which is tested).

`crates/pges-watch-proto/src/generated/` is checked in. After editing the proto, regenerate it with
`PGES_REGENERATE=1 cargo test -p pges-watch-proto --test generated`; without the variable the test fails on
any difference. protox and tonic-prost-build are dev-dependencies only, so no build needs `protoc`.

The binary's gRPC test (`the_binary_serves_the_grpc_watch`) picks a free port by binding and dropping a
listener before the binary binds it, so another process can take the port in between. It then listens on
`localhost:<port>`, a port found free on 127.0.0.1 only: if it is taken on `::1`, the bind fails with
`AddrInUse` and the test fails. This is rare.

CI runs doctests with `--all-features` only; with fewer features (for example
`cargo test -p pges --no-default-features --doc`) they are not checked.

## CI

`.github/workflows/ci.yml` runs formatting, clippy, the test suite (testcontainers on the runner's Docker),
the MSRV build and `cargo deny`. The lint and test jobs are pinned to Rust 1.97.1
(`dtolnay/rust-toolchain@master`), the toolchain the compile-fail snapshots match; the MSRV job uses 1.94. CI
also builds each crate's rustdoc with no features and with each feature alone (`-D warnings`); the list of
crates and features in that step is generated from `cargo metadata`, and it also runs `cargo test --doc` per
feature. GitHub service containers cannot pass `-c wal_level=logical` to PostgreSQL, so the tests start their
own container. A separate nightly workflow repeats the concurrency tests under contention (Stress, below).

The test job also runs `cargo test -p pges-core -p pges-postgres -p pges-subscribe -p pges-sequencer` on
default features (since 79b7004). `pges-examples` enables optional features of `pges` (`otel` among them),
and feature unification turns them on for every crate in a workspace run, even without `--all-features`. So
`cargo test --workspace` never builds pges-postgres without `otel`, and only a per-crate run tests the paths
that exist without a feature (for example metadata resolution without a span).

## Stress

`.github/workflows/stress.yml` runs every night (03:17 UTC) and on demand (`workflow_dispatch`, with a budget
in minutes and a filter). It is not a required check, and `ci.yml` is unchanged. It runs the tests in
`.github/stress/tests.txt` over and over, one test per process, against a server held to few CPUs, several at
once, to find races that a single run rarely hits: the `append_many` deadlock that the retry lock's
`lock_timeout` fixed passed more than 1000 local runs of `batches_creating_the_same_streams_never_deadlock`
and showed up once in CI, on a starved runner.

`.github/stress/tests.txt` is the one list, a line per test: package, target (`lib` for unit tests, otherwise
the integration test file), the exact test name or `*` for every test of the target not `#[ignore]`d, and an
optional weight (runs per round of the list). It holds the tests where repetition adds signal: `append_many`'s
lock order and retries (`the_retry_lock_of_a_batch_never_waits` and the deadlock tests weighted highest), the
contended and racing appends, concurrent store creation, the cancellation tests of all three crates, the
sequencer's lease, term and crash tests, and the subscriptions' takeover, fencing, lease and shard tests.
Tests that start private containers or clusters (`replica::Pair`, `invalidation.rs`, TLS, auth) are left out.
The script checks every line against the built test binaries (`--list`) before it starts and fails if one
names a package, target or test that no longer exists, so a renamed test cannot drop out silently. A new
concurrency test belongs in the list; a `*` line picks up new tests of its file by itself.

`.github/stress/run.sh` builds the workspace's tests once (`--all-features`), starts its own `postgres:18`
container (the settings of pges-testing's `SERVER_ARGS`, plus `max_connections = 600`,
`deadlock_timeout = 50ms` so a deadlock fails its run sooner, and `log_lock_waits`), points the tests at it
with `PGES_TEST_DATABASE_URL`, and runs `STRESS_JOBS` workers until `STRESS_BUDGET_SECS` is over. Each worker
goes round the list from its own starting point, running one test per process (`--exact --test-threads 1`,
in the package's directory as `cargo test` would); a run still going after `STRESS_RUN_TIMEOUT_SECS` (600) is
killed and counted as timed out. Failures do not stop the run. At the end it prints a table (test, runs,
failed, timed out, mean and max seconds) and the count of `deadlock detected` lines in the server log, writes
it to the job summary, and exits 1 if any run failed. The workflow uploads `target/stress/` (the summary, each
failed run's output named by time, test and outcome, and the server log with each deadlock's DETAIL) as the
`stress-<profile>` artifact.

A failure counts only while the server is up. After each run that fails or times out the script checks the
container (`docker inspect`, then `pg_isready` with retries); if the server is gone (killed, out of memory,
Docker restarted) the run is recorded as `server-lost`, not as a failure, every worker stops, the server's
container state goes to `server-state.json`, and the job fails with an error saying the server stopped and the
run is incomplete. Without that check, a server lost mid-run turned into dozens of `connecting to the test
server` / `Connection refused` failures in a few seconds, as the workers kept starting tests against nothing.

The workflow runs two profiles side by side, each with a 45 minute budget: `slow-server` (server on 1 CPU, 4
runs at once) and `starved-runner` (server on half a CPU, 3 runs at once, 3 busy loops taking CPU from the
test processes too). A build takes about ten minutes more. A manual run takes a budget of 1 to 75 minutes, so
the budget, the build and a last run past the budget fit in the job's 110 minute limit.

Run it locally with a short budget (it needs Docker, and `STRESS_FILTER` narrows the tests by an extended
regex over `package/target/test`):

```sh
STRESS_BUDGET_SECS=180 STRESS_JOBS=2 STRESS_FILTER='append_many|retry_lock' .github/stress/run.sh
```

`STRESS_PG_CPUS`, `STRESS_PG_CPUSET` (docker `--cpuset-cpus`, to keep the server on given CPUs of a shared
machine), `STRESS_CPU_HOGS` and `STRESS_OUT` (default `target/stress`) set the rest. Changing
`set_config('lock_timeout', '1ms', true)` to `'0'` in `crates/pges-postgres/sql/append/try_lock_stream.sql`
brings the deadlock back; the stress run should then fail.
