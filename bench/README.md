# pges-bench

Benchmarks for pges: the measurements its design choices rest on. Not published.

```sh
cargo run -p pges-bench --release -- append
cargo run -p pges-bench --release -- sequencer
cargo run -p pges-bench --release -- outbox-notify
cargo run -p pges-bench --release -- read
cargo run -p pges-bench --release -- partitions
cargo run -p pges-bench --release -- wake
cargo run -p pges-bench --release -- server start
cargo run -p pges-bench --release -- <command> --help
```

| Command | Measures |
|---|---|
| `append` | Appends per second and p50/p99/p99.9 latency for one hot stream and for `--streams` streams (default 100k), single appends and batches (`append` with `--batch` events to the hot stream or, with `many-events`, to a random one; `append_many` of `--batch` streams), at `--concurrency`. The tables are analysed after the prefill, as autovacuum would; `--statistics autovacuum` skips that, so the measured appends start with the plans the prefill cached, as on a store just created and filled. WAL bytes per event from the insert LSN, WAL records and full-page images from `pg_stat_wal`, and the full-page-image share of the bytes from `pg_walinspect` when the role may create it. A `CHECKPOINT` runs first unless `--checkpoint skip`. |
| `sequencer` | For the logical and the outbox source, at payloads below and above the TOAST threshold (`--payloads 100,4000`): the sequencer's throughput draining a backlog, then the lag from an append returning to the in-process watch announcing its position, under `--rate` events per second. |
| `outbox-notify` | Appends per second of an outbox store against a store without it, at each `--concurrency` level, to show the server-wide NOTIFY lock. |
| `read` | The rate of a subscription catching up from the beginning over `--events` events, and `load` latency for streams of each of `--lengths` events. |
| `partitions` | For a category with each of `--leaves` daily time leaves (default 100 and 1000) holding `--events` commits: catch-up of a category and a store-wide subscription; planning and execution time with custom and generic plans, the plan cache's memory and the relation locks (and how many were fast-path) of the category read's two statements (a page of the category's log, then its commits by id, the fetch the store-wide read uses too) and of the earlier one-statement form with a `LATERAL` lookup; `--readers` concurrent category reads in each form with the share of samples waiting on the lock manager. |
| `wake` | Commit to subscriber: for each `--wakers` (the sequencer's in-process watch, its gRPC watch) and each `--subscriptions` count, that many live subscriptions of one category, on a pool of `--subscriber-connections`, while writers append single events at `--rate`. The delivery lag of every event in every subscription, from its append returning to a handler receiving it (sequencing, the wake-up and the read); the delivered rate; the subscriber connections in use; `Recovering` states. |
| `scale` | The scale targets, in a database that outlives the command (below): `fill` generates a store of `--streams` streams and their commits and log rows in SQL; `append` (to random existing or new streams; with `--batch`, `append_many` of that many streams per call), `load`, `catch-up` (a category or the store, from a share of the log), `drain` (bootstrap and resequence, then the sequencer draining a backlog) and `retention` (a window on one category, timed) measure it. |
| `server` | `start` runs a private PostgreSQL 18 container with production-like durability (below) and prints its URL; `--replica async` or `sync` adds a streaming hot standby. `stop` removes it with its data. `settings` lists the settings. |

The closed-loop measurements (`append`, `outbox-notify`, `load`) run unmeasured for `--warmup` (1 s) first,
so connections are open and statements prepared. The sequencer's writers are an open loop: a call's latency
counts from when it was due, so a stall shows in the calls it delayed. Its lag is exact per event: each
append's position is read back from the log by commit id.

Each result is a JSON line on stdout; the first line describes the machine and the server settings. A
table of the results follows on stderr.

Every measurement runs in a scratch database that is dropped afterwards, with its replication slot:

- on the server of `--database-url`, whose role needs `CREATEDB` (and `REPLICATION` for the logical
  source; `pg_checkpoint` for `append`'s `CHECKPOINT`);
- otherwise on the PostgreSQL 18 from `pges-testing`: `PGES_TEST_DATABASE_URL`, the local `pg-up` cluster,
  or Docker. Those servers run with `fsync`, `synchronous_commit` and `full_page_writes` off, so their WAL
  and latency numbers are not those of a durable server.

WAL counters are server-wide: run the benchmarks on a server nothing else writes to.

## Scale

`scale` needs `--database-url`. Its actions work in the database `--database` (default `pges_scale`) on that
server, which `fill` creates and nothing drops: drop it, or stop the server, when done. At `--streams
100000000` a fill takes about an hour and about 180 GB of disk, WAL included (about 1.8 GB per million
streams). `retention` runs until every eligible leaf is dropped or refused, which can take hours on a large
store.

```sh
pges-bench --database-url $URL scale fill --streams 100000000
pges-bench --database-url $URL scale append --streams existing --concurrency 1,16,64
pges-bench --database-url $URL scale load
pges-bench --database-url $URL scale catch-up --scope store --from 0.5
pges-bench --database-url $URL scale drain
pges-bench --database-url $URL scale retention --category jobs --keep-days 7
```

`fill` writes three categories: `orders` (monthly leaves over `--months`, single-event commits, 60% of the
streams, 2.5 commits each), `audit` (monthly, 10-event commits, 20%, 2 each) and `jobs` (daily leaves over
`--days`, single-event commits, 20%, 2 each), with commits spread evenly in time up to a minute before the
fill. Rows go into the tables the crate's DDL created, but unlike appends: in multi-row inserts, `streams`
rows inserted once and never updated, and log rows written in commit id order instead of by a sequencer.
`--content streams` writes only `streams` rows, to compare `--stream-partitions` counts. `drain` and
`retention` need the writer role (`--writer-role`, default `postgres`) to cap its transactions: `ALTER ROLE
postgres SET transaction_timeout = '1h'`. `drain` leaves the replication slot behind; drop it before long
runs, or it holds WAL.

## A durable server

`pges-bench server start` creates the container `pges-bench-durable` (port 54330 on 127.0.0.1; `--name`,
`--port`) and keeps it until `pges-bench server stop`. Its data lives in a Docker volume, so it is on the
host's disk, not in the container's overlay. Benchmark it with
`--database-url postgres://postgres:postgres@127.0.0.1:54330/postgres`.

The settings (`pges-bench server settings`; `--set name=value` overrides one or adds another):

| Setting | Value | Why |
|---|---|---|
| `fsync`, `synchronous_commit`, `full_page_writes` | on | durability as in production |
| `wal_level` | logical | the logical source |
| `wal_compression` | lz4 | data checksums (on by default in PostgreSQL 18) make hint-bit writes emit full-page images; compression shrinks them |
| `shared_buffers` / `effective_cache_size` | 8GB / 32GB | a share of the box's 91 GiB, which other work also uses |
| `max_wal_size` / `min_wal_size` | 16GB / 2GB | checkpoints come from `checkpoint_timeout` up to about 30 MB/s of WAL; faster runs (`many-batch` writes about 50 MB/s) also start them on WAL volume |
| `checkpoint_timeout` / `checkpoint_completion_target` | 5min / 0.9 | a 10-minute run sees two checkpoints and the full-page images after each |
| `wal_buffers` | 64MB | |
| `logical_decoding_work_mem` | 256MB | the sequencer receives whole transactions (`streaming = off`), so a smaller value spills large units of work to disk |
| `max_locks_per_transaction` | 256 | many live leaves; in PostgreSQL 18 it also sets the fast-path lock slots per backend |
| `random_page_cost` / `effective_io_concurrency` / `io_workers` | 1.1 / 64 / 8 | NVMe |
| `autovacuum_max_workers` / `autovacuum_vacuum_cost_limit` | 6 / 2000 | autovacuum keeps up with the hot tables |
| `max_connections` / `max_wal_senders` / `max_replication_slots` | 300 / 16 / 16 | |
| `max_slot_wal_keep_size` | 50GB | an abandoned slot cannot fill the disk |
| `hot_standby_feedback` | on | for the replica |
| `shared_preload_libraries` | pg_stat_statements | created in `postgres` and `template1`, with `pg_walinspect` |
| `track_io_timing`, `track_wal_io_timing`, `log_checkpoints`, `log_lock_waits` | on | |
| `log_autovacuum_min_duration` | 0 | every autovacuum run is in the container's log |

For WAL and full-page-image figures over runs longer than a checkpoint, add `--set wal_keep_size=64GB`, so
`pg_walinspect` can still read the run's WAL at the end.

The container shares the machine with the benchmark client. Numbers from it describe that machine: its WAL
flush latency (`pg_test_fsync` in the container) bounds a single writer's commits per second.

The numbers are a baseline for one machine and one configuration, to compare changes against. They are not
targets. `tests/smoke.rs` runs each command for about a second under `cargo test`, so the benchmarks keep
compiling and working.
