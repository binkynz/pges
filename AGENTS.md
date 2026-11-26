# pges

Guide for contributors and coding agents working on pges, an event store on PostgreSQL 18+ published on
crates.io. The design is in `docs/`: `design.md` (architecture and decisions), `schema.md` (tables, and the
design of each statement; it points at the SQL files), `api.md` (crates, features, Rust API, versioning),
`testing.md`. Read the relevant one before changing behaviour, and update it in the same change. Record design
decisions in `design.md` ("Decided").

## Contributing

- Work on a branch and open a pull request to `main`. CI (`lint`, `test`, `msrv`, `deny` in
  `.github/workflows/ci.yml`) must pass before it merges.
- Pull requests are merged by rebase only, so each commit lands on `main` as written.
- Commit messages follow Conventional Commits: `type(scope): subject`, with `!` for a breaking change
  (`fix(postgres): ...`, `build(deps)!: ...`). The scope is usually the crate without its `pges-`
  prefix.

## Imports

Import the module, not the leaf type, for workspace crates and for this crate's own modules. Reference types
through their module so the reader always knows where a type lives. Types from `std` and third-party crates
(`Uuid`, `PgConnection`, `Serialize`) are imported directly.

```rust
use pges_core::{stream, version};

fn load(id: stream::StreamId, from: version::Version) { ... }
```

Never:

```rust
use pges_core::stream::StreamId;
```

Import groups, separated by a blank line:

1. `std`
2. third-party crates
3. workspace crates
4. `crate` / `super`

## Comments and docs

This is a published library, so every public item has a `///` doc and each crate root has `//!` docs; docs.rs
is the user manual. Docs say what the item guarantees and when it fails, not how it is implemented.
`#![warn(missing_docs)]` in every published crate.

The workspace and each crate have a `README.md`. A hidden `#[cfg(doctest)]` item in the crate's `lib.rs`
includes it, so its code runs as doctests; `crates/pges` also includes the workspace README. README code
follows these rules and must compile. Examples that need a database are `no_run`.

Everywhere else, the code names things; if a name doesn't carry the meaning, rename it. Comments are only for:

- a constraint invisible from the code (why an order, a lock, a sequence setting matters);
- TODOs and design questions.

## Whitespace

Blank line between logical steps inside a function: after a guard clause, between "get" and "use", before a
return block.

## Types

Types are self-descriptive. If a type needs a comment to explain what it is, rename or restructure it.

- No `bool` fields or parameters: a two-state fact is a two-variant enum (`Outbox::Enabled | Disabled`).
- No bare `Uuid` or unit-less integers in the API: ids, versions and positions are newtypes (`StreamId`,
  `CommitId`, `Version`, `Position`, `StoreId`, `CategoryId`). Conversion to and from SQL types happens in
  the crate that owns the statement (`pges-postgres` for the store, `pges-sequencer` for the sequencer),
  never in `pges-core`, except the infallible signed accessors a type's bound makes possible
  (`Position` / `Version::{to_i64, from_i64}`, `EventCount::to_i16`).
- Option values with a range use a checked type: `interval::Interval` (1 ms to 30 days),
  `non_empty::NonEmpty`, `store::StoreName`. Validation lives in the field's type, not in a constructor:
  config structs have public fields.
- Positions and versions are at most `i64::MAX`; their constructors return `Option` and `to_i64` cannot fail.
- Literals use the macros (`store_name!`, `category!`, `interval!`, `non_empty!`, `position!`, `version!`,
  and each crate's own), which check them at compile time.
- No `String` for a closed set: it is an enum, with labels derived through `strum`, never a hand-written
  `label()` match.
- Persisted shapes are versioned: the payload frame has a `format`, events have a schema version, snapshots a
  state version. A shape change is a new version, never an edit of an old one.
- Public enums and config structs are `#[non_exhaustive]`, so adding a variant or field is not a breaking change.
- Small Postgres enums are `int2` with a `CHECK`, mirrored one-to-one by a Rust enum; a test asserts parity.

## SQL

- Each crate's `sql/<group>/<name>.sql` files are the source of truth for its statements; `docs/schema.md`
  points at them and explains the design, and changes with them. Every statement is in the crate's registry
  (`sql::statement::ALL`, in a `#[doc(hidden)] pub mod sql`), and the crate's `tests/statement.rs` checks it
  against PostgreSQL 18 with `pges_testing::statement`.
- No `sqlx::query*` or `execute` with a literal, a `format!` or a `const` holding SQL outside tests
  (`check_no_inline_sql` rejects all three). This covers SQL the crate writes, not what sqlx sends itself
  (`PgListener`'s `LISTEN`).
- Queries bind and read only through typed statements (`pges_postgres::sql::typed`, declared with
  `pges_postgres::typed_query!`), so a wrong Rust type for a parameter or column does not compile. No
  `sqlx::query*`, `query_as`, `query_scalar`, `try_get`, `sqlx::Row`, or `use sqlx::` of `query`,
  `query_with` or `Row` in `src/` (`check_typed_calls` rejects them; it exempts named files only:
  pges-postgres's `sql/typed.rs`, pges-testing's `statement.rs`, bench's `partitions/composed.rs`). DDL, and
  statements without parameters whose rows are ignored, run as text through an `Executor`:
  `Statement::sql()` (a `&'static str`) as it is, rendered SQL through `Statement::render_text` (or the
  doc-hidden `template::text`, for pges-testing and the bench), which assert to sqlx 0.9 that a non-`'static` text is safe (`AssertSqlSafe`), as
  `typed::Rendered` does for queries. `sqlx::AssertSqlSafe` itself appears only around SQL that is not the
  registry's: tests' own `format!` SQL, the examples' `CREATE DATABASE`, the bench's composed `EXPLAIN`.
- pges-testing's fixtures and the bench render the crates' statements; they do not copy them.
- Placeholder values are typed: `StoreName`, `CategoryName`, `CategoryId`, `Ident` through
  `IdentPlaceholder` (leaves, partitions, indexes, bounds, `{database}`, `{template}`), `Literal` (`{from}`,
  `{to}`) and `SnapshotName`. Everything else is a bind parameter.
- Column order: NOT NULL fixed-width columns widest first, then nullable fixed-width, then variable-width.
- Store and category names are interpolated into SQL, so they are validated against `^[a-z][a-z0-9_]{0,39}$`
  first (`StoreName`, `CategoryName`) and never quoted from user input.
- Hot tables (`streams`, `commits`, `log`, `pending`) carry no foreign keys.
- Replication slot and origin names are server-wide; derive them from the database.

Adding a statement:

1. Write `crates/<crate>/sql/<group>/<name>.sql`, starting with a `-- ` header: what it does, its `$n`
   parameters and why it is shaped that way. Never refer to `docs/`. Keep comments out of the SQL body:
   rendering drops only the header. Write `'{from}'`/`'{to}'` quoted for `Literal::timestamptz` and unquoted
   for `Literal::int`; `'{snapshot}'` is quoted. A new placeholder is a variant of `template::Placeholder`
   (and of `IdentPlaceholder`, with its `From` arm, if it takes an identifier) plus a typed setter on
   `Values`, in `crates/pges-postgres/src/sql/template.rs`.
2. Register it in the crate's `src/sql/statement.rs`. A query is declared with `typed_query!`, which makes
   the `template::Statement` constant and a module of the same name in lower case holding `Query`, `Params`
   and `Row`:

   ```rust
   pges_postgres::typed_query! {
       /// Locks a stream's row.
       pub LOCK_STREAM, mod lock_stream {
           name: "append/lock_stream",
           files: [sql_file!("append/lock_stream.sql")],
           params { stream_id: Uuid },
           row { last_commit_id: Uuid },
       }
   }
   ```

   List it in `ALL` and add `typed::Shape::of::<lock_stream::Query>()` to `TYPED`.
   - `params<'q>` / `row<'r>` take the lifetime only when a field borrows; write `params {}` / `row {}` when
     there are none. Field types resolve in `statement.rs` (the macro does `use super::*`), so import them
     there.
   - A column whose name is not an identifier is `field: T as "?column?"`. Columns that share a name
     (`count(*), count(*) FILTER (..)`) are read by position, from 0: `field: T as "count" at 1`. A column read
     by name whose name is not unique, or an `at N` that is not the field's own position, does not compile.
   - A variant with the same parameter and row types is `like other` in place of `params` and `row`
     (pges-postgres's `APPEND_OUTBOX` is `like append`); `other` may also be a module imported from another
     crate. Variants whose types differ are separate statements, chosen by an enum at the call site
     (pges-subscribe's `sql::FetchCommits`).
   - Rust types: int2 `i16`, int4 `i32`, int8 `i64`, float8 `f64`, bool `bool`, text and name `&'q str` or
     `String`, bytea `&'q [u8]` or `Vec<u8>`, uuid `Uuid`, interval `PgInterval`, oid `Oid`, pg_lsn
     `typed::PgLsn`, void `()`, arrays `Vec<T>` or `&'q [T]`, nullable `Option<T>`. Another type needs a
     `typed::Value` impl (and `typed::Element` for its arrays) in `crates/pges-postgres/src/sql/typed.rs`.
     Domain newtypes have no sqlx impls; convert at the call site (`.get()`, `to_i64()`).
   - `Kind::Ddl` (DDL, `SET`, `VACUUM`) stays a plain `template::Statement` constant with no parameters or
     columns.
   - A variant is a list of fragment files joined by newlines. If the crate's `Cargo.toml` has an `include`
     list, it names `sql/**/*`.
3. Render it once per store, into a struct built at open (like `pges_postgres::sql::StoreSql`), as a
   `typed::Rendered<statement::lock_stream::Query>`: `lock_stream::Query::render(&values)`, `::fixed()` for
   one file with no placeholders, or `typed::Rendered::<Base>::variant::<V>(&values)` for a `like` variant
   (`render` and `fixed` need `typed::Query` in scope). A statement whose fragments repeat in code (one part
   per store) registers one composition and is built with `typed::Rendered::compose` from the same files. Run
   it with `.bind(statement::lock_stream::Params { .. })` and then `.execute`, `.fetch_one`, `.fetch_optional`
   or `.fetch_all(&mut *conn)`, reading the `Row` fields; rows that borrow use `fetch_optional_row` /
   `fetch_all_rows` and `.row()?`. DDL runs as `conn.execute(statement::X.render_text(&values))`, or
   `conn.execute(statement::X.sql())` without placeholders.
4. Test it in the crate's `tests/statement.rs`: queries are checked with `check_typed` on a real store; a DDL
   statement must run in the test's scenario, which compares the DDL that ran with the registry's. To choose
   the Rust types, declare your best guess and run the test: a failure lists the parameter types PostgreSQL
   infers and each column's type and nullability. Where PostgreSQL reports nullability, a column is `Option`
   exactly when it is nullable; when it reports a column nullable that never holds NULL, read `Option<T>`
   and turn `None` into an error. If a call site bound a different type than PostgreSQL infers, fix the call
   site, not the type. `every_query_is_typed` fails if a query is missing from `TYPED`.
5. Make sure some test runs the call site. A forgotten placeholder value, or wrong `{from}`/`{to}` quoting,
   fails only when the statement runs.

## Errors

One error enum per operation (`AppendError`, `OpenError`). Variants describe what went wrong, not which crate
produced the error, and never expose `sqlx::Error` as the only information. No `unwrap`/`expect`/`panic!`
outside tests.

Exceptions: `pges_core::__private::{category, store_name, schema_version, check_types}`,
`pges_subscribe::__private::subscription_name`, and the macros `category!`, `store_name!`, `interval!`,
`position!`, `version!`, `subscription_name!`, `shards!`, `lanes!` and `source_kind!` panic on an invalid
literal inside a `const` item. `pges_replication::{slot_name!, publication_name!}` do the same with
`Option::expect`. The panic is a compile error, never reached at run time; a `const fn` has no
non-panicking alternative. `pges_testing::{position, version}` panic on an out-of-range number: they are
test helpers.

## Structure

- Public API at the top of the file, private helpers at the bottom.
- The struct a caller sees comes before its constituent enums.
- No re-exports, except the `pges` facade, which re-exports each crate as a module (`pges::core`,
  `pges::postgres`, `pges::derive`, ...) and also re-exports the `Event` derive at its root (`pges::Event`;
  `pges::derive::Event` still works), and a `#[doc(hidden)] pub mod __private` for macro support
  (`pges-core` re-exports `serde` there for code generated by `pges-derive`). It is not part of the API.
  The gRPC modules of pges-sequencer and pges-subscribe also re-export `pges_core::tls::TlsError`, so both
  TLS settings types return one error under their own module.
- Module names are singular.
- Everything not part of the documented API is `pub(crate)`.

## Tests

- Clean up only what your own runs started: stop processes by the PIDs you recorded, and drop databases, slots
  and containers by the names you created. Never kill by a name pattern (`pkill -f`, `pgrep | xargs kill`):
  shared machines and CI runners run other people's work.
- Unit tests inline in `#[cfg(test)] mod tests`. Integration tests under each crate's `tests/`; each file starts
  with `#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]`, because clippy does not treat
  helpers in `tests/` as test code.
- Tests that touch the database use `pges-testing`: one database per test (`TestDb::new`), stores and
  categories through its helpers, `TestDb::cleanup` at the end. No mocks of Postgres; fakes of the `Source`
  and `Waker` traits are fine.
- Ordering guarantees (multi-category merge, token resume, outbox drain) get property tests.
- Recovery loops hide bugs. A test fails when a component recovered from an error it did not expect (for the
  sequencer, any `Status::Recovering`; for a subscription, any `State::Recovering`); tests that expect one say
  so.
- A test that crashes a component by aborting its task gives it a pool of its own and closes that pool
  after the abort (pges-subscribe's `Crashable`), as a real crash closes its connections.
- Each transactional future gets a drop test with `pges_testing::cancel` (`docs/testing.md`, Cancellation).
- Each crate's trybuild `tests/compile_fail.rs` runs only on the stable toolchain CI pins (`TOOLCHAIN`,
  1.97.1); bump it with the ci.yml pin and re-bless.
- A failing test's `TestDb` drops its database and slots when dropped. Only killed processes leak, and the
  next run sweeps `t_*` databases older than an hour (`docs/testing.md`, Isolation).

Run them (`docs/testing.md` has the detail):

```sh
cargo test                       # uses ./.pg if pg-up is running, otherwise Docker (Testcontainers)
PGES_TEST_BACKEND=docker cargo test
nix develop -c pg-up             # optional local PostgreSQL 18
```

Before opening a pull request, run what CI runs: `cargo fmt --all --check`,
`cargo clippy --workspace --all-targets --all-features -- -D warnings`, `cargo test --workspace --all-features`,
`cargo test -p pges-core -p pges-postgres -p pges-subscribe -p pges-sequencer` (default features: the
examples crate turns on `otel` and other optional features for every crate in a workspace run),
`RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps --all-features`, `cargo deny check`, and the
per-feature `cargo doc` and doctest loop in `.github/workflows/ci.yml` (generated from `cargo metadata`; needs
`jq`).

## Async

Async functions call sqlx through the `Executor` trait (`conn.execute(sql)`, `conn.fetch_one(query)`,
`conn.fetch_optional(query)`), not `sqlx::raw_sql(&non_static).execute(conn)`. The latter hits a rustc
higher-ranked lifetime bug: the future is not `Send` "for any lifetime", and `tokio::spawn` or `Box<dyn Future + Send>` fail with "implementation of Executor/Send is not
general enough". `crates/pges-postgres/tests/spawn.rs` runs every public operation inside `tokio::spawn` to
catch regressions; `crates/pges-sequencer/tests/spawn.rs` and `crates/pges-subscribe/tests/spawn.rs` do
the same.

Transactions begin with sqlx (`pool.begin()`, `Connection::begin` on a pooled connection): since 0.9.1 a begin
dropped at any point queues its `ROLLBACK`. Never begin a savepoint (a begin on a connection already in a
transaction): sqlx 0.9.1 still rolls back the whole outer transaction when a savepoint begin is dropped in
flight or fails, and the outer `commit` then does nothing (design.md, Decided, sqlx 0.9.1). Users may begin
them on connections we hand out; before ending such a transaction, check `Connection::is_in_transaction`
as `deliver_pg` and `unit_of_work::Work` do.

## Dependencies

- Published crates only: crates.io rejects git dependencies, even optional ones. `cargo deny` enforces it.
- Keep the MSRV in `Cargo.toml` (`rust-version`) and CI's MSRV job in step.
- Maintainers publish releases to crates.io. The crates' metadata (repository, keywords, categories,
  `include`, docs.rs) lives in each `Cargo.toml`; path dependencies carry `version` too, and a version bump
  changes the workspace version and every one of them.
- MSRV policy: the MSRV follows sqlx (currently 1.94); raising it is a minor-version change, with a
  CHANGELOG entry.

## Releases and migrations

- **Migration 0001 is frozen from the 0.1.0 release on.** Never edit a released migration (the `0001_init.sql` migrations,
  shared and store, or the option scripts): every schema change is a new migration (`0002_<name>.sql`) added
  to `migrate::SHARED` or `migrate::STORE`, expand/contract, so the previous crate release still works on the
  new schema. An edit fails every existing database with `ChecksumMismatch`.
- Every user-visible change gets a line under `## [Unreleased]` in `CHANGELOG.md`.

## Writing

Docs and comments use short, plain sentences. Don't name companies or private projects anywhere in the repository.

## Crate layout

```text
crates/
  pges/              facade; re-exports the crates below as modules (pges::core, ::derive, ::postgres,
                     ::subscribe, ::sequencer, ::replication), and the derive as pges::Event
  pges-core/         ids, versions, positions, checked values (StoreName, Interval, NonEmpty, EventCount)
                     and their literal macros, Event, Aggregate, codecs, frame format, commit metadata
                     (follow-ups, W3C trace context; otel feature); no IO
  pges-derive/       #[derive(Event)]; finds the path to pges-core from the caller's Cargo.toml
                     (proc-macro-crate)
  pges-postgres/     Store, UnitOfWork, ambient metadata scope, migrations, Admin (categories, maintenance,
                     retention, writer limits), delete_stream; the hidden sql module (template, registry,
                     typed statements)
    migrations/      SQL: pges/ (shared schema), store/ (each store, plus opt_outbox.sql, and opt_dcb.sql,
                     which no public path applies, only a test)
    sql/             one file per statement, sql/<group>/<name>.sql (also in pges-sequencer,
                     pges-subscribe, pges-testing and bench)
  pges-replication/  minimal logical replication client (pgoutput subset, optional TLS)
  pges-sequencer/    Sequencer, Source trait, outbox and logical sources, slot bootstrap, resequence,
                     in-process watch, NOTIFY notifier, gRPC watch server (optional TLS), binary
  pges-subscribe/    subscriptions (listed categories or the whole store, sharded or not), run_pg
                     checkpoints, merge cut, replica reads, backfills, Waker trait, gRPC (optional TLS),
                     in-process, poll and LISTEN/NOTIFY wakers
  pges-watch-proto/  gRPC watch service pges.watch.v1: proto file, checked-in prost/tonic code (client, server
                     features); not re-exported by the facade
  pges-testing/      PostgreSQL 18 for tests, template databases, fixtures, fault-injection proxy, a
                     primary with a streaming replica (replica::Pair), cancellation harness (cancel), TLS
                     test certificates (tls::Pki, feature tls)
examples/            pges-examples (publish = false): runnable examples
                     (cargo run -p pges-examples --example <name>), run by cargo test through tests/run.rs
bench/               pges-bench (publish = false): benchmarks for design.md's spikes
                     (cargo run -p pges-bench --release --
                     <append|sequencer|outbox-notify|read|partitions|wake|scale|server>);
                     tests/smoke.rs runs each for about a second under cargo test
docs/                design, schema, api, testing
README.md            workspace README; each crate also has one
```

Planned, not yet created:

```text
crates/
  pges-relay/        subscribers that forward to brokers
```
