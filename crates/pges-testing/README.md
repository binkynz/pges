# pges-testing

Test support for pges, an event store on PostgreSQL 18+. It is for the pges crates' own tests and for
applications that test against a real server.

- It finds PostgreSQL 18 with logical replication: `PGES_TEST_DATABASE_URL`, the local cluster from
  `nix develop -c pg-up`, or a reusable Docker container, in that order. `PGES_TEST_BACKEND=docker|local`
  forces one.
- `TestDb::new` gives each test its own database, cloned in milliseconds from a migrated template. A
  `TestDb` dropped without `cleanup`, as in a test that failed, drops its database and replication slots,
  and the first `TestDb::new` of a process drops test databases more than an hour old, left by killed
  processes.
- `create_store` (with `definition`, a small store definition), `register_category` and
  `create_log_partition` set up fixtures; `position` and `version` build ids from test numbers.
- `proxy::Proxy` sits between a test and the server, and can drop connections or hold traffic one way.
- `statement` checks a crate's SQL statement registry against PostgreSQL 18: parameter and column types,
  DDL, the `sql/` files, and that no SQL bypasses the registry.
- `replica::Pair` starts a private primary with a streaming hot standby (two Docker containers, or two
  local clusters with the `pg-up` backend), whose replay can be paused, for tests of replica reads.

It is not part of the [`pges`](https://docs.rs/pges) facade; add it as a dev-dependency.

Features: `tls` adds `tls::Pki`, throwaway certificates for TLS tests (rcgen).

## Example

```rust,no_run
use pges_core::category;
use pges_postgres::store;

async fn a_store_opens() -> anyhow::Result<()> {
    let db = pges_testing::TestDb::new().await?;
    let mut conn = db.connect().await?;
    let name = pges_core::store_name!("ops_es");
    pges_testing::create_store(&mut conn, &name, &pges_testing::definition(store::Outbox::Disabled))
        .await?;
    pges_testing::register_category(&mut conn, &name, category!("jobs"), 1, 2).await?;
    store::Store::open(db.pool().await?, store::StoreConfig::new(name)).await?;

    db.cleanup().await
}
```

Docs: <https://docs.rs/pges-testing>, once the crate is published.
