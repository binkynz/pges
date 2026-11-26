# pges-replication

A minimal PostgreSQL logical replication client: the protocol subset the pges sequencer needs. It opens a
replication connection over TCP or a unix socket, creates and drops slots, streams pgoutput messages
decoded into typed values, answers keepalives, and confirms progress to the slot. IO is Tokio; message
encoding and SCRAM come from `postgres-protocol`.

The sequencer's logical source uses it. You need it directly only to configure that source's connection,
or to read a replication slot yourself. Through the [`pges`](https://docs.rs/pges) facade it is
`pges::replication` (feature `replication`).

## Features

- `tls` (off by default): TLS through rustls with the ring provider, following libpq's `sslmode`,
  `sslrootcert`, `sslnegotiation` and `channel_binding`. No OpenSSL.

## Example

```rust,no_run
use pges_replication::{config, connection};

async fn identify() -> Result<(), Box<dyn std::error::Error>> {
    let config = config::Config::from_url("postgres://sequencer:secret@db:5432/app")?;
    let mut connection = connection::ReplicationConnection::connect(&config).await?;
    let system = connection.identify_system().await?;
    println!("{system:?}");
    Ok(())
}
```

Docs: <https://docs.rs/pges-replication>, once the crate is published.
