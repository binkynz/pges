# pges-core

Core types for pges, an event store on PostgreSQL 18+. No IO: ids (`StreamId`, `CommitId`, `StoreId`,
`CategoryId`), names (`StoreName`, `CategoryName`), versions, positions, event counts and schema versions,
checked option values (`Interval`, `NonEmpty`), high-water marks, the `Event` and `Aggregate` traits,
per-commit metadata, the payload frame and the codecs. The `category!`, `store_name!`, `interval!`,
`position!`, `version!` and `non_empty!` macros check a literal at compile time.

Applications reach it as `pges::core` through the [`pges`](https://docs.rs/pges) facade. Depend on it
directly for code that shares these types without a database, such as a crate of events used by several
services.

## Features

- `json` (default): the JSON codec, `codec::json`.
- `otel`: `metadata::TraceContext::current` reads the current `tracing` span's W3C trace context through
  `tracing-opentelemetry`; `TraceContext::to_context` and `StreamEvent::trace_parent` turn a stored one into
  an OpenTelemetry parent context.

## Example

```rust
use pges_core::{category, stream, version};

let jobs = category!("jobs");
let job_id = stream::StreamId::new_v7();
let expected = version::ExpectedVersion::Exact(version!(3));

assert_eq!(jobs.as_str(), "jobs");
assert_ne!(job_id, stream::StreamId::new_v7());
assert!(category::CategoryName::new("Jobs").is_err());
assert_ne!(expected, version::ExpectedVersion::NoStream);
```

Docs: <https://docs.rs/pges-core>, once the crate is published.
