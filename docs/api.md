# pges — Rust API

Edition 2024, Tokio, `sqlx` for every connection except the logical replication stream. Statements are
built per store and per category at `Store::open` (schema names are runtime values, so the `query!` macros
do not apply); integration tests against PG18 cover them instead.

## Crates and features

| Crate | Contents | Features |
|---|---|---|
| `pges` | facade: each crate as a module (`pges::core`, `pges::derive`, `pges::postgres`, `pges::subscribe`, `pges::sequencer`, `pges::replication`), and the derive as `pges::Event` | `derive`, `subscribe`, `wake-grpc` (default); `wake-grpc-tls`, `wake-poll`, `wake-inproc`, `wake-notify`; `sequencer` (outbox and logical sources, as `pges-sequencer`'s default), `sequencer-outbox` (outbox source only, no `pges-replication`), `source-logical` (turns on `replication`, for the connection config the logical source takes), `watch-notify`, `watch-grpc`, `watch-grpc-tls`; `replication`, `tls`; `otel` (turns on `pges-core`'s, `pges-postgres`'s and, with `subscribe`, `pges-subscribe`'s). A wake, source or watch feature turns on its crate's feature. `bin` and `pges-core`'s `json` are not mirrored, because `pges-postgres` always turns `json` on. `pges-watch-proto` is not re-exported: no API needs its types by name. |
| `pges-core` | ids, versions, positions, `Event`, `Aggregate`, codecs (`codec::json` with `Upcaster` and `decode_upcast`), frame format, errors, `metadata` (typed correlation and causation ids, follow-up metadata, W3C trace context); no I/O | `json` (default); `otel` (opentelemetry 0.33 API with `trace`, `tracing`, tracing-opentelemetry 0.34, all optional: `TraceContext::{current, from_span, to_context}`, `trace_parent`); `protobuf`, `msgpack` planned |
| `pges-derive` | `#[derive(Event)]` | |
| `pges-postgres` | `Store` (with `delete_stream`), `UnitOfWork`, migrations, `Admin` (categories, bucket changes, maintenance, retention and `prune_log`; `retention` module), closed-partition freeze and partition ages (`freeze` module), the store's writer limits (`limit` module), the ambient metadata scope (`ambient` module) | `otel` (= `pges-core/otel`): appends without a trace record the current span's. Always enables tokio's `rt` feature (for the task-local), which sqlx's `runtime-tokio` already enables |
| `pges-replication` | minimal logical replication client over TCP (optionally TLS) or a unix socket (trust, cleartext, MD5, SCRAM-SHA-256, SCRAM-SHA-256-PLUS with channel binding over TLS); libpq `sslmode`, `sslrootcert`, `sslnegotiation`, `channel_binding`: `IDENTIFY_SYSTEM`, `CREATE_REPLICATION_SLOT` / `DROP_REPLICATION_SLOT` (`SNAPSHOT 'nothing'` or `'export'`), `START_REPLICATION` with pgoutput v4 and `streaming 'off'`; pgoutput decoding of Begin, Commit, Origin, Relation, Type, Insert, Update, Delete, Truncate and logical messages; keepalives and standby status | `tls` (off by default): rustls with the ring provider, platform roots through rustls-native-certs; no OpenSSL |
| `pges-sequencer` | `Sequencer`, `Source` trait, outbox source (always built: it needs nothing beyond sqlx), logical source (`logical::LogicalSource`, with a slot bootstrap and `resequence`), in-process `Watch`, NOTIFY notifier, gRPC watch server (`grpc::WatchService`), binary `pges-sequencer` | sources: `source-logical` (default; turns on `pges-replication/tls`), planned `source-kafka`; watch: `watch-notify` (off by default), `watch-grpc` (off by default; tonic server without its router, `pges-watch-proto`), `watch-grpc-tls` (off by default; turns on `watch-grpc`, adds TLS and optional client certificates through tonic's `tls-ring`: rustls with ring, no OpenSSL); `bin` (the binary: clap, tracing-subscriber, tokio `rt-multi-thread` and `signal`) |
| `pges-subscribe` | subscriptions over one or several categories or the whole store, sharded by stream or not, `run_pg` (exactly once) and `run` (at least once), replica reads with an opt-in fallback, backfills, `Waker` trait, gRPC, in-process, poll and LISTEN/NOTIFY wakers | wakers: `wake-grpc` (default; adds tonic and `pges-watch-proto`), `wake-grpc-tls` (off by default; turns on `wake-grpc`, adds `https` endpoints through tonic's `tls-ring` and `tls-native-roots`: rustls with ring, the system's roots or a given CA, an optional client certificate), `wake-poll`, `wake-inproc`, `wake-notify`. `wake-poll`, `wake-inproc` and `wake-notify` add no dependencies; `otel` (`opentelemetry`, `pges-core/otel`, `pges-postgres/otel`): `RawEvent::trace_parent` |
| `pges-watch-proto` | the gRPC watch service `pges.watch.v1`: the proto file, the prost messages and the tonic client and server, generated and checked in (no `protoc` or build script); `v1::WatchResponse::new(fence, &HighWater)` and `.high_water() -> Option<HighWater>` (`None` when a position is above `Position::MAX`) | `client` (`v1::watch_client`), `server` (`v1::watch_server`); with neither, the messages only |
| `pges-relay` | subscribers that forward events to brokers | `relay-kafka`, `relay-pubsub` |
| `pges-testing` | PG18 testcontainer, template databases, fixtures, fault-injection proxy (`proxy::Proxy`), cancellation harness (`cancel`), TLS test certificates (`tls::Pki`) | `tls` (off by default; rcgen): `tls::Pki` |

Cargo features are only for heavy optional dependencies: sources, watch, wakers and relays. What a store
needs on the write path is database state, read at `Store::open`: `pges.stores.outbox` adds the `pending`
insert to the append statement, and `pges.stores.dcb` adds the tag steps. There is no `dcb` cargo feature
until DCB appends exist. Until then `Store::open` refuses every DCB store with
`OpenError::StoreRequiresFeature("dcb")`: a feature that enabled nothing would let a writer skip `tag_heads`.

### TLS for replication connections

`pges-replication`'s `tls` feature follows libpq. Defaults: `sslmode=prefer` and `channel_binding=prefer` with
the feature, `disable` without it. Settings that need TLS without the feature
(`sslmode=require|verify-ca|verify-full`, `channel_binding=require`, `sslnegotiation=direct`) fail with
`ConfigError::TlsFeatureRequired`. `SslMode` keeps all six variants whatever the features. Unix sockets never
use TLS.

- Roots: the `sslrootcert` file, else the platform trust store (rustls-native-certs, which honours
  `SSL_CERT_FILE` and `SSL_CERT_DIR`). libpq reads `~/.postgresql/root.crt` instead; it is never read here.
  `sslrootcert=system` without an `sslmode` means `verify-full`, and a weaker explicit mode is rejected.
- An `sslrootcert` file that is missing or holds no certificate is an error in every mode that tries TLS
  (libpq ignores a missing file below `verify-ca`).
- `verify-full` checks subject alternative names only; rustls ignores the common name, so server certificates
  need SANs.
- `sslnegotiation=direct` (PostgreSQL 17+, ALPN `postgresql`) is supported with `sslmode=require` or stronger.
- With channel binding, the SCRAM-SHA-256-PLUS hash follows the server certificate's signature algorithm
  (SHA-384 for a P-384 CA). An Ed25519 certificate has no defined hash: the client then does not bind under
  `prefer` and fails under `require`.
- A TLS handshake failure (`TlsError::Handshake` or `TlsError::Certificate`) or SQLSTATE 28000 makes `prefer`
  retry without TLS and `allow` retry with it. Other errors, including I/O errors during the handshake and a
  wrong password (28P01), are not retried.
- Client certificates (`sslcert`, `sslkey`, `sslpassword`), `sslcrl`, `sslsni=0` and
  `ssl_min_protocol_version` are not supported; as unknown URL parameters they are rejected.
- `Config` setters are infallible; contradictory settings fail `Config::check`, which `Config::from_url` and
  `connect` run (`ConnectError::InvalidConfig`). `ConfigError` variants: `InvalidUrl`, `MissingUser`,
  `InvalidValue`, `TlsFeatureRequired`, `IncompatibleSettings`, `UnsupportedParameter`.

Getters follow tokio-postgres's `get_*` scheme: `get_host`, `get_port`, `get_user`, `get_database`,
`get_application_name`, `get_ssl_mode`, `get_root_certificates`, `get_ssl_negotiation`,
`get_channel_binding`. The password has no public getter, so secrets are not handed out.

### Replication slots

`ReplicationConnection::create_slot(slot, failover)` sends `SNAPSHOT 'nothing'`, as before.
`create_slot_with(&mut self, slot, failover, connection::SlotSnapshot) -> Result<CreatedSlot, CommandError>`
also chooses the snapshot:

```rust
#[non_exhaustive]
pub enum SlotSnapshot { Nothing, Export }    // connection::SlotSnapshot: SNAPSHOT 'nothing' | 'export'

#[non_exhaustive]
pub struct CreatedSlot {
    pub name: name::SlotName,
    pub consistent_point: Lsn,               // transactions committing after it are streamed
    pub snapshot: Option<name::SnapshotName>,   // Some with Export
}
```

- With `Export`, a transaction that imports the snapshot (`REPEATABLE READ`, then
  `SET TRANSACTION SNAPSHOT '<name>'` as its first statement) sees exactly the transactions that committed
  before the consistent point, which the slot will not stream. It stays importable until the replication
  connection runs its next command or closes.
- `alter_slot_failover(&mut self, slot, failover) -> Result<(), CommandError>`:
  `ALTER_REPLICATION_SLOT <slot> (FAILOVER true|false)` (PostgreSQL 17+). It waits while another process
  uses the slot; the server has no option to fail instead. 42704 when the slot is missing.
- `name::SnapshotName` is 1 to 63 characters from `0-9`, `A-F`, `a-f` and `-` (`new`, `as_str`, `Display`,
  `FromStr`), so it can be interpolated into SQL. A bad name is `NameError::Snapshot(String)`.

### Slot and publication names

- `StartOptions::new(slot, publications: impl connection::Publications)`: one `PublicationName`, an array
  of one to eight, or a `pges_core::non_empty::NonEmpty` (for a list known at run time,
  `NonEmpty::try_from(vec)`). An empty list does not compile: `Publications` is sealed and has no methods.
- `SlotName` and `PublicationName` have `const fn from_static(&'static str) -> Option<Self>` and
  `const fn is_valid(&str) -> bool`, besides `new(impl Into<String>) -> Result<Self, NameError>`. The
  `pges_replication::slot_name!` and `publication_name!` macros check a literal at compile time.

### The `pges` facade

An application can depend on `pges` alone. Each crate is a module; the only other re-export is the derive,
`pges::Event`:

```rust
#[derive(Debug, pges::Event)]
#[event(category = "jobs")]
pub enum JobEvent { /* … */ }

use pges::postgres::store;
use pges::subscribe::subscription::StoreExt;
```

- The derive is `pges::Event` (also `pges::derive::Event`). Enums need no `crate` attribute: the derive
  finds `pges` in the caller's Cargo.toml (Events and aggregates).
- There is no prelude and no top-level `pges::Error`; errors stay one enum per operation in each crate.
- `pges-watch-proto` is not re-exported: no user-facing API needs the proto types by name.
- The crate docs are the manual's front page, with a full example (derive, open, register a category,
  append, load, read a stream, a subscription with `run_pg`) and sequencer examples for the logical and
  outbox sources. Links to modules behind a feature are written twice, as a link when the feature is on and
  as plain code when it is off, so the docs build with any feature set.
- The workspace and each crate have a `README.md`, compiled as doctests through a hidden
  `#[cfg(doctest)]` item in `lib.rs`. The workspace README is outside every published package, so
  `pges-examples` (not published) includes it, with `pges`'s `derive`. The `pges-sequencer` README builds only with `source-logical`.
- Runnable code is in `examples/` (`pges-examples`, not published): `quickstart`, `unit_of_work`,
  `subscription`, `multi_category`, `sharded`, `backfill`, `sequencer_logical`, `outbox`, `grpc_watch` and
  `metadata` (a request scope, a follow-up from a handler, and otel trace context).
  Run one with `cargo run -p pges-examples --example <name>`.

`pges-core` also has a `#[doc(hidden)] pub mod __private` for code generated by the derive and the literal
macros (a `serde` re-export and the `const fn`s `category`, `store_name`, `schema_version` and `check_types`).
It is not part of the API and changes without notice.

## Core types

```rust
pub struct StreamId(Uuid);              // StreamId::new_v7() (preferred without a natural key: new streams
                                        // land at the right edge of the primary key), StreamId::from_key(..)
pub struct StreamRef { pub id: StreamId, pub key: Option<Arc<str>> }
                                        // StreamId::from_key(&category, "JOB-123") returns a StreamRef
                                        // (uuidv5 plus the key, stored in streams.stream_key on creation);
                                        // append takes impl Into<StreamRef>, and StreamId converts with key None
pub struct CommitId(Uuid);              // uuidv7, always generated by the client
pub struct Version(NonZeroU64);         // 1..=i64::MAX; a new stream's first event is version 1
pub struct Position(u64);               // 0..=i64::MAX; per store, gap-free across the store
pub struct StoreId(i32);                // pges_core::store; pges.stores.store_id
                                        // pges_core::store::{DEFAULT_MAX_CLOCK_SKEW (5 s),
                                        //   DEFAULT_MAX_TXN_AGE (1 h)}: the writer limits' defaults
pub struct StoreName(/* Cow<'static, str> */);   // pges_core::store; ^[a-z][a-z0-9_]{0,39}$, not pges, public,
                                        // information_schema, pg_* or a reserved SQL keyword
pub struct CategoryId(i32);             // pges_core::category; categories.category_id
pub struct SchemaVersion(i32);          // pges_postgres::migrate; pges.scopes.schema_version,
                                        // pges.migrations.version; NONE = 0. Not the event's
                                        // pges_core::event::SchemaVersion (Events and aggregates)
pub struct CategoryName(&'static str);  // validated against ^[a-z][a-z0-9_]{0,39}$ and no __ (it
                                        // separates the category in partition names); names read from
                                        // the database are interned (leaked once per category) at
                                        // Store::open

pub enum ExpectedVersion { Any, NoStream, Exact(Version) }

#[non_exhaustive]
pub struct Metadata {                   // pges_core::metadata; per commit; encoded as a protobuf message
                                        // inside the frame (correlation and causation still bytes fields 1, 2)
    pub correlation_id: Option<CorrelationId>,
    pub causation_id: Option<CausationId>,
    pub actor: Option<String>,
    pub trace: Option<TraceContext>,    // W3C traceparent/tracestate; with `otel`, filled from the current
                                        // span when neither the append nor the scope sets it
    pub extra: BTreeMap<String, String>,
}
impl Metadata {
    pub fn follow_up(&self, cause: CommitId) -> Metadata;   // for commits `cause` causes: correlation carried
                                        // over (or `cause` when there is none), causation = `cause`, trace
                                        // carried over, no actor, no extra
    pub fn fill_unset(&mut self, fallback: &Metadata);   // each None field from `fallback`; extra keys
                                        // `self` lacks are added; set fields are kept
    #[cfg(feature = "otel")]
    pub fn trace_parent(&self) -> Option<opentelemetry::Context>;   // the trace as a parent context
}

pub struct CorrelationId(Uuid);         // new_v7(), from_uuid(Uuid), as_uuid(), From<CommitId>, Display
pub struct CausationId(Uuid);           // from_uuid(Uuid), as_uuid(), From<CommitId>, Display; no new_v7: a
                                        // cause always comes from somewhere. Not a CommitId: an outside cause
                                        // may be an id that is not UUIDv7. Neither id converts into the other,
                                        // and a bare Uuid in either field does not compile (trybuild)

#[non_exhaustive]
pub struct TraceContext { pub traceparent: String, pub tracestate: Option<String> }   // TraceContext::new(tp)
#[cfg(feature = "otel")]
impl TraceContext {
    pub fn current() -> Option<Self>;   // the current tracing span's; None without a valid OTel span context
    pub fn from_span(span: &tracing::Span) -> Option<Self>;   // written as the SDK's TraceContextPropagator
                                        // writes it (version 00, sampled flag only)
    pub fn to_context(&self) -> Option<opentelemetry::Context>;   // remote parent; None for an invalid
                                        // traceparent; an invalid tracestate is dropped
}

pub struct StreamEvent<E> {             // from read_stream and load
    pub stream_id: StreamId,
    pub category: CategoryName,
    pub version: Version,
    pub commit_id: CommitId,
    pub recorded_at: OffsetDateTime,    // from the commit id
    pub metadata: Arc<Metadata>,
    pub event: E,
}

pub struct LogEvent<E> {                // from subscriptions and Batch
    pub position: Position,
    pub inner: StreamEvent<E>,          // Deref to StreamEvent
}

impl<E> StreamEvent<E> {                // LogEvent has these too, through Deref
    pub fn follow_up(&self) -> Metadata;   // self.metadata.follow_up(self.commit_id)
    #[cfg(feature = "otel")]
    pub fn trace_parent(&self) -> Option<opentelemetry::Context>;
}

impl Position {
    pub const BEGINNING: Self;          // 0
    pub const MAX: Self;                // i64::MAX
    pub const fn new(u64) -> Option<Self>;        // None above MAX
    pub const fn from_i64(i64) -> Option<Self>;   // None below 0
    pub const fn get(self) -> u64;
    pub const fn to_i64(self) -> i64;   // cannot fail
    pub const fn checked_add(self, n: u64) -> Option<Self>;
}
impl Version {
    pub const FIRST: Self;              // 1
    pub const MAX: Self;                // i64::MAX
    pub const fn new(u64) -> Option<Self>;        // None for 0 or above MAX
    pub const fn from_i64(i64) -> Option<Self>;   // None below 1
    pub const fn get(self) -> u64;
    pub const fn to_i64(self) -> i64;   // cannot fail
    pub const fn checked_add(self, n: u64) -> Option<Self>;
}
```

Values with a range are checked types, so an out-of-range value cannot be built:

```rust
// pges_core::store
impl StoreName {
    pub const MAX_LEN: usize = 40;
    pub const fn new(name: &'static str) -> Result<Self, StoreNameError>;
    pub fn parse(name: impl Into<String>) -> Result<Self, StoreNameError>;   // also FromStr
    pub fn as_str(&self) -> &str;                                            // also Display, AsRef<str>
}
#[non_exhaustive] pub enum StoreNameError { Empty, TooLong, InvalidStart, InvalidCharacter, Reserved, Keyword }

// pges_core::interval: 1 ms to 30 days
pub struct Interval(Duration);           // Copy, Ord; From<Interval> for Duration;
                                         // TryFrom<Duration, Error = IntervalError>
impl Interval {
    pub const MIN: Self;                 // 1 ms
    pub const MAX: Self;                 // 30 days
    pub const fn new(Duration) -> Option<Self>;
    pub const fn from_millis(u64) -> Option<Self>;
    pub const fn from_secs(u64) -> Option<Self>;
    pub const fn get(self) -> Duration;
}
#[non_exhaustive] pub enum IntervalError { TooShort, TooLong }

// pges_core::tls: no TLS code; re-exported as pges_sequencer::grpc::TlsError (watch-grpc-tls) and
// pges_subscribe::waker::grpc::TlsError (wake-grpc-tls), the same type
#[non_exhaustive] pub enum TlsError { /* table below */ }   // Clone, PartialEq, Eq

// pges_core::non_empty
pub struct NonEmpty<T> { /* first, rest */ }
impl<T> NonEmpty<T> {
    pub fn new(first: T, rest: impl IntoIterator<Item = T>) -> Self;
    pub fn one(item: T) -> Self;
    pub fn try_from_iter(items: impl IntoIterator<Item = T>) -> Result<Self, EmptyError>;
    pub fn first(&self) -> &T;
    pub fn rest(&self) -> &[T];
    pub fn len(&self) -> NonZeroUsize;
    pub fn push(&mut self, item: T);
    pub fn iter(&self) -> Iter<'_, T>;
    pub fn map<U>(self, f: impl FnMut(T) -> U) -> NonEmpty<U>;
    pub fn into_vec(self) -> Vec<T>;
}   // TryFrom<Vec<T>> (Error = EmptyError), From<NonEmpty<T>> for Vec<T>, IntoIterator, Extend

// pges_core::commit
pub struct EventCount(NonZeroU16);       // 1..=32767 (frame::MAX_EVENTS)
impl EventCount {
    pub const ONE: Self;
    pub const MAX: Self;
    pub const fn new(u16) -> Option<Self>;
    pub const fn from_len(usize) -> Option<Self>;
    pub const fn get(self) -> u16;
    pub const fn to_i16(self) -> i16;    // cannot fail
}

pges_core::store_name!("ops_es")         // store::StoreName
pges_core::category!("jobs")             // category::CategoryName
pges_core::interval!(500 ms)             // interval::Interval; units ms, s, min, h, d, or a const Duration
pges_core::non_empty![a, b, c]           // non_empty::NonEmpty; at least one item
pges_core::position!(42)                 // position::Position
pges_core::version!(3)                   // version::Version
```

- The macros check their literal in a `const` item, so an invalid one is a compile error, also under
  `cargo check` and inside generic functions. Nothing can panic at run time. The argument must be a
  constant.
- Values known only at run time go through `StoreName::parse`, `CategoryName::leak`,
  `Interval::try_from(Duration)`, `NonEmpty::try_from(vec)`, `Position::new` and `Version::new`.
- Positions and versions are at most `i64::MAX`, the largest PostgreSQL `int8`, so `to_i64` cannot fail and
  nothing clamps.

`tls::TlsError` is why `ServerTls` (gRPC watch server) or `ClientTls` (gRPC waker) could not be built from the
PEM given. Library messages are carried as text (`reason`), so no TLS type is in the API.

| Variant | Cause |
|---|---|
| `NoCertificate` | the certificate chain's PEM holds no certificate |
| `UnreadableCertificate { reason }` | the chain's PEM does not parse (a section without its end, bad base64) |
| `InvalidCertificate { reason }` | the chain's first certificate is not valid X.509 |
| `NoPrivateKey` | the key's PEM holds no PKCS#1, PKCS#8 or SEC1 private key |
| `UnreadablePrivateKey { reason }` | the key's PEM does not parse |
| `MultiplePrivateKeys { count }` | the key's PEM holds more than one private key |
| `KeyMismatch` | the key is not the first certificate's |
| `NoCaCertificate` | the CA's PEM holds no certificate |
| `UnreadableCaCertificate { reason }` | the CA's PEM does not parse |
| `InvalidCaCertificate { reason }` | a certificate in the CA's PEM cannot be a trust anchor |
| `Rejected { reason }` | rustls refused the settings otherwise: a key of an unsupported algorithm or corrupt DER, or (`ClientTls::system_roots().identity(..)`) no system roots |

## Events and aggregates

```rust
#[derive(Event)]
#[event(category = "jobs", codec = "json")]
pub enum JobEvent {
    #[event(name = "job.created", version = 3, upcast(1 => upcast::created_v1, 2 => upcast::created_v2))]
    Created { order_id: Uuid, title: String, priority: u8 },
    #[event(name = "job.assigned")]
    Assigned { assignee: Uuid },
}
```

- `category` is required, must match `^[a-z][a-z0-9_]{0,39}$` and may not contain `__`.
- `name` is the stored type name and never changes; the Rust variant can be renamed freely. Names must be
  unique within the enum. Names starting with `$` are reserved for system events.
- `version` (default 1) is the event's schema version. Old versions are never rewritten; upcasters chain
  v1 → v2 → v3 and the derive composes them when decoding.
- Variant shapes: a struct variant is stored as an object of its fields (the variant name is not stored); a
  newtype variant as its inner value, which needs `Serialize` and `DeserializeOwned`; a unit variant as `{}`.
  Generic enums, empty enums and tuple variants with more than one field are compile errors.
- Field-level `#[serde(...)]` attributes are honoured without deriving serde on the enum, but only those that
  write data the enum reads back: `rename` (the same name both ways), `alias`, `default`, `skip`, `with`,
  `flatten`; `skip_serializing`/`skip_serializing_if` with `default`; `serialize_with` with
  `deserialize_with`. Any other field attribute, two fields stored or read under one name, and `#[serde]` on
  the enum, a variant or a newtype variant's field are compile errors; for container attributes such as
  `rename_all`, use a newtype variant around a struct that derives serde. A name collision with a field
  inside a `#[serde(flatten)]` struct cannot be seen by the derive and stays a decode error.
- `codec` is optional and defaults to `"json"`, the only codec so far. Any other value is a compile error.
  Per-version codecs (for example `codec(1 = "json")`) are future work, needed once a second codec exists.
- With no `#[event(crate = "path")]`, the generated code names `pges-core` by a path found in the calling
  crate's Cargo.toml (proc-macro-crate): `::<name>::core` if it depends on `pges` under any name, else
  `::<name>` if it depends on `pges-core` (with its default `json` feature), else `::pges_core`. The facade
  wins when both are present. Dev-dependencies count too. `crate = "..."` overrides this, as serde's
  `crate` attribute does: use it for re-exports, or when the facade is a dev-only or optional dependency.
  The path must lead to `pges-core`'s root module. A value that is not a path, has generic arguments, or is
  given twice is a compile error.

Upcasters have the type `pges_core::codec::json::Upcaster`:

```rust
pub type Upcaster = fn(serde_json::Value) -> Result<serde_json::Value, BoxError>;
```

- `upcast(k => f)` takes version k to k+1. The chain must cover exactly 1..version-1; a gap, a duplicate
  or a version outside that range is a compile error, and so is a function with the wrong signature.
- An event stored at version k runs the upcasters from k up to the current version, in order.
- To stop supporting an old version, its upcaster returns an error. An upcaster error, or data that does not
  deserialize, is `CodecError::Decode` with the stored schema version.
- An unknown name or a version above the current one decodes to `Decoded::Unknown`; a stored version 0 is a
  corrupt frame (`FrameError::OutOfRange`).
- Upcaster authors need their own `serde_json` dependency, because the signature uses `serde_json::Value`.
  Events without upcasters need nothing beyond the derive and `pges-core`.

The derive implements:

```rust
pub trait Event: Sized + Send + Sync + 'static {
    const CATEGORY: CategoryName;
    const TYPES: &'static [&'static str];
    fn type_name(&self) -> &'static str;
    fn schema_version(&self) -> SchemaVersion;
    fn encode(&self, out: &mut Vec<u8>) -> Result<(), CodecError>;
    fn decode(type_name: &str, schema_version: SchemaVersion, bytes: &[u8])
        -> Result<Decoded<Self>, CodecError>;
}

pub struct SchemaVersion(NonZeroU16);    // pges_core::event; FIRST, const fn new -> Option, get, Display

pub enum Decoded<E> {
    Event(E),
    Unknown { type_name: String, schema_version: SchemaVersion },   // a newer writer; not a malformed payload
}

pub enum OnUnknown { Skip, Fail }        // default Fail for load; configurable per subscription

pub const SYSTEM_PREFIX: &str = "$";     // pges_core::event; reserved for system event types
pub const STREAM_DELETED: &str = "$stream-deleted";   // the tombstone delete_stream appends
pub fn is_system_type(type_name: &str) -> bool;

pub trait Aggregate: Default + Send + Sync + 'static {
    type Event: Event;
    fn apply(&mut self, event: &Self::Event);
}

pub trait Snapshotted: Aggregate + Serialize + DeserializeOwned {
    /// Bump when the snapshot shape changes; snapshots of another version are then ignored.
    const SNAPSHOT_VERSION: NonZeroU16;  // stored in snapshots.state_version (int4)
    fn policy() -> SnapshotPolicy { SnapshotPolicy::Manual }   // or EveryN(n)
}
```

`TYPES` is checked (`Event::__CHECK`, hidden) where a store registers, appends or decodes: an empty name, a
`$` name or a duplicate is a compile error under `cargo build` (not `cargo check`). The derive already
refuses them; the check catches a hand-written impl.

Command handling stays outside the crate. The pattern is `load`, decide, then `append` with
`loaded.expected()`.

## Store

```rust
use pges_postgres::{admin, append, limit, migrate, retention, store, unit_of_work};

let mut config = store::StoreConfig::new(pges_core::store_name!("ops_es"));
config.migrate = store::AutoMigrate::CreateOrMigrate(store::StoreDefinition::default());   // default Disabled
let store = store::Store::open(pool.clone(), config).await?;   // checks schema compatibility, loads
                                                              // category and type ids, reads the outbox
                                                              // and dcb flags
store.register::<JobEvent>().await?;                    // optional; otherwise implicit on first use of E

let job = store.load::<Job>(id).await?;                 // Loaded<Job>
let job = store.load_snapshotted::<Job>(id).await?;     // Job: Snapshotted; snapshot, then replay
let res = store
    .append(id, job.expected(), &[JobEvent::Assigned { assignee }])   // NoStream or Exact(v)
    .metadata(meta)                                      // optional; unset fields come from the ambient scope
    .commit_id(cid)                                      // optional; the crate generates one otherwise
    .snapshot_after(&new_state)                          // Snapshotted only; same transaction
    .await?;                                             // append::AppendResult
store.save_snapshot(&job).await?;                        // manual snapshot of a Loaded<A>
store.delete_stream::<JobEvent>(id, version).await?;    // AppendResult of the $stream-deleted commit;
                                                         // impl Into<delete::DeleteExpected>

let events = store.read_stream::<JobEvent>(id, Version::FIRST);
    // impl Stream<Item = Result<StreamEvent<JobEvent>, read::ReadError>>; OnUnknown::Fail
let events = store.read_stream_with::<JobEvent>(id, Version::FIRST, OnUnknown::Skip);

let mut uow = store.begin().await?;                     // UnitOfWork<'_>: one transaction, several streams,
uow.append(job_id, ExpectedVersion::Exact(v1), &job_events).await?;   // plus your own tables
uow.append(order_id, ExpectedVersion::Any, &order_events).await?;
sqlx::query("UPDATE my_table …").execute(uow.conn()).await?;
uow.commit().await?;

let mut uow = unit_of_work::UnitOfWork::wrap(&store, &mut tx);   // inside a transaction you already own:
uow.append(job_id, ExpectedVersion::Exact(v1), &job_events).await?;   // a WrappedUnitOfWork
uow.finish().await?;                                     // no commit or rollback
tx.commit().await?;
store.append_in(&mut *conn, id, expected, &events).await?;   // on any connection, e.g. a PgHandler's
store.append_many([                                      // one statement; locks streams in stream id order
    append::AppendEntry::new(job_id, e1, &job_events),
    append::AppendEntry::new(order_id, e2, &order_events),
]).metadata(meta).await?;                                // Vec<AppendResult>, in entry order
```

```rust
#[non_exhaustive]
pub struct AppendResult {
    pub stream_id: StreamId,
    pub commit_id: CommitId,
    pub versions: RangeInclusive<Version>,
    pub outcome: append::Outcome,                        // Appended | Deduplicated
}
impl AppendResult {
    pub fn last_version(&self) -> Version;
    pub fn expected(&self) -> ExpectedVersion;           // Exact(last_version)
}

pub struct Loaded<A> { /* private */ }                   // read-only
impl<A> Loaded<A> {
    pub fn state(&self) -> &A;
    pub fn into_state(self) -> A;
    pub fn version(&self) -> Option<Version>;
    pub fn expected(&self) -> ExpectedVersion;           // NoStream when version is None, otherwise Exact(v)
    pub fn stream_id(&self) -> StreamId;
    pub fn commit_id(&self) -> Option<CommitId>;         // the commit holding `version`
}

#[non_exhaustive]
pub struct StoreConfig {                                 // how a writer opens a store; StoreConfig::new(name)
    pub name: StoreName,
    pub max_clock_skew: Option<Duration>,                // None by default: the store's recorded skew;
                                                         // Some must not exceed it (Store::open)
    pub write_format: frame::Format,
    pub migrate: store::AutoMigrate,                     // CreateOrMigrate(StoreDefinition)
                                                         // | Disabled (default)
    pub migrate_config: migrate::MigrateConfig,          // how CreateOrMigrate migrates: lock_wait, 60 s
    pub append_routing: store::AppendRouting,            // Auto (default): name the stream's partition
                                                         // when it fits | Streams: through `streams`
}

#[non_exhaustive]
pub struct StoreDefinition {                             // what a store is created with; Clone, Default
    pub stream_partitions: NonZeroU16,                   // 64; fixed at store creation
    pub outbox: store::Outbox,                           // Enabled | Disabled (default)
    pub max_clock_skew: Duration,                        // 5 s; recorded at creation, only raised
    pub max_txn_age: Interval,                           // 1 h; recorded at creation, only raised
}
```

- `StoreConfig` opens a store; `StoreDefinition` is what a store is created with. Only
  `AutoMigrate::CreateOrMigrate(definition)` and `migrate::run(conn, &name, &definition, config)` read a
  definition, and only to create a missing store: an existing store keeps its own. `Store::name() -> &StoreName`.
- `StoreDefinition`, `AutoMigrate`, `admin::CategoryConfig`, `retention::Retention` and `AfterWindow` are
  `Clone` but not `Copy`, so a later variant or field may hold owned data (an archive destination).
- DCB is not a public option. A DCB store made by another build is refused at open
  (`OpenError::StoreRequiresFeature("dcb")`).
- `AppendRouting::Auto`: a single append names its stream's hash partition of `streams`, so it locks only
  that partition whatever the planner's statistics and `plan_cache_mode`, and reuses its plan. `Store::open`
  falls back to `Streams` when the partition statements do not fit the pool's statement cache (below), when
  the role may not select, insert and update every partition directly, or when the partitions are not the
  ones the store was created with. Routing is fixed at open. `AppendRouting::Streams`: appends always go
  through `streams` with one prepared statement; a generic plan (a nearly empty analyzed store, or
  `plan_cache_mode = force_generic_plan`) then locks every partition. `append_many` always goes through
  `streams`, with custom plans whatever the statistics and `plan_cache_mode`, so it locks only the
  partitions of its streams (4 locks each).

Statement cache and memory: routed appends prepare one statement per partition on each connection. Each costs
about 130 KB of server plan-cache memory per connection: about 8.6 MB per connection at the default 64
partitions (0.6 MB through `streams`). Routing runs only when sqlx's `statement-cache-capacity` (default 100)
is at least `stream_partitions + 36`; the default 64 partitions fit. For a pool that several stores share,
size the capacity at `stream_partitions + 36` per store; an application that runs many statements of its own
on the pool, or reads many categories, needs more. Otherwise the partition statements evict each other and a
round trip comes back on each evicted statement's next use. To spend neither the cache entries nor the memory,
set `append_routing` to `AppendRouting::Streams`.

- The commit id is always generated on the client, so the crate can retry by itself. Outside a unit of
  work, an append that loses its connection is retried once on a new one; if the first attempt committed,
  the retry returns the original result with `Outcome::Deduplicated`. A caller-supplied commit id that
  already committed on this stream also returns `Deduplicated`, in every mode (autocommit, a unit of work,
  `append_in` inside a transaction) and however long after `max_clock_skew` the retry comes. Only a new id
  is checked against the skew; a late id the stream has not stored is `CommitIdOutOfOrder`.
- `snapshot_after` with `SnapshotPolicy::Manual` writes a snapshot on every call, because the caller asked
  for it. With `EveryN(n)` it writes when the stored snapshot is at least `n` versions behind.
  `load_snapshotted` with `EveryN(n)` saves a new snapshot when it replayed at least `n` events. `load` and
  `load_snapshotted` use `OnUnknown::Fail`.
- A read or load returns the stream as of its head at the start; it never includes commits appended while
  it pages.
- A save replaces the stored snapshot only when the stored version and state version do not go down. Of the
  same state version, it replaces one at least `n` versions behind (1 for `Manual` and `save_snapshot`). Of
  a higher state version, it replaces one at the same version or older. Of a lower state version, it never
  replaces: during a rolling deploy old code cannot overwrite new code's snapshot, and a snapshot retention
  relied on stays at or above the stream's floor. The cost: after rolling back to a lower
  `SNAPSHOT_VERSION`, that code's saves are skipped until it raises the version again.
- Separate `append` calls lock streams in call order, so two units of work touching {A, B} and {B, A} can
  deadlock; that surfaces as a `DbError` with `is_retryable() == true`. `append_many` first locks the rows
  of the batch's existing streams in stream id order, then inserts new streams in stream id order, so
  concurrent batches lock in the same order whatever plan PostgreSQL picks. It is all or nothing and returns
  per-stream results in entry order. A batch naming a stream twice fails with `AppendError::DuplicateStream`.
  A tuple array cannot mix event types, hence `AppendEntry`. `.metadata(m)` applies to every commit. Outside
  a unit of work it runs in its own transaction (BEGIN with `SET LOCAL plan_cache_mode =
  force_custom_plan`, statement, COMMIT: three round trips), and starts over in a new transaction when a
  stream it must retry is locked by another one. Inside a unit of work it sets `force_custom_plan` before
  the statement and the transaction's previous mode after it (two more round trips) (schema.md, Several
  streams).
- `UnitOfWork`: after a `DuplicateCommit`, a `CommitIdSkew`, a database error, or an `append_many` that
  fails or is dropped before it completes (it may have written part of its batch), the transaction is
  unusable. `commit()` then rolls back and returns `TransactionError::Aborted` (also when
  that rollback fails, for example on a lost connection; `Aborted` carries no source), because
  PostgreSQL would silently turn the COMMIT into a ROLLBACK. Conflicts from a single append write nothing
  and leave the unit of work usable. A single append dropped before it completes also makes `commit()`
  return `Aborted`: its statement may still raise, and sqlx would return that error at the next statement
  (or the statement may have written the commit). A failed statement of the caller's through `uow.conn()` also makes
  `commit()` roll back and return `Aborted`: once `conn()` was called (and always in a wrapped unit of
  work's `finish()`), it first runs `SELECT 1` and treats SQLSTATE 25P02 as aborted. sqlx does not expose
  the transaction status the server reports, so that costs one round trip; a unit of work that only appends
  pays nothing.
- `UnitOfWork::wrap(&store, &mut tx)` takes `&mut PgConnection` and returns a `WrappedUnitOfWork`, which has
  `conn`, `append`, `append_many` and `finish`, and no `commit` or `rollback`. `finish()` fails with `Aborted`
  if the transaction was aborted (a failed append or a failed statement); the caller ends its own
  transaction.
- Savepoints on `conn()` are allowed, but in sqlx 0.9.1 a savepoint begin that fails or is dropped ends the
  whole transaction. A unit of work that sees its connection leave the transaction it was in at `begin` /
  `wrap` refuses appends (`AppendError::TransactionEnded`) and fails `commit` / `finish` with `Aborted`; the
  caller's own statements after it run in autocommit (design.md, Decided, sqlx 0.9.1).
- `append_in` is not retried after a lost connection, and after an error the caller must roll its
  transaction back. A crate-generated commit id that fell behind is replaced under the stream's row lock
  inside the caller's transaction. Outside a transaction that lock ends with its statement, so
  `CommitIdOutOfOrder` remains possible after 5 new ids.
- Crate-generated commit ids never fail an `ExpectedVersion::Any` append under contention: an id that fell
  behind the stream's last one is replaced while the stream's row is locked (schema.md, Append). A
  caller-supplied id fails at once with `CommitIdOutOfOrder`.
- Event type registration runs on a separate pooled connection, never on the caller's. A unit of work that
  appends a type not yet registered therefore needs a second connection. With a pool of size 1, a unit of
  work from `store.begin()` that appends a type not yet registered fails at once with
  `AppendError::NoSpareConnection(category)`; a type whose ids were already in the database when the store
  was opened needs no second connection. With a larger pool that is exhausted (or `append_in` on a
  connection of the same pool), the append waits for the pool's acquire timeout. Call
  `store.register::<E>()` up front.

Migrations and administration:

```rust
let config = migrate::MigrateConfig::default()      // Copy; lock_wait: Interval, 60 s (lock::DEFAULT_WAIT),
    .with_lock_wait(interval!(5 min));              // counting only time spent waiting for the lock
migrate::run(&mut conn, &name, &definition, config).await?;       // run_shared, then run_store; both public
migrate::run_shared(&mut conn, config).await?;                    // the shared pges schema
migrate::run_store(&mut conn, &name, &definition, config).await?; // creates the store from the
                                                            // StoreDefinition if missing, then migrates it;
                                                            // each takes &mut PgConnection and returns a
                                                            // BoxFuture
let admin = store.admin();                        // or admin::Admin::new(pool, store_name!("ops_es")),
                                                  // which cannot fail
let admin = admin.with_lock_wait(interval!(5 min));   // wait for the maintenance lock; default 60 s
admin.register_category(JobEvent::CATEGORY, admin::CategoryConfig::default()).await?;
                                                  // CategoryConfig { bucket: Bucket::Day | Month (default),
                                                  //   retention: Option<Retention> (None: store default) }
admin.change_bucket(JobEvent::CATEGORY, admin::Bucket::Day).await?;
                                                  // applies from the newest leaf's upper bound
admin.maintain().await?;                          // leaves up to admin::LEAVES_AHEAD (3) buckets ahead
admin.set_retention(JobEvent::CATEGORY, retention::Retention::Window {
    keep: Duration::from_secs(30 * 86_400), then: retention::AfterWindow::Drop }).await?;
admin.set_default_retention(retention::Retention::Forever).await?;   // for categories registered later
admin.set_writer_limits(limit::WriterLimits::new(skew, interval!(2 h))).await?;
                                                  // raises the store's writer limits; never lowers them
let roles = limit::WriterRoles::one("app_writer");
let report = admin.apply_retention(&retention::RetentionConfig::new(roles)).await?;
                                                  // RetentionReport
let pruned = admin.prune_log(JobEvent::CATEGORY).await?;   // PrunedLog
let frozen = admin.freeze_closed_partitions().await?;      // freeze::FreezeReport { frozen, busy }
let ages = admin.partition_ages().await?;                  // Vec<freeze::PartitionAge>
let headroom = admin.partition_headroom().await?;
                                                  // PartitionHeadroom { categories: Vec<CategoryHeadroom
                                                  //   { category, bucket, leaves_until: Option<SystemTime>,
                                                  //   ahead: Duration }>, log: LogHeadroom { end:
                                                  //   Option<Position>, remaining: u64 } }
```

The migration functions begin transactions on the caller's connection. Run them on a connection outside a
transaction: each migration then runs in a transaction of its own, which a dropped future rolls back. On a
connection already inside a transaction each is a savepoint, and a future of `migrate::run`, `run_shared` or
`run_store` dropped while one begins rolls back the caller's whole transaction without its `Transaction`
knowing, so its `commit` does nothing (sqlx 0.9.1's savepoint begin; design.md, Decided, sqlx 0.9.1).
`Store::open` runs them on a pooled connection outside a transaction.

`migrate::run` and `run_shared`, and so an auto-migrating `Store::open`, fail with
`MigrateError::SharedSchemaNotOwned { owner }` when the schema `pges` exists and its owner is not
`current_user`, also when nothing is left to apply; a superuser or a member of the owner is refused too
(schema.md, Migrations). Migrate as the owner: a login role that inherits from a NOLOGIN owner group runs
`SET ROLE <owner>` on the connection first. An application whose pool role does not own `pges` keeps
auto-migrate off and migrates separately. `run_store` alone does not check, so a store creator that does
not own `pges` can create stores in the schema the owner migrated; call it only after the owner's
`run_shared`.

```rust
// pges_postgres::retention
#[non_exhaustive] pub enum Retention {            // Default Forever; Clone, not Copy
    Forever,
    Window { keep: Duration, then: AfterWindow },
    SnapshotAndTruncate { keep: Duration },
}                                                 // keep is at most Retention::MAX_KEEP (1,000 years of
                                                  // 365 days); a longer one is KeepTooLong where it is set
#[non_exhaustive] pub enum AfterWindow { Drop }   // Default Drop; Clone, not Copy: Archive (not built)
                                                  // may carry a destination
#[non_exhaustive] pub struct RetentionConfig {    // RetentionConfig::new(WriterRoles)
    pub writer_roles: limit::WriterRoles,         // each needs a transaction_timeout of at most the
}                                                 // store's max_txn_age
#[non_exhaustive] pub struct RetentionReport {
    pub dropped: Vec<DroppedLeaf>,                // { category: CategoryName, leaf: PartitionName,
                                                  //   horizon: Position, start: Position }
    pub refused: Vec<Refusal>,                    // { category: CategoryName, leaf: Option<PartitionName>,
                                                  //   reason }
    pub log_partitions: Vec<PartitionName>,
}
pub struct PartitionName(String);                 // as_str, Display, AsRef<str>, PartialEq<str>;
                                                  // Debug is the quoted name
#[non_exhaustive] pub enum RefusalReason {
    NotSequenced,
    MissingSnapshots(Vec<StreamId>),              // SnapshotAndTruncate; at most REPORTED_STREAMS (100)
    LiveStreams(Vec<StreamId>),                   // Window; at most REPORTED_STREAMS
    UnsupportedPolicy,                            // Window then Archive (code 2)
}
#[non_exhaustive] pub struct PrunedLog { pub rows: u64, pub log_partitions: Vec<PartitionName> }

// pges_postgres::limit
#[non_exhaustive] pub struct WriterLimits {       // WriterLimits::new(Duration, Interval);
    pub max_clock_skew: Duration,                 // Default: the pges_core::store defaults
    pub max_txn_age: Interval,
}
pub struct WriterRoles(NonEmpty<String>);         // new(first, rest), one(role), iter();
                                                  // From<NonEmpty<String>>,
                                                  // TryFrom<Vec<String>> (Error = non_empty::EmptyError)
// Admin::set_writer_limits(&self, WriterLimits) -> Result<(), SetWriterLimitsError>
pub async fn check_writer_roles(conn: &mut PgConnection, writer_roles: &WriterRoles, max_txn_age: Duration)
    -> Result<(), WriterRoleError>;               // max_txn_age is the store's, read from the database
```

`admin::LEAVES_AHEAD` and `retention::REPORTED_STREAMS` are `u16`; `admin::LOG_PARTITION_POSITIONS` (10^8) is
a `u64`.

`migrate::SHARED`, `migrate::STORE` and `migrate::OUTBOX` (the outbox of a store created with it, scope
`<store>.outbox`) list the embedded `Migration`s (version, name, SQL,
`min_compatible`, SHA-256 checksum); versions and `min_compatible` are `migrate::SchemaVersion`. `migrate::latest_version(migrations)`
is the newest version a list knows, and `migrate::crate_too_old(conn, Scopes::OneStore(&name) | Scopes::AllStores)`
returns the first scope whose `min_compatible` is above it (`migrate::CrateTooOld { scope, min_compatible,
crate_version }`), the check `Store::open` makes at start. `register_category` is idempotent, creates the category and its partitions in one
transaction, and never changes an existing category's bucket. When a rollback fails too, it returns the
error of the failed attempt, not the rollback's. `change_bucket` does, from the upper bound of
the newest leaf, so leaves never overlap. `maintain` must run on a schedule: besides leaves, it creates the
log partitions the sequencer writes to.

- `set_retention` applies from the next `apply_retention` and drops nothing now. `register_category` takes
  `CategoryConfig::retention`, or the store's default when it is `None` (`Forever` unless set); it never
  changes an existing category's retention.
- `apply_retention` drops, per category and oldest first, the time leaves its policy no longer keeps, then
  the log partitions no category needs (schema.md, Retention). It stops a category at its first refused
  leaf and tries again on the next run. Before a leaf goes, the category's horizon, its start position and
  its streams' `floor_version` are raised, so subscriptions behind the horizon fail with
  `CursorBeforeHorizon`, subscriptions created at `Start::Retained` and backfills read the category after
  its start, and reads below a floor fail with `ReadError::Truncated` (load such streams with
  `load_snapshotted`). `DroppedLeaf::horizon` is the newest position of a dropped commit;
  `DroppedLeaf::start` (at most the horizon) is the position before the oldest commit that stays. Commits
  between the two stay when they committed before the dropped leaf's last commit. It refuses while the
  sequencer is halted and unless every writer role's `transaction_timeout`, set on the role or the
  database, is at most the store's `max_txn_age` (`WriterRole`). The margins it keeps are the store's
  writer limits. What was dropped before an error stays dropped.
- `prune_log` deletes a category's log rows that only name commits retention already dropped, in batches,
  vacuums the partitions that lost rows, then drops the log partitions no category needs. A row whose commit
  still exists is kept. Use it for short-lived categories in a store whose other categories keep the log
  partitions alive.
- `freeze_closed_partitions` freezes closed leaves and full log partitions that are not all frozen, or that
  hold dead rows, one partition per maintenance-lock session; busy partitions are retried on the next run.
  It can read whole partitions, so run it off-peak. It fails with `StoreNotFound`. `partition_ages` lists
  every partition's pages, all-frozen pages, dead rows, xid and multixact age; it takes no lock (schema.md,
  Closed partitions).
- `partition_headroom` is what to alert on for a stopped `maintain`. Per category it gives the upper bound of
  the newest time leaf (`leaves_until`) and how far ahead of the database's clock it is (`ahead`, zero once
  passed); `CategoryHeadroom::below_one_bucket` holds when that is under one bucket (a day, or 28 days for
  `Month`), which with `maintain` running happens only after it has stopped for about `LEAVES_AHEAD` buckets.
  Appends past `leaves_until` fail with `AppendError::NoPartition`. For the log it gives the end of the
  newest log partition and the positions left in it; `maintain` adds a partition once half is used, so
  `remaining` dips just below `LOG_PARTITION_POSITIONS / 2` until its next run: alert when it falls below
  `LOG_PARTITION_POSITIONS / 4`. It reads only the catalog and takes no lock.
  The crate has no logging dependency, so `Store::open` does not warn about low headroom itself.
- `set_writer_limits` raises the store's `max_clock_skew` and `max_txn_age` under the maintenance lock, so a
  retention run in progress finishes with the old values; retention and `resequence` use the new ones from
  their next run. It never lowers them: a lower value fails with `Lowering { max_clock_skew, max_txn_age }`
  (the recorded values) and changes nothing, because commits written under the larger value stay out of
  order by up to that much. Raise the skew before opening writers with a larger explicit
  `StoreConfig::max_clock_skew`, and `max_txn_age` before lengthening the writer roles'
  `transaction_timeout`. A writer whose `max_clock_skew` is `None` uses the store's value, so raising the
  store's is enough.
- `check_writer_roles` checks that each role has a `transaction_timeout` other than 0 and at most
  `max_txn_age`, set with `ALTER ROLE … SET` (optionally `IN DATABASE`) or `ALTER DATABASE … SET`. A value
  set only in `postgresql.conf`, or by a session with `SET`, is not seen. Retention and `resequence` run it
  first.
- `delete_stream::<E>` deletes a stream of `E`'s category if its version is still `expected`: in one
  transaction it appends a `$stream-deleted` event, marks the stream deleted and deletes its other commits,
  its snapshot and its string key. Subscribers get `SystemEvent::StreamDeleted`; later appends fail with
  `StreamDeleted`, reads and loads with `ReadError::StreamDeleted`, and the id stays taken. `expected` is a
  `delete::DeleteExpected { Any, Exact(Version) }`; a `Version` converts into `Exact`. There is no
  `NoStream`: `ExpectedVersion` does not convert, so `NoStream` does not compile. A lost connection is not
  retried. Prefer crypto-shredding for personal data: this does not
  reach replicas or backups. The deleted commit ids are recorded, so store-wide subscriptions skip their log
  rows. The `$stream-deleted` commit records the ambient metadata (below), so the deleting actor and request
  are kept.
- Ambient metadata (`pges_postgres::ambient`, reached as `pges::postgres::ambient`):

  ```rust
  pub fn scope<F: IntoFuture>(metadata: Metadata, future: F) -> impl Future<Output = F::Output>;
  pub fn current() -> Metadata;          // the innermost scope's, or empty outside any scope
  ```

  An append records, field by field: its explicit `.metadata(m)` (on `Append` and `AppendMany`; it fills
  only the fields `m` leaves unset), then the innermost scope, then, with `otel`, the current span's trace
  (trace only), then unset. Every append path resolves the same way: `append`, `append_in`, `append_many`,
  `UnitOfWork` and `WrappedUnitOfWork` appends, and `delete_stream`. The scope and span are read at the
  append future's first poll, so `ambient::scope(m, store.append(..))` and
  `store.append(..).into_future().instrument(span)` both work. Nested scopes merge: the inner scope's set
  fields win, and its unset fields come from the enclosing scope, read when `scope` is called (a scope built
  outside and polled inside another does not merge). The scope is a tokio task-local, so `tokio::spawn` does
  not inherit it; wrap the task in `ambient::scope(ambient::current(), ..)`. Without a scope or `otel`,
  metadata is stored as given.

Errors (`thiserror`, one enum per operation, all `#[non_exhaustive]`; there is no top-level `pges::Error`,
design.md Open decisions):

```rust
#[non_exhaustive]
pub enum AppendError {
    Conflict { stream_id: StreamId, expected: ExpectedVersion, actual: Option<Version> },
    CategoryMismatch { stream_id: StreamId, stream_category: CategoryName, event_category: CategoryName },
    StreamDeleted(StreamId),
    StreamArchived(StreamId),
    CommitIdOutOfOrder { stream_id: StreamId, commit_id: CommitId },   // caller-supplied id at or below the
                                                                       // stream's last; a generated one is
                                                                       // replaced under the stream's row lock
                                                                       // (fails only from append_in outside a
                                                                       // transaction, after 5 new ids)
    CommitIdSkew(CommitId),                                            // id timestamp outside max_clock_skew
    DuplicateCommit(CommitId),                                         // same id on two appends, inside a
                                                                       // transaction
    TooManyEvents { max: u16 },                                        // 32767 per commit
    NoEvents,
    DuplicateStream(StreamId),                                         // append_many names a stream twice
    UnknownCategory(CategoryName),
    NoPartition { category: CategoryName, commit_id: CommitId },       // no leaf of the category takes the
                                                                       // commit id: maintain has stopped;
                                                                       // in append_many, the first commit
                                                                       // of that category; also when the
                                                                       // category partition is missing
    NoSpareConnection(CategoryName),                                   // registering needs a second pooled
                                                                       // connection; the unit of work holds
                                                                       // the pool's only one
    Encode(CodecError),
    SnapshotEncode(BoxError),
    Database(DbError),
}

pub struct DbError { /* opaque */ }
impl DbError {
    pub fn is_retryable(&self) -> bool;  // serialization failure, deadlock, connection loss, shutdown
    pub fn code(&self) -> Option<String>; // SQLSTATE
}
impl std::error::Error for DbError {}    // source() exposes the driver error without naming its type
```

An append that fails writes nothing: `Conflict`, `CategoryMismatch`, `StreamDeleted` and the other "no row"
outcomes are classified after the statement wrote no row.

| Enum | Variants |
|---|---|
| `store::OpenError` | `StoreNotFound`, `SchemaBehind { scope, database, required }`, `CrateTooOld`, `ChecksumMismatch`, `ClockSkewAboveStore { configured, store }`, `StoreRequiresFeature`, `Migrate`, `Database` |
| `store::RegisterError` | `UnknownCategory`, `Database` |
| `read::ReadError` | `UnknownCategory`, `CategoryMismatch`, `StreamDeleted`, `Truncated`, `UnknownEventType`, `UnknownEvent`, `Frame`, `Decode`, `Corrupt`, `Database` |
| `load::LoadError` | `Read(ReadError)`, `SnapshotDecode`, `SnapshotSave(SnapshotError)` |
| `load::SnapshotError` | `Encode`, `UnknownCategory`, `Database` |
| `unit_of_work::TransactionError` | `Aborted`, `Database` |
| `migrate::MigrateError` | `SharedSchemaMissing`, `SharedSchemaNotOwned { owner }`, `ChecksumMismatch`, `CrateTooOld`, `OutboxMismatch { store, outbox }`, `Failed`, `LockTimeout(lock::LockTimeout)`, `Database` |
| `admin::RegisterCategoryError` | `UnsupportedBucket`, `NameTaken { name }`, `KeepTooLong(Duration)`, `Database`, `LockTimeout` |
| `admin::MaintainError` | `UnsupportedBucket`, `NameTaken { name }`, `Database`, `LockTimeout` |
| `admin::ChangeBucketError` | `UnknownCategory`, `NameTaken { name }`, `Database`, `LockTimeout` |
| `admin::PartitionHeadroomError` | `UnsupportedBucket`, `Database` |
| `retention::SetRetentionError` | `UnknownCategory`, `KeepTooLong(Duration)`, `Database`, `LockTimeout` |
| `retention::SetDefaultRetentionError` | `StoreNotFound`, `KeepTooLong(Duration)`, `Database`, `LockTimeout` |
| `retention::RetentionError` | `SequencerHalted`, `StoreNotFound`, `WriterRole(WriterRoleError)`, `Database`, `LockTimeout` |
| `retention::PruneLogError` | `UnknownCategory`, `Database`, `LockTimeout` |
| `freeze::FreezeError` | `StoreNotFound`, `Database`, `LockTimeout` |
| `freeze::PartitionAgesError` | `Database` |
| `limit::SetWriterLimitsError` | `StoreNotFound`, `Lowering { max_clock_skew, max_txn_age }` (the recorded values, as `Duration`s), `Database`, `LockTimeout` |
| `limit::WriterRoleError` | `UnknownRole`, `TransactionTimeoutNotSet { role }`, `TransactionTimeoutTooLong { role, transaction_timeout, max_txn_age }`, `InvalidTransactionTimeout { role, value }`, `Database` |
| `delete::DeleteStreamError` | `NotFound`, `Conflict { expected: DeleteExpected, .. }`, `CategoryMismatch`, `StreamDeleted`, `StreamArchived`, `UnknownCategory`, `Write(AppendError)`, `Database` |

How each "no row" outcome is classified is in schema.md (Append).

`LockTimeout` carries a `lock::LockTimeout { lock, waited, holder }`: `lock` is `lock::Lock::Migration` or
`Maintenance(StoreName)`, `waited` the configured wait, and `holder` an `Option<lock::LockHolder { pid:
BackendPid, role: Option<String> }>`, the session that held the lock when the wait ran out, from `pg_locks`
and `pg_stat_activity`. It is never retried by the crate: run the operation again, or end the holder. Every
`Admin` operation but `partition_headroom` and `partition_ages` can return it; an operation waiting for the
lock holds a pooled connection only for each try (schema.md, Partition maintenance).

Errors that name a store carry a `StoreName`: `OpenError::StoreNotFound`,
`SetWriterLimitsError::StoreNotFound`, `SetDefaultRetentionError::StoreNotFound`,
`RetentionError::StoreNotFound`, `FreezeError::StoreNotFound` and `MigrateError::OutboxMismatch`'s
`store`. Retention reports carry `CategoryName` and `PartitionName`, freeze reports `PartitionName`.
`NameTaken { name }` carries an `admin::RelationName` (a table's or an index's name, without the schema;
`as_str`, `Display`, `AsRef<str>`, `PartialEq<str>`).

`StoreDefinition::outbox` must match the database's other stores. A store whose outbox setting differs fails
with `MigrateError::OutboxMismatch` (through `OpenError::Migrate` when auto-migrating, and from
`pges_testing::create_store(conn, &StoreName, &StoreDefinition)` as an `anyhow` error that downcasts to it),
and nothing is created. A trigger on `pges.stores` enforces the rule in SQL (schema.md, Migrations).

Test helpers in `pges-testing` take the same types: `definition(Outbox)` (the default definition with 4
stream partitions), `create_store(conn, &StoreName, &StoreDefinition)`,
`register_category(conn, &StoreName, CategoryName, months_back: u16, months_ahead: u16) -> CategoryId`,
`create_log_partition(conn, &StoreName, u32)`, and `position(n)` / `version(n)`, which panic out of range
(literals use `position!` / `version!`).

## Subscriptions and projections

`pges-subscribe`. The entry point is the extension trait `subscription::StoreExt`, which gives `Store` a
`subscription(name)` and a `backfill(name, until)` method; `subscription::Builder::new(&store, name)` and
`backfill::Builder::new(&store, name, until)` are the same. Through the facade it is
`pges::subscribe::subscription::StoreExt`. It is sealed (implemented for `Store` only), so it can gain
methods in a minor version.

```rust
use pges_core::{category, interval, non_empty};
use pges_subscribe::{batch, handler, replica, shards, subscription, subscription_name, waker};
use pges_subscribe::subscription::StoreExt;

let handle = store.subscription(subscription_name!("jobs-board.v3"))
    .categories_of::<(JobEvent, OrderEvent)>()      // tuples of up to 8; or .category::<JobEvent>(),
                                                    // or .categories(non_empty![category!("jobs"),
                                                    //   category!("orders")]),
                                                    // or .all_categories(): the whole store
    .sharded(shards!(8))                            // optional: 1 to 1024 shards by stream; default 1
    .start(subscription::Start::Beginning)          // or Now, Position(p), Retained; only when the rows
                                                    // are created
    .redefine(subscription::Start::Now)             // optional: accept changed categories or shards
    .lease(interval!(15 s))                         // default 15 s; run_pg is always exclusive
    .batch_size(const { NonZeroU32::new(1024).unwrap() })  // commits per category per batch; default 1024
                                                    // (63k ev/s catch-up at 256, 182k at 1024; a shard's
                                                    // batch holds about this many / shards;
                                                    // design.md, Durable dev-box run)
    .on_unknown(OnUnknown::Skip)                    // default Fail; applies to Batch::decode
    .on_error(subscription::RetryPolicy::new(interval!(100 ms), max_attempts))
    .waker(waker::poll::PollWaker::new(&store, waker::poll::PollConfig::default()))
                                                    // default: waker::recheck::Recheck,
                                                    // every 500 ms
    .acquire_interval(interval!(1 s))               // a standby's attempts; default 1 s
    .retry_interval(interval!(1 s))                 // pause after a database or waker error; default 1 s
    .max_idle(interval!(30 s))                      // read the log at least this often while caught up,
                                                    // whatever the waker; default 30 s
    .replica(replica::Replica::new(replica_pool).fallback(interval!(5 s), limit.clone()))
                                                    // optional: read pages from a streaming replica;
                                                    // or .replica(pool): no fallback
    .run_pg(JobsBoard::default())                   // exactly once; or .run(handler): at least once
    .spawn();                                       // subscription::SubscriptionHandle

handle.status();             // Status { state, position, high_water, shards }, Status::lag()
handle.status_changes();     // watch::Receiver<Status>
handle.is_finished();
handle.shutdown().await?;    // finishes the batch in hand, checkpoints, releases the lease, waits; a batch
                             // waiting for a retry backoff is not retried: shutdown returns at once and the
                             // batch is delivered again on the next start
// handle.join().await: waits for a stop by itself (only on a fatal error). Dropping the handle shuts the
// subscription down without waiting.

// Or run it on the current task (shutdown: impl Future<Output = ()> + Send + 'static):
// subscription.run_until(shutdown).await?;    // subscription.status() -> watch::Receiver<Status>

#[non_exhaustive] pub enum State { Starting, Standby, Running, Recovering(Arc<SubscribeError>), Stopped }
pub struct Shard(u16);                     // Shard::get(), Display; Status.shards lists the held ones

pub struct SubscriptionName(/* Cow<'static, str> */);   // ^[a-z][a-z0-9_.-]{0,99}$; MAX_LEN = 100;
                                           // const fn new(&'static str) -> Result<_, SubscriptionNameError>,
                                           // parse, FromStr, as_str, Display
pub struct Shards(u16);                    // 1..=1024; ONE, MAX, const fn new -> Option, get, Display
pges_subscribe::subscription_name!("jobs-board.v3")   // checks a literal at compile time
pges_subscribe::shards!(8)

// pges_subscribe::replica
pub struct Replica;                        // From<PgPool>: no fallback
impl Replica {
    pub fn new(pool: PgPool) -> Self;
    pub fn fallback(self, after: Interval, limit: FallbackLimit) -> Self;
}
pub struct FallbackLimit;                  // Clone shares the permits
impl FallbackLimit {
    pub fn new(concurrent: usize) -> Self;                       // 0 never falls back
}

pub trait PgHandler: Send {                // run_pg: writes only through tx; never commits it
    fn handle(&mut self, tx: &mut PgConnection, batch: &batch::Batch)
        -> impl Future<Output = Result<(), HandlerError>> + Send;
    fn isolation(&self) -> Isolation { Isolation::Default }   // tx's level; also for backfills
}
#[non_exhaustive]
#[derive(Default)]
pub enum Isolation { #[default] Default, ReadCommitted, RepeatableRead, Serializable }
// Savepoints on tx are allowed; one whose begin fails or is dropped ends tx (sqlx 0.9.1), and the batch is
// retried as transient like an aborted one: no checkpoint moves.

pub trait Handler: Send {                  // run: at least once, for external sinks
    fn handle(&mut self, batch: &batch::Batch) -> impl Future<Output = Result<(), HandlerError>> + Send;
}

#[non_exhaustive]
pub enum HandlerError { Transient(BoxError), Fatal(BoxError) }
// From<batch::DecodeError>: Fatal. From<sqlx::Error>: Transient when DbError::is_retryable(), else Fatal.

#[non_exhaustive]
pub struct RetryPolicy { pub backoff: Interval, pub max_attempts: NonZeroU32 }   // RetryPolicy::new
// Default: 100 ms, 5 attempts (the first included). The wait doubles after each attempt, up to 30 s.

impl Batch {
    pub fn iter(&self) -> impl Iterator<Item = &RawRecorded>;   // RawRecorded::Event(RawEvent)
                                                                // | RawRecorded::System(SystemEvent)
    pub fn decode<E: Event>(&self)
        -> impl Iterator<Item = Result<LogEvent<E>, batch::DecodeError>>;   // only E::CATEGORY; skips system
                                                                             // events; applies OnUnknown;
                                                                             // a category the subscription
                                                                             // does not read yields nothing
                                                                             // and logs a tracing warning
                                                                             // once per process and category
    pub fn system(&self) -> impl Iterator<Item = &SystemEvent>;
    pub fn last_position(&self) -> Position;   // the merge cut; can advance with no records (schema.md)
    pub fn len(&self) -> usize;
    pub fn is_empty(&self) -> bool;
}

#[non_exhaustive]
pub enum DecodeError {
    UnknownEvent { position: Position, type_name: String, schema_version: SchemaVersion },   // OnUnknown::Fail
    Codec { position: Position, source: CodecError },
}

#[non_exhaustive]
pub struct RawEvent {
    pub position: Position, pub stream_id: StreamId, pub category: CategoryName, pub category_id: CategoryId,
    pub version: Version, pub commit_id: CommitId, pub recorded_at: OffsetDateTime,
    pub metadata: Arc<Metadata>, pub type_name: Arc<str>, pub schema_version: SchemaVersion, pub data: Vec<u8>,
}
impl RawEvent {
    pub fn follow_up(&self) -> Metadata;   // metadata for commits this event causes (Metadata::follow_up)
    #[cfg(feature = "otel")]
    pub fn trace_parent(&self) -> Option<opentelemetry::Context>;   // parent for the handler's span
}
// Subscriptions set no ambient scope around handlers: a batch holds commits with different correlations
// and causes. A handler that appends passes the event's follow_up(), e.g. ambient::scope(event.follow_up(),
// store.append_in(..)).

#[non_exhaustive]
pub enum SystemEvent {                     // type names starting with $; pges_core::event::STREAM_DELETED
    StreamDeleted { position: Position, stream_id: StreamId, category: CategoryName },   // $stream-deleted
    Unrecognised { position: Position, type_name: String, stream_id: StreamId },
}
```

`Builder<S>` tracks the scope (`NoScope`, `Listed`, `AllCategories`): `run` and `run_pg` exist only once
categories are chosen, and listed categories and `all_categories()` cannot be combined; neither compiles.
The first `.categories(..)` takes a `NonEmpty<CategoryName>`, later ones any iterator. `sharded` takes
`Shards`, `batch_size` a `NonZeroU32`, and `lease`, `acquire_interval`, `retry_interval` and `max_idle` an
`Interval`.

- Reads follow schema.md (Read a category from a cursor): `H` first, then each category up to `H`, merged
  at the cut. Batches are in position order, and no event of the subscribed categories is skipped or
  delivered twice. A batch whose cut moves the cursor without events is checkpointed without calling the
  handler.
- Frames decode through `pges_core::frame`. Type ids come from the store's `event_types`, reloaded once when
  an id is unknown (`SubscribeError::UnknownEventType` if it stays unknown).
- `run_pg`: the transaction begins at `PgHandler::isolation()` (`handler::Isolation::{Default,
  ReadCommitted, RepeatableRead, Serializable}`, `#[non_exhaustive]`; `Default` is the session's
  `default_transaction_isolation`, which may be SERIALIZABLE, so `ReadCommitted` asks for READ COMMITTED
  whatever it says), takes the savepoint `pges_batch`, and locks the subscription's row, fenced, before the
  handler runs, so the handler's statements run in a subtransaction (a writing batch uses two
  transaction ids; more than 63 savepoints of the handler's own overflow the subtransaction cache, a
  SubtransSLRU cost for other sessions while it runs). `pges_batch` is reserved: rolling back to it or
  releasing it releases the row lock early (fencing still keeps the batch exactly once), and releasing it
  makes any later failure of the batch fatal. Then
  the handler runs, the checkpoint runs in the same transaction, and it commits. The read model must live in
  the store's database. After any retried error the subscriber re-reads its stored cursor, and the checkpoint
  moves the cursor only from where the subscriber holds it, so a COMMIT whose reply was lost does not deliver
  the batch twice, also when it lands after the re-read. A handler cannot change the level itself:
  `SET TRANSACTION ISOLATION LEVEL` to another level fails after the lock, and the run stops with
  `HandlerFailed` (the level the transaction has passes). `SET TRANSACTION READ ONLY` passes, then fails the
  checkpoint with 25006, which is transient and retried until the `RetryPolicy` stops the run.
- `run`: the handler runs first, then an autocommit checkpoint. A crash or lost lease in between delivers the
  batch again; make the effect idempotent.
- A handler that leaves its transaction aborted is detected (25P02 at the checkpoint), rolled back, and
  counted as a transient failure, because the failed statement may have been a serialization failure.
- A handler that ends the transaction itself with a raw `COMMIT` or `ROLLBACK` (sqlx does not see it, so
  `is_in_transaction()` still says yes) is detected by the checkpoint, which checks it runs in the
  transaction the batch began (the id the row lock returned, `pg_current_xact_id()`). When the handler
  returns an error instead, or the checkpoint finds a transaction aborted (25P02) or fails, the subscriber
  rolls back to the savepoint `pges_batch` before rolling back, which fails if the batch's transaction is
  gone (also after `COMMIT; BEGIN`). Either way nothing is checkpointed, and the run stops with
  `HandlerFailed` (fatal, whatever the handler returned: its writes may already have committed, so a retry
  could apply them twice). So does a transaction sqlx ended itself (a savepoint begin of the handler's that
  failed or was dropped; the statements after it ran in autocommit). The same holds for a backfill's lanes.
  It costs no extra statement in `run_pg` on success (the savepoint rides on the `BEGIN`), one on a failure,
  and one per lane batch in a backfill (its lane lock).
- An error the server reports (severity ERROR) for the checkpoint or the COMMIT, such as a deferred
  constraint or a serialization failure, rolled the batch back for sure. It counts as the handler's
  failure and follows the `RetryPolicy`, so a batch that can never commit stops the run with
  `HandlerFailed` once its attempts run out. It is transient, since most such errors are not the batch's
  fault and pass (a lock or statement timeout, say behind a lost COMMIT's row lock, a serialization failure,
  a read-only transaction during a failover), except an integrity constraint violation or a data exception
  (SQLSTATE class 23 or 22, such as a deferred unique constraint), which fails every attempt the same way
  and is fatal. A lost connection or a FATAL error may hide a commit, and stays a database error. Any
  isolation level works: nothing updates the subscription's row during the handler, so a serialization
  failure comes only from the handler's own tables or the COMMIT. An error the row lock gets before the
  handler runs (40001 under REPEATABLE READ behind a renewal still in flight, a lock timeout) is classified
  the same way and counts as one of the batch's attempts, although the handler was not called. A backfill
  lane's lock that fails with 40001 (it waited for another run that moved the lane) is `Overtaken`, as at
  READ COMMITTED.
- Category names are checked against `categories` at start (`UnknownCategory`). The requested categories are
  compared with `subscriptions.category_ids`; a mismatch fails with
  `SubscribeError::DefinitionChanged { stored, requested }` unless `.redefine(Start::…)` is given. A changed
  number of shards fails with `ShardsChanged { stored, requested }`, checked after `DefinitionChanged`, so a
  changed category set is reported first. A redefine moves every shard's cursor to the start and ends the
  instance's term; it then takes a lease again.
- `.all_categories()`: every category of the store in position order, including categories registered after
  start (schema.md, All categories). It cannot be combined with listed categories (it does not compile); a
  builder with neither has no `run`. A cursor behind any category's horizon fails with
  `CursorBeforeHorizon`. A deleted stream's log rows are skipped; a log row whose commit is missing for
  any other reason, with no horizon past the cursor, fails with `Corrupt`. Wakers get an empty
  `known.categories()`, which means the whole store.
- `.sharded(n)`: `n` shards (1 to 1024) by the stream id's hash (schema.md, Sharded reads), each with its own
  cursor, lease and fence. Order holds within a stream only; a handler sees no order between streams of
  different shards. Instances share the shards by themselves: each takes at most `ceil(n / live instances)`,
  so with more instances than that needs, some hold none and stand by (4 shards over 3 instances is 2+2+0;
  pick `n` as a multiple of the instance count to use every instance). One instance calls its one handler for
  one shard at a time; run more instances for parallel handlers. `Status.position` of a sharded instance is
  the lowest cursor of its held shards, and can step back when it takes a shard that is behind.
- `.replica(pool)`: pages are read from a streaming replica after it has replayed the waker's LSN, or the
  primary's insert LSN when the waker gives none (schema.md, Replica reads). The waker's LSN is untrusted:
  one the replica has not replayed is lowered to the primary's insert LSN before the wait. A shard that was
  busy through more than one wake waits for the primary's insert LSN, not the latest wake's. Checkpoints, leases, `run_pg`
  transactions, horizon checks and event-type loads stay on the store's pool. It waits however long the
  replica lags, and a pool whose server is not in recovery is read without waiting. With
  `Replica::fallback(after, limit)` it reads one page at a time from the primary once the replica has lagged
  longer than `after`, while a permit of `limit` is free; the permit is held only for that read. Give every
  subscription of a process a clone of one `FallbackLimit`. There is no process-wide limit.
- `.replica(pool)` composes with `.all_categories()` and `.sharded(n)`. Every shard waits on its own, and
  all shards take fallback permits from the same `FallbackLimit`, so a lagging replica can take up to `n`
  permits per instance.
- Leases: takeover with a fence bump, renewal only when less than half the lease is left: piggybacked on a
  checkpoint where possible, otherwise in autocommit on another connection of the store's pool, including
  while a `run` handler runs and while a failed batch waits for its retry. A `run_pg` batch holds the
  subscription's row locked from before its handler to its COMMIT; acquire skips locked rows, so the lock
  is the lease meanwhile, however long the batch takes, and the checkpoint renews the stored lease. A `run`
  batch slower than the lease keeps it, provided the pool has a connection to spare. A side renewal that
  finds the fence moved stops a `run` batch from being checkpointed once the handler's call returns; the
  handler is not cancelled, and the instance goes back to standby. If the handler also failed in that call,
  the lost lease wins over its error, fatal or transient. A `run_pg` batch whose lock finds the fence moved
  runs no handler and stands by. A failed side renewal is retried after a lease/8 and is not shown as
  `Recovering`; the fenced checkpoint keeps a lapsed lease safe. A backoff whose renewal found the fence
  moved still runs to its end before the instance stands by. Release on shutdown and on a fatal error, so a
  standby takes over at once. A `run_pg` holder that dies releases its lock with its connection: at once
  when the process exits, and for a vanished host once the server finds the connection dead. The batch's
  transaction sets `tcp_keepalives_idle`, `_interval` and `_count` from the lease (`set_config(…, true)`,
  local to the transaction), which bounds that to about one lease over TCP (at least 4 s, since keepalives
  count whole seconds); the server's defaults take about 2 h 11 min. Over a Unix socket they do nothing. A
  backfill's lane lock sets the same as for a 15 s lease. Not `tcp_user_timeout`: on Linux it also bounds a full receive window, so it dropped a healthy handler that read a large result slower than the lease, on every attempt. A holder
  that hangs in its handler, connected, keeps the shard, as a side renewal would have kept it;
  `idle_in_transaction_session_timeout` on the role bounds a holder stuck between statements, but ends any
  handler that pauses longer (an external call, a slow computation) with a FATAL error, so set it above the
  longest such pause.
- Pool size: `run_pg` holds one connection for the handler's transaction, which is all it needs; `run`
  needs one; a waker adds `Waker::store_connections()` (1 for a `NotifyWaker` on the store's pool). A subscription whose store's pool allows fewer fails at start with
  `SubscribeError::PoolTooSmall { max_connections, needed }`. Other users of the pool need their own
  connections on top. Size the pool for the sum: appends' and reads' concurrency plus each subscription's
  share (documented on `Builder`, Pool size). A sharded instance reads its shards at the same time: add one
  connection per shard it may hold. The start check ignores shards, because an instance may only ever hold
  a few. With a replica, a sharded instance holds one replica-pool connection per held shard while reading,
  and a fallback read briefly borrows a store connection.
- `HandlerError::Transient` is retried per `RetryPolicy`; `Fatal`, or running out of attempts, stops the
  subscription with `HandlerFailed { first, last }`. Nothing of that batch is checkpointed. A dead-letter
  table is out of scope for v1.
- A cursor behind a category's retention horizon fails with `CursorBeforeHorizon`; the check is part of
  every read (schema.md). `Start::Beginning` and `Start::Position(p)` keep that check from the first read: a
  subscription resumed at a position must not skip events after it.
- `Start::Retained` starts a new read model in a store with retention: it delivers every event retention
  kept and skips the ones it dropped. The subscription stores the newest position the store had assigned at
  creation (`skip_through`: the head, or the highest horizon if `prune_log` deleted the newest log row); a
  category whose horizon is at or below it is read after its start position, and its
  missing commits are skipped. A category whose horizon rises above it (retention dropped events written
  after the subscription was created) is checked against its horizon again. `Start::Position(horizon)` is
  not a substitute: a kept commit can be positioned below the horizon. After `CursorBeforeHorizon`, rebuild
  under a new name (or `.redefine(Start::Retained)` with a changed definition) from `Start::Retained`.
  `Start::Beginning` does not skip: it fails with `CursorBeforeHorizon` on a category whose retention
  dropped events.
- Limit of `Start::Retained`: until a store-wide subscription's cursor passes `skip_through`, a commit
  missing at or below the horizon of a skipped category is taken as dropped without consulting
  `deleted_commits`, so a commit missing there for another reason (deleted by hand) is skipped, not
  reported as `Corrupt` (design.md, Decided, Retention start position).
- Without a waker, a caught-up subscription uses `waker::recheck::Recheck::default()`: it reads the log again
  every 500 ms. A plain timer, which works with no features; a custom interval is
  `.waker(waker::recheck::Recheck::every(interval!(..)))`.
- Whatever the waker, a caught-up subscription reads the log again after `max_idle` (default 30 s) without
  a wake-up, and each shard of a sharded one does on its own. A waker can stay quiet without failing (a
  `NotifyWaker` on a half-open connection), while its holder keeps renewing the lease, so no standby would
  take over. The backstop bounds the delay to `max_idle`, at one log read per `max_idle` while nothing is
  appended. (2026-11-27)

| Error | Variants |
|---|---|
| `subscription::SubscribeError` | `PoolTooSmall { max_connections, needed }`, `UnknownCategory(CategoryName)`, `DefinitionChanged { stored, requested }`, `ShardsChanged { stored, requested }`, `CursorBeforeHorizon { category, cursor, horizon }`, `HandlerFailed { first, last, source }`, `UnknownEventType { position, category, type_id }`, `Frame { position, source }`, `Corrupt { position, reason }`, `CrateTooOld { scope, min_compatible, crate_version }`, `Wake(WakeError)`, `Database(DbError)`, `Aborted` |
| `waker::WakeError` | `Closed`, `Database(DbError)`, `Other(BoxError)` |
| `waker::grpc::TlsError` (`pges_core::tls::TlsError`) | `NoCertificate`, `UnreadableCertificate { reason }`, `InvalidCertificate { reason }`, `NoPrivateKey`, `UnreadablePrivateKey { reason }`, `MultiplePrivateKeys { count }`, `KeyMismatch`, `NoCaCertificate`, `UnreadableCaCertificate { reason }`, `InvalidCaCertificate { reason }`, `Rejected { reason }` (Core types) |
| `backfill::BackfillError` | `PoolTooSmall { max_connections, needed }`, `UnknownCategory(CategoryName)`, `UnknownSubscription(SubscriptionName)`, `DefinitionChanged { stored, requested: Definition }`, `UntilAhead { until, head }`, `Overtaken`, `HandlerFailed { first, last, source }`, `UnknownEventType { position, category, type_id }`, `Frame { position, source }`, `Corrupt { position, reason }`, `Database(DbError)` |

`SubscribeError::is_retried()` is true for `Database` and `Wake`: those are retried every `retry_interval`
and shown as `State::Recovering`, including errors while redefining (the lease is released and taken
again). Every other error releases the lease and ends the run. `Aborted` means the task panicked or was
cancelled. The status becomes `State::Stopped` when the run returns, and also when its future is dropped
before that (a panicking handler, an aborted task); a dropped run keeps its lease until it lapses.

### Backfills

A backfill replays some categories once, up to a position `P`, to fill a new read-model field from older
events (design.md, Changing a read model):

```rust
use pges_subscribe::{backfill, lanes, subscription::StoreExt, subscription_name};

let finished = store
    .backfill(                                // its own namespace; same name rule
        subscription_name!("jobs-board.v3.assignee"),
        backfill::Until::Subscription(subscription_name!("jobs-board.v3")),   // required; or Position(p), Head
    )
    .categories(non_empty![category!("jobs")])   // or .category::<E>(), .categories_of::<(A, B)>()
    .parallelism(lanes!(16))                  // lanes by stream; default 1; order within a stream is kept
    .batch_size(const { NonZeroU32::new(1000).unwrap() })   // commits per category per page; default 1000
    .on_unknown(OnUnknown::Fail)              // default Fail
    .on_error(subscription::RetryPolicy::default())
    .run_pg(AssigneeBackfill)                 // H: PgHandler + Clone; one clone per lane
    .await?;                                  // backfill::Finished { until, events }

#[non_exhaustive] pub enum Until { Position(Position), Subscription(SubscriptionName), Head }
                                          // From<Position>, From<SubscriptionName>
#[non_exhaustive] pub struct Definition { pub categories: Vec<CategoryId>, pub parallelism: Lanes,
                                          pub until: Position }
pub struct Lanes(NonZeroU16);             // 1..=32767; ONE, MAX, const fn new -> Option, get; lanes!(n)
```

- The builder tracks its scope as a subscription's does: `run_pg` exists only once categories are listed, and
  a backfill has no `all_categories`. Leaving out `until` or the categories does not compile.

- It reads the log once, in position order, each page bounded by `least(H, P)`, and splits it into lanes by
  stream (schema.md, Backfill). Lanes run at the same time; each lane's batch is handled in a transaction
  that also moves the lane's cursor, so every event is handled exactly once.
- `P` is resolved and stored on the first run. `Until::Subscription` is the subscription's cursor; for a
  sharded subscription, the lowest cursor of its shards. `UnknownSubscription` if it does not exist,
  `UntilAhead` if `P` is past the store's newest position. A `P` inside a commit stops before that commit.
- A stopped run (an error, a crash, a dropped future) resumes when run again. Transient handler errors are
  retried per `RetryPolicy`; database errors end the run. A later run with other categories, parallelism or
  `Until::Position` fails with `DefinitionChanged`; use a new name to start over.
- Run one instance at a time: a second run that finds a lane moved stops with `Overtaken`. Two first runs
  at once with different parallelism can mix lanes, which later runs report as `Corrupt`.
- The handler guards its writes, so a live projection and the backfill can run in any order:
  `WHERE <field>_pos IS NULL OR <field>_pos < $pos`. The `backfill` example shows it.
- Pool size: one store connection per lane (`PoolTooSmall` otherwise). Backfills read from the primary,
  unsharded.
- It reads each category from its retention start position: events retention dropped are skipped, so a
  backfill never fails on a horizon (a projection that needs them starts from snapshots), and events it kept
  below the horizon are read. If retention raises a start during a read, the page is read again. There is no API to delete a finished backfill's rows yet.

`pges-subscribe` reads `categories` and `event_types` itself and interns category names (leaked once per
process), because `pges-postgres` keeps its catalog private.

## Sequencer

```rust
use pges_sequencer::{outbox, sequencer};

// `store` is the pges_postgres::store::Store the sequencer serves.

let source = outbox::OutboxSource::new(pool.clone(), outbox::OutboxConfig::default());
let config = sequencer::SequencerConfig::default();
let sequencer = sequencer::Sequencer::new(pool, source, config);   // cannot fail
let watch = sequencer.watch();                       // take it before run
let mut high_water = watch.high_water(store.store_id());          // tokio::sync::watch::Receiver<HighWater>
let mut status = watch.status();                     // tokio::sync::watch::Receiver<sequencer::Status>
sequencer.run(shutdown).await?;                      // shutdown: impl Future<Output = ()> + Send;
                                                     // Err(Arc<RunError>)
```

- One sequencer leads per database and serves every store in it; others stand by (schema.md, Sequencer
  lease). A database is sequenced by one kind of source: store creation refuses to mix outbox and other
  stores (`MigrateError::OutboxMismatch`).
- `run` takes a shutdown future, not a cancellation token type (pass `token.cancelled_owned()` for
  tokio-util's). On shutdown a leader finishes its batch, stores its token, closes the source and releases
  the lease, so a standby takes over within `acquire_interval`. Shutdown is also watched while the source
  opens. After a crash, a standby waits for the lease to expire.
- Database errors and `SourceError::Database` / `Transient` while leading or standing by are retried every
  `retry_interval` and shown as `Status::Recovering`; they do not end `run`. Every other `RunError` ends it.
- `SourceError::Halt` stops `run` like `Fatal`, but first writes the error text to `pges.sequencer.halted`
  (under the term's fence), then closes the source and releases the lease. Every later acquire then stops
  with `RunError::Halted`.
- Whenever `run` returns an error, the status becomes `Status::Halted(error)` with the same error: `run`
  returns `Arc<RunError>`, since `RunError` cannot be cloned. After a shutdown it becomes `Stopped`. When
  `run`'s future is dropped before it returns (a panic, an aborted task), it becomes `Abandoned`: the lease
  is not released, so a standby takes over only once it expires. Alert on `Halted` and `Abandoned`.
- `Status::Leading` is announced once the term's source is open and the stores are loaded, so a logical
  source's slot exists when a caller sees `Leading`.
- The pool needs one connection for the leader, plus whatever the source uses (the logical source reads
  `pg_replication_slots`, `pges.stores` and the publication through it), plus one for NOTIFY with
  `watch-notify`.

```rust
#[non_exhaustive]
pub struct SequencerConfig {                 // Default; no builder
    pub batch_size: NonZeroUsize,            // 1000 commits per batch transaction
    pub lease: Lease,                        // Lease::DEFAULT: 10 s, renewed every 5 s
    pub acquire_interval: Interval,          // 1 s; a standby's acquire attempts
    pub token_interval: TokenInterval,       // Every(1 s); coarse token and high-water marks
    pub retry_interval: Interval,            // 1 s; pause after an error
    pub dedupe_tail: usize,                  // 1024 older commit ids per store in the dedupe window
    pub leader: Option<String>,              // announced in pges.sequencer.leader; default none
    #[cfg(feature = "watch-notify")]
    pub notify: Notify,                      // Notify::Enabled; NOTIFY pges_<store> after each batch that
                                             // wrote rows
}

#[non_exhaustive]
pub enum TokenInterval {                     // sequencer::TokenInterval; Copy, Eq, Default Every(1 s)
    EveryBatch,                              // store the token with every batch
    Every(Interval),                         // once this long has passed, in a batch or alone when idle
}

#[cfg(feature = "watch-notify")]
#[non_exhaustive]
pub enum Notify { Enabled, Disabled }        // sequencer::Notify

pub struct Lease { /* ttl: Interval, renew: Duration */ }   // sequencer::Lease; Copy, Eq, Default
impl Lease {
    pub const DEFAULT: Self;                                 // 10 s, renewed every 5 s
    pub const fn new(ttl: Interval) -> Self;                 // renewed every half of ttl
    pub const fn with_renew(ttl: Interval, renew: Interval) -> Option<Self>;   // None unless renew < ttl
    pub const fn ttl(self) -> Interval;
    pub const fn renew(self) -> Duration;
}

#[non_exhaustive]
pub struct OutboxConfig {                    // Default
    pub recheck_interval: Interval,          // 1 s: fallback probe on the listening connection after this
                                             // long without a notification; also replaces a listener that
                                             // stopped answering; not the live path
    pub store_refresh: Interval,             // 10 s between reads of the outbox store list
}

#[non_exhaustive]
pub enum Status { Standby, Leading(Fence), Recovering(Arc<RunError>), Halted(Arc<RunError>), Stopped,
                  Abandoned }
                                             // Halted: run returned this error;
                                             // Stopped: run returned after a shutdown;
                                             // Abandoned: its future was dropped before it returned;
                                             // the lease is kept until it expires
pub struct Fence(u64);                       // pges.sequencer.fence of the term; Fence::get

#[non_exhaustive]
pub enum RunError {
    Database(DbError),                       // retried
    Source(SourceError),                     // retried for Database and Transient, otherwise ends run
    Halted(String),                          // pges.sequencer.halted is set; stop the sequencers, then
                                             // run pges-sequencer resequence
    SourceMismatch { stored: String, configured: SourceKind },     // a non-empty token of another kind
    UnknownStore(StoreId),                   // a source named a store pges.stores does not list
    InvalidStoreName(String),                // pges.stores lists a schema name that is not a store name
    PositionOverflow(StoreId),               // positions would pass i64::MAX
    CrateTooOld { scope: String, min_compatible: SchemaVersion, crate_version: SchemaVersion },
                                             // a migration dropped support for this release; found
                                             // at the next lease renewal or acquire (Versioning)
}
```

Every config value is valid by construction, so `Sequencer::new` cannot fail and there is no `validate`.

`Watch` is cheap to clone and hands out `tokio::sync::watch` receivers. A store's channel is created on
demand and holds `HighWater::default()` (all zero, `lsn` None) until a leader announces the store, which it
does for every store at term start and after each batch that wrote to the store's log. The values are
level-triggered hints: a receiver sees the newest value, not every value. The NOTIFY notifier
(`watch-notify`, Wake-ups) and the gRPC watch server (`watch-grpc`, below) send the same values.

A store's channel is created for any store id asked for, including through the gRPC watch. The channel of a
store no leader announced (a store with no events yet, or no store at all) lasts only while someone holds a
receiver of it; such channels are dropped the next time a new unannounced id is asked for. So asking for
made-up store ids does not grow the watch: channels are bounded by the announced stores plus live receivers,
and through the gRPC watch by `WatchConfig::max_streams`. A channel a leader announced stays.

### gRPC watch server

```rust
use pges_sequencer::grpc;                    // watch-grpc

let service = grpc::WatchService::new(sequencer.watch());
let listener = tokio::net::TcpListener::bind("0.0.0.0:50051").await?;
service.serve(listener, shutdown).await?;    // shutdown: impl Future<Output = ()> + Send; grpc::ServeError
// or on several listeners at once (one per address a host name resolves to):
service.serve_all(listeners, shutdown).await?;   // listeners: NonEmpty<tokio::net::TcpListener>
// or, on a tonic server of your own:
let server = service.into_server();          // pges_watch_proto::v1::watch_server::WatchServer<WatchService>

let mut config = grpc::WatchConfig::default();
config.max_streams = NonZeroUsize::new(2000).unwrap();
config.tls = Some(grpc::ServerTls::new(cert_pem, key_pem)?.client_ca(clients_ca_pem)?);   // watch-grpc-tls
let service = grpc::WatchService::with_config(sequencer.watch(), config);

#[non_exhaustive]
pub struct WatchConfig {                     // pges_sequencer::grpc; Default
    pub max_streams: NonZeroUsize,           // 10 000: Watch streams open at once, over all connections
    pub tls: Option<ServerTls>,              // watch-grpc-tls; None: plain HTTP/2; used by serve and serve_all
}
impl ServerTls {                             // watch-grpc-tls; Clone, Debug; all PEM
    pub fn new(cert: impl AsRef<[u8]>, key: impl AsRef<[u8]>) -> Result<Self, TlsError>;
    pub fn client_ca(self, ca: impl AsRef<[u8]>) -> Result<Self, TlsError>;   // mutual TLS
}
pub use pges_core::tls::TlsError;            // watch-grpc-tls; Core types
```

`WatchService` serves `pges.watch.v1.Watch` (Watch protocol, below) from the in-process `Watch`.

- `Watch` answers `UNAVAILABLE` ("this sequencer is not leading: standing by", "recovering", "halted",
  "stopped" or "abandoned") unless the status is `Leading(fence)`. A client given every sequencer's address
  finds the leader by trying each.
- A stream sends the current value at once, then each change. Each value carries the term's fence. If the
  fence changes within one stream, the current value is sent again with the new fence.
- An open stream ends with `UNAVAILABLE` when the status becomes `Standby`, `Halted`, `Stopped` or
  `Abandoned`, and when `serve`'s shutdown resolves. `Recovering` keeps it open: the
  sequencer is still in its term if it renews the lease in time, and says `Standby` if not.
- A store the leader has not announced streams all zeros, as the in-process `Watch` does; it is not
  `NOT_FOUND`, so a new store with no events yet is not refused.
- `Status` returns the sequencer's state and, while leading, the fence.
- `serve` (and `serve_all`) stops accepting connections on shutdown, ends every open stream itself and
  returns once the connections have closed (tonic's graceful shutdown alone would wait for open streams
  forever). HTTP/2 keepalive pings every 10 s close connections whose peer has not answered within 20 s.
  `ServeError` means the server could not accept connections.
- `into_server()` gives the service for a tonic server of the caller's own. Its streams end when the
  sequencer stops leading; that server's own shutdown waits for them, so stop the sequencer first or bound
  the shutdown.
- Stop the server after `run` returns, so streams end with the last values. A dropped `run` also ends them.
- A leader whose sequencing hangs (on a database call, say) still says `Leading`, but once the term's lease
  passes without renewal every open stream ends with `UNAVAILABLE` ("this sequencer's lease expired without
  renewal: it is stalled, and another may lead"), and new `Watch` calls are refused the same way while it
  stays expired. The deadline is the sequencer's local one (`sent + ttl`, taken before each renew query is
  sent), which falls before the database lease expires, so streams end before another sequencer can take
  over. `Status` still answers `Leading` for such a sequencer.
- At most `WatchConfig::max_streams` (default 10 000) `Watch` streams are open at once, over all connections;
  one more is refused with `RESOURCE_EXHAUSTED`, which `GrpcWaker` retries like `UNAVAILABLE`. An ended
  stream gives its place back. Each subscription with a gRPC waker holds one stream (one per shard
  instance). There is no per-connection or per-client limit.
- TLS (`watch-grpc-tls`; rustls with ring): `ServerTls::new(cert, key)` serves with a certificate chain and
  its key; `.client_ca(ca)` also requires every client to present a certificate that CA signed (mutual TLS).
  Both check the PEM when called, so a file with no certificate, an unreadable one, more than one key, a CA
  certificate that cannot be a trust anchor or a key that does not match fails with a `TlsError` variant
  there, not when serving (tonic itself accepts a PEM with no certificate, takes the first of several keys
  and skips unusable CA certificates). The
  TLS handshake must finish within 10 s. TLS applies to `serve` and `serve_all` only; an `into_server` user
  sets up TLS on their own server.

### Sequencer binary

`pges-sequencer` (feature `bin`) runs one sequencer for one database. Every flag can also be set through an
environment variable:

| Flag | Variable | Default |
|---|---|---|
| `--database-url` | `PGES_DATABASE_URL` | required |
| `--source logical\|outbox` | `PGES_SOURCE` | `logical` with `source-logical`, else `outbox` |
| `--replication-url` (`source-logical`; logical only) | `PGES_REPLICATION_URL` | the database URL |
| `--slot` (`source-logical`; logical only) | `PGES_SLOT` | `pges_<oid of the database>` |
| `--failover` (`source-logical`; logical only) | `PGES_FAILOVER=on` | not set: an existing slot keeps its setting |
| `--no-failover` (`source-logical`; logical only) | `PGES_FAILOVER=off` | not set |
| `--leader` | `PGES_LEADER` | none |
| `--batch-size` | `PGES_BATCH_SIZE` | 1000 |
| `--lease-ttl`, `--renew-interval`, `--acquire-interval` | `PGES_LEASE_TTL`, `PGES_RENEW_INTERVAL`, `PGES_ACQUIRE_INTERVAL` | `10s`, half the lease, `1s` |
| `--token-interval`, `--retry-interval` | `PGES_TOKEN_INTERVAL`, `PGES_RETRY_INTERVAL` | `1s`, `1s` |
| `--recheck-interval` (outbox only) | `PGES_RECHECK_INTERVAL` | `1s` |
| `--notify on\|off` (`watch-notify`) | `PGES_NOTIFY` | `on` |
| `--grpc-listen` (`watch-grpc`) | `PGES_GRPC_LISTEN` | none: no gRPC server |
| `--grpc-max-streams` (`watch-grpc`) | `PGES_GRPC_MAX_STREAMS` | 10000 |
| `--grpc-tls-cert`, `--grpc-tls-key` (`watch-grpc-tls`) | `PGES_GRPC_TLS_CERT`, `PGES_GRPC_TLS_KEY` | none: plain HTTP/2 |
| `--grpc-tls-client-ca` (`watch-grpc-tls`) | `PGES_GRPC_TLS_CLIENT_CA` | none: no client certificate required |
| `--max-connections` | `PGES_MAX_CONNECTIONS` | 4; at least 2 (exit 2 below) |

- Durations take a unit: `ms`, `s` or `m`, whole numbers only (`250ms`, `10s`, `1m`). Each is at most
  30 days; every one except `--token-interval` is also at least 1 ms, and `--token-interval` accepts `0s`.
  `--renew-interval` must be shorter than `--lease-ttl`.
- Every option is checked before anything connects; an invalid one exits 2 with the usage. This covers a
  duration out of range (including `--token-interval` over 30 days), a slot name PostgreSQL would refuse,
  `--max-connections` below 2, a renew interval not shorter than the lease, an invalid `PGES_FAILOVER`, a
  `--grpc-listen` that is neither a socket address nor a host and port, `--grpc-max-streams` or a TLS option
  without `--grpc-listen`, `--grpc-tls-cert` without `--grpc-tls-key` (or the reverse, or
  `--grpc-tls-client-ca` without them), a TLS file that cannot be read or holds no usable certificate or
  key, and an option of the other source. Under `--source outbox` that is `--replication-url`, `--slot`,
  `--failover`, `--no-failover`, their variables, or a non-empty `PGES_FAILOVER`; under `--source logical`
  it is `--recheck-interval` / `PGES_RECHECK_INTERVAL`. A deployment that shares one environment file
  between sources must drop the other source's variables.
- Logs go to stderr, filtered by `RUST_LOG` (default `info`); every status change is logged.
- SIGINT or SIGTERM: finish the batch, store the token, release the lease, exit 0. A `RunError` exits 1
  (status `Halted`; the status logger skips it, so the error is logged once).
- A second SIGINT or SIGTERM once shutdown has begun (after the first signal, or after the gRPC server
  failed) exits at once with 128 plus the signal's number: 130 for SIGINT, 143 for SIGTERM. It logs the
  forced shutdown as an error and does not wait for the batch, the token or the release, so the lease may be
  left to expire; a statement already sent may still complete on the server. Use it when shutdown hangs, for
  example releasing the lease while the database is unreachable (sqlx waits up to its 30 s acquire timeout).
- A panic in the run exits with Rust's status 101 and leaves the status `Abandoned`; the lease stays until
  it expires. The binary logs `Abandoned` as an error, but that line may not be written while the panic
  unwinds, so watch the exit status. This relies on `panic = "unwind"` (design.md, Decided, gRPC watch).
- `pges-sequencer resequence [--database-url, --replication-url, --slot, --failover | --no-failover,
  --writer-role, --lease-ttl]` (`source-logical`) recovers a halted database
  (`LogicalSource::resequence`, schema.md, Resequence). Stop every sequencer first and start them again
  afterwards. `--writer-role` (repeatable, or `PGES_WRITER_ROLES`, comma-separated; required) fills
  `ResequenceConfig::writer_roles`, and `--lease-ttl` (`PGES_LEASE_TTL`, `10s`, renewed every half) fills
  `ResequenceConfig::lease`. The search window comes from each
  store's recorded writer limits. It uses
  a pool of 3 connections, logs each store's recovered commits and the new slot's consistent point, and
  exits 0 when it recovered, or 1 when it refused or failed.
- `pges-sequencer bootstrap [--database-url, --replication-url, --slot, --failover | --no-failover]`
  (`source-logical`)
  creates the publication and the slot if they are missing (`LogicalSource::bootstrap`, schema.md, Slot
  bootstrap). It exits 0, logging `created the replication slot`, `the replication slot exists` or
  `changed the replication slot's failover setting`, or 1 on an error. Run it after the migrations and before
  writers start. Run options cannot be combined with the subcommand.
- `--failover` creates the slot with `FAILOVER` (PostgreSQL 17+), so standbys with `sync_replication_slots`
  on keep a copy. `--no-failover` creates it without. A run applies either flag only to a slot it creates;
  `bootstrap` also ALTERs an existing slot to it, and fails while a sequencer streams from the slot. Without
  either flag, an existing slot keeps its setting and a new slot is created without `FAILOVER`
  (`logical::SlotFailover::Keep`).
- The two flags exclude each other (exit 2), and either overrides `PGES_FAILOVER`. `PGES_FAILOVER` takes only
  `on` or `off`; empty counts as unset. Another value exits 2, and so does any value under
  `--source outbox`.
- `bin` does not turn on `watch-notify`; build with both to get `--notify`.
- `--grpc-listen <addr>` serves the gRPC watch on a socket address (`0.0.0.0:50051`) or a host name and port
  (`localhost:50051`). Build with `bin` and `watch-grpc`. A host name is resolved at start, and the watch is
  served on every address it resolves to (deduplicated). An address the host does not have
  (`AddrNotAvailable`, for example `::1` without IPv6) is skipped with a warning; any other bind error, or
  no address bound, fails the start (exit 1). With port 0 and a name with several addresses, each address
  gets a different port. The startup log line for each address carries `tls=`. The server is stopped after
  `run` returns, so open streams end first. If the server fails, the binary shuts the sequencer down and
  exits 1.
- `--grpc-max-streams` sets `WatchConfig::max_streams`. `--grpc-tls-cert` and `--grpc-tls-key` (build with
  `watch-grpc-tls`) serve over TLS; `--grpc-tls-client-ca` also requires client certificates signed by that
  CA. The files are read and checked before anything connects.

## Sequencer sources

How the sequencer learns which commits exist, in an order that respects causality. One trait:

```rust
pub trait Source: Send {
    /// Stored in pges.sequencer.source; a token of another kind is never passed to open.
    fn kind(&self) -> SourceKind;             // source_kind!("…"), checked at compile time
    /// Starts or restarts delivery after the durable token (None: wherever the source starts). The
    /// sequencer renews its lease while this runs, drops it on shutdown or a lost lease, then calls close.
    fn open(&mut self, token: Option<&[u8]>) -> impl Future<Output = Result<(), SourceError>> + Send;
    /// Resolves when next may return commits. Outside any transaction; cancel safe.
    fn ready(&mut self) -> impl Future<Output = Result<(), SourceError>> + Send;
    /// Inside the fenced batch transaction; changes through tx commit or roll back with the log rows.
    fn next(&mut self, tx: &mut PgConnection, limit: NonZeroUsize)
        -> impl Future<Output = Result<SourceBatch, SourceError>> + Send;
    /// After the token and every log row it covers are durable.
    fn ack(&mut self, token: &[u8]) -> impl Future<Output = Result<(), SourceError>> + Send;
    /// While the sequencer writes a batch (after next), concurrently with its database work; dropped when
    /// the batch transaction ends. An error aborts the batch. Cancel safe. Default: never resolves.
    fn keep_alive(&mut self) -> impl Future<Output = Result<(), SourceError>> + Send {
        std::future::pending()
    }
    /// When the term ends (lease lost, shutdown, fatal error), before the lease is released. Cannot fail;
    /// should finish within about a second. Default: does nothing.
    fn close(&mut self) -> impl Future<Output = ()> + Send {
        std::future::ready(())
    }
}

pub struct SourceKind(&'static str);         // pges_sequencer::source; ^[a-z][a-z0-9_]{0,39}$; MAX_LEN = 40;
                                             // const fn new -> Option, as_str, Display, AsRef<str>
pges_sequencer::source_kind!("fake")         // invalid literal: compile error

#[non_exhaustive]
pub struct SourceBatch { pub commits: Vec<CommitRef>, pub token: Vec<u8> }   // SourceBatch::new

#[non_exhaustive]
pub struct CommitRef {                                                       // CommitRef::new
    pub store: store::StoreId,
    pub commit_id: CommitId,
    pub category: category::CategoryId,
    pub event_count: commit::EventCount,                                     // 1 to 32767
}

#[non_exhaustive]
pub enum SourceError {
    Database(DbError),
    Transient(BoxError),
    Fatal(BoxError),
    Halt(BoxError),                       // must persist pges.sequencer.halted (a lost slot)
    Invalid(String),
}
```

- The outbox must drain `pending` in the same transaction as the log rows, so `next` gets the batch
  transaction, after the fence check. `next` returns at most `limit` commits, except to keep one source
  transaction whole; an empty batch is allowed.
- A logical source reads its stream in `ready` into a buffer of whole transactions, and `next` returns from
  that buffer without touching `tx`. It keeps its replication connection alive through `keep_alive` while
  the batch runs.
- If a batch transaction does not commit, the sequencer calls `open` again with the durable token.
- `close` is called on every term end, not on a recoverable error inside a term: the next `open` resets the
  source anyway. It runs after the token is stored and before the lease is released.
- `ack` is called only with tokens that were persisted (the coarse token), not after every batch.
- `Database` and `Transient` errors are retried after reopening the source; `Fatal`, `Halt` and `Invalid`
  stop the sequencer. `Halt` is for failures after which sequencing must not resume by itself (a lost slot):
  the sequencer records it in `pges.sequencer.halted`. `Invalid` is for malformed rows, such as a stored
  event count outside 1 to 32767: a source parses the count into an `EventCount` where it reads it, so the
  log writer does not check it again.

`Source` and `Waker` use `impl Future` returns, so they are not dyn-compatible. Each has an object-safe twin,
`DynSource` (with `keep_alive` and `close` too) and `DynWaker`, whose methods return `BoxFuture<'_, …>`,
with a blanket impl for every `T: Source` or `T: Waker`. `Box<dyn DynSource>` implements `Source` (and
`Box<dyn DynWaker>` implements `Waker`), so configuration can pick a source or waker at run time.

Contract for any source, built in or custom:

- Every committed append is delivered at least once; rolled-back appends never are.
- If append B could have seen append A's commit, A is delivered before B.
- Resuming from a stored token replays nothing already covered by it. The logical source also drops any
  transaction that ends at or below the stored token. Replays that remain (Kafka after a crash between
  produce and commit, or the coarse token in schema.md, Sequencer batch) are deduplicated by the sequencer
  with a window of recent commit ids. The window holds, per store, the commit ids sequenced since the last
  durable token, plus `SequencerConfig::dedupe_tail` (default 1024) older ones. It is seeded whenever a term
  starts or restarts after an error, for each store, from
  `SELECT commit_id FROM <store>.log ORDER BY last_position DESC LIMIT $1` (`$1` = `dedupe_tail`) and
  `SELECT commit_id FROM <store>.log WHERE last_position > $1 ORDER BY last_position`
  (`$1` = `pges.stores.last_position`). After each persisted token it is trimmed back to the newest
  `dedupe_tail` ids of each store, except the log rows above that mark, which stay until the source has
  replayed them (schema.md, Sequencer batch). A source that replays further back than its token needs
  `dedupe_tail` at least as large as that span.
- A replay delivers the commits it repeats in the order they were delivered before, and before any commit
  not delivered before. The built-in sources do (the logical source replays in WAL commit order; the outbox
  source never replays). The sequencer relies on it to keep the rows above the mark deduplicated: once a
  commit not delivered before arrives, or a replayed row arrives, the rows delivered before it count as
  replayed. A source that breaks this order is deduplicated only by `dedupe_tail`.

| Source | Feature | How | Needs | Trade-offs |
|---|---|---|---|---|
| Logical replication (`logical::LogicalSource`, built) | `source-logical` (default) | pgoutput from slot `pges_<database oid>` via `pges-replication`, `streaming = off`; Inserts are routed by root relation id to the store named by `Relation.namespace`, and `commits` columns are looked up by name; token = 8 bytes, big-endian end LSN of the last transaction in the batch (empty: none); `open` drops transactions ending at or below it, and halts with `SlotLost::Ahead` if the slot is confirmed past it; `ack` confirms only persisted tokens, and nothing else confirms the slot; idle keepalives (and transactions with nothing to sequence) give an empty batch whose token is `Keepalive.wal_end`, at most once per `status_interval`, once nothing is buffered and no transaction is open; status updates at least every `status_interval`, also during batches (`keep_alive`); outbox stores' inserts are skipped | `wal_level = logical`, REPLICATION role; `pges-replication/tls` for TLS, which `source-logical` turns on | true commit order; slot WAL retention must be watched, and slot loss halts until `pges-sequencer resequence` (design.md); a new slot is checked in the snapshot exported at its consistent point; if a store already has commits it is dropped and sequencing halts (`SlotLost::CommitsBeforeSlot`): run `pges-sequencer bootstrap` before writers start |
| Outbox queue (`outbox::OutboxSource`, built) | none (always built) | appends insert into `pending`; the source drains it with `DELETE … RETURNING` inside the batch transaction and sorts the returned rows by `seq` (schema.md); woken by `NOTIFY pges__outbox` from a trigger on `pending`; drains until short; fallback probe on the listening connection every `recheck_interval` (1 s) without a notification; `close` stops listening, so a standby holds no connection; empty token (presence is the state) | nothing beyond Postgres | transactional outbox; an extra insert and delete per commit, vacuum churn on `pending`; NOTIFY serialises committing appends (server-wide lock), so the logical source is the scale option; one extra listening connection, not from the pool |
| Kafka (planned) | `source-kafka` | consume a single-partition topic carrying commit refs (e.g. produced by an existing CDC pipeline capturing `commits`); token = offset | Kafka | reuses existing CDC infrastructure; adds a broker hop and its latency |

The logical source:

```rust
use pges_sequencer::logical;

let replication = pges_replication::config::Config::from_url(url)?;
let source = logical::LogicalSource::new(pool.clone(), replication, logical::LogicalConfig::default());
// LogicalSource::slot(&self) -> Option<&name::SlotName>: the configured slot, or pges_<oid> once opened
// LogicalSource::bootstrap(&mut self) -> Result<logical::Bootstrap, SourceError>: creates the publication
//   and the slot before writers start; no lease
// LogicalSource::resequence(&mut self, &logical::ResequenceConfig)
//   -> Result<logical::Resequenced, logical::ResequenceError>: recovers a halted database

#[non_exhaustive]
pub enum Bootstrap { Created, Exists, FailoverChanged }   // logical::Bootstrap; FailoverChanged: changed
                                                          // to the value SlotFailover::Set asked for

#[non_exhaustive]
pub enum SlotFailover { Keep, Set(connection::Failover) } // logical::SlotFailover; Default Keep

#[non_exhaustive]
pub struct LogicalConfig {                   // Default
    pub slot: Option<name::SlotName>,        // None: pges_<oid of the database>
    pub failover: SlotFailover,              // Keep; Set(Enabled) for FAILOVER on a slot it creates
                                             // (PostgreSQL 17+), and Set for bootstrap to change an
                                             // existing slot
    pub status_interval: Interval,           // 10 s; longest gap between status updates; keep it well
                                             // below wal_sender_timeout (60 s by default)
    pub read_ahead: NonZeroUsize,            // 1000 commits buffered ahead while messages keep arriving
    pub start_timeout: Interval,             // 30 s retrying START_REPLICATION while the slot is active
}

#[non_exhaustive]
pub enum SlotLost {                          // boxed in SourceError::Halt
    Invalidated { slot: String, reason: String },   // pg_replication_slots.invalidation_reason
    Missing { slot: String },                       // no slot, but a token is stored
    Ahead { slot: String, token: Lsn, confirmed: Lsn },  // confirmed_flush_lsn past the stored token
                                                         // (0/0 when none is stored and stores hold commits)
    CommitsBeforeSlot { slot: String, store: String },   // the new slot's exported snapshot shows
                                                         // commits; the slot was dropped again
}

#[non_exhaustive]
pub struct ResequenceConfig {                // ResequenceConfig::new(WriterRoles); no Default
    pub writer_roles: pges_postgres::limit::WriterRoles,   // each caps transaction_timeout at the stores'
                                             // shortest max_txn_age (limit::check_writer_roles)
    pub lease: sequencer::Lease,             // Lease::DEFAULT: 10 s, renewed every half of it
}

#[non_exhaustive]
pub struct Resequenced { pub slot: name::SlotName, pub consistent_point: Lsn, pub stores: Vec<Recovered> }

#[non_exhaustive]
pub struct Recovered { pub store: StoreId, pub commits: usize, pub last_position: Position }

#[non_exhaustive]
pub enum ResequenceError {
    NotHalted,                               // pges.sequencer.halted is not set
    LeaseHeld,                               // a sequencer holds the lease
    LeaseLost,                               // taken over meanwhile; nothing written, the new slot left
    SlotInUse { slot: String },              // a process streams from the old slot
    PositionOverflow(StoreId),
    WriterRole(WriterRoleError),             // a writer role does not exist or is not capped at
                                             // max_txn_age; nothing done
    CommitUnknown(DbError),                  // the final COMMIT failed; it may have landed
    Source(SourceError),                     // From<sqlx::Error> too
}
```

- `open` creates the slot and the `pges` publication if they are missing, and adds missing non-outbox
  stores' `commits` to the publication, with a warning (schema.md, Logical source).
- `ready` waits on the stream only while nothing is buffered. Otherwise it reads only what has already
  arrived and stops after `read_ahead` commits, so sustained traffic cannot keep it from returning.
- Slot loss is a typed `SlotLost` boxed in `SourceError::Halt`; callers downcast it. `CommitsBeforeSlot` is a
  `Halt` too, which the sequencer records in `pges.sequencer.halted`; recover with `pges-sequencer
  resequence`.
- The slot is confirmed only through `ack`, so never past a stored token. While idle, the keepalives' WAL
  position goes out as the token of an empty batch, at most once per `status_interval`, and is confirmed
  once stored. A slot the source creates has its consistent point stored as the token at once, if none is
  stored. Every `open` checks the slot's `confirmed_flush_lsn` against the token and halts with
  `SlotLost::Ahead` when it is past it, as after promoting an asynchronous standby with a synchronised
  failover slot (design.md, Deployment and Slot loss). Recover with `pges-sequencer resequence` on the new
  primary.
- `resequence` refuses with `WriterRole` unless every writer role caps its transactions at the shortest
  `max_txn_age` of the logical stores, with `NotHalted` unless `halted` is set, and with `LeaseHeld` while a
  sequencer holds the lease. Otherwise it takes the lease, drops the old slot (`SlotInUse` while it is
  active), creates a new one (keeping the old slot's failover unless `LogicalConfig::failover` is `Set`),
  finds every commit without a log row as of the new slot's consistent point, among commits at most the
  store's `max_txn_age + 2 × max_clock_skew` older than its newest sequenced commit, and writes their log
  rows in commit id order. It records the positions each category got, which retention scans whole. In the
  same transaction it stores the consistent point as the token, clears `halted` and releases the lease.
  Within a stream the recovered commits keep their order; across streams causal order is best effort. A
  writer transaction open longer than `max_txn_age` can make a commit be missed or sequenced twice; a session
  that overrides `transaction_timeout` with `SET` is not seen by the check.
- A failed `resequence` writes nothing, drops the new slot (not after `LeaseLost`) and releases the lease;
  run it again. After `CommitUnknown` running it again is safe too: it answers `NotHalted` if the commit
  landed.
- The sequencer renews its lease while `open` retries START, and drops the open on shutdown. `close` ends
  streaming when the term ends (CopyDone, then Terminate; 1 s timeout), so the walsender lets go of the slot
  at once.
- `bootstrap` (schema.md, Slot bootstrap) is idempotent and takes no lease: an existing valid slot is kept
  (`Exists`), also when `failover` is `Set` to the slot's current value; an existing slot whose failover
  differs from `Set` is altered (`FailoverChanged`), or refused `Transient` while active; with `Keep` its
  failover is never changed; a missing one is created with the same check as `open` (`Created`); a missing
  slot with a stored token is `Halt(Missing)`, an invalidated one `Halt(Invalidated)`, one confirmed past
  the token `Halt(Ahead)`, and commits found before a new slot `Halt(CommitsBeforeSlot)`. Bootstrap records `CommitsBeforeSlot` in
  `pges.sequencer.halted` while no process holds the lease (a reason already recorded is kept), so
  `resequence` can run at once; it does not record `Missing`, `Invalidated` or `Ahead`, which only arise
  after a sequencer ran and records them itself. A foreign slot, or a database whose stores use the outbox, is
  `Invalid`. Do not run it while a sequencer creates the slot on its first start. The sequencer still
  creates a missing slot itself on `open`.
- Requirements: `wal_level = logical`; a role with `REPLICATION`, allowed by `pg_hba.conf` to connect to the
  database; room in `max_replication_slots` and `max_wal_senders`. Writers with `synchronous_commit = off`
  add up to `wal_writer_delay` to sequencing latency.

The outbox source keeps causal order because each `pending` row is inserted at the end of its append: if B
saw A's commit, B's insert ran after A committed, so B's `seq` is higher. It needs an uncached sequence, and
positions are assigned in `seq` order.

`outbox::CHANNEL` (`"pges__outbox"`) is the channel outbox stores notify on. The payload is the store's
schema name, sent once per store per committing transaction.

Long-running transactions do not delay the built-in sources: a commit is delivered when it commits. They
do pin the slot's `restart_lsn`, so the logical source retains WAL for as long as the oldest open writer
transaction. Cap them with `transaction_timeout` and `idle_in_transaction_session_timeout` on the writer
role; the crate calls that value `max_txn_age`, recorded per store in `pges.stores`.

Supabase ETL is not on crates.io (git dependency only), so it cannot be a feature of a published crate.
Anyone who runs ETL can implement `Source` as an ETL `Destination` in their own crate; dedupe on
`(commit_lsn, tx_ordinal)`.

## Wake-ups

How subscribers learn that new positions exist. All are hints; the log table is always the truth. A waker
that fires too often costs a read; one that misses a change delays the subscriber until the next one.

```rust
pub trait Waker: Send {                  // pges_subscribe::waker; DynWaker is its object-safe twin
    /// Resolves when the high-water mark of any watched category may have passed `known`. Cancel safe.
    fn changed(&mut self, known: &HighWater) -> impl Future<Output = Result<HighWater, WakeError>> + Send;
    fn store_connections(&self) -> u32 { 0 }   // connections of the store's pool kept between calls
}
pub fn has_passed(high: &HighWater, known: &HighWater) -> bool;   // the shared test
let any = waker::Wakers::new(inproc).or(poll);   // or Wakers::any(NonEmpty<Box<dyn DynWaker>>); never empty
let recheck = waker::recheck::Recheck::every(interval!(500 ms));   // no feature; the default
let inproc = waker::inproc::InprocWaker::new(sequencer.watch().high_water(store_id));   // wake-inproc
let poll = waker::poll::PollWaker::new(&store, waker::poll::PollConfig::default());     // wake-poll
                                         // PollConfig::new(interval, max_interval) -> Option<PollConfig>:
                                         // None if max_interval < interval; Default 100 ms, 5 s
let notify = waker::notify::NotifyWaker::new(&store);   // wake-notify
                                         // or NotifyWaker::with_separate_pool(&store, pool)
let grpc = waker::grpc::GrpcWaker::new(
    store.store_id(),
    non_empty!["http://seq-0:50051".parse()?, "http://seq-1:50051".parse()?],
);                                       // wake-grpc; cannot fail; grpc::Endpoint: FromStr, Display;
                                         // grpc::EndpointError { Invalid, UnsupportedScheme }
                                         // or GrpcWaker::with_config(store_id, endpoints, grpc::GrpcConfig { .. })

#[non_exhaustive]
pub struct GrpcConfig {                  // pges_subscribe::waker::grpc; Default
    pub connect_timeout: Interval,       // 5 s: connect, handshake and response headers of one attempt
    pub keepalive_interval: Interval,    // 10 s: HTTP/2 ping interval
    pub keepalive_timeout: Interval,     // 20 s: an unanswered ping loses the connection
    pub unreachable_after: Duration,     // 30 s without any endpoint serving before calls fail; zero: at
                                         // the first failed round
    pub tls: ClientTls,                  // wake-grpc-tls; ClientTls::system_roots(); http endpoints ignore it
}

impl ClientTls {                         // pges_subscribe::waker::grpc; wake-grpc-tls; Clone; all PEM
    pub fn system_roots() -> Self;       // the operating system's roots
    pub fn ca(ca: impl AsRef<[u8]>) -> Result<Self, TlsError>;   // trust only this CA
    pub fn identity(self, cert: impl AsRef<[u8]>, key: impl AsRef<[u8]>) -> Result<Self, TlsError>;
}                                        // identity: a client certificate, for mutual TLS
pub use pges_core::tls::TlsError;        // wake-grpc-tls; Core types

pub struct Rejected { /* private */ }    // an endpoint refused in a way retrying does not fix
impl Rejected {
    pub fn rejection(&self) -> Rejection;
    pub fn endpoint(&self) -> &str;      // the endpoint, as a URI
}
#[non_exhaustive]
pub enum Rejection {                     // Clone, Copy, PartialEq, Eq
    Unimplemented, InvalidArgument, PermissionDenied, Unauthenticated,   // the gRPC status of that name
    FailedPrecondition,                  // that status, or the endpoint's TLS could not be set up locally
    Tls,                                 // a certificate failure in the TLS handshake
    Protocol,                            // a position above Position::MAX
}

pub struct HighWater { /* private */ }   // pges_core::high_water; per store; Default is all zero, lsn None
impl HighWater {
    pub fn new(store: Position, categories: Vec<(category::CategoryId, Position)>, lsn: Option<Lsn>) -> Self;
    pub fn store(&self) -> Position;
    pub fn categories(&self) -> &[(category::CategoryId, Position)];   // sorted by id
    pub fn lsn(&self) -> Option<Lsn>;
    pub fn category(&self, id: category::CategoryId) -> Position;   // Position::BEGINNING if none
}
```

`PollConfig`'s fields are private. In a `const` item, `PollConfig::new(a, b).unwrap()` runs at compile time,
so a bad pair does not compile.

```rust
pub struct Notice { /* private */ }      // pges_core::high_water; the NOTIFY payload
#[non_exhaustive]
pub enum NoticeCategories { Listed(Vec<(CategoryId, Position)>), Omitted }   // Omitted: any may have moved
impl Notice {
    pub const MAX_LEN: usize = 7999;     // PostgreSQL refuses payloads of 8000 bytes or more
    pub fn new(high_water: &HighWater, touched: impl IntoIterator<Item = CategoryId>) -> Self;
    pub fn store(&self) -> Position;
    pub fn lsn(&self) -> Option<Lsn>;
    pub fn categories(&self) -> &NoticeCategories;
    pub fn encode(&self) -> String;      // "1;42;0/16B3D848;3:40,5:42"; "1;42;;*" when too long
    pub fn decode(payload: &str) -> Result<Self, NoticeError>;
}
#[non_exhaustive]
pub enum NoticeError { UnsupportedVersion(String), Malformed(String) }
```

The fields of `HighWater` and `Notice` are private, so `categories` stays sorted.

The `known` contract: the subscriber passes what it has read up to. `known.categories()` lists the watched
categories, each with the position read up to; wakers treat it as the watched set, so they need no category
configuration of their own. An empty list means the whole store, compared through `known.store()`.
`waker::has_passed(high, known)` is the test every waker applies. A subscription passes its merge cut, not
`H`, for the store and every watched category: a cursor can be ahead of `H` (`Start::Position` past the log),
and a level-triggered waker given `H` would fire again at once.

The high-water mark a waker resolves with is a hint, not a fact. A subscription never carries it forward as
a position to compare later wakes with: `known` always comes from what it read (for a sharded subscription,
from what its waiting shards read). It uses only the LSN, as the WAL position to wait for on a replica, and
lowers that to the primary's WAL insert position when the replica has not replayed it. A custom waker that
forwards untrusted input should do the same: wake on it, and never let it raise `known`.

- `Wakers::any` resolves with the first waker that fires, not the highest value seen (the subscription does
  not use the value). A failing waker is left out until the next call; the call fails only when all failed,
  with the last error. `Wakers` cannot be empty: build it with `Wakers::new(w).or(w2)` or from a
  `NonEmpty`.
- `InprocWaker` takes any `tokio::sync::watch::Receiver<HighWater>` for one store; it does not depend on
  `pges-sequencer`. A dropped sender whose last value has not passed `known` gives `WakeError::Closed`.
- `PollWaker` reads at once, then waits `interval`, doubling up to `max_interval`, and resets after it fires.
- `waker::recheck::Recheck` (no feature) is a plain timer: `Recheck::every(interval)`, `Default` 500 ms. A
  subscription without a waker uses `Recheck::default()`. It is the only way to set the recheck interval,
  so the interval cannot be set and then ignored next to another waker. It keeps its deadline across calls
  dropped before it fires (a lease renewal, or another waker in `Wakers` firing first), so it fires about once
  per interval even when renewals or a push waker come more often: within `Wakers`, at most one extra log
  read per interval. A call made after the deadline passed (after a batch that took longer than the interval)
  returns at once.
- `NotifyWaker` (`wake-notify`) listens on `pges_<store schema name>` through sqlx's `PgListener` on one
  connection, opened on the first call, of the store's pool (or of the pool given to `with_separate_pool`, for
  example when the store's pool sits behind a pooler in transaction mode, where LISTEN does not work). It
  decodes with `Notice::decode`.
- Notices are untrusted: `NOTIFY` needs no privilege, so any role that can connect can send one on the
  store's channel, with any payload. The waker keeps the newest positions (store, per category, LSN) of the
  notices received since its last call resolved, resolves the next call whose `known` they have passed, and
  then forgets them. So each notice wakes at most one call: a notice that arrived while the subscriber was
  busy still wakes the next call it has passed, one the subscriber already read past wakes nothing, and a
  forged one costs one log read, never a loop of them, and hides no notice after it. It is not
  level-triggered across calls: a notice that woke a call does not wake the next one, even if it is still
  ahead of `known`. `NoticeCategories::Omitted` (`*`) counts as every category having reached that
  notice's store position; it wakes only a `known` below that position.
- It resolves once, whatever `known` says, after the LISTEN connection opens (the first time and after a
  lost connection, because notices sent while nobody listened are gone) and after a payload it cannot read
  (`UnsupportedVersion`, `Malformed`). Any receive error counts as a lost connection, and so does a closed
  one; `WakeError::Database` comes only from opening a new one. Notices of quick successive batches may be
  merged into one carrying the union of the touched categories.
- Reconnects back off: 100 ms doubling to 5 s after a failed listen or a connection lost within 5 s of
  opening; a connection that stayed up 5 s is replaced at once. The wait happens inside `changed`, so
  dropping the call (shutdown, lease renewal) does not wait for it.
- `store_connections()` is 1 for `new`, 0 for `with_separate_pool`: a separate pool's listening connection
  does not count against the store's pool.
- The listening connection only reads, so one that dies silently (dropped by a firewall or a NAT without a
  reset) may never be noticed, and the waker then stays quiet. The subscription still reads the log every
  `max_idle` (30 s by default). For a shorter bound, lower `max_idle`, or combine it with a `PollWaker`
  with a long `max_interval` through `Wakers::any`.
- Dropping a `NotifyWaker` needs a tokio runtime: sqlx runs `UNLISTEN *` on a spawned task before the
  connection goes back to the pool. A subscription drops its waker inside its own task.
- `GrpcWaker` (`wake-grpc`, the default) streams one store's values from the sequencers' gRPC watch. Give it
  every sequencer's address as a `NonEmpty<grpc::Endpoint>`; it connects on the first call. Endpoints are
  `http://host:port`, or `https://host:port` with `wake-grpc-tls`; without that feature parsing `https` fails
  with `EndpointError::UnsupportedScheme`, whose message names the feature.
- `https` endpoints (`wake-grpc-tls`; rustls with ring) are verified against `GrpcConfig::tls`: the system's
  root certificates (`ClientTls::system_roots()`, the default) or one CA (`ClientTls::ca`), with an optional
  client certificate (`.identity(cert, key)`) for servers that require one. The server's certificate must
  name the endpoint's host (a name or an IP). `ca` and `identity` check the PEM when called and fail with
  a `TlsError` variant, as `ServerTls` does. If an endpoint's TLS cannot be set up (no system roots, say), that endpoint is refused as
  `Rejection::FailedPrecondition`.
- It tries the endpoints in turn, starting with the last one that served; only the leader serves, the others
  answer `UNAVAILABLE`. Each attempt (connect, handshake, response headers) is bounded by `connect_timeout`.
  After a round in which every endpoint failed it waits 100 ms, doubling up to 5 s. A stream that ended within
  5 s of opening counts as a failed round; one that lasted longer is replaced at once.
- It keeps the highest store, category and LSN values seen from any leader, so it never reports less than
  before. It resolves at once while those values have passed `known`, even with no stream open. It also
  resolves once, whatever `known` says, on the first value of each new stream (values sent while no stream
  was open are gone), and on a value with a higher fence than any before.
- Transient failures are retried inside `changed`, silently: `UNAVAILABLE`, `RESOURCE_EXHAUSTED`,
  `DEADLINE_EXCEEDED`, `INTERNAL`, timeouts and broken connections, including a TLS failure that is not about
  a certificate. Under TLS 1.3 a server that requires a client certificate the client lacks may just close
  the connection, and a plain `http` client against a TLS server sees a broken connection; both are retried.
  Retrying is silent because nothing new is sequenced while no sequencer leads, so a restart or a handover
  does not show as `Recovering`. Once no endpoint has served for `unreachable_after`, each failed round fails
  the call with `WakeError::Other(grpc::Unreachable)`, which names how long and the last endpoint's error.
  The subscription then shows `Recovering` and re-reads the log on its retry interval until an endpoint
  serves again.
- Backoff and progress through the endpoints are kept on the waker, so a dropped call (shutdown, lease
  renewal) is safe to repeat and a later call waits only for what is left of the backoff. An attempt cut short
  by a dropped call counts as failed, and the next call tries the next endpoint. A round of failures can span
  calls, so `Unreachable` surfaces even when every call is dropped before an attempt completes.
- Refusals that retrying does not fix fail the call at once with `WakeError::Other(grpc::Rejected)`: a round
  in which no endpoint served and one answered `UNIMPLEMENTED`, `INVALID_ARGUMENT`, `PERMISSION_DENIED`,
  `UNAUTHENTICATED` or `FAILED_PRECONDITION` (a wrong server or port), or failed the TLS handshake on a
  certificate (untrusted, expired, for another host, or the client's refused: `Rejection::Tls`). Certificate
  failures are found by walking the error's source chain for a `rustls::Error`. A refusal that another
  endpoint in the round makes up for is only logged (`tracing`, warn), so one wrong address in the list
  does not flash `Recovering` on every handover.
- A stream that sends a position above `Position::MAX` (no sequencer sends one) fails the call that received
  it with `Rejected` (`Rejection::Protocol`), naming the endpoint that served the stream.
- A `Rejected` does not stop the waker: the next call tries again after the backoff, so the subscription
  shows `Recovering` with the error and re-reads the log on its retry interval until the endpoint is fixed.
- Error messages carry the transport error's whole source chain.
- HTTP/2 keepalive pings detect a connection that died silently. `store_connections()` is 0.

`HighWater`, `StoreId` and `CategoryId` live in `pges-core`, so wakers and subscribers do not depend on the
sequencer. `Store::store_id()` returns `StoreId`; `pges-postgres` converts store and category ids to and from
`int4` only where it binds or reads them.

`Lsn` lives in `pges_core::lsn` (it has no IO), so wakers and the sequencer token use it without depending on
`pges-replication`.

Every message carries `lsn`, the WAL insert LSN the sequencer read with `pg_current_wal_insert_lsn()` after
the batch committed. `pg_current_wal_lsn()` is the write position: with `synchronous_commit = off` it can be
behind the batch's own commit record. Every `commits` row it sequenced committed earlier in WAL, so a replica
whose `pg_last_wal_replay_lsn()` has reached `lsn` sees both the log and the commits rows. A subscriber
reading from a replica waits for that before reading and does not fall back to the primary by default.
`Replica::fallback(after, limit)` opts in per subscription, with a caller-shared cap on concurrent fallbacks.
When a waker gives no LSN, the subscription reads the primary's `pg_current_wal_insert_lsn()` instead.

When the last record ended at a WAL page boundary, the insert LSN points just past the next page's header,
while the replay LSN stops at the boundary. A reader comparing the two must allow for that offset (24 bytes,
or 40 on a segment's first page); `pges-subscribe` does (schema.md, Replica reads).

| Waker | Feature | Mechanism | Use when |
|---|---|---|---|
| gRPC watch (`waker::grpc::GrpcWaker`, built) | `wake-grpc` (default; + `watch-grpc` on the sequencer) | the leading sequencer streams the store's high-water marks, per category, with the LSN and the term's fence (`pges.watch.v1`); the waker merges them and is level-triggered | production default |
| In-process (`waker::inproc::InprocWaker`, built) | `wake-inproc` | `tokio::sync::watch` from the sequencer's `Watch` when the sequencer runs in the same binary | single-service deployments, tests |
| LISTEN/NOTIFY (`waker::notify::NotifyWaker`, built) | `wake-notify` (+ `watch-notify` on the sequencer) | the sequencer (`SequencerConfig::notify`, on by default with `watch-notify`) sends `NOTIFY pges_<store schema name>` for every store a batch wrote to, concurrently with sequencing (polled by `Sequencer::run`), on another pool connection, after the batch committed, one statement for all stores; payload = `high_water::Notice` (format 1: `1;<store position>;<lsn or empty>;<category id>:<position>,…` for the categories the batch touched, or `*` when they do not fit in 7999 bytes); notifications of quick successive batches may be merged; a failure is logged and dropped; notifications still queued when `run` returns are dropped | no extra network endpoint wanted; the single notifier avoids commit-lock contention, and a full notify queue only delays wake-ups, never fails a log write |
| Poll (`waker::poll::PollWaker`, built) | `wake-poll` | reads the newest `log_by_category` entry per watched category, at once and then on an interval with backoff | fallback, degraded mode, environments where nothing else is possible |

Code that uses `PollWaker` must name `wake-poll`: it is no longer a default feature.

### Watch protocol

`pges-watch-proto` ships `proto/pges/watch/v1/watch.proto`; clients in other languages generate their code
from it. The Rust code is generated and checked in, so a build needs no `protoc` and no build script.

```proto
package pges.watch.v1;

service Watch {
  rpc Watch(WatchRequest) returns (stream WatchResponse);   // UNAVAILABLE unless leading
  rpc Status(StatusRequest) returns (StatusResponse);
}
message WatchRequest { int32 store_id = 1; }                // pges.stores.store_id
message WatchResponse {
  uint64 fence = 1;                                         // the announcing term's fence
  uint64 store = 2;                                         // the store's newest position
  repeated CategoryPosition categories = 3;                 // sorted by category id
  optional uint64 lsn = 4;                                  // LSN X/Y as (X << 32) | Y; absent until read
}
message CategoryPosition { int32 category_id = 1; uint64 position = 2; }
message StatusRequest {}
message StatusResponse { SequencerState state = 1; optional uint64 fence = 2; }   // fence while leading
enum SequencerState {
  SEQUENCER_STATE_UNSPECIFIED = 0; SEQUENCER_STATE_STANDBY = 1; SEQUENCER_STATE_LEADING = 2;
  SEQUENCER_STATE_RECOVERING = 3; SEQUENCER_STATE_HALTED = 4; SEQUENCER_STATE_STOPPED = 5;
  SEQUENCER_STATE_ABANDONED = 6;                            // the run was dropped; the lease is kept
}
```

Values are hints, as for every waker. Within one term positions never go down; a new leader starts from the
log, so they do not go down across terms either. The package is versioned: fields and enum values may be
added to `v1` under new numbers, a number is never reused, and a shape change is a new package
(`pges.watch.v2`), never an edit of `v1` (Versioning). An older reader's `state()` getter returns
`UNSPECIFIED` for a value it does not know, such as `ABANDONED`.


## Relays

Planned, not built. A future `pges-relay` would provide ready-made `Handler`s that forward a subscription to Kafka or Pub/Sub: key = stream id,
headers carry type, version and position, single partition per category topic when strict order is needed.
They are ordinary subscribers, so they get leases, checkpoints and backfills for free.

## Publishing

- Every dependency is a published crate; no git or path dependencies outside the workspace. Path
  dependencies between workspace crates also carry `version`, and `cargo deny` checks licences, advisories
  and sources.
- `pges-replication` exists because the mature replication clients are only available as git forks of
  `rust-postgres`. It implements only the protocol subset the sequencer needs. Code adapted from those forks
  keeps their MIT/Apache-2.0 notices.
- TLS uses rustls with the ring provider only (`default-features = false` on rustls and tokio-rustls), passed
  explicitly to the client config. The gRPC TLS features (`watch-grpc-tls`, `wake-grpc-tls`) use tonic's
  `tls-ring`, so they too use ring. aws-lc-rs is avoided because it needs cmake and nasm to build. No OpenSSL
  anywhere.
- Licence: MIT OR Apache-2.0, copyright "The pges Authors".
- MSRV: declared with `rust-version` and checked in CI. The MSRV follows sqlx (currently 1.94); raising it is
  a minor-version change.
- docs.rs: `[package.metadata.docs.rs] all-features = true`, with `doc_cfg` on docs.rs so feature-gated
  items say which feature enables them. Nothing is generated at build time: `pges-watch-proto` ships its
  generated code, so docs build without `protoc`, network or Docker.
- Published crates: `pges`, `pges-core`, `pges-derive`, `pges-postgres`, `pges-subscribe`, `pges-sequencer`,
  `pges-replication`, `pges-watch-proto`, `pges-testing`. `pges-bench` and `pges-examples` are not published.
- Public API stability: before 1.0, breaking changes bump the minor version (0.1 to 0.2); patch releases
  do not break. `#[non_exhaustive]` on public error enums and config structs so new variants and fields are
  not breaking. The README's Stability section lists the experimental features. Public error types are
  `Error + Send + Sync + 'static` and the shared handles `Send` (`Sync` where shared by reference); a test
  (`crates/pges/tests/auto_traits.rs`) holds them to it.
- Public dependencies: these crates' types appear in the pges API, so a semver-incompatible upgrade of one
  is a breaking pges release (a minor version before 1.0). sqlx 0.9 (`PgPool`, `PgConnection`, and
  `From<sqlx::Error>` for the error enums), uuid 1 (`from_uuid`, `as_uuid`), time 0.3
  (`OffsetDateTime` in `recorded_at`), serde 1 and serde_json 1 (`Snapshotted`, `codec::json::Upcaster`),
  tokio 1 (`tokio::sync::watch::Receiver` in `Watch::status` and `InprocWaker`), bytes 1
  (`pges-replication`'s pgoutput and XLogData), prost 0.14 and tonic 0.14 (`pges-watch-proto`, the
  sequencer's `grpc::WatchService`), and, behind `otel`, opentelemetry 0.33 (`opentelemetry::Context`
  from `trace_parent` and `to_context`). An application that names these types depends on the same
  versions.

## Versioning

| What | Where | Rule |
|---|---|---|
| Crates | Cargo | semver; before 1.0 a minor bump is breaking |
| Database schema, per scope | `pges.migrations`, `pges.scopes` | migrations are embedded and checksummed. `Store::open` fails if the database is behind (run `pges migrate` or enable auto-migrate), and fails if the crate's version is below `min_compatible`. Running subscriptions and sequencers check again whenever they renew or take a lease (Rolling upgrades). Migrations are expand/contract: a new schema version stays usable by the previous crate release, so `min_compatible` only moves in a later release. Migration 0001 is frozen from 0.1.0 on: schema changes are new migrations, never edits (schema.md, Migrations) |
| Payload frame | `commits.format`, `snapshots.format` | readers support every format ever written. Writers switch to a new format only via `StoreConfig::write_format`, after all readers are upgraded |
| Event schemas | per event, inside the frame | additive, defaulted fields need no bump. Anything else, including a codec change, bumps `version` and adds an upcaster. Type names never change |
| New event types and versions | writers | a writer may emit a new type or a new version only after every reader understands it, or after readers use `OnUnknown::Skip`; the same rule as the frame format |
| Snapshots | `snapshots.state_version` | a mismatch with `Snapshotted::SNAPSHOT_VERSION` means the snapshot is ignored and rebuilt by replay |
| Projections | subscription name | the version is part of the name (`jobs-board.v3`); a new version is a blue/green rebuild or a field backfill. Changing the categories or the number of shards under the same name needs `.redefine(..)` |
| Watch protocol | gRPC package | `pges.watch.v1`; fields and enum values are added under new numbers, never reused; breaking changes get `v2`, served side by side |
| NOTIFY payload | first field of the `Notice` text | `1` today; a new shape is a new version; decoders reject unknown versions, and the subscriber just reads the log again |

### Rolling upgrades

A release N migrates the schema so that release N-1 still works on it (expand). A later release contracts:
it raises `min_compatible`, and the releases below it no longer work. (2026-11-27)

- Upgrade one release at a time, and finish rolling out N before a release that contracts runs its
  migrations. A process of N-1 or older that is still running then is refused, as follows.
- `Store::open` fails with `OpenError::CrateTooOld`, so a process of a release the database no longer
  supports does not start.
- A running subscription or sequencer finds out at its next lease renewal (a subscription renews every
  half lease, 7.5 s by default; the sequencer every `Lease::renew`, 5 s) or its next attempt to take a
  lease (a standby tries every `acquire_interval`, 1 s by default). The renewal and acquire statements
  refuse while the shared scope's, a store's or a store's outbox scope's `min_compatible` is above the
  crate's version for it: a subscription checks the shared scope and its own store's scopes, the sequencer
  every store's, since it writes to all of them. After a refused renewal the process reads `pges.scopes`,
  releases its lease and stops with
  `SubscribeError::CrateTooOld` or `RunError::CrateTooOld`, instead of standing by. A `run_pg` batch whose
  checkpoint carried the refused renewal is rolled back. A refused acquire takes no lease, changes no fence
  and stops the standby with the same error, whether or not the lease was free; it is not retried. So only
  a standby of a supported release takes over the released lease.
- Between the migration and that renewal, an old leader keeps working for up to half a lease. Appends,
  reads and backfills run no lease and are not checked after `Store::open`.
- The check costs nothing extra while the crate is supported: it is a subquery on `pges.scopes` in the
  renewal and acquire statements. Roles that renew or take leases need `SELECT` on `pges.scopes`
  (schema.md, Roles).

### Frame format 1

```
payload     := header event{event_count}
header      := varint metadata_len  metadata (protobuf CommitMetadata)
event       := varint event_type_id  varint schema_version  varint data_len  data
```

`schema_version` is at least 1; 0 is `FrameError::OutOfRange`.

`event_type_id` refers to `event_types` in the same store; `data` is the codec output for that event.
Event type ids are registered and committed before any frame uses them (schema.md, Event type registration).
