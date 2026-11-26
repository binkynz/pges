# pges-derive

`#[derive(Event)]` for pges, an event store on PostgreSQL 18+. It implements `pges_core::event::Event` for
an enum with one variant per stored event type: stable type names, schema versions, and chains of
upcasters for older versions.

Through the [`pges`](https://docs.rs/pges) facade it is `#[derive(pges::Event)]`. Depend on it directly,
together with `pges-core`, when you do not use the facade. The generated code finds `pges-core` through
whichever of the two your `Cargo.toml` names.

## Example

```rust
use pges_core::event::Event as _;

#[derive(Debug, PartialEq, pges_derive::Event)]
#[event(category = "jobs")]
enum JobEvent {
    #[event(name = "job.created")]
    Created { title: String },
    #[event(name = "job.closed")]
    Closed,
}

assert_eq!(JobEvent::CATEGORY.as_str(), "jobs");
assert_eq!(JobEvent::TYPES, &["job.created", "job.closed"]);
assert_eq!(JobEvent::Closed.type_name(), "job.closed");
```

Docs: <https://docs.rs/pges-derive>, once the crate is published.
