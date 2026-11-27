//! `#[derive(Event)]` for pges, an event store on PostgreSQL 18+.
//!
//! The derive implements [`pges_core::event::Event`] for an enum with one variant per event type. See
//! [`macro@Event`] for the attributes, the upcaster signature and the compile-time checks.
//!
//! The generated code names `pges-core` by absolute path, found in the calling crate's `Cargo.toml`: through
//! the `pges` facade (`::pges::core`) when the crate depends on it, else `pges-core` itself (`::pges_core`),
//! each under whatever name the crate gives it. The crate needs no other import than the derive itself. A
//! crate that reaches `pges-core` some other way, such as a re-export of its own, names the path with
//! `#[event(crate = "...")]`.
//!
//! [`pges_core::event::Event`]: https://docs.rs/pges-core/latest/pges_core/event/trait.Event.html
#![cfg_attr(docsrs, feature(doc_cfg))]

mod core_path;
mod expand;
mod input;

#[cfg(doctest)]
#[doc = include_str!("../README.md")]
struct ReadmeDoctests;

use proc_macro::TokenStream;

/// Implements `pges_core::event::Event` for an enum, one variant per stored event type.
///
/// ```
/// use pges_derive::Event;
///
/// #[derive(Debug, PartialEq, Event)]
/// #[event(category = "jobs", codec = "json")]
/// enum JobEvent {
///     #[event(name = "job.created")]
///     Created { title: String, priority: u8 },
///     #[event(name = "job.closed")]
///     Closed,
/// }
///
/// use pges_core::event::{Decoded, Event as _, SchemaVersion};
///
/// let event = JobEvent::Created { title: "Brakes".into(), priority: 3 };
/// let mut data = Vec::new();
/// event.encode(&mut data)?;
///
/// assert_eq!(JobEvent::CATEGORY.as_str(), "jobs");
/// assert_eq!(JobEvent::TYPES, &["job.created", "job.closed"]);
/// assert_eq!(event.type_name(), "job.created");
/// assert_eq!(event.schema_version(), SchemaVersion::FIRST);
/// assert_eq!(data, br#"{"title":"Brakes","priority":3}"#);
/// assert_eq!(JobEvent::decode("job.created", SchemaVersion::FIRST, &data)?, Decoded::Event(event));
/// # Ok::<(), pges_core::event::CodecError>(())
/// ```
///
/// # Enum attributes
///
/// `#[event(category = "...", codec = "...", crate = "...")]`
///
/// - `category` (required) is the category the events belong to, becoming `Event::CATEGORY`. It must match
///   `^[a-z][a-z0-9_]{0,39}$` and not contain `__`, the rule of `CategoryName`.
/// - `codec` (default `"json"`) is how event data is written. `"json"` is the only codec so far; it uses
///   `serde_json` through `pges_core::codec::json`. Any other value is a compile error.
/// - `crate` is the path the generated code names `pges-core` by, as serde's `#[serde(crate = "...")]`.
///   Without it, the path comes from the calling crate's `Cargo.toml`: `::<name>::core` when it depends
///   on `pges` (named `<name>`, which may be a rename), else `::<name>` when it depends on `pges-core`,
///   else `::pges_core`. The facade wins when both are present. Set it when the crate reaches `pges-core`
///   in a way its `Cargo.toml` does not show, such as a re-export, or when the facade is a dev-dependency
///   or an optional dependency that may be off: `Cargo.toml` alone does not say which build sees it. The path must lead to `pges-core`'s root module, re-exported or renamed.
///
/// ```
/// use pges_derive::Event;
///
/// // The path could be `pges::core`; here a renamed import stands in for the facade.
/// mod facade {
///     pub use pges_core as core;
/// }
///
/// #[derive(Debug, PartialEq, Event)]
/// #[event(category = "jobs", crate = "facade::core")]
/// enum JobEvent {
///     #[event(name = "job.created")]
///     Created { title: String },
/// }
///
/// use pges_core::event::Event as _;
///
/// assert_eq!(JobEvent::CATEGORY.as_str(), "jobs");
/// ```
///
/// The enum must not be generic.
///
/// # Variant attributes
///
/// `#[event(name = "...", version = N, upcast(1 => path, 2 => path, ...))]`
///
/// - `name` (required) is the stored type name. It never changes once events are written; the Rust variant
///   can be renamed freely. Names must be unique within the enum, and names starting with `$` are reserved
///   for system events. `Event::TYPES` lists the names in variant order.
/// - `version` (default 1) is the schema version written for this event. Bump it for any change that is
///   not an additive field with a default. Old events are never rewritten; they are upcast when decoded.
/// - `upcast` lists one upcaster per older version; see [Upcasting](#upcasting).
///
/// # Variant shapes
///
/// A struct variant is stored as an object of its fields, without the variant name. A unit variant is
/// stored as `{}`. A newtype variant is stored as its inner value, which must implement `Serialize` and
/// `DeserializeOwned`. Tuple variants with more than one field are a compile error.
///
/// ```
/// use pges_derive::Event;
/// use serde::{Deserialize, Serialize};
///
/// #[derive(Debug, PartialEq, Serialize, Deserialize)]
/// struct Assigned {
///     assignee: String,
/// }
///
/// #[derive(Debug, PartialEq, Event)]
/// #[event(category = "jobs")]
/// enum JobEvent {
///     #[event(name = "job.created")]
///     Created { title: String },
///     #[event(name = "job.assigned")]
///     Assigned(Assigned),
///     #[event(name = "job.closed")]
///     Closed,
/// }
///
/// use pges_core::event::Event as _;
///
/// let encode = |event: &JobEvent| {
///     let mut data = Vec::new();
///     event.encode(&mut data).map(|()| String::from_utf8(data))
/// };
///
/// assert_eq!(encode(&JobEvent::Created { title: "Glass".into() })?.unwrap(), r#"{"title":"Glass"}"#);
/// assert_eq!(
///     encode(&JobEvent::Assigned(Assigned { assignee: "ana".into() }))?.unwrap(),
///     r#"{"assignee":"ana"}"#
/// );
/// assert_eq!(encode(&JobEvent::Closed)?.unwrap(), "{}");
/// # Ok::<(), pges_core::event::CodecError>(())
/// ```
///
/// # Serde attributes on fields
///
/// Fields of struct variants accept `#[serde(...)]` field attributes, such as `default` for a field added
/// without a version bump, or `rename`. They apply to the stored object as they would on a struct, without
/// deriving serde on the enum. Only attributes that write data the same enum reads back are accepted:
///
/// - `rename = "..."` (or `rename(serialize = "x", deserialize = "x")` with one name), `alias`, `default`
///   and `default = "path"`, `skip`, `with` and `flatten`;
/// - `skip_serializing` and `skip_serializing_if` together with `default`, also on an `Option` field;
/// - `serialize_with` together with `deserialize_with`.
///
/// Anything else is a compile error, as is a field stored or read under a name (after `rename`, or as an
/// `alias`) that another field of the variant already uses. `#[serde]` on the enum, on a variant or on a
/// newtype variant's field would not change the stored data, so it is a compile error too: for container
/// attributes such as `rename_all`, use a newtype variant around a struct of your own that derives serde.
///
/// ```
/// use pges_derive::Event;
///
/// #[derive(Debug, PartialEq, Event)]
/// #[event(category = "jobs")]
/// enum JobEvent {
///     #[event(name = "job.created")]
///     Created {
///         #[serde(rename = "name")]
///         title: String,
///         #[serde(default)]
///         priority: u8,
///     },
/// }
///
/// use pges_core::event::{Decoded, Event as _, SchemaVersion};
///
/// assert_eq!(
///     JobEvent::decode("job.created", SchemaVersion::FIRST, br#"{"name":"Paint"}"#)?,
///     Decoded::Event(JobEvent::Created { title: "Paint".into(), priority: 0 })
/// );
/// # Ok::<(), pges_core::event::CodecError>(())
/// ```
///
/// # Upcasting
///
/// An event at `version = N` with `N > 1` lists one upcaster for each older version:
/// `upcast(1 => a, 2 => b, ..., N-1 => z)`. Upcaster `k` turns the stored value of version `k` into the value
/// of version `k + 1`. Decoding version `k` runs upcasters `k` to `N-1` in order, then reads the variant from
/// the result, so each upcaster only knows about two adjacent versions.
///
/// Upcasters work on the codec's value representation. For `"json"` they have the type
/// `pges_core::codec::json::Upcaster`:
///
/// ```text
/// fn(serde_json::Value) -> Result<serde_json::Value, pges_core::event::BoxError>
/// ```
///
/// An upcaster's error makes decoding fail with `CodecError::Decode`, naming the stored schema version.
///
/// ```
/// use pges_derive::Event;
///
/// mod upcast {
///     use pges_core::event::BoxError;
///     use serde_json::Value;
///
///     /// Version 2 renamed `name` to `title`.
///     pub fn created_v1(mut value: Value) -> Result<Value, BoxError> {
///         let object = value.as_object_mut().ok_or("job.created v1 is not an object")?;
///         let name = object.remove("name").ok_or("job.created v1 has no name")?;
///         object.insert("title".into(), name);
///         Ok(value)
///     }
///
///     /// Version 3 added a required priority; older jobs were all priority 0.
///     pub fn created_v2(mut value: Value) -> Result<Value, BoxError> {
///         let object = value.as_object_mut().ok_or("job.created v2 is not an object")?;
///         object.insert("priority".into(), 0.into());
///         Ok(value)
///     }
/// }
///
/// #[derive(Debug, PartialEq, Event)]
/// #[event(category = "jobs")]
/// enum JobEvent {
///     #[event(name = "job.created", version = 3, upcast(1 => upcast::created_v1, 2 => upcast::created_v2))]
///     Created { title: String, priority: u8 },
/// }
///
/// use pges_core::event::{Decoded, Event as _, SchemaVersion};
///
/// let v = |n| SchemaVersion::new(n).unwrap();
/// let expected = Decoded::Event(JobEvent::Created { title: "Paint".into(), priority: 0 });
/// assert_eq!(JobEvent::decode("job.created", v(1), br#"{"name":"Paint"}"#)?, expected);
/// assert_eq!(JobEvent::decode("job.created", v(2), br#"{"title":"Paint"}"#)?, expected);
/// assert_eq!(JobEvent::decode("job.created", v(3), br#"{"title":"Paint","priority":0}"#)?, expected);
/// # Ok::<(), pges_core::event::CodecError>(())
/// ```
///
/// # Unknown events and malformed data
///
/// A type name the enum does not list, or a version above the variant's `version`, decodes to
/// `Decoded::Unknown`: it was written by newer code, and the reader decides whether to skip it. Data that
/// the codec or an upcaster cannot read is a `CodecError::Decode`.
///
/// ```
/// use pges_derive::Event;
///
/// #[derive(Debug, PartialEq, Event)]
/// #[event(category = "jobs")]
/// enum JobEvent {
///     #[event(name = "job.created")]
///     Created { title: String },
/// }
///
/// use pges_core::event::{CodecError, Decoded, Event as _, SchemaVersion};
///
/// let first = SchemaVersion::FIRST;
/// let second = SchemaVersion::new(2).unwrap();
///
/// assert_eq!(
///     JobEvent::decode("job.closed", first, b"{}")?,
///     Decoded::Unknown { type_name: "job.closed".into(), schema_version: first }
/// );
/// assert_eq!(
///     JobEvent::decode("job.created", second, b"{}")?,
///     Decoded::Unknown { type_name: "job.created".into(), schema_version: second }
/// );
/// assert!(matches!(
///     JobEvent::decode("job.created", first, b"not json"),
///     Err(CodecError::Decode { schema_version, .. }) if schema_version == first
/// ));
/// # Ok::<(), CodecError>(())
/// ```
///
/// # Compile-time checks
///
/// The derive rejects, with an error pointing at the attribute:
///
/// - a missing `category`, or one that is not a valid category name;
/// - a codec other than `"json"`;
/// - a variant without `name`, a name starting with `$`, or a name used by two variants;
/// - `version = 0`;
/// - an upcast chain that does not cover every version from 1 to `version - 1` exactly once, or an entry
///   outside that range;
/// - a `crate` value that is not a path, or has generic arguments;
/// - a key given twice;
/// - a field `#[serde]` attribute outside the list in [Serde attributes on fields](#serde-attributes-on-fields),
///   two fields stored under one name, and `#[serde]` on the enum, a variant or a newtype variant's field;
/// - unknown keys, generic enums, structs, and tuple variants with more than one field.
///
/// ```compile_fail
/// use pges_derive::Event;
///
/// #[derive(Event)]
/// #[event(category = "jobs")]
/// enum JobEvent {
///     #[event(name = "job.created", version = 3, upcast(1 => v1))] // missing upcaster for version 2
///     Created { title: String },
/// }
/// # fn v1(v: serde_json::Value) -> Result<serde_json::Value, pges_core::event::BoxError> { Ok(v) }
/// ```
///
/// ```compile_fail
/// use pges_derive::Event;
///
/// #[derive(Event)]
/// #[event(category = "jobs")]
/// enum JobEvent {
///     #[event(name = "job.created")]
///     Created {
///         #[serde(skip_serializing_if = "String::is_empty")] // needs `default`, or decoding fails
///         note: String,
///     },
/// }
/// ```
#[proc_macro_derive(Event, attributes(event, serde))]
pub fn derive_event(input: TokenStream) -> TokenStream {
    let input = syn::parse_macro_input!(input as syn::DeriveInput);

    match input::EventEnum::parse(&input) {
        Ok(event) => expand::event_impl(&event).into(),
        Err(error) => error.to_compile_error().into(),
    }
}
