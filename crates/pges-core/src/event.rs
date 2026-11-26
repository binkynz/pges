//! The `Event` trait and decoding outcomes.

use std::{fmt, num::NonZeroU16};

use crate::category;

/// An application's events for one category, usually an enum with one variant per event type.
///
/// Implement it with `#[derive(Event)]` from `pges-derive`; hand-written impls must follow the same rules:
///
/// - type names are stored and never change; names starting with `$` are reserved for system events;
/// - each type has a schema version, bumped for any change that is not an additive, defaulted field
///   (a codec change included), and older versions are upcast when decoding;
/// - a type name or version this code does not know decodes to [`Decoded::Unknown`], not an error, so a
///   reader can skip events from a newer writer.
///
/// `TYPES` is checked where the store registers, appends or decodes the type: an empty name, a name
/// starting with `$` or a name listed twice is a compile error there (from `cargo build`; `cargo check`
/// does not evaluate it).
///
/// ```compile_fail
/// use pges_core::{category, event};
///
/// enum Twice {}
///
/// impl event::Event for Twice {
///     const CATEGORY: category::CategoryName = category!("twice");
///     const TYPES: &'static [&'static str] = &["created", "created"];
///
///     fn type_name(&self) -> &'static str { match *self {} }
///     fn schema_version(&self) -> event::SchemaVersion { match *self {} }
///     fn encode(&self, _: &mut Vec<u8>) -> Result<(), event::CodecError> { match *self {} }
///     fn decode(
///         type_name: &str,
///         schema_version: event::SchemaVersion,
///         _: &[u8],
///     ) -> Result<event::Decoded<Self>, event::CodecError> {
///         Ok(event::Decoded::Unknown { type_name: type_name.to_owned(), schema_version })
///     }
/// }
///
/// let () = <Twice as event::Event>::__CHECK;
/// ```
pub trait Event: Sized + Send + Sync + 'static {
    /// The category these events belong to.
    const CATEGORY: category::CategoryName;

    /// Every type name this enum can encode or decode.
    const TYPES: &'static [&'static str];

    /// Checks [`Event::TYPES`] where a crate forces it. Not part of the API; never override it.
    #[doc(hidden)]
    const __CHECK: () = crate::__private::check_types(Self::TYPES);

    /// The stored type name of this event.
    fn type_name(&self) -> &'static str;

    /// The schema version this code writes for this event.
    fn schema_version(&self) -> SchemaVersion;

    /// Appends the codec output for this event to `out`.
    fn encode(&self, out: &mut Vec<u8>) -> Result<(), CodecError>;

    /// Decodes stored data, upcasting older schema versions.
    fn decode(
        type_name: &str,
        schema_version: SchemaVersion,
        data: &[u8],
    ) -> Result<Decoded<Self>, CodecError>;
}

/// The schema version of an event type: 1 for its first shape, bumped for every change that is not an
/// additive, defaulted field. There is no version 0.
///
/// Stored data is checked where it is read: a payload frame holding version 0 is corrupt.
///
/// ```
/// use pges_core::event;
///
/// const SECOND: event::SchemaVersion = event::SchemaVersion::new(2).unwrap();
///
/// assert_eq!(SECOND.get(), 2);
/// assert_eq!(event::SchemaVersion::new(0), None);
/// assert_eq!(event::SchemaVersion::FIRST.get(), 1);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct SchemaVersion(NonZeroU16);

impl SchemaVersion {
    /// Version 1, an event type's first shape.
    pub const FIRST: Self = Self(NonZeroU16::MIN);

    /// `version`, or `None` for 0.
    pub const fn new(version: u16) -> Option<Self> {
        match NonZeroU16::new(version) {
            Some(version) => Some(Self(version)),
            None => None,
        }
    }

    /// The number.
    pub const fn get(self) -> u16 {
        self.0.get()
    }
}

impl fmt::Display for SchemaVersion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

/// The result of decoding stored event data.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum Decoded<E> {
    /// A known event.
    Event(E),
    /// A type name or schema version this code does not know, written by a newer writer. Not an error.
    Unknown {
        /// The stored type name.
        type_name: String,
        /// The stored schema version.
        schema_version: SchemaVersion,
    },
}

/// What a reader does with [`Decoded::Unknown`] events.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[non_exhaustive]
pub enum OnUnknown {
    /// Ignore them and continue.
    Skip,
    /// Stop with an error. The default for loading aggregates, whose state would otherwise be wrong.
    #[default]
    Fail,
}

/// Prefix reserved for system event types, such as [`STREAM_DELETED`].
pub const SYSTEM_PREFIX: &str = "$";

/// The type name of the system event a stream deletion appends to the stream, in the stream's category.
/// Subscribers receive it as a system event, not through an application's event type.
pub const STREAM_DELETED: &str = "$stream-deleted";

/// Whether a type name is a system event type.
pub fn is_system_type(type_name: &str) -> bool {
    type_name.starts_with(SYSTEM_PREFIX)
}

/// The error a codec or upcaster reports.
pub type BoxError = Box<dyn std::error::Error + Send + Sync + 'static>;

/// An event could not be encoded or decoded.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum CodecError {
    /// The codec could not encode the event.
    #[error("could not encode {type_name}: {source}")]
    Encode {
        /// The event's type name.
        type_name: &'static str,
        /// The codec's error.
        source: BoxError,
    },
    /// The codec or an upcaster could not decode stored data.
    #[error("could not decode {type_name} version {schema_version}: {source}")]
    Decode {
        /// The stored type name.
        type_name: String,
        /// The stored schema version.
        schema_version: SchemaVersion,
        /// The codec's or upcaster's error.
        source: BoxError,
    },
}
