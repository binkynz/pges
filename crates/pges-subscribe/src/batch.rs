//! What a handler receives: a [`Batch`] of events in position order, decoded lazily per event type.

use std::{
    collections::BTreeSet,
    sync::{Arc, Mutex, PoisonError},
};

use time::OffsetDateTime;

use pges_core::{category, commit, event, metadata, position, stream, version};

/// Events in store position order, from one or more categories.
///
/// A batch holds every event the subscription's categories have between the previous cursor and
/// [`Batch::last_position`]: positions increase, and none is skipped or repeated, also across restarts.
/// Payload frames are already unpacked; event data is decoded per type with [`Batch::decode`].
#[derive(Debug, Clone)]
pub struct Batch {
    records: Vec<RawRecorded>,
    last_position: position::Position,
    on_unknown: event::OnUnknown,
    /// The categories the subscription or backfill was given; `None` when it reads every category.
    listed: Option<Arc<[category::CategoryName]>>,
}

impl Batch {
    /// Every record, in position order.
    pub fn iter(&self) -> impl Iterator<Item = &RawRecorded> {
        self.records.iter()
    }

    /// The events of `E`'s category, decoded, in position order. System events and other categories are
    /// skipped.
    ///
    /// A subscription or backfill of listed categories never has events of another category, so decoding
    /// one it does not read yields nothing. That is almost always a mistake in the handler: the first such
    /// call for each category logs a `tracing` warning.
    ///
    /// An event type or schema version `E` does not know (a newer writer) is skipped or yields
    /// [`DecodeError::UnknownEvent`], following the subscription's
    /// [`on_unknown`](crate::subscription::Builder::on_unknown).
    pub fn decode<E: event::Event>(
        &self,
    ) -> impl Iterator<Item = Result<position::LogEvent<E>, DecodeError>> {
        let () = E::__CHECK;
        let on_unknown = self.on_unknown;
        if let Some(listed) = &self.listed
            && !listed.contains(&E::CATEGORY)
        {
            warn_not_read(E::CATEGORY, listed);
        }

        self.records.iter().filter_map(move |record| {
            let RawRecorded::Event(raw) = record else {
                return None;
            };
            if raw.category != E::CATEGORY {
                return None;
            }

            match E::decode(&raw.type_name, raw.schema_version, &raw.data) {
                Ok(event::Decoded::Event(event)) => Some(Ok(position::LogEvent::new(
                    raw.position,
                    stream::StreamEvent::new(
                        raw.stream_id,
                        raw.category,
                        raw.version,
                        raw.commit_id,
                        raw.recorded_at,
                        raw.metadata.clone(),
                        event,
                    ),
                ))),
                Ok(event::Decoded::Unknown { .. }) if on_unknown == event::OnUnknown::Skip => None,
                Ok(event::Decoded::Unknown {
                    type_name,
                    schema_version,
                }) => Some(Err(DecodeError::UnknownEvent {
                    position: raw.position,
                    type_name,
                    schema_version,
                })),
                Ok(_) => None,
                Err(source) => Some(Err(DecodeError::Codec {
                    position: raw.position,
                    source,
                })),
            }
        })
    }

    /// The system events, in position order.
    pub fn system(&self) -> impl Iterator<Item = &SystemEvent> {
        self.records.iter().filter_map(|record| match record {
            RawRecorded::System(system) => Some(system),
            RawRecorded::Event(_) => None,
        })
    }

    /// The cursor after this batch: the merge cut. It can be past the last record, and a batch can have no
    /// records at all, when other categories took the positions in between.
    pub fn last_position(&self) -> position::Position {
        self.last_position
    }

    /// The number of records.
    pub fn len(&self) -> usize {
        self.records.len()
    }

    /// Whether the batch has no records.
    pub fn is_empty(&self) -> bool {
        self.records.is_empty()
    }

    pub(crate) fn new(
        records: Vec<RawRecorded>,
        last_position: position::Position,
        on_unknown: event::OnUnknown,
        listed: Option<Arc<[category::CategoryName]>>,
    ) -> Self {
        Self {
            records,
            last_position,
            on_unknown,
            listed,
        }
    }

    pub(crate) fn first_position(&self) -> Option<position::Position> {
        self.records.first().map(RawRecorded::position)
    }

    pub(crate) fn into_records(self) -> Vec<RawRecorded> {
        self.records
    }
}

impl RawRecorded {
    /// The stream the record belongs to.
    pub(crate) fn stream_id(&self) -> stream::StreamId {
        match self {
            Self::Event(raw) => raw.stream_id,
            Self::System(
                SystemEvent::StreamDeleted { stream_id, .. }
                | SystemEvent::Unrecognised { stream_id, .. },
            ) => *stream_id,
        }
    }
}

/// One record of a batch: an application event, or a system event written by the store itself.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum RawRecorded {
    /// An application event, not yet decoded.
    Event(RawEvent),
    /// A system event (type name starting with `$`).
    System(SystemEvent),
}

impl RawRecorded {
    /// The record's position in the store's log.
    pub fn position(&self) -> position::Position {
        match self {
            Self::Event(raw) => raw.position,
            Self::System(system) => system.position(),
        }
    }
}

/// An event as stored: its type name, schema version and codec output, with where and when it was written.
///
/// A handler passes [`RawEvent::follow_up`] (or the decoded event's `follow_up`) to the appends the event
/// causes. Subscriptions do not set an [`ambient::scope`](pges_postgres::ambient::scope) around a handler,
/// because a batch holds events of many commits and no single follow-up fits it.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct RawEvent {
    /// Its position in the store's log.
    pub position: position::Position,
    /// The stream it belongs to.
    pub stream_id: stream::StreamId,
    /// The stream's category.
    pub category: category::CategoryName,
    /// The category's id in the store.
    pub category_id: category::CategoryId,
    /// Its version within the stream.
    pub version: version::Version,
    /// The commit that wrote it.
    pub commit_id: commit::CommitId,
    /// When the commit was recorded, from the commit id.
    pub recorded_at: OffsetDateTime,
    /// The commit's metadata, shared by every event of the commit.
    pub metadata: Arc<metadata::Metadata>,
    /// The stored type name.
    pub type_name: Arc<str>,
    /// The stored schema version.
    pub schema_version: event::SchemaVersion,
    /// The codec output.
    pub data: Vec<u8>,
}

impl RawEvent {
    /// The metadata for commits this event causes: correlation carried over (or this commit's id when the
    /// commit had none), causation this commit's id, trace carried over, and no actor. See
    /// [`metadata::Metadata::follow_up`].
    pub fn follow_up(&self) -> metadata::Metadata {
        self.metadata.follow_up(self.commit_id)
    }

    /// The trace context the commit was appended in, as an OpenTelemetry parent context for the span that
    /// handles this event. See [`metadata::TraceContext::to_context`].
    #[cfg(feature = "otel")]
    pub fn trace_parent(&self) -> Option<opentelemetry::Context> {
        self.metadata.trace_parent()
    }
}

/// An event the store writes itself. Typed decoding with [`Batch::decode`] skips them.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum SystemEvent {
    /// [`event::STREAM_DELETED`]: the stream was deleted. Projections purge what they hold for it.
    StreamDeleted {
        /// The event's position.
        position: position::Position,
        /// The deleted stream.
        stream_id: stream::StreamId,
        /// The stream's category.
        category: category::CategoryName,
    },
    /// A system event type this version of the crate does not know.
    Unrecognised {
        /// The event's position.
        position: position::Position,
        /// The stored type name.
        type_name: String,
        /// The stream it was written to.
        stream_id: stream::StreamId,
    },
}

impl SystemEvent {
    /// The event's position in the store's log.
    pub fn position(&self) -> position::Position {
        match self {
            Self::StreamDeleted { position, .. } | Self::Unrecognised { position, .. } => *position,
        }
    }
}

/// An event of a batch could not be decoded as the requested type.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum DecodeError {
    /// A type name or schema version this code does not know, and the subscription uses
    /// [`OnUnknown::Fail`](event::OnUnknown::Fail).
    #[error(
        "event at position {position} is {type_name} version {schema_version}, which this code does not know"
    )]
    UnknownEvent {
        /// The event's position.
        position: position::Position,
        /// The stored type name.
        type_name: String,
        /// The stored schema version.
        schema_version: event::SchemaVersion,
    },
    /// The codec or an upcaster failed.
    #[error("event at position {position}: {source}")]
    Codec {
        /// The event's position.
        position: position::Position,
        /// The codec's error.
        source: event::CodecError,
    },
}

/// Warns once per process for each category decoded from batches that cannot hold it.
fn warn_not_read(category: category::CategoryName, listed: &[category::CategoryName]) {
    static WARNED: Mutex<BTreeSet<category::CategoryName>> = Mutex::new(BTreeSet::new());

    let first = WARNED
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .insert(category);
    if first {
        let listed: Vec<&str> = listed.iter().map(|name| name.as_str()).collect();
        tracing::warn!(
            %category,
            ?listed,
            "Batch::decode was called for a category the subscription does not read; it yields nothing"
        );
    }
}
