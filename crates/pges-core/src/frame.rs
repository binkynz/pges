//! The payload frame: how one commit's metadata and events are packed into `commits.payload`.
//!
//! Format 1:
//!
//! ```text
//! payload := header event{event_count}
//! header  := varint metadata_len  metadata (protobuf)
//! event   := varint event_type_id  varint schema_version  varint data_len  data
//! ```
//!
//! `event_type_id` refers to the store's `event_types` table; `data` is the event's codec output. The event
//! count is stored next to the payload (`commits.event_count`), not inside it.

use prost::bytes::Buf;

use crate::{event, metadata};

/// Payload frame formats, stored in `commits.format`. Readers support every format ever written; writers
/// switch to a newer one only after every reader is upgraded.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum Format {
    /// Format 1.
    V1,
}

impl Format {
    /// The newest format this crate writes.
    pub const LATEST: Self = Self::V1;

    /// The value stored in `commits.format`.
    pub const fn code(self) -> i16 {
        match self {
            Self::V1 => 1,
        }
    }

    /// The format for a stored code.
    pub const fn from_code(code: i16) -> Result<Self, FrameError> {
        match code {
            1 => Ok(Self::V1),
            other => Err(FrameError::UnknownFormat(other)),
        }
    }
}

/// An id from the store's `event_types` table.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct EventTypeId(u32);

impl EventTypeId {
    /// Wraps an id.
    pub const fn new(id: u32) -> Self {
        Self(id)
    }

    /// The number.
    pub const fn get(self) -> u32 {
        self.0
    }
}

/// One event inside a frame, borrowing its data from the payload.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub struct FrameEvent<'a> {
    /// The event's type.
    pub type_id: EventTypeId,
    /// The event's schema version.
    pub schema_version: event::SchemaVersion,
    /// The codec output.
    pub data: &'a [u8],
}

impl<'a> FrameEvent<'a> {
    /// Describes one event to encode.
    pub const fn new(
        type_id: EventTypeId,
        schema_version: event::SchemaVersion,
        data: &'a [u8],
    ) -> Self {
        Self {
            type_id,
            schema_version,
            data,
        }
    }
}

/// A decoded frame.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct Frame<'a> {
    /// The commit's metadata.
    pub metadata: metadata::Metadata,
    /// The commit's events, in version order.
    pub events: Vec<FrameEvent<'a>>,
}

/// Most events one commit can hold (`commits.event_count` is an `int2`).
pub const MAX_EVENTS: usize = i16::MAX as usize;

/// Packs a commit. Fails if there are no events or more than [`MAX_EVENTS`].
pub fn encode(
    format: Format,
    metadata: &metadata::Metadata,
    events: &[FrameEvent<'_>],
) -> Result<Vec<u8>, FrameError> {
    match format {
        Format::V1 => encode_v1(metadata, events),
    }
}

/// Unpacks a commit written in `format` holding `event_count` events. The whole payload must be consumed.
pub fn decode(format: Format, payload: &[u8], event_count: u16) -> Result<Frame<'_>, FrameError> {
    match format {
        Format::V1 => decode_v1(payload, event_count),
    }
}

/// A payload frame could not be encoded or decoded.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum FrameError {
    /// `commits.format` holds a format this crate does not know: a newer writer.
    #[error("unknown payload format {0}")]
    UnknownFormat(i16),
    /// A commit must hold at least one event.
    #[error("a commit must hold at least one event")]
    NoEvents,
    /// A commit holds more than [`MAX_EVENTS`] events.
    #[error("a commit can hold at most {MAX_EVENTS} events, got {0}")]
    TooManyEvents(usize),
    /// The payload ends before the frame does.
    #[error("payload is truncated")]
    Truncated,
    /// Bytes are left after the last event.
    #[error("payload has {0} bytes after the last event")]
    TrailingBytes(usize),
    /// A varint does not fit the field it encodes, or a schema version is 0.
    #[error("payload field {field} is out of range")]
    OutOfRange {
        /// The field's name.
        field: &'static str,
    },
    /// The metadata could not be decoded.
    #[error(transparent)]
    Metadata(#[from] metadata::MetadataError),
}

fn encode_v1(
    metadata: &metadata::Metadata,
    events: &[FrameEvent<'_>],
) -> Result<Vec<u8>, FrameError> {
    if events.is_empty() {
        return Err(FrameError::NoEvents);
    }
    if events.len() > MAX_EVENTS {
        return Err(FrameError::TooManyEvents(events.len()));
    }

    let metadata = metadata.encode();
    let data_len: usize = events.iter().map(|e| e.data.len() + 3 * 10).sum();
    let mut out = Vec::with_capacity(10 + metadata.len() + data_len);

    write_varint(&mut out, metadata.len());
    out.extend_from_slice(&metadata);
    for event in events {
        prost::encoding::encode_varint(u64::from(event.type_id.get()), &mut out);
        prost::encoding::encode_varint(u64::from(event.schema_version.get()), &mut out);
        write_varint(&mut out, event.data.len());
        out.extend_from_slice(event.data);
    }

    Ok(out)
}

fn decode_v1(payload: &[u8], event_count: u16) -> Result<Frame<'_>, FrameError> {
    if event_count == 0 {
        return Err(FrameError::NoEvents);
    }

    let mut rest = payload;
    let metadata_bytes = take(&mut rest, "metadata_len")?;
    let metadata = metadata::Metadata::decode(metadata_bytes)?;

    let mut events = Vec::with_capacity(usize::from(event_count));
    for _ in 0..event_count {
        let type_id = read_varint(&mut rest)?;
        let schema_version = read_varint(&mut rest)?;
        let data = take(&mut rest, "data_len")?;
        events.push(FrameEvent {
            type_id: EventTypeId(u32::try_from(type_id).map_err(|_| FrameError::OutOfRange {
                field: "event_type_id",
            })?),
            schema_version: u16::try_from(schema_version)
                .ok()
                .and_then(event::SchemaVersion::new)
                .ok_or(FrameError::OutOfRange {
                    field: "schema_version",
                })?,
            data,
        });
    }

    if !rest.is_empty() {
        return Err(FrameError::TrailingBytes(rest.len()));
    }

    Ok(Frame { metadata, events })
}

fn write_varint(out: &mut Vec<u8>, value: usize) {
    prost::encoding::encode_varint(value as u64, out);
}

fn read_varint(rest: &mut &[u8]) -> Result<u64, FrameError> {
    if rest.is_empty() {
        return Err(FrameError::Truncated);
    }

    prost::encoding::decode_varint(rest).map_err(|_| FrameError::Truncated)
}

/// Reads a varint length and then that many bytes.
fn take<'a>(rest: &mut &'a [u8], field: &'static str) -> Result<&'a [u8], FrameError> {
    let len = usize::try_from(read_varint(rest)?).map_err(|_| FrameError::OutOfRange { field })?;
    if rest.remaining() < len {
        return Err(FrameError::Truncated);
    }

    let (taken, remaining) = rest.split_at(len);
    *rest = remaining;

    Ok(taken)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;

    fn v(n: u16) -> event::SchemaVersion {
        event::SchemaVersion::new(n).unwrap()
    }

    fn metadata() -> metadata::Metadata {
        let mut metadata = metadata::Metadata::default();
        metadata.correlation_id = Some(metadata::CorrelationId::new_v7());
        metadata.actor = Some("user:42".into());
        metadata.trace = Some(metadata::TraceContext::new(
            "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01",
        ));
        metadata.extra = BTreeMap::from([("tenant".into(), "t1".into())]);
        metadata
    }

    #[test]
    fn round_trips() {
        let events = [
            FrameEvent::new(EventTypeId::new(7), v(2), br#"{"a":1}"#),
            FrameEvent::new(EventTypeId::new(300), v(1), b""),
            FrameEvent::new(EventTypeId::new(u32::MAX), v(u16::MAX), &[0xff; 200]),
        ];
        let metadata = metadata();

        let payload = encode(Format::V1, &metadata, &events).unwrap();
        let frame = decode(Format::V1, &payload, 3).unwrap();

        assert_eq!(frame.metadata, metadata);
        assert_eq!(frame.events, events);
    }

    #[test]
    fn empty_metadata_costs_one_byte() {
        let payload = encode(
            Format::V1,
            &metadata::Metadata::default(),
            &[FrameEvent::new(EventTypeId::new(1), v(1), b"x")],
        )
        .unwrap();

        assert_eq!(payload, [0, 1, 1, 1, b'x']);
    }

    #[test]
    fn rejects_bad_payloads() {
        let payload = encode(
            Format::V1,
            &metadata(),
            &[FrameEvent::new(EventTypeId::new(1), v(1), b"data")],
        )
        .unwrap();

        assert!(matches!(
            decode(Format::V1, &payload, 2),
            Err(FrameError::Truncated)
        ));
        assert!(matches!(
            decode(Format::V1, &payload[..payload.len() - 1], 1),
            Err(FrameError::Truncated)
        ));
        assert!(matches!(
            decode(Format::V1, &payload, 0),
            Err(FrameError::NoEvents)
        ));

        let mut trailing = payload.clone();
        trailing.push(0);
        assert!(matches!(
            decode(Format::V1, &trailing, 1),
            Err(FrameError::TrailingBytes(1))
        ));
    }

    #[test]
    fn a_stored_schema_version_of_zero_is_out_of_range() {
        let payload = [0, 1, 0, 1, b'x'];

        assert!(matches!(
            decode(Format::V1, &payload, 1),
            Err(FrameError::OutOfRange {
                field: "schema_version"
            })
        ));
    }

    #[test]
    fn enforces_event_count_limits() {
        assert!(matches!(
            encode(Format::V1, &metadata::Metadata::default(), &[]),
            Err(FrameError::NoEvents)
        ));

        let many = vec![FrameEvent::new(EventTypeId::new(1), v(1), b""); MAX_EVENTS + 1];
        assert!(matches!(
            encode(Format::V1, &metadata::Metadata::default(), &many),
            Err(FrameError::TooManyEvents(n)) if n == MAX_EVENTS + 1
        ));
    }

    #[test]
    fn ignores_metadata_fields_it_does_not_know() {
        // Field 99, wire type 2 (length-delimited), 3 bytes: what a newer writer might add.
        let mut unknown_field = vec![0x9a, 0x06, 3, b'n', b'e', b'w'];
        let mut payload = Vec::new();
        prost::encoding::encode_varint(unknown_field.len() as u64, &mut payload);
        payload.append(&mut unknown_field);
        payload.extend_from_slice(&[1, 1, 1, b'x']);

        let frame = decode(Format::V1, &payload, 1).unwrap();

        assert_eq!(frame.metadata, metadata::Metadata::default());
    }

    #[test]
    fn knows_its_format_codes() {
        assert_eq!(
            Format::from_code(Format::LATEST.code()).unwrap(),
            Format::V1
        );
        assert!(matches!(
            Format::from_code(2),
            Err(FrameError::UnknownFormat(2))
        ));
    }
}
