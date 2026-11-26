//! Streams: their identity and the events read from them.

use std::{fmt, sync::Arc};

use time::OffsetDateTime;
use uuid::Uuid;

use crate::{category, commit, metadata, version};

/// Identifies a stream within a store.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct StreamId(Uuid);

impl StreamId {
    /// A new time-ordered id. Prefer it when the stream has no natural key: new streams land at the right
    /// edge of the primary key index.
    pub fn new_v7() -> Self {
        Self(Uuid::now_v7())
    }

    /// Wraps an existing id.
    pub const fn from_uuid(id: Uuid) -> Self {
        Self(id)
    }

    /// The id for a natural key, such as an order number: a UUIDv5 of the key in a namespace derived from
    /// the category, so the same key in two categories gives two ids. The key is kept and stored in
    /// `streams.stream_key` when the stream is created.
    pub fn from_key(category: category::CategoryName, key: &str) -> StreamRef {
        let namespace = Uuid::new_v5(&KEY_NAMESPACE, category.as_str().as_bytes());

        StreamRef {
            id: Self(Uuid::new_v5(&namespace, key.as_bytes())),
            key: Some(Arc::from(key)),
        }
    }

    /// The underlying id.
    pub const fn as_uuid(&self) -> Uuid {
        self.0
    }
}

impl fmt::Display for StreamId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

/// A stream id, with the natural key it was derived from when there is one. Appends take
/// `impl Into<StreamRef>`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub struct StreamRef {
    /// The stream.
    pub id: StreamId,
    /// The natural key, if the id came from [`StreamId::from_key`].
    pub key: Option<Arc<str>>,
}

impl From<StreamId> for StreamRef {
    fn from(id: StreamId) -> Self {
        Self { id, key: None }
    }
}

/// An event read from a stream, with where and when it was recorded.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct StreamEvent<E> {
    /// The stream it belongs to.
    pub stream_id: StreamId,
    /// The stream's category.
    pub category: category::CategoryName,
    /// Its version within the stream.
    pub version: version::Version,
    /// The commit that wrote it, together with the other events of the same append.
    pub commit_id: commit::CommitId,
    /// When the commit was recorded, taken from the commit id (millisecond precision).
    pub recorded_at: OffsetDateTime,
    /// The commit's metadata, shared by every event of the commit.
    pub metadata: Arc<metadata::Metadata>,
    /// The event.
    pub event: E,
}

impl<E> StreamEvent<E> {
    /// Assembles an event read from storage. Used by the store crates.
    pub fn new(
        stream_id: StreamId,
        category: category::CategoryName,
        version: version::Version,
        commit_id: commit::CommitId,
        recorded_at: OffsetDateTime,
        metadata: Arc<metadata::Metadata>,
        event: E,
    ) -> Self {
        Self {
            stream_id,
            category,
            version,
            commit_id,
            recorded_at,
            metadata,
            event,
        }
    }

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

/// Namespace for [`StreamId::from_key`]. Fixed forever: changing it changes every key-derived stream id.
const KEY_NAMESPACE: Uuid = Uuid::from_u128(0x6f1c_29d4_5b7e_4a83_9e2d_0c41_7a55_b3e8);

#[cfg(test)]
mod tests {
    use super::*;

    const JOBS: category::CategoryName = category!("jobs");
    const ORDERS: category::CategoryName = category!("orders");

    #[test]
    fn key_derived_ids_are_stable_and_scoped_by_category() {
        let a = StreamId::from_key(JOBS, "JOB-1");
        let b = StreamId::from_key(JOBS, "JOB-1");
        let other_category = StreamId::from_key(ORDERS, "JOB-1");

        assert_eq!(a, b);
        assert_ne!(a.id, other_category.id);
        assert_eq!(a.key.as_deref(), Some("JOB-1"));
        assert_eq!(a.id.as_uuid().get_version_num(), 5);
    }

    #[test]
    fn plain_ids_have_no_key() {
        let id = StreamId::new_v7();

        assert_eq!(StreamRef::from(id).key, None);
    }
}
