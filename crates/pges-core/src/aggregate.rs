//! Aggregates: state folded from a stream's events.

use std::num::{NonZeroU16, NonZeroU32};

use serde::{Serialize, de::DeserializeOwned};

use crate::event;

/// State rebuilt by applying a stream's events in version order, starting from `Default`.
///
/// Command handling stays in the application: load the aggregate, decide, then append with the version it
/// was loaded at.
pub trait Aggregate: Default + Send + Sync + 'static {
    /// The events this aggregate is built from.
    type Event: event::Event;

    /// Applies one event. Must not fail: an event that was appended has happened.
    fn apply(&mut self, event: &Self::Event);
}

/// An aggregate whose state can be stored as a snapshot, so loading replays only the events after it.
pub trait Snapshotted: Aggregate + Serialize + DeserializeOwned {
    /// Raise when the snapshot shape changes; snapshots with another version are ignored and rebuilt by
    /// replay. A stored snapshot is replaced only by one of the same or a higher version, so after going back
    /// to code with a lower version its snapshots are not saved until that code raises it again.
    const SNAPSHOT_VERSION: NonZeroU16;

    /// When the store writes snapshots.
    fn policy() -> SnapshotPolicy {
        SnapshotPolicy::Manual
    }
}

/// When the store writes snapshots of an aggregate.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum SnapshotPolicy {
    /// Only when the application asks.
    Manual,
    /// After loading, when at least this many events were replayed since the last snapshot.
    EveryN(NonZeroU32),
}
