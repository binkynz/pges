//! Deleting a stream and its events, for personal data that cannot be crypto-shredded.

use std::{any::TypeId, marker::PhantomData};

use sqlx::Connection;

use pges_core::{category, event, frame, metadata, stream, version};

use crate::{ambient, append, catalog, error, sql::statement, store};

impl store::Store {
    /// Deletes a stream of `E`'s category, if its version is still `expected`: a [`version::Version`], or
    /// [`DeleteExpected::Any`].
    ///
    /// In one transaction it appends a `$stream-deleted` system event to the stream, marks the stream
    /// deleted, and deletes the stream's other commits, its snapshot and its string key. Subscribers see the
    /// system event as `SystemEvent::StreamDeleted` once it is sequenced, so projections can purge what they
    /// hold for the stream; the deleted commits are never delivered. Afterwards appends fail with
    /// [`append::AppendError::StreamDeleted`], and reads and loads with
    /// [`crate::read::ReadError::StreamDeleted`]. The stream's id stays taken.
    ///
    /// Prefer crypto-shredding for personal data: deleting a key also covers replicas and backups, which this
    /// does not. Returns the `$stream-deleted` commit. Its metadata is the [`ambient::scope`]'s, with the
    /// current span's trace under the `otel` feature.
    ///
    /// Fails with [`DeleteStreamError::NotFound`] for a stream that does not exist and
    /// [`DeleteStreamError::Conflict`] when its version is not `expected`.
    /// Archived streams cannot be deleted ([`DeleteStreamError::StreamArchived`]). A lost connection is not
    /// retried: the error is a [`DeleteStreamError::Database`] whose
    /// [`is_retryable`](error::DbError::is_retryable) is true, and the stream is either deleted or not.
    pub async fn delete_stream<E: event::Event>(
        &self,
        stream_id: stream::StreamId,
        expected: impl Into<DeleteExpected>,
    ) -> Result<append::AppendResult, DeleteStreamError> {
        let expected = expected.into();
        let tombstone = Tombstone::<E>(PhantomData);
        let metadata = ambient::resolve(metadata::Metadata::default());
        let mut prepared = append::prepare(
            self,
            append::PoolLease::Unknown,
            &tombstone,
            stream_id.into(),
            expected.to_expected_version(),
            &metadata,
            None,
        )
        .await
        .map_err(|error| DeleteStreamError::from_append(error, expected))?;

        let mut conn = self.inner.pool.acquire().await?;
        let mut tx = conn.begin().await?;
        let result = delete_in(self, &mut tx, stream_id, expected, &mut prepared).await;
        match result {
            Ok(result) => {
                tx.commit().await?;
                Ok(result)
            }
            Err(error) => {
                let _rolled_back = tx.rollback().await;
                Err(error)
            }
        }
    }
}

/// The version [`store::Store::delete_stream`] expects the stream at. A stream that does not exist cannot be
/// deleted, so unlike [`version::ExpectedVersion`] there is no `NoStream`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum DeleteExpected {
    /// Whatever its version.
    Any,
    /// Exactly this version.
    Exact(version::Version),
}

impl DeleteExpected {
    fn to_expected_version(self) -> version::ExpectedVersion {
        match self {
            Self::Any => version::ExpectedVersion::Any,
            Self::Exact(version) => version::ExpectedVersion::Exact(version),
        }
    }
}

impl From<version::Version> for DeleteExpected {
    fn from(version: version::Version) -> Self {
        Self::Exact(version)
    }
}

/// A stream could not be deleted. Nothing was changed.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum DeleteStreamError {
    /// The stream does not exist.
    #[error("stream {0} does not exist")]
    NotFound(stream::StreamId),
    /// The stream's version is not the expected one.
    #[error("stream {stream_id} is at version {actual:?}, not {expected:?}")]
    Conflict {
        /// The stream.
        stream_id: stream::StreamId,
        /// What the caller expected.
        expected: DeleteExpected,
        /// The stream's version.
        actual: Option<version::Version>,
    },
    /// The stream belongs to another category than the events'.
    #[error("stream {stream_id} belongs to category {stream_category}, not {event_category}")]
    CategoryMismatch {
        /// The stream.
        stream_id: stream::StreamId,
        /// The stream's category.
        stream_category: category::CategoryName,
        /// The events' category.
        event_category: category::CategoryName,
    },
    /// The stream was already deleted.
    #[error("stream {0} was already deleted")]
    StreamDeleted(stream::StreamId),
    /// The stream is archived.
    #[error("stream {0} is archived")]
    StreamArchived(stream::StreamId),
    /// The events' category is not registered in the store.
    #[error("category {0} is not registered in the store")]
    UnknownCategory(category::CategoryName),
    /// The `$stream-deleted` commit could not be written for another reason, such as a commit id outside
    /// the clock skew tolerance.
    #[error("the $stream-deleted commit could not be written: {0}")]
    Write(append::AppendError),
    /// A database error.
    #[error(transparent)]
    Database(#[from] error::DbError),
}

impl From<sqlx::Error> for DeleteStreamError {
    fn from(source: sqlx::Error) -> Self {
        Self::Database(source.into())
    }
}

impl DeleteStreamError {
    fn from_append(error: append::AppendError, expected: DeleteExpected) -> Self {
        match error {
            append::AppendError::Conflict {
                stream_id, actual, ..
            } => Self::Conflict {
                stream_id,
                expected,
                actual,
            },
            append::AppendError::CategoryMismatch {
                stream_id,
                stream_category,
                event_category,
            } => Self::CategoryMismatch {
                stream_id,
                stream_category,
                event_category,
            },
            append::AppendError::StreamDeleted(stream_id) => Self::StreamDeleted(stream_id),
            append::AppendError::StreamArchived(stream_id) => Self::StreamArchived(stream_id),
            append::AppendError::UnknownCategory(name) => Self::UnknownCategory(name),
            append::AppendError::Database(error) => Self::Database(error),
            other => Self::Write(other),
        }
    }
}

/// The `$stream-deleted` event's data.
const TOMBSTONE_DATA: &[u8] = b"{}";

/// The one event a deletion appends, in `E`'s category. Its type key differs per category, so the catalog
/// registers the type name in each.
struct Tombstone<E>(PhantomData<fn() -> E>);

impl<E: event::Event> append::EventBatch for Tombstone<E> {
    fn type_key(&self) -> TypeId {
        TypeId::of::<Tombstone<E>>()
    }

    fn category(&self) -> category::CategoryName {
        E::CATEGORY
    }

    fn types(&self) -> &'static [&'static str] {
        &[event::STREAM_DELETED]
    }

    fn len(&self) -> usize {
        1
    }

    fn encode(
        &self,
        types: &catalog::TypeTable,
        metadata: &metadata::Metadata,
        format: frame::Format,
    ) -> Result<Vec<u8>, append::AppendError> {
        let Some(type_id) = types.id(event::STREAM_DELETED) else {
            return Err(append::AppendError::UnknownCategory(E::CATEGORY));
        };

        frame::encode(
            format,
            metadata,
            &[frame::FrameEvent::new(
                type_id,
                event::SchemaVersion::FIRST,
                TOMBSTONE_DATA,
            )],
        )
        .map_err(|source| {
            append::AppendError::Encode(event::CodecError::Encode {
                type_name: event::STREAM_DELETED,
                source: source.into(),
            })
        })
    }
}

/// Locks the stream, appends the tombstone, marks the stream deleted and deletes its data.
async fn delete_in(
    store: &store::Store,
    conn: &mut sqlx::PgConnection,
    stream_id: stream::StreamId,
    expected: DeleteExpected,
    prepared: &mut append::Prepared,
) -> Result<append::AppendResult, DeleteStreamError> {
    // Locked first, so an `Any` expectation cannot create the stream.
    let locked = store
        .inner
        .sql
        .lock_stream
        .bind(statement::lock_stream::Params {
            stream_id: stream_id.as_uuid(),
        })
        .fetch_optional(&mut *conn)
        .await?;
    if locked.is_none() {
        return Err(DeleteStreamError::NotFound(stream_id));
    }

    let result = append::append_in_transaction(store, conn, prepared, None)
        .await
        .map_err(|error| DeleteStreamError::from_append(error, expected))?;
    let first_commit_id = store
        .inner
        .sql
        .mark_deleted
        .bind(statement::mark_deleted::Params {
            stream_id: stream_id.as_uuid(),
        })
        .fetch_one(&mut *conn)
        .await?
        .first_commit_id;
    store
        .inner
        .sql
        .delete_commits
        .bind(statement::delete_commits::Params {
            stream_id: stream_id.as_uuid(),
            category_id: prepared.category_id().get(),
            first_commit_id,
            deleted_commit_id: result.commit_id.as_uuid(),
        })
        .execute(&mut *conn)
        .await?;
    store
        .inner
        .sql
        .delete_snapshot
        .bind(statement::delete_snapshot::Params {
            stream_id: stream_id.as_uuid(),
        })
        .execute(&mut *conn)
        .await?;

    Ok(result)
}
