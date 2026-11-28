//! Reading one stream: its bounds first (`read/stream_head.sql`), then its commits from its category's
//! partition, one page at a time (`read/stream.sql`).

use std::{
    any::TypeId, collections::VecDeque, marker::PhantomData, ops::RangeInclusive, sync::Arc,
};

use futures_util::{Stream, stream as futures_stream};
use uuid::Uuid;

use pges_core::{category, commit, event, frame, stream, version};

use crate::{append, catalog, error, sql::statement, store};

impl store::Store {
    /// The events of one stream from version `from` on, in version order. An event this code does not know
    /// (written by a newer writer) ends the stream with [`ReadError::UnknownEvent`].
    ///
    /// A stream that does not exist yields nothing. The read ends at the stream's version when it began:
    /// events appended while it runs are not returned. Commits are fetched a page at a time, so a long stream
    /// is never held in memory at once.
    pub fn read_stream<E: event::Event>(
        &self,
        stream_id: stream::StreamId,
        from: version::Version,
    ) -> impl Stream<Item = Result<stream::StreamEvent<E>, ReadError>> + Send + 'static {
        self.read_stream_with(stream_id, from, event::OnUnknown::Fail)
    }

    /// Like [`store::Store::read_stream`], choosing what happens to events this code does not know.
    pub fn read_stream_with<E: event::Event>(
        &self,
        stream_id: stream::StreamId,
        from: version::Version,
        on_unknown: event::OnUnknown,
    ) -> impl Stream<Item = Result<stream::StreamEvent<E>, ReadError>> + Send + 'static {
        let state = ReadState::<E> {
            store: self.clone(),
            stream_id,
            from,
            on_unknown,
            pager: PagerState::NotOpened,
            buffer: VecDeque::new(),
        };

        futures_stream::try_unfold(state, |mut state| async move {
            loop {
                if let Some(event) = state.buffer.pop_front() {
                    return Ok(Some((event, state)));
                }

                match &mut state.pager {
                    PagerState::NotOpened => {
                        state.pager = match open_stream::<E>(&state.store, state.stream_id).await? {
                            Some((category, head)) => {
                                if head.floor_version >= append::to_i64(state.from) {
                                    return Err(truncated(state.stream_id, head.floor_version));
                                }
                                PagerState::Open(Pager::new(
                                    state.store.clone(),
                                    category,
                                    state.stream_id,
                                    state.from,
                                    head.commits(),
                                    head.version,
                                    state.on_unknown,
                                ))
                            }
                            None => PagerState::Finished,
                        };
                    }
                    PagerState::Open(pager) if pager.is_finished() => return Ok(None),
                    PagerState::Open(pager) => pager.next_page(&mut state.buffer).await?,
                    PagerState::Finished => return Ok(None),
                }
            }
        })
    }
}

/// A stream could not be read.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum ReadError {
    /// The events' category is not registered in the store.
    #[error("category {0} is not registered in the store")]
    UnknownCategory(category::CategoryName),
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
    /// The stream was deleted.
    #[error("stream {0} was deleted")]
    StreamDeleted(stream::StreamId),
    /// Retention removed the requested versions; versions up to `floor` are gone. Load from a snapshot.
    #[error("stream {stream_id} was truncated up to version {floor}")]
    Truncated {
        /// The stream.
        stream_id: stream::StreamId,
        /// The newest removed version.
        floor: version::Version,
    },
    /// A stored event type id is missing from the store's `event_types`.
    #[error("event {version} of stream {stream_id} has unknown event type id {type_id:?}")]
    UnknownEventType {
        /// The stream.
        stream_id: stream::StreamId,
        /// The event's version.
        version: version::Version,
        /// The stored id.
        type_id: frame::EventTypeId,
    },
    /// An event this code does not know: a type or schema version from a newer writer.
    #[error(
        "event {version} of stream {stream_id} is {type_name} version {schema_version}, which this code does not know"
    )]
    UnknownEvent {
        /// The stream.
        stream_id: stream::StreamId,
        /// The event's version.
        version: version::Version,
        /// The stored type name.
        type_name: String,
        /// The stored schema version.
        schema_version: event::SchemaVersion,
    },
    /// A commit's payload frame could not be decoded.
    #[error(transparent)]
    Frame(#[from] frame::FrameError),
    /// An event's data could not be decoded.
    #[error(transparent)]
    Decode(#[from] event::CodecError),
    /// A stored row breaks an invariant of the schema.
    #[error("stream {stream_id} has an invalid row: {reason}")]
    Corrupt {
        /// The stream.
        stream_id: stream::StreamId,
        /// What is wrong.
        reason: &'static str,
    },
    /// A database error.
    #[error(transparent)]
    Database(#[from] error::DbError),
}

impl From<sqlx::Error> for ReadError {
    fn from(source: sqlx::Error) -> Self {
        Self::Database(source.into())
    }
}

impl From<store::RegisterError> for ReadError {
    fn from(error: store::RegisterError) -> Self {
        match error {
            store::RegisterError::UnknownCategory(name) => Self::UnknownCategory(name),
            store::RegisterError::Database(error) => Self::Database(error),
        }
    }
}

/// Commits fetched per query.
const PAGE: i64 = 256;

/// A stream's row in `streams`.
pub(crate) struct Head {
    pub(crate) version: i64,
    pub(crate) floor_version: i64,
    pub(crate) first_commit_id: Uuid,
    pub(crate) last_commit_id: Uuid,
}

impl Head {
    /// The commit ids of all the stream's commits when the head was read.
    pub(crate) fn commits(&self) -> RangeInclusive<Uuid> {
        self.first_commit_id..=self.last_commit_id
    }
}

/// Registers `E`, then reads the stream's head and checks it can be read as `E`. `None` if the stream does
/// not exist.
pub(crate) async fn open_stream<E: event::Event>(
    store: &store::Store,
    stream_id: stream::StreamId,
) -> Result<Option<(catalog::Category, Head)>, ReadError> {
    let () = E::__CHECK;
    let category = store
        .ensure_registered(TypeId::of::<E>(), E::CATEGORY, E::TYPES)
        .await?;
    let row = store
        .inner
        .sql
        .stream_head
        .bind(statement::stream_head::Params {
            stream_id: stream_id.as_uuid(),
        })
        .fetch_optional(&store.inner.pool)
        .await?;
    let Some(row) = row else {
        return Ok(None);
    };

    let category_id = category::CategoryId::new(row.category_id);
    if category_id != category.id {
        let stream_category = store.category_by_id(category_id).await?;
        return Err(match stream_category {
            Some(stream_category) => ReadError::CategoryMismatch {
                stream_id,
                stream_category: stream_category.name,
                event_category: category.name,
            },
            None => ReadError::UnknownCategory(category.name),
        });
    }
    if append::StreamState::from_code(row.state) == Some(append::StreamState::Deleted) {
        return Err(ReadError::StreamDeleted(stream_id));
    }

    Ok(Some((
        category,
        Head {
            version: row.version,
            floor_version: row.floor_version,
            first_commit_id: row.first_commit_id,
            last_commit_id: row.last_commit_id,
        },
    )))
}

pub(crate) fn truncated(stream_id: stream::StreamId, floor: i64) -> ReadError {
    match append::to_version(floor) {
        Some(floor) => ReadError::Truncated { stream_id, floor },
        None => ReadError::Corrupt {
            stream_id,
            reason: "negative floor_version",
        },
    }
}

/// Fetches and decodes one stream's commits a page at a time, from the category's own partition.
pub(crate) struct Pager<E> {
    store: store::Store,
    category: catalog::Category,
    stream_id: stream::StreamId,
    /// The first version to emit.
    from: version::Version,
    /// The `last_version` bound of the next page.
    next_last_version: i64,
    /// From the stream's lowest commit id, or the snapshot's commit, to its last commit id when the read
    /// began; lets the planner skip the leaves outside.
    commits: RangeInclusive<Uuid>,
    /// The stream's version when the read began, the version of `commits.end()`: the pages must reach
    /// it, and no page holds a commit above it.
    last_version: i64,
    on_unknown: event::OnUnknown,
    finished: Finished,
    events: PhantomData<fn() -> E>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Finished {
    No,
    Yes,
}

impl<E: event::Event> Pager<E> {
    pub(crate) fn new(
        store: store::Store,
        category: catalog::Category,
        stream_id: stream::StreamId,
        from: version::Version,
        commits: RangeInclusive<Uuid>,
        last_version: i64,
        on_unknown: event::OnUnknown,
    ) -> Self {
        let next_last_version = append::to_i64(from);

        Self {
            store,
            category,
            stream_id,
            from,
            next_last_version,
            commits,
            last_version,
            on_unknown,
            finished: finished_after(next_last_version, last_version),
            events: PhantomData,
        }
    }

    pub(crate) fn is_finished(&self) -> bool {
        self.finished == Finished::Yes
    }

    /// Appends the next page's events to `out`; marks the pager finished after the last page: a short page,
    /// or one that reached the stream's version when the read began.
    pub(crate) async fn next_page(
        &mut self,
        out: &mut VecDeque<stream::StreamEvent<E>>,
    ) -> Result<(), ReadError> {
        let rows = self
            .category
            .read_stream
            .bind(statement::read_stream::Params {
                stream_id: self.stream_id.as_uuid(),
                from_version: self.next_last_version,
                first_commit: *self.commits.start(),
                last_commit: *self.commits.end(),
                page: PAGE,
            })
            .fetch_all_rows(&self.store.inner.pool)
            .await?;
        if rows.len() < PAGE as usize {
            self.finished = Finished::Yes;
        }

        for row in &rows {
            self.decode_commit(row.row()?, out).await?;
        }
        if self.is_finished() && self.next_last_version <= self.last_version {
            return Err(self.missing_versions().await);
        }
        if finished_after(self.next_last_version, self.last_version) == Finished::Yes {
            self.finished = Finished::Yes;
        }

        Ok(())
    }

    /// Why versions from `next_last_version` on that the head promised are not there: retention raised the
    /// floor and dropped them after the head was read, or `delete_stream` deleted the stream.
    async fn missing_versions(&self) -> ReadError {
        match open_stream::<E>(&self.store, self.stream_id).await {
            Ok(Some((_, head))) if head.floor_version >= self.next_last_version => {
                truncated(self.stream_id, head.floor_version)
            }
            Ok(_) => ReadError::Corrupt {
                stream_id: self.stream_id,
                reason: "versions below the stream's version are missing",
            },
            Err(error) => error,
        }
    }

    async fn decode_commit(
        &mut self,
        row: statement::read_stream::Row<'_>,
        out: &mut VecDeque<stream::StreamEvent<E>>,
    ) -> Result<(), ReadError> {
        let statement::read_stream::Row {
            last_version,
            event_count,
            commit_id,
            format,
            payload,
        } = row;

        let stream_id = self.stream_id;
        let corrupt = move |reason| ReadError::Corrupt { stream_id, reason };
        let commit_id = commit::CommitId::from_uuid(commit_id)
            .ok_or_else(|| corrupt("commit id is not a UUIDv7"))?;
        let event_count =
            u16::try_from(event_count).map_err(|_| corrupt("negative event_count"))?;
        let frame = frame::decode(frame::Format::from_code(format)?, payload, event_count)?;
        let metadata = Arc::new(frame.metadata);
        let recorded_at = commit_id.recorded_at();
        let first_version = last_version - i64::from(event_count) + 1;
        if first_version > self.next_last_version {
            return Err(self.missing_versions().await);
        }
        self.next_last_version = last_version + 1;

        for (offset, stored) in (0_i64..).zip(&frame.events) {
            let version = append::to_version(first_version + offset)
                .ok_or_else(|| corrupt("version below 1"))?;
            if version < self.from {
                continue;
            }

            if self.category.types.name(stored.type_id).is_none() {
                self.category.types = self.store.reload_types(self.category.id).await?;
            }
            let Some(type_name) = self.category.types.name(stored.type_id) else {
                return Err(ReadError::UnknownEventType {
                    stream_id: self.stream_id,
                    version,
                    type_id: stored.type_id,
                });
            };

            match E::decode(type_name, stored.schema_version, stored.data)? {
                event::Decoded::Event(event) => out.push_back(stream::StreamEvent::new(
                    self.stream_id,
                    self.category.name,
                    version,
                    commit_id,
                    recorded_at,
                    metadata.clone(),
                    event,
                )),
                event::Decoded::Unknown {
                    type_name,
                    schema_version,
                } if self.on_unknown != event::OnUnknown::Skip => {
                    return Err(ReadError::UnknownEvent {
                        stream_id: self.stream_id,
                        version,
                        type_name,
                        schema_version,
                    });
                }
                _ => {}
            }
        }

        Ok(())
    }
}

/// Whether a pager whose next page starts at `next_last_version` is done: the commits it reads end at the
/// commit of `last_version`, so no page above it holds a row.
fn finished_after(next_last_version: i64, last_version: i64) -> Finished {
    if next_last_version > last_version {
        Finished::Yes
    } else {
        Finished::No
    }
}

struct ReadState<E> {
    store: store::Store,
    stream_id: stream::StreamId,
    from: version::Version,
    on_unknown: event::OnUnknown,
    pager: PagerState<E>,
    buffer: VecDeque<stream::StreamEvent<E>>,
}

enum PagerState<E> {
    NotOpened,
    Open(Pager<E>),
    Finished,
}
