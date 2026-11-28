//! Loading aggregates, with or without snapshots.

use std::{collections::VecDeque, ops::RangeInclusive};

use uuid::Uuid;

use pges_core::{aggregate, category, commit, event, stream, version};

use crate::{append, catalog, error, read, sql::statement, store};

impl store::Store {
    /// Rebuilds an aggregate by replaying its stream from the start, up to its version when the load began.
    ///
    /// A stream that does not exist loads as `A::default()` with no version. An event this code does not
    /// know fails the load: the state would otherwise be wrong.
    pub async fn load<A: aggregate::Aggregate>(
        &self,
        stream_id: stream::StreamId,
    ) -> Result<Loaded<A>, LoadError> {
        let Some((category, head)) = read::open_stream::<A::Event>(self, stream_id).await? else {
            return Ok(Loaded::new(stream_id));
        };
        if head.floor_version > 0 {
            return Err(read::truncated(stream_id, head.floor_version).into());
        }

        let start = Replay {
            state: A::default(),
            from: version::Version::FIRST,
            commits: head.commits(),
            last_version: head.version,
            version: None,
            commit_id: None,
        };

        Ok(replay(self, category, stream_id, start).await?)
    }

    /// Loads an aggregate from its snapshot, then replays the events after it.
    ///
    /// A snapshot written with another [`aggregate::Snapshotted::SNAPSHOT_VERSION`] is ignored and the
    /// aggregate is rebuilt from the start. With [`aggregate::SnapshotPolicy::EveryN`], a load that replayed at
    /// least `n` events saves a new snapshot.
    ///
    /// A stream that retention truncated loads from its snapshot, also one saved while the load runs. When
    /// retention truncates the stream above where the replay started, during the load, the load starts again
    /// from the snapshot that truncation relied on. It fails with [`read::ReadError::Truncated`] only when
    /// the stored snapshot is of another state version, or behind the floor, or when the stream was truncated
    /// during each of several attempts in a row. A stream deleted during the load fails with
    /// [`read::ReadError::StreamDeleted`].
    pub async fn load_snapshotted<A: aggregate::Snapshotted>(
        &self,
        stream_id: stream::StreamId,
    ) -> Result<Loaded<A>, LoadError> {
        let mut retries = 0;
        let loaded = loop {
            let Some((category, start)) = start::<A>(self, stream_id, &mut retries).await? else {
                return Ok(Loaded::new(stream_id));
            };
            match replay(self, category, stream_id, start).await {
                // The start was at or above the floor, so retention raised the floor above it and dropped the
                // versions after the start during the replay. It raises a floor only under a snapshot at or
                // above it, and a stored snapshot's version never goes down: the next start finds that
                // snapshot, or one newer.
                Err(read::ReadError::Truncated { .. }) if retries < RETRIES => retries += 1,
                result => break result?,
            }
        };

        if let aggregate::SnapshotPolicy::EveryN(n) = A::policy()
            && loaded.replayed >= u64::from(n.get())
        {
            self.save_snapshot(&loaded)
                .await
                .map_err(LoadError::SnapshotSave)?;
        }

        Ok(loaded)
    }

    /// Saves a snapshot of a loaded aggregate, unless the stored one is as new. Does nothing for a stream
    /// that does not exist or was deleted, also when it was deleted after the load.
    ///
    /// A stored snapshot of a lower [`aggregate::Snapshotted::SNAPSHOT_VERSION`] is replaced when this one is
    /// at its version or newer; one of a higher state version is never replaced, so code that still has an
    /// older state shape cannot overwrite a newer one. The stored version never goes down.
    ///
    /// A [`Loaded`] cannot be changed after the load, so the snapshot always matches the version and commit
    /// it is stored under.
    pub async fn save_snapshot<A: aggregate::Snapshotted>(
        &self,
        loaded: &Loaded<A>,
    ) -> Result<(), SnapshotError> {
        let (Some(version), Some(commit_id)) = (loaded.version, loaded.commit_id) else {
            return Ok(());
        };

        let state =
            serde_json::to_vec(&loaded.state).map_err(|e| SnapshotError::Encode(e.into()))?;
        let category = self
            .category(<A::Event as event::Event>::CATEGORY)
            .await
            .map_err(|error| match error {
                store::RegisterError::UnknownCategory(name) => SnapshotError::UnknownCategory(name),
                store::RegisterError::Database(error) => SnapshotError::Database(error),
            })?;

        // Locks the stream's row, so a deletion since the load stores nothing (snapshot/live_stream.sql).
        // Unprepared: a prepared statement per partition of streams would crowd the statement cache.
        self.inner
            .sql
            .snapshot_save(loaded.stream_id.as_uuid())
            .bind(statement::snapshot_upsert::Params {
                version: append::to_i64(version),
                stream_id: loaded.stream_id.as_uuid(),
                commit_id: commit_id.as_uuid(),
                category_id: category.id.get(),
                state_version: i32::from(A::SNAPSHOT_VERSION.get()),
                format: SNAPSHOT_FORMAT_JSON,
                state: &state,
                min_distance: 1,
            })
            .unprepared()
            .execute(&self.inner.pool)
            .await
            .map_err(|e| SnapshotError::Database(e.into()))?;

        Ok(())
    }
}

/// An aggregate rebuilt from its stream.
///
/// Read-only: the state is always the one at [`Loaded::version`]. Decide on [`Loaded::state`], or take it
/// with [`Loaded::into_state`].
#[derive(Debug, Clone)]
pub struct Loaded<A> {
    state: A,
    version: Option<version::Version>,
    stream_id: stream::StreamId,
    commit_id: Option<commit::CommitId>,
    replayed: u64,
}

impl<A> Loaded<A> {
    /// The state after the last event.
    pub fn state(&self) -> &A {
        &self.state
    }

    /// The state after the last event, consuming the load.
    pub fn into_state(self) -> A {
        self.state
    }

    /// The version of the last event applied; `None` when the stream does not exist.
    pub fn version(&self) -> Option<version::Version> {
        self.version
    }

    /// What to expect when appending the result of a decision on this state: `NoStream` for a stream that
    /// does not exist, otherwise `Exact(version)`.
    pub fn expected(&self) -> version::ExpectedVersion {
        match self.version {
            Some(version) => version::ExpectedVersion::Exact(version),
            None => version::ExpectedVersion::NoStream,
        }
    }

    /// The stream the aggregate was loaded from.
    pub fn stream_id(&self) -> stream::StreamId {
        self.stream_id
    }

    /// The commit holding [`Loaded::version`].
    pub fn commit_id(&self) -> Option<commit::CommitId> {
        self.commit_id
    }

    fn new(stream_id: stream::StreamId) -> Self
    where
        A: Default,
    {
        Self {
            state: A::default(),
            version: None,
            stream_id,
            commit_id: None,
            replayed: 0,
        }
    }
}

/// An aggregate could not be loaded.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum LoadError {
    /// The stream's events could not be read.
    #[error(transparent)]
    Read(#[from] read::ReadError),
    /// A snapshot with the current state version could not be decoded.
    #[error("could not decode the snapshot of stream {stream_id}: {source}")]
    SnapshotDecode {
        /// The stream.
        stream_id: stream::StreamId,
        /// The decoder's error.
        source: event::BoxError,
    },
    /// The snapshot the policy asked for could not be saved.
    #[error(transparent)]
    SnapshotSave(SnapshotError),
}

impl From<sqlx::Error> for LoadError {
    fn from(source: sqlx::Error) -> Self {
        Self::Read(source.into())
    }
}

/// A snapshot could not be saved.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum SnapshotError {
    /// The state could not be encoded.
    #[error("could not encode the snapshot: {0}")]
    Encode(event::BoxError),
    /// The aggregate's category is not registered in the store.
    #[error("category {0} is not registered in the store")]
    UnknownCategory(category::CategoryName),
    /// A database error.
    #[error(transparent)]
    Database(error::DbError),
}

/// `snapshots.format` for state encoded as JSON.
pub(crate) const SNAPSHOT_FORMAT_JSON: i16 = 1;

struct Replay<A> {
    state: A,
    from: version::Version,
    commits: RangeInclusive<Uuid>,
    /// The stream's version at the head read, the version of `commits.end()`.
    last_version: i64,
    version: Option<version::Version>,
    commit_id: Option<commit::CommitId>,
}

/// How often `load_snapshotted` starts again: reads the head again for a snapshot saved after its head read,
/// or the head and the snapshot again after retention truncated the stream above where its replay started.
/// Each retry needs another snapshot saved, or another truncation of the stream, during the attempt before.
const RETRIES: u32 = 8;

/// The stream's snapshot as a load at `head` finds it.
enum StoredSnapshot<A> {
    /// The load starts from it.
    Usable(Replay<A>),
    /// Above the head's version: saved after the head was read.
    AheadOfHead,
    /// None, or one of another state version, format or category, or behind the stream's retention floor.
    Unusable,
}

/// Reads the stream's head and its snapshot, and where the replay starts: from the snapshot when it is usable,
/// otherwise from the stream's first version. `None` if the stream does not exist. Fails as truncated when the
/// stream is truncated and its snapshot is not usable.
async fn start<A: aggregate::Snapshotted>(
    store: &store::Store,
    stream_id: stream::StreamId,
    retries: &mut u32,
) -> Result<Option<(catalog::Category, Replay<A>)>, LoadError> {
    loop {
        let Some((category, head)) = read::open_stream::<A::Event>(store, stream_id).await? else {
            return Ok(None);
        };

        match read_snapshot::<A>(store, stream_id, &category, &head).await? {
            StoredSnapshot::Usable(start) => return Ok(Some((category, start))),
            // Saved after the head was read. Its version is committed, so the head read again is at or above
            // it. Ignoring it would replay from the start, which retention may have truncated because this
            // snapshot covers it.
            StoredSnapshot::AheadOfHead if *retries < RETRIES => *retries += 1,
            // delete_stream deletes the snapshot in the transaction that marks the stream deleted: a deletion
            // committed after the head read leaves none. The head read again fails with StreamDeleted then.
            StoredSnapshot::AheadOfHead | StoredSnapshot::Unusable if head.floor_version > 0 => {
                return match read::open_stream::<A::Event>(store, stream_id).await? {
                    Some((_, head)) => Err(read::truncated(stream_id, head.floor_version).into()),
                    None => Ok(None),
                };
            }
            StoredSnapshot::AheadOfHead | StoredSnapshot::Unusable => {
                let start = Replay {
                    state: A::default(),
                    from: version::Version::FIRST,
                    commits: head.commits(),
                    last_version: head.version,
                    version: None,
                    commit_id: None,
                };
                return Ok(Some((category, start)));
            }
        }
    }
}

/// The stream's snapshot, usable when it has the same state version and format, and lies between the
/// stream's retention floor and the head's version.
async fn read_snapshot<A: aggregate::Snapshotted>(
    store: &store::Store,
    stream_id: stream::StreamId,
    category: &catalog::Category,
    head: &read::Head,
) -> Result<StoredSnapshot<A>, LoadError> {
    let fetched = store
        .inner
        .sql
        .snapshot_read
        .bind(statement::snapshot_read::Params {
            stream_id: stream_id.as_uuid(),
        })
        .fetch_optional_row(&store.inner.pool)
        .await?;
    let Some(fetched) = fetched else {
        return Ok(StoredSnapshot::Unusable);
    };
    let row = fetched.row()?;

    let category_id = category::CategoryId::new(row.category_id);
    let same_shape = row.state_version == i32::from(A::SNAPSHOT_VERSION.get())
        && row.format == SNAPSHOT_FORMAT_JSON
        && category_id == category.id;
    if same_shape && row.version > head.version {
        return Ok(StoredSnapshot::AheadOfHead);
    }
    let usable = same_shape && row.version >= head.floor_version;
    let (Some(snapshot_version), Some(commit_id), true) = (
        append::to_version(row.version),
        commit::CommitId::from_uuid(row.commit_id),
        usable,
    ) else {
        return Ok(StoredSnapshot::Unusable);
    };

    let state: A = serde_json::from_slice(row.state).map_err(|e| LoadError::SnapshotDecode {
        stream_id,
        source: e.into(),
    })?;
    let Some(from) = snapshot_version.checked_add(1) else {
        return Ok(StoredSnapshot::Unusable);
    };

    Ok(StoredSnapshot::Usable(Replay {
        state,
        from,
        commits: commit_id.as_uuid()..=head.last_commit_id,
        last_version: head.version,
        version: Some(snapshot_version),
        commit_id: Some(commit_id),
    }))
}

async fn replay<A: aggregate::Aggregate>(
    store: &store::Store,
    category: catalog::Category,
    stream_id: stream::StreamId,
    start: Replay<A>,
) -> Result<Loaded<A>, read::ReadError> {
    let Replay {
        mut state,
        from,
        commits,
        last_version,
        mut version,
        mut commit_id,
    } = start;
    let mut pager = read::Pager::<A::Event>::new(
        store.clone(),
        category,
        stream_id,
        from,
        commits,
        last_version,
        event::OnUnknown::Fail,
    );
    let mut buffer = VecDeque::new();
    let mut replayed = 0;

    while !pager.is_finished() {
        pager.next_page(&mut buffer).await?;
        for event in buffer.drain(..) {
            aggregate::Aggregate::apply(&mut state, &event.event);
            version = Some(event.version);
            commit_id = Some(event.commit_id);
            replayed += 1;
        }
    }

    Ok(Loaded {
        state,
        version,
        stream_id,
        commit_id,
        replayed,
    })
}
