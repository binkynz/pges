//! Backfills: a one-off replay of some categories up to a position, to fill a new read-model field from
//! events that were written before the projection knew about it.
//!
//! The usual sequence to change a read model:
//!
//! 1. Deploy the projection's new handler. For every live event it writes the new field and stamps
//!    `<field>_pos` with the event's position.
//! 2. Run a backfill of the relevant categories up to `P`, a position the live projection had passed when
//!    it was deployed: its cursor ([`Until::Subscription`]) or the store's newest position ([`Until::Head`]).
//! 3. The backfill's handler writes the field only where the live handler has not written a newer value:
//!
//! ```sql
//! UPDATE jobs_board SET assignee = $2, assignee_pos = $3
//! WHERE job_id = $1 AND (assignee_pos IS NULL OR assignee_pos < $3);
//! ```
//!
//! With that guard the backfill and the live projection can run at the same time, in any order, and the
//! newest event of each stream wins. Events after `P` are the live projection's.
//!
//! # Retention
//!
//! A backfill reads each category from its retention horizon when its cursor is below it: the events
//! before the horizon were removed and are skipped, without an error. A read model that needs the state
//! they built starts from the application's own snapshots; the backfill does not replay them.
//!
//! # Lanes
//!
//! A backfill reads the log once, in position order, and splits each page by stream into
//! [`Builder::parallelism`] lanes. Lanes run their handlers at the same time, each in its own transaction;
//! a stream always falls into the same lane, so its events are handled in order. Each lane's cursor is
//! stored in the store's `backfills` table and moves in the transaction of the handler that handled the
//! events, so every event is handled exactly once, also across crashes: a run that stops (an error, a crash,
//! or a dropped future) is resumed by running the same backfill again, not restarted.
//!
//! The categories, the lane count and `P` are fixed when the backfill first runs. A later run with other
//! categories, another parallelism or another explicit [`Until::Position`] fails with
//! [`BackfillError::DefinitionChanged`]; to start over, use a new name. Run one instance of a backfill at a
//! time: a second one only finds its lanes moved and stops with [`BackfillError::Overtaken`].
//!
//! ```no_run
//! # async fn example(
//! #     store: pges_postgres::store::Store,
//! # ) -> Result<(), Box<dyn std::error::Error>> {
//! use sqlx::PgConnection;
//!
//! use pges_core::{category, non_empty};
//! use pges_subscribe::{backfill, batch, handler, subscription::StoreExt, subscription_name};
//!
//! #[derive(Clone)]
//! struct AssigneeBackfill;
//!
//! impl handler::PgHandler for AssigneeBackfill {
//!     async fn handle(
//!         &mut self,
//!         tx: &mut PgConnection,
//!         batch: &batch::Batch,
//!     ) -> Result<(), handler::HandlerError> {
//!         for record in batch.iter() {
//!             let batch::RawRecorded::Event(event) = record else {
//!                 continue;
//!             };
//!             sqlx::query(
//!                 "UPDATE jobs_board SET assignee = $2, assignee_pos = $3 \
//!                  WHERE job_id = $1 AND (assignee_pos IS NULL OR assignee_pos < $3)",
//!             )
//!             .bind(event.stream_id.as_uuid())
//!             .bind(&event.type_name[..])
//!             .bind(event.position.get() as i64)
//!             .execute(&mut *tx)
//!             .await?;
//!         }
//!         Ok(())
//!     }
//! }
//!
//! let finished = store
//!     .backfill(
//!         subscription_name!("jobs-board.v3.assignee"),
//!         backfill::Until::Subscription(subscription_name!("jobs-board.v3")),
//!     )
//!     .categories(non_empty![category!("jobs")])
//!     .parallelism(pges_subscribe::lanes!(16))
//!     .run_pg(AssigneeBackfill)
//!     .await?;
//! println!("backfilled up to {}", finished.until);
//! # Ok(())
//! # }
//! ```

use std::{
    fmt,
    marker::PhantomData,
    num::{NonZeroU16, NonZeroU32},
    sync::Arc,
};

use futures_util::future;
use sqlx::{Connection, PgPool};

use pges_core::{category, event, frame, non_empty, position, stream};
use pges_postgres::store;

use crate::{batch, handler, read, sql, sql::statement, subscription};

/// Configures a backfill. Choose its categories first ([`Builder::category`], [`Builder::categories_of`] or
/// [`Builder::categories`]); only then can it run, with [`Builder::run_pg`]. The type parameter tracks that
/// choice, so a backfill without categories does not compile.
///
/// A backfill is a set of rows in the store's `backfills` table, keyed by its name: one per lane, each with
/// the lane's cursor. Names follow the subscription name rule ([`subscription::SubscriptionName`]), in a
/// namespace of their own.
pub struct Builder<S = subscription::NoScope> {
    store: store::Store,
    name: subscription::SubscriptionName,
    listed: Vec<category::CategoryName>,
    until: Until,
    parallelism: Lanes,
    batch_size: NonZeroU32,
    on_unknown: event::OnUnknown,
    retry: subscription::RetryPolicy,
    chosen: PhantomData<fn() -> S>,
}

impl Builder<subscription::NoScope> {
    /// A backfill of `store` named `name`, up to `until`. The same as `store.backfill(name, until)` with
    /// [`subscription::StoreExt`].
    ///
    /// The events at or before `until` are handled. It is resolved and stored when the backfill first runs;
    /// later runs keep the stored position. A position inside a commit (between its first and last event)
    /// ends before that commit: a commit's events are delivered together. Subscription cursors are never
    /// inside a commit.
    pub fn new(
        store: &store::Store,
        name: subscription::SubscriptionName,
        until: impl Into<Until>,
    ) -> Self {
        Self {
            store: store.clone(),
            name,
            listed: Vec::new(),
            until: until.into(),
            parallelism: Lanes::ONE,
            batch_size: DEFAULT_BATCH_SIZE,
            on_unknown: event::OnUnknown::Fail,
            retry: subscription::RetryPolicy::default(),
            chosen: PhantomData,
        }
    }

    /// Replays `E`'s category.
    pub fn category<E: event::Event>(self) -> Builder<subscription::Listed> {
        self.listed([E::CATEGORY])
    }

    /// Replays the categories of every event type in the tuple `C`, for example `(JobEvent, OrderEvent)`.
    pub fn categories_of<C: subscription::Categories>(self) -> Builder<subscription::Listed> {
        self.listed(C::names())
    }

    /// Replays the categories `names`, at least one. Whether they are registered in the store is checked
    /// when the backfill runs ([`BackfillError::UnknownCategory`]).
    pub fn categories(
        self,
        names: non_empty::NonEmpty<category::CategoryName>,
    ) -> Builder<subscription::Listed> {
        self.listed(names)
    }

    fn listed(
        self,
        names: impl IntoIterator<Item = category::CategoryName>,
    ) -> Builder<subscription::Listed> {
        let mut listed = self.listed;
        listed.extend(names);

        Builder {
            store: self.store,
            name: self.name,
            listed,
            until: self.until,
            parallelism: self.parallelism,
            batch_size: self.batch_size,
            on_unknown: self.on_unknown,
            retry: self.retry,
            chosen: PhantomData,
        }
    }
}

impl Builder<subscription::Listed> {
    /// Adds `E`'s category.
    pub fn category<E: event::Event>(mut self) -> Self {
        self.listed.push(E::CATEGORY);
        self
    }

    /// Adds the categories of every event type in the tuple `C`.
    pub fn categories_of<C: subscription::Categories>(mut self) -> Self {
        self.listed.extend(C::names());
        self
    }

    /// Adds the categories `names`.
    pub fn categories(mut self, names: impl IntoIterator<Item = category::CategoryName>) -> Self {
        self.listed.extend(names);
        self
    }
}

impl<S> Builder<S> {
    /// How many lanes handle streams at the same time, each in its own transaction. Default
    /// [`Lanes::ONE`]. The store's pool needs at least this many connections. Fixed when the backfill first
    /// runs. Write a literal with [`lanes!`](crate::lanes).
    pub fn parallelism(mut self, lanes: Lanes) -> Self {
        self.parallelism = lanes;
        self
    }

    /// The most commits read per category per page. A page is split between the lanes. Default 1000.
    pub fn batch_size(mut self, batch_size: NonZeroU32) -> Self {
        self.batch_size = batch_size;
        self
    }

    /// What [`batch::Batch::decode`] does with event types or versions the code does not know. Default
    /// [`event::OnUnknown::Fail`].
    pub fn on_unknown(mut self, on_unknown: event::OnUnknown) -> Self {
        self.on_unknown = on_unknown;
        self
    }

    /// How often a lane's batch is retried after [`handler::HandlerError::Transient`]. Default
    /// [`subscription::RetryPolicy::default`].
    pub fn on_error(mut self, retry: subscription::RetryPolicy) -> Self {
        self.retry = retry;
        self
    }
}

impl Builder<subscription::Listed> {
    /// Runs the backfill to its end, with one clone of `handler` per lane, and returns where it ended.
    ///
    /// Each lane's batch is handled in a transaction that also moves the lane's cursor, so its effect is
    /// applied exactly once ([`handler::PgHandler`]). A batch holds the events of the lane's streams in a
    /// page, in position order; its [`batch::Batch::last_position`] is the page's end.
    ///
    /// It returns at the first error, after the lanes already running finish; what they committed stays.
    /// Database errors are not retried here: run the backfill again to resume it. Dropping the future
    /// stops the backfill, rolling back the lanes' open transactions; it also resumes on the next run.
    pub async fn run_pg<H>(self, handler: H) -> Result<Finished, BackfillError>
    where
        H: handler::PgHandler + Clone,
    {
        let until = self.until.clone();
        let pool = self.store.pool().clone();
        let max_connections = pool.options().get_max_connections();
        let needed = u32::from(self.parallelism.get());
        if max_connections < needed {
            return Err(BackfillError::PoolTooSmall {
                max_connections,
                needed,
            });
        }

        let store = pges_postgres::sql::store_name(&self.store);
        let statements = Arc::new(sql::Statements::new(store));
        let backfill_sql = sql::Backfill::new(store);
        let watched = read::resolve(
            &pool,
            &statements,
            store,
            &self.listed,
            sql::Sharding::Whole,
        )
        .await
        .map_err(BackfillError::from_read)?;
        let requested: Vec<category::CategoryId> = watched.iter().map(|w| w.id).collect();
        let mut reader = read::Reader::new(
            pool.clone(),
            statements.clone(),
            store,
            watched,
            read::Reading {
                scope: read::Scope::Listed,
                limit: self.batch_size.get(),
                on_unknown: self.on_unknown,
                shards: 1,
                before_horizon: read::BeforeHorizon::Skip,
            },
        );
        reader
            .load_types()
            .await
            .map_err(BackfillError::from_read)?;

        let head = head(&pool, &statements).await?;
        let name = self.name.as_str();
        let mut lanes = load(&pool, &backfill_sql, name).await?;
        if lanes.is_empty() {
            let position = match &until {
                Until::Position(position) => *position,
                Until::Head => head,
                Until::Subscription(live) => {
                    subscription_position(&pool, &backfill_sql, live.as_str())
                        .await?
                        .ok_or_else(|| BackfillError::UnknownSubscription(live.clone()))?
                }
            };
            if position > head {
                return Err(BackfillError::UntilAhead {
                    until: position,
                    head,
                });
            }
            let ids: Vec<i32> = requested.iter().map(|id| id.get()).collect();
            backfill_sql
                .create
                .bind(statement::backfill_create::Params {
                    name,
                    lanes: i32::from(self.parallelism.to_i16()),
                    category_ids: &ids,
                    until: read::to_i64(position),
                })
                .execute(&pool)
                .await?;
            lanes = load(&pool, &backfill_sql, name).await?;
        }
        let until = self.check_definition(&lanes, &requested, &until)?;
        if until > head {
            return Err(BackfillError::UntilAhead { until, head });
        }

        let mut handlers: Vec<H> = (1..lanes.len()).map(|_| handler.clone()).collect();
        handlers.push(handler);
        let mut cursors: Vec<position::Position> = lanes.iter().map(|lane| lane.position).collect();
        let ids: Vec<i16> = lanes.iter().map(|lane| lane.lane).collect();
        let mut events = 0_u64;
        let listed = reader.listed();
        let ctx = LaneCtx {
            pool: &pool,
            sql: &backfill_sql,
            name,
            retry: self.retry,
        };

        loop {
            let Some(cursor) = cursors.iter().min().copied() else {
                return Err(BackfillError::Corrupt {
                    position: position::Position::BEGINNING,
                    reason: "a backfill without lanes",
                });
            };
            if cursor >= until {
                return Ok(Finished { until, events });
            }

            // `until` is at or below `head`, read above on this pool, so every position up to it is visible:
            // `H` need not be read again, and a page never reads past `until`.
            let page = reader
                .read_through(&pool, cursor, until)
                .await
                .map_err(BackfillError::from_read)?;
            let cut = page.batch.last_position();
            let mut split: Vec<Vec<batch::RawRecorded>> = vec![Vec::new(); cursors.len()];
            for record in page.batch.into_records() {
                let lane = lane_of(record.stream_id(), cursors.len());
                if record.position() > cursors[lane] {
                    split[lane].push(record);
                }
            }

            let delivered = future::join_all(
                handlers
                    .iter_mut()
                    .zip(split)
                    .zip(&cursors)
                    .enumerate()
                    .filter(|(_, ((_, records), _))| !records.is_empty())
                    .map(|(lane, ((handler, records), from))| {
                        let batch =
                            batch::Batch::new(records, cut, self.on_unknown, listed.clone());
                        ctx.deliver(lane, ids[lane], *from, batch, handler)
                    }),
            )
            .await;
            for result in delivered {
                let (lane, handled) = result?;
                cursors[lane] = cut;
                events += handled;
            }

            let behind: Vec<i16> = (0..cursors.len())
                .filter(|lane| cursors[*lane] < cut)
                .map(|lane| ids[lane])
                .collect();
            if !behind.is_empty() {
                backfill_sql
                    .advance
                    .bind(statement::backfill_advance::Params {
                        name,
                        lanes: &behind,
                        position: read::to_i64(cut),
                    })
                    .execute(&pool)
                    .await?;
                for cursor in cursors.iter_mut() {
                    *cursor = (*cursor).max(cut);
                }
            }
        }
    }

    /// Compares the stored lanes with what this run asked for, and returns the stored end.
    fn check_definition(
        &self,
        lanes: &[Lane],
        requested: &[category::CategoryId],
        until: &Until,
    ) -> Result<position::Position, BackfillError> {
        let Some(first) = lanes.first() else {
            return Err(BackfillError::Corrupt {
                position: position::Position::BEGINNING,
                reason: "a backfill without lanes",
            });
        };
        let stored = Definition {
            categories: first.categories.clone(),
            parallelism: first.lanes,
            until: first.until,
        };
        let consistent = lanes.iter().enumerate().all(|(index, lane)| {
            usize::try_from(lane.lane) == Ok(index)
                && lane.lanes == stored.parallelism
                && lane.until == stored.until
                && lane.categories == stored.categories
        }) && lanes.len() == usize::from(stored.parallelism.get());
        if !consistent {
            return Err(BackfillError::Corrupt {
                position: position::Position::BEGINNING,
                reason: "the backfill's lanes disagree; was it first run twice at once?",
            });
        }

        let requested = Definition {
            categories: requested.to_vec(),
            parallelism: self.parallelism,
            until: match until {
                Until::Position(position) => *position,
                Until::Head | Until::Subscription(_) => stored.until,
            },
        };
        if requested != stored {
            return Err(BackfillError::DefinitionChanged { stored, requested });
        }

        Ok(stored.until)
    }
}

impl<S> fmt::Debug for Builder<S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Builder")
            .field("name", &self.name)
            .field("categories", &self.listed)
            .field("until", &self.until)
            .field("parallelism", &self.parallelism)
            .finish_non_exhaustive()
    }
}

/// Where a backfill ends, resolved when it first runs.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum Until {
    /// This position.
    Position(position::Position),
    /// The stored cursor of this subscription, typically the live projection the backfill completes; for
    /// a sharded subscription, the lowest cursor of its shards. [`BackfillError::UnknownSubscription`] if it
    /// does not exist.
    Subscription(subscription::SubscriptionName),
    /// The store's newest position.
    Head,
}

impl From<position::Position> for Until {
    fn from(position: position::Position) -> Self {
        Self::Position(position)
    }
}

impl From<subscription::SubscriptionName> for Until {
    fn from(name: subscription::SubscriptionName) -> Self {
        Self::Subscription(name)
    }
}

/// How many lanes a backfill has: 1 to [`Lanes::MAX`].
///
/// Write a literal with [`lanes!`](crate::lanes), which checks it at compile time; a number known only at
/// run time goes through [`Lanes::new`].
///
/// ```
/// use pges_subscribe::backfill;
///
/// assert_eq!(pges_subscribe::lanes!(16).get(), 16);
/// assert_eq!(backfill::Lanes::new(0), None);
/// assert_eq!(backfill::Lanes::new(32768), None);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Lanes(NonZeroU16);

impl Lanes {
    /// One lane.
    pub const ONE: Self = Self(NonZeroU16::MIN);

    /// The most lanes a backfill can have: 32767 (`backfills.lanes` is an `int2`).
    pub const MAX: Self = match NonZeroU16::new(i16::MAX.unsigned_abs()) {
        Some(lanes) => Self(lanes),
        None => Self::ONE,
    };

    /// `lanes`, or `None` unless it is between 1 and [`Lanes::MAX`].
    pub const fn new(lanes: u16) -> Option<Self> {
        if lanes > Self::MAX.0.get() {
            return None;
        }

        match NonZeroU16::new(lanes) {
            Some(lanes) => Some(Self(lanes)),
            None => None,
        }
    }

    /// The number.
    pub const fn get(self) -> u16 {
        self.0.get()
    }

    /// The number as the `int2` `backfills.lanes` stores; [`Lanes::MAX`] fits.
    pub(crate) const fn to_i16(self) -> i16 {
        self.0.get().cast_signed()
    }
}

impl fmt::Display for Lanes {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

/// [`Lanes`](crate::backfill::Lanes) from a literal, checked at compile time.
///
/// ```compile_fail
/// let none = pges_subscribe::lanes!(0);
/// ```
#[macro_export]
macro_rules! lanes {
    ($lanes:expr $(,)?) => {{
        const LANES: $crate::backfill::Lanes = match $crate::backfill::Lanes::new($lanes) {
            ::core::option::Option::Some(lanes) => lanes,
            #[allow(clippy::panic)]
            ::core::option::Option::None => {
                ::core::panic!("invalid number of lanes: it must be between 1 and 32767")
            }
        };

        LANES
    }};
}

/// A backfill that reached its end.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub struct Finished {
    /// Where it ended: every event of its categories at or before this position was handled.
    pub until: position::Position,
    /// The records this run handed to the handlers. Events handled by earlier runs do not count.
    pub events: u64,
}

/// What a backfill was started with, or is stored with.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct Definition {
    /// The category ids, sorted.
    pub categories: Vec<category::CategoryId>,
    /// The number of lanes.
    pub parallelism: Lanes,
    /// Where it ends.
    pub until: position::Position,
}

/// A backfill could not start or stopped with an error. Running it again resumes from the lanes' cursors.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum BackfillError {
    /// The store's pool allows fewer connections than the backfill has lanes.
    #[error(
        "the store's pool allows {max_connections} connections; the backfill needs at least {needed}"
    )]
    PoolTooSmall {
        /// The pool's `max_connections`.
        max_connections: u32,
        /// The backfill's parallelism.
        needed: u32,
    },
    /// A requested category is not registered in the store.
    #[error("category {0} is not registered in the store")]
    UnknownCategory(category::CategoryName),
    /// [`Until::Subscription`] names a subscription that does not exist.
    #[error("subscription {0} does not exist")]
    UnknownSubscription(subscription::SubscriptionName),
    /// The backfill exists with another definition.
    #[error("backfill definition changed from {stored:?} to {requested:?}")]
    DefinitionChanged {
        /// The stored definition.
        stored: Definition,
        /// The requested one.
        requested: Definition,
    },
    /// The end is past the store's newest position.
    #[error("backfill end {until} is past the store's newest position {head}")]
    UntilAhead {
        /// The backfill's end.
        until: position::Position,
        /// The store's newest position.
        head: position::Position,
    },
    /// A lane's cursor moved while this run handled its batch: another run of the same backfill is
    /// working. Nothing of the batch was committed.
    #[error("another run of this backfill moved a lane's cursor")]
    Overtaken,
    /// The handler failed with a fatal error, or a transient one on every attempt. Nothing of that lane's
    /// batch was committed.
    #[error("handler failed on the batch from position {first} to {last}: {source}")]
    HandlerFailed {
        /// The batch's first position.
        first: position::Position,
        /// The batch's last position (the page's end).
        last: position::Position,
        /// The handler's error.
        source: handler::BoxError,
    },
    /// A stored event type id is missing from the store's `event_types`.
    #[error(
        "event at position {position} of category {category} has unknown event type id {type_id:?}"
    )]
    UnknownEventType {
        /// The event's position.
        position: position::Position,
        /// Its category.
        category: category::CategoryId,
        /// The stored id.
        type_id: frame::EventTypeId,
    },
    /// A commit's payload frame could not be decoded.
    #[error("commit at position {position}: {source}")]
    Frame {
        /// The commit's last position.
        position: position::Position,
        /// The frame error.
        source: frame::FrameError,
    },
    /// A stored row breaks an invariant of the schema.
    #[error("invalid row at position {position}: {reason}")]
    Corrupt {
        /// The commit's last position, or 0 for a row that is not a commit.
        position: position::Position,
        /// What is wrong.
        reason: &'static str,
    },
    /// A database error.
    #[error(transparent)]
    Database(pges_postgres::error::DbError),
}

impl From<sqlx::Error> for BackfillError {
    fn from(error: sqlx::Error) -> Self {
        Self::Database(error.into())
    }
}

impl BackfillError {
    /// The reader's errors, which share the subscription's error type.
    fn from_read(error: subscription::SubscribeError) -> Self {
        use subscription::SubscribeError as E;

        match error {
            E::UnknownCategory(name) => Self::UnknownCategory(name),
            E::UnknownEventType {
                position,
                category,
                type_id,
            } => Self::UnknownEventType {
                position,
                category,
                type_id,
            },
            E::Frame { position, source } => Self::Frame { position, source },
            E::Corrupt { position, reason } => Self::Corrupt { position, reason },
            E::Database(error) => Self::Database(error),
            E::PoolTooSmall { .. }
            | E::CursorBeforeHorizon { .. }
            | E::DefinitionChanged { .. }
            | E::ShardsChanged { .. }
            | E::HandlerFailed { .. }
            | E::CrateTooOld { .. }
            | E::Wake(_)
            | E::Aborted => Self::Corrupt {
                position: position::Position::BEGINNING,
                reason: "unexpected error while reading the log",
            },
        }
    }
}

/// The most commits a backfill reads per category per page, unless [`Builder::batch_size`] says otherwise.
const DEFAULT_BATCH_SIZE: NonZeroU32 = match NonZeroU32::new(1000) {
    Some(size) => size,
    None => NonZeroU32::MIN,
};

/// One stored lane.
#[derive(Debug)]
struct Lane {
    /// Its number, as stored: from 0 to `lanes - 1`, the lane's index.
    lane: i16,
    lanes: Lanes,
    until: position::Position,
    position: position::Position,
    categories: Vec<category::CategoryId>,
}

/// What a lane needs to deliver a batch.
struct LaneCtx<'a> {
    pool: &'a PgPool,
    sql: &'a sql::Backfill,
    name: &'a str,
    retry: subscription::RetryPolicy,
}

impl LaneCtx<'_> {
    /// Handles `batch` and moves lane `lane`'s cursor from `from` to the batch's end in one transaction,
    /// retrying transient handler errors. Returns the lane and the records handled.
    async fn deliver<H: handler::PgHandler>(
        &self,
        lane: usize,
        lane_id: i16,
        from: position::Position,
        batch: batch::Batch,
        handler: &mut H,
    ) -> Result<(usize, u64), BackfillError> {
        let mut attempt = 1_u32;
        loop {
            let mut conn = self.pool.acquire().await?;
            let mut tx = handler::begin(&mut conn, handler.isolation()).await?;
            let held = self
                .sql
                .hold
                .bind(statement::backfill_hold::Params {
                    name: self.name,
                    lane: lane_id,
                    from: read::to_i64(from),
                })
                .fetch_optional(&mut *tx)
                .await?;
            let Some(held) = held else {
                let _ = tx.rollback().await;
                return Err(BackfillError::Overtaken);
            };
            let xact = held.xact.ok_or_else(|| {
                sqlx::Error::Protocol("a held backfill lane had no transaction id".into())
            })?;

            let error = match handler.handle(&mut tx, &batch).await {
                // Without this, the checkpoint would move in autocommit and the commit do nothing.
                Ok(()) if !tx.is_in_transaction() => {
                    handler::HandlerError::Transient(Box::new(subscription::EndedTransaction))
                }
                Ok(()) => {
                    // sqlx does not see a raw COMMIT or ROLLBACK the handler sent: the checkpoint checks it
                    // runs in the transaction the hold began, or it would move the cursor in autocommit.
                    let moved = self
                        .sql
                        .checkpoint
                        .bind(statement::backfill_checkpoint::Params {
                            name: self.name,
                            lane: lane_id,
                            position: read::to_i64(batch.last_position()),
                            from: read::to_i64(from),
                            xact,
                        })
                        .fetch_one(&mut *tx)
                        .await
                        .map(|row| (row.in_batch, row.moved));
                    match moved {
                        Ok((Some(false), _)) => {
                            let _ = tx.rollback().await;
                            handler::HandlerError::Fatal(Box::new(
                                subscription::HandlerEndedTransaction,
                            ))
                        }
                        Ok((Some(true), Some(false))) => {
                            let _ = tx.rollback().await;
                            return Err(BackfillError::Overtaken);
                        }
                        Ok((Some(true), Some(true))) => {
                            tx.commit().await?;
                            return Ok((lane, u64::try_from(batch.len()).unwrap_or(u64::MAX)));
                        }
                        Ok(_) => {
                            let _ = tx.rollback().await;
                            return Err(sqlx::Error::Protocol(
                                "the backfill checkpoint came back with a NULL column".into(),
                            )
                            .into());
                        }
                        Err(error) if subscription::is_aborted_transaction(&error) => {
                            let _ = tx.rollback().await;
                            handler::HandlerError::Transient(Box::new(
                                subscription::AbortedTransaction,
                            ))
                        }
                        Err(error) => return Err(error.into()),
                    }
                }
                Err(error) => {
                    let _ = tx.rollback().await;
                    error
                }
            };

            match error {
                handler::HandlerError::Transient(_) if attempt < self.retry.max_attempts.get() => {
                    tokio::time::sleep(self.retry.wait(attempt)).await;
                    attempt = attempt.saturating_add(1);
                }
                handler::HandlerError::Transient(source) | handler::HandlerError::Fatal(source) => {
                    return Err(BackfillError::HandlerFailed {
                        first: batch.first_position().unwrap_or(batch.last_position()),
                        last: batch.last_position(),
                        source,
                    });
                }
            }
        }
    }
}

/// The lane of a stream: its id modulo the lane count. Stored cursors depend on it, so it never changes.
fn lane_of(stream_id: stream::StreamId, lanes: usize) -> usize {
    let lanes = u128::try_from(lanes.max(1)).unwrap_or(1);

    usize::try_from(stream_id.as_uuid().as_u128() % lanes).unwrap_or(0)
}

async fn head(
    pool: &PgPool,
    statements: &sql::Statements,
) -> Result<position::Position, BackfillError> {
    let head = statements
        .head
        .bind(statement::head::Params {})
        .fetch_one(pool)
        .await?
        .head;

    read::to_position(head).map_err(BackfillError::from_read)
}

async fn subscription_position(
    pool: &PgPool,
    sql: &sql::Backfill,
    name: &str,
) -> Result<Option<position::Position>, BackfillError> {
    let row = sql
        .subscription
        .bind(statement::backfill_subscription::Params { name })
        .fetch_optional(pool)
        .await?;
    let Some(row) = row else {
        return Ok(None);
    };

    Ok(Some(
        read::to_position(row.position).map_err(BackfillError::from_read)?,
    ))
}

async fn load(pool: &PgPool, sql: &sql::Backfill, name: &str) -> Result<Vec<Lane>, BackfillError> {
    let rows = sql
        .lanes
        .bind(statement::backfill_lanes::Params { name })
        .fetch_all(pool)
        .await?;
    let mut lanes = Vec::with_capacity(rows.len());
    for row in rows {
        let corrupt = |reason| BackfillError::Corrupt {
            position: position::Position::BEGINNING,
            reason,
        };
        let lane = row.lane;
        if lane < 0 {
            return Err(corrupt("negative lane"));
        }
        let mut categories: Vec<category::CategoryId> = row
            .category_ids
            .into_iter()
            .map(category::CategoryId::new)
            .collect();
        categories.sort_unstable();
        lanes.push(Lane {
            lane,
            lanes: u16::try_from(row.lanes)
                .ok()
                .and_then(Lanes::new)
                .ok_or_else(|| corrupt("a lane count below 1"))?,
            until: read::to_position(row.until).map_err(BackfillError::from_read)?,
            position: read::to_position(row.position).map_err(BackfillError::from_read)?,
            categories,
        });
    }

    Ok(lanes)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_stream_always_falls_into_the_same_lane() {
        let id = stream::StreamId::new_v7();

        assert_eq!(lane_of(id, 16), lane_of(id, 16));
        assert!(lane_of(id, 16) < 16);
        assert_eq!(lane_of(id, 1), 0);
    }

    #[test]
    fn streams_spread_over_the_lanes() {
        let mut seen = [0_u32; 8];
        for _ in 0..800 {
            seen[lane_of(stream::StreamId::new_v7(), 8)] += 1;
        }

        assert!(seen.iter().all(|n| *n > 40), "{seen:?}");
    }
}
