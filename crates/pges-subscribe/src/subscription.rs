//! Subscriptions: a named cursor over one or several categories of a store, with a lease so one consumer
//! runs at a time, delivering batches to a handler.

use std::{
    borrow::Cow, fmt, future::Future, marker::PhantomData, num::NonZeroU32, pin::Pin, str::FromStr,
    sync::Arc, time::Duration,
};

use futures_util::FutureExt;
use sqlx::{
    Connection, PgConnection, PgPool,
    postgres::{PgDatabaseError, PgSeverity, types::PgInterval},
};
use tokio::{
    sync::{oneshot, watch},
    task::JoinHandle,
    time::Instant,
};
use uuid::Uuid;

use pges_core::{category, event, frame, high_water, interval, non_empty, position};
use pges_postgres::{migrate, store};

use crate::{backfill, batch, handler, read, replica, sql, waker};

mod shard;

/// Starts subscriptions and backfills from a [`store::Store`]:
/// `store.subscription(subscription_name!("jobs-board.v3"))`,
/// `store.backfill(subscription_name!("jobs-board.v3.assignee"), until)`. Sealed: implemented for
/// [`store::Store`] only.
pub trait StoreExt: sealed::Store {
    /// A subscription named `name`, to configure and run. See [`Builder`].
    fn subscription(&self, name: SubscriptionName) -> Builder<NoScope>;

    /// A backfill named `name`: a one-off replay of some categories up to `until`. See
    /// [`backfill::Builder`].
    fn backfill(
        &self,
        name: SubscriptionName,
        until: impl Into<backfill::Until>,
    ) -> backfill::Builder<NoScope>;
}

impl StoreExt for store::Store {
    fn subscription(&self, name: SubscriptionName) -> Builder<NoScope> {
        Builder::new(self, name)
    }

    fn backfill(
        &self,
        name: SubscriptionName,
        until: impl Into<backfill::Until>,
    ) -> backfill::Builder<NoScope> {
        backfill::Builder::new(self, name, until)
    }
}

/// The name of a subscription or a backfill: `^[a-z][a-z0-9_.-]{0,99}$`, such as `jobs-board.v3`.
///
/// Put the read model's version in it, so a new version starts its own cursor. Subscriptions and backfills
/// have a namespace each.
///
/// Write a literal name with [`subscription_name!`](crate::subscription_name), which checks it at compile
/// time. Parse a name read at run time with [`SubscriptionName::parse`].
///
/// ```
/// use pges_subscribe::subscription;
///
/// const BOARD: subscription::SubscriptionName = pges_subscribe::subscription_name!("jobs-board.v3");
/// let configured = subscription::SubscriptionName::parse("jobs-board.v4")?;
///
/// assert_eq!(BOARD.as_str(), "jobs-board.v3");
/// assert_eq!(configured.as_str(), "jobs-board.v4");
/// assert_eq!(
///     subscription::SubscriptionName::parse("Jobs"),
///     Err(subscription::SubscriptionNameError::InvalidStart)
/// );
/// # Ok::<(), subscription::SubscriptionNameError>(())
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct SubscriptionName(Cow<'static, str>);

impl SubscriptionName {
    /// Longest allowed name, in bytes.
    pub const MAX_LEN: usize = 100;

    /// Validates a static name. It is a `const fn`; for a literal,
    /// [`subscription_name!`](crate::subscription_name) turns an invalid name into a compile error instead.
    pub const fn new(name: &'static str) -> Result<Self, SubscriptionNameError> {
        match validate_name(name) {
            Ok(()) => Ok(Self(Cow::Borrowed(name))),
            Err(error) => Err(error),
        }
    }

    /// Validates a name read at run time, such as from configuration.
    pub fn parse(name: impl Into<String>) -> Result<Self, SubscriptionNameError> {
        let name = name.into();
        validate_name(&name)?;

        Ok(Self(Cow::Owned(name)))
    }

    /// The name.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// A name `validate_name` accepted.
    pub(crate) const fn from_validated(name: &'static str) -> Self {
        Self(Cow::Borrowed(name))
    }
}

impl fmt::Display for SubscriptionName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl AsRef<str> for SubscriptionName {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl FromStr for SubscriptionName {
    type Err = SubscriptionNameError;

    fn from_str(name: &str) -> Result<Self, Self::Err> {
        Self::parse(name)
    }
}

/// Why a subscription or backfill name is invalid.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum SubscriptionNameError {
    /// The name is empty.
    #[error("subscription name is empty")]
    Empty,
    /// The name is longer than [`SubscriptionName::MAX_LEN`] bytes.
    #[error("subscription name is longer than 100 bytes")]
    TooLong,
    /// The name does not start with `a`–`z`.
    #[error("subscription name must start with a lowercase ASCII letter")]
    InvalidStart,
    /// The name contains a byte other than `a`–`z`, `0`–`9`, `_`, `.` or `-`.
    #[error("subscription name may only contain a-z, 0-9, _, . and -")]
    InvalidCharacter,
}

/// A [`SubscriptionName`](crate::subscription::SubscriptionName) from a literal, checked at compile time.
///
/// The macro expands to a block with a `const` item that checks the name, so an invalid name is a compile
/// error (`cargo check` reports it too) and never a run-time panic.
///
/// ```
/// let board = pges_subscribe::subscription_name!("jobs-board.v3");
///
/// assert_eq!(board.as_str(), "jobs-board.v3");
/// ```
///
/// ```compile_fail
/// let invalid = pges_subscribe::subscription_name!("Jobs Board");
/// ```
#[macro_export]
macro_rules! subscription_name {
    ($name:expr $(,)?) => {{
        const NAME: $crate::subscription::SubscriptionName =
            match $crate::__private::is_valid_subscription_name($name) {
                true => $crate::__private::subscription_name($name),
                #[allow(clippy::panic)]
                false => ::core::panic!(
                    "invalid subscription name: it must match ^[a-z][a-z0-9_.-]{{0,99}}$"
                ),
            };

        NAME
    }};
}

/// How many shards a [sharded](Builder::sharded) subscription has: 1 to [`Shards::MAX`].
///
/// Write a literal with [`shards!`](crate::shards), which checks it at compile time; a number known only
/// at run time goes through [`Shards::new`].
///
/// ```
/// use pges_subscribe::subscription;
///
/// let eight = pges_subscribe::shards!(8);
///
/// assert_eq!(eight.get(), 8);
/// assert_eq!(subscription::Shards::new(0), None);
/// assert_eq!(subscription::Shards::new(1025), None);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Shards(u16);

impl Shards {
    /// One shard: not sharded.
    pub const ONE: Self = Self(1);

    /// The most shards a subscription can have: 1024 (`subscriptions.shards`).
    pub const MAX: Self = Self(1024);

    /// `shards`, or `None` unless it is between 1 and [`Shards::MAX`].
    pub const fn new(shards: u16) -> Option<Self> {
        if shards >= Self::ONE.0 && shards <= Self::MAX.0 {
            Some(Self(shards))
        } else {
            None
        }
    }

    /// The number.
    pub const fn get(self) -> u16 {
        self.0
    }

    /// The number as the `int2` `subscriptions.shards` stores; [`Shards::MAX`] fits.
    pub(crate) const fn to_i16(self) -> i16 {
        self.0.cast_signed()
    }
}

impl fmt::Display for Shards {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

/// [`Shards`](crate::subscription::Shards) from a literal, checked at compile time.
///
/// ```compile_fail
/// let none = pges_subscribe::shards!(0);
/// ```
#[macro_export]
macro_rules! shards {
    ($shards:expr $(,)?) => {{
        const SHARDS: $crate::subscription::Shards =
            match $crate::subscription::Shards::new($shards) {
                ::core::option::Option::Some(shards) => shards,
                #[allow(clippy::panic)]
                ::core::option::Option::None => {
                    ::core::panic!("invalid number of shards: it must be between 1 and 1024")
                }
            };

        SHARDS
    }};
}

/// The categories of a [`Builder`] (and a [`backfill::Builder`]) are not chosen yet: it cannot run.
#[derive(Debug)]
pub enum NoScope {}

/// The [`Builder`] (or [`backfill::Builder`]) reads the categories it was given, at least one.
#[derive(Debug)]
pub enum Listed {}

/// The [`Builder`] reads every category of the store ([`Builder::all_categories`]).
#[derive(Debug)]
pub enum AllCategories {}

/// A choice of categories a [`Builder`] can run with: [`Listed`] or [`AllCategories`]. Sealed.
pub trait Scope: sealed::Sealed {}

impl Scope for Listed {}
impl Scope for AllCategories {}

mod sealed {
    pub trait Sealed {}

    impl Sealed for super::Listed {}
    impl Sealed for super::AllCategories {}

    pub trait Store {}

    impl Store for super::store::Store {}
}

/// Configures a subscription. Choose its categories first ([`Builder::category`], [`Builder::categories_of`],
/// [`Builder::categories`] or [`Builder::all_categories`]); only then can it run, with [`Builder::run_pg`] or
/// [`Builder::run`], then [`Subscription::spawn`] or [`Subscription::run_until`]. The type parameter tracks
/// that choice, so a subscription without categories, or one that mixes listed categories with
/// `all_categories`, does not compile:
///
/// ```compile_fail
/// # async fn example(store: pges_postgres::store::Store, h: impl pges_subscribe::handler::Handler + 'static) {
/// use pges_subscribe::subscription::StoreExt;
///
/// // No categories: there is no `run` to call.
/// store.subscription(pges_subscribe::subscription_name!("nothing")).run(h);
/// # }
/// ```
///
/// A subscription is a row in the store's `subscriptions` table, keyed by its [`SubscriptionName`]: its
/// cursor (the position of the last event handled), its categories, and a lease. Only the lease holder reads
/// and checkpoints; other instances of the same subscription stand by and take over when the lease lapses or
/// is released.
///
/// # Pool size
///
/// Every subscription of a store shares the store's pool with the store's other users. Give it at least:
///
/// - the connections the application's appends and reads use at once, plus
/// - for each subscription on the store: 2 for [`Builder::run_pg`] (the handler's transaction and a lease
///   renewal beside it) or 1 for [`Builder::run`], plus the connections its waker keeps
///   ([`waker::Waker::store_connections`]: 1 for a `NotifyWaker` on the store's pool, 0 otherwise).
///
/// An instance of a [sharded](Builder::sharded) subscription reads its shards at the same time, and calls
/// its handler for one shard at a time: give it one more connection per shard it may hold.
///
/// A waker that only borrows a connection while it checks, such as a `PollWaker`, adds a short wait, not a
/// connection of its own. At start a subscription checks only its own share, against the pool's
/// `max_connections` ([`SubscribeError::PoolTooSmall`]); it cannot see the other users. A pool smaller than
/// the sum makes renewals wait for a connection, and a lease can lapse while its holder waits.
pub struct Builder<S = NoScope> {
    store: store::Store,
    name: SubscriptionName,
    listed: Vec<category::CategoryName>,
    scope: read::Scope,
    shards: Shards,
    start: Start,
    redefine: Option<Start>,
    lease: interval::Interval,
    batch_size: NonZeroU32,
    on_unknown: event::OnUnknown,
    retry: RetryPolicy,
    waker: Option<Box<dyn waker::DynWaker>>,
    acquire_interval: interval::Interval,
    retry_interval: interval::Interval,
    max_idle: interval::Interval,
    replica: Option<replica::Replica>,
    chosen: PhantomData<fn() -> S>,
}

impl Builder<NoScope> {
    /// A subscription of `store` named `name`. The same as `store.subscription(name)` with [`StoreExt`].
    pub fn new(store: &store::Store, name: SubscriptionName) -> Self {
        Self {
            store: store.clone(),
            name,
            listed: Vec::new(),
            scope: read::Scope::Listed,
            shards: Shards::ONE,
            start: Start::Beginning,
            redefine: None,
            lease: DEFAULT_LEASE,
            batch_size: DEFAULT_BATCH_SIZE,
            on_unknown: event::OnUnknown::Fail,
            retry: RetryPolicy::default(),
            waker: None,
            acquire_interval: DEFAULT_ACQUIRE_INTERVAL,
            retry_interval: DEFAULT_RETRY_INTERVAL,
            max_idle: DEFAULT_MAX_IDLE,
            replica: None,
            chosen: PhantomData,
        }
    }

    /// Reads `E`'s category.
    pub fn category<E: event::Event>(self) -> Builder<Listed> {
        self.listed([E::CATEGORY])
    }

    /// Reads the categories of every event type in the tuple `C`, for example `(JobEvent, OrderEvent)`.
    pub fn categories_of<C: Categories>(self) -> Builder<Listed> {
        self.listed(C::names())
    }

    /// Reads the categories `names`, at least one. Write literal names with
    /// [`category!`](pges_core::category!) and the list with [`non_empty!`](pges_core::non_empty!):
    /// `.categories(non_empty![category!("jobs"), category!("orders")])`. Whether they are registered in
    /// the store is checked when the subscription starts ([`SubscribeError::UnknownCategory`]).
    pub fn categories(self, names: non_empty::NonEmpty<category::CategoryName>) -> Builder<Listed> {
        self.listed(names)
    }

    /// Reads every category of the store, in store position order, including categories registered after
    /// the subscription started. It cannot be combined with listed categories.
    ///
    /// Batches hold events of any category: read them with [`batch::Batch::iter`], or decode one category
    /// at a time with [`batch::Batch::decode`], which skips the others. A cursor behind the retention horizon
    /// of any category fails with [`SubscribeError::CursorBeforeHorizon`].
    pub fn all_categories(self) -> Builder<AllCategories> {
        let mut builder = self.scoped();
        builder.scope = read::Scope::All;

        builder
    }

    fn listed(self, names: impl IntoIterator<Item = category::CategoryName>) -> Builder<Listed> {
        let mut builder = self.scoped();
        builder.listed.extend(names);

        builder
    }
}

impl Builder<Listed> {
    /// Adds `E`'s category.
    pub fn category<E: event::Event>(mut self) -> Self {
        self.listed.push(E::CATEGORY);
        self
    }

    /// Adds the categories of every event type in the tuple `C`.
    pub fn categories_of<C: Categories>(mut self) -> Self {
        self.listed.extend(C::names());
        self
    }

    /// Adds the categories `names`. Whether they are registered in the store is checked when the
    /// subscription starts.
    pub fn categories(mut self, names: impl IntoIterator<Item = category::CategoryName>) -> Self {
        self.listed.extend(names);
        self
    }
}

impl<S> Builder<S> {
    /// Splits the subscription into `shards` shards by stream (default [`Shards::ONE`], not sharded). Each
    /// shard has its own cursor and lease, and gets the events of the streams that hash to it, so several
    /// instances handle one subscription in parallel. Write a literal with [`shards!`](crate::shards).
    ///
    /// Events of one stream stay in order, as each stream belongs to one shard. Events of different streams
    /// are in position order within a shard only: a handler sees no order between streams of other shards,
    /// so it must not depend on one. A stream's shard is fixed by its id and the number of shards: the first
    /// 8 bytes of the SHA-256 of the id's 16 bytes, read as a big-endian integer without its sign bit,
    /// modulo `shards`.
    ///
    /// Instances share the shards fairly by themselves. Each running instance of the subscription takes at
    /// most its share, `shards` divided by the number of live instances and rounded up, from the free
    /// shards; an instance holding more than its share releases the rest after its current batch. A new
    /// instance therefore gets shards from the others within a few [`Builder::acquire_interval`]s, and the
    /// shards of an instance that died are taken over once their leases lapse. One instance handles its
    /// shards' batches one at a time, through its one handler; run more instances (processes, or several
    /// subscriptions with the same name in one process) for parallel handlers.
    ///
    /// The number of shards is part of the definition: changing it needs [`Builder::redefine`], which moves
    /// every shard's cursor to the given start. Otherwise the run fails with
    /// [`SubscribeError::ShardsChanged`].
    pub fn sharded(mut self, shards: Shards) -> Self {
        self.shards = shards;
        self
    }

    /// Where a new subscription starts. Ignored once the subscription exists; see [`Builder::redefine`].
    /// Default [`Start::Beginning`].
    pub fn start(mut self, start: Start) -> Self {
        self.start = start;
        self
    }

    /// Accepts a change of definition: when the stored categories or number of shards differ from the
    /// requested ones, store the new ones and move the cursor of every shard to `start`. Instances still
    /// running the old definition lose their leases. Without it such a change fails with
    /// [`SubscribeError::DefinitionChanged`] or [`SubscribeError::ShardsChanged`].
    pub fn redefine(mut self, start: Start) -> Self {
        self.redefine = Some(start);
        self
    }

    /// How long the lease lasts without renewal. Default 15 s.
    ///
    /// The holder renews it once less than half of it is left: with a checkpoint where possible, otherwise
    /// on another connection of the store's pool, also while a handler runs and while a failed batch waits
    /// for its retry. A batch slower than the lease therefore keeps it, as long as the store's pool has a
    /// connection to spare for the renewal. A renewal that finds another instance took over stops the
    /// batch from being checkpointed; the handler's call still finishes first.
    pub fn lease(mut self, lease: interval::Interval) -> Self {
        self.lease = lease;
        self
    }

    /// The most commits read per category per batch. A batch can hold up to this many commits of each
    /// category. A sharded subscription counts the commits of every shard, so a shard's batch holds about
    /// this many divided by the shards. Default 1024: a larger batch costs fewer reads per event while
    /// catching up, and a subscription that keeps up gets small batches anyway.
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

    /// How often a batch is retried after [`handler::HandlerError::Transient`].
    pub fn on_error(mut self, retry: RetryPolicy) -> Self {
        self.retry = retry;
        self
    }

    /// What tells the caught-up subscription that new events may exist. Call it once; to combine several,
    /// pass a [`waker::Wakers`]. Without a waker, a caught-up subscription reads the log again every 500 ms
    /// ([`waker::recheck::Recheck`]). Whatever the waker, it also reads the log after
    /// [`Builder::max_idle`] without a wake-up.
    pub fn waker(mut self, waker: impl waker::Waker + 'static) -> Self {
        self.waker = Some(Box::new(waker));
        self
    }

    /// How often a standby tries to take the lease. Default 1 s.
    pub fn acquire_interval(mut self, interval: interval::Interval) -> Self {
        self.acquire_interval = interval;
        self
    }

    /// The pause after a database or waker error before trying again. Default 1 s.
    pub fn retry_interval(mut self, interval: interval::Interval) -> Self {
        self.retry_interval = interval;
        self
    }

    /// The longest a caught-up subscription waits for its waker before it reads the log anyway. Default 30 s.
    ///
    /// A waker can stay quiet without failing: a `NotifyWaker` (`wake-notify`) whose connection died
    /// silently (a half-open TCP connection) never hears another notification. Its holder still renews the
    /// lease, so no standby takes over. This backstop bounds how late such a subscription delivers, at the
    /// cost of one read of the log per `max_idle` while nothing is appended. It applies to each shard of
    /// [`Builder::sharded`] on its own.
    pub fn max_idle(mut self, max_idle: interval::Interval) -> Self {
        self.max_idle = max_idle;
        self
    }

    /// Reads the log and the commits from a streaming replica of the store's database instead of the
    /// store's pool: a [`replica::Replica`], or its pool alone (no fallback). Checkpoints, leases and the
    /// handler's transaction stay on the store's pool.
    ///
    /// Before each read the subscription waits until the replica has replayed the WAL position the waker
    /// reported (the sequencer's position after the batch that woke it), or, for a waker that reports none,
    /// the primary's current WAL insert position. It waits however long the replica lags, unless the
    /// replica has a [fallback](replica::Replica::fallback). A pool whose server is not in recovery, such as
    /// a promoted replica, is read without waiting. See [`replica`].
    ///
    /// This holds for [`Builder::all_categories`] and for each shard of [`Builder::sharded`]: every shard
    /// waits and falls back on its own, and all of them take fallback permits from the same limit.
    pub fn replica(mut self, replica: impl Into<replica::Replica>) -> Self {
        self.replica = Some(replica.into());
        self
    }

    fn scoped<T>(self) -> Builder<T> {
        Builder {
            store: self.store,
            name: self.name,
            listed: self.listed,
            scope: self.scope,
            shards: self.shards,
            start: self.start,
            redefine: self.redefine,
            lease: self.lease,
            batch_size: self.batch_size,
            on_unknown: self.on_unknown,
            retry: self.retry,
            waker: self.waker,
            acquire_interval: self.acquire_interval,
            retry_interval: self.retry_interval,
            max_idle: self.max_idle,
            replica: self.replica,
            chosen: PhantomData,
        }
    }
}

impl<S: Scope> Builder<S> {
    /// Delivers batches to a projection in the same database, exactly once: the handler's writes and the
    /// checkpoint commit in one transaction ([`handler::PgHandler`]).
    ///
    /// The store's pool must allow at least 2 connections, plus those the waker keeps
    /// ([`waker::Waker::store_connections`]), so the lease can be renewed while the handler's transaction is
    /// open; otherwise the run fails at start with [`SubscribeError::PoolTooSmall`].
    pub fn run_pg(self, handler: impl handler::PgHandler + 'static) -> Subscription {
        self.finish(Delivery::Pg(Box::new(handler)))
    }

    /// Delivers batches to anything else, at least once: the checkpoint follows the handler
    /// ([`handler::Handler`]).
    ///
    /// The store's pool must allow at least 1 connection plus those the waker keeps
    /// ([`waker::Waker::store_connections`]); otherwise the run fails at start with
    /// [`SubscribeError::PoolTooSmall`].
    pub fn run(self, handler: impl handler::Handler + 'static) -> Subscription {
        self.finish(Delivery::External(Box::new(handler)))
    }

    fn finish(self, delivery: Delivery) -> Subscription {
        let waker = self
            .waker
            .unwrap_or_else(|| Box::new(waker::recheck::Recheck::default()));
        let pool_needs = delivery
            .store_connections()
            .saturating_add(waker::DynWaker::store_connections(&*waker));
        let (status, _) = watch::channel(Status {
            state: State::Starting,
            position: position::Position::BEGINNING,
            high_water: position::Position::BEGINNING,
            shards: Vec::new(),
        });

        Subscription {
            setup: Setup {
                store: self.store,
                name: self.name,
                categories: self.listed,
                scope: self.scope,
                shards: self.shards,
                start: self.start,
                redefine: self.redefine,
                lease: self.lease.get(),
                batch_size: self.batch_size.get(),
                on_unknown: self.on_unknown,
                retry: self.retry,
                acquire_interval: self.acquire_interval.get(),
                retry_interval: self.retry_interval.get(),
                max_idle: self.max_idle.get(),
                pool_needs,
                replica: self.replica,
            },
            delivery,
            waker,
            status,
        }
    }
}

impl<S> fmt::Debug for Builder<S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Builder")
            .field("name", &self.name)
            .field("categories", &self.listed)
            .finish_non_exhaustive()
    }
}

/// A configured subscription, ready to run.
pub struct Subscription {
    setup: Setup,
    delivery: Delivery,
    waker: Box<dyn waker::DynWaker>,
    status: watch::Sender<Status>,
}

impl Subscription {
    /// The subscription's status, updated as it runs.
    pub fn status(&self) -> watch::Receiver<Status> {
        self.status.subscribe()
    }

    /// Runs on a new task. See [`Subscription::run_until`] for what it does.
    pub fn spawn(self) -> SubscriptionHandle {
        let status = self.status();
        let (stop, stopped) = oneshot::channel::<()>();
        let task = tokio::spawn(self.run_until(async {
            let _ = stopped.await;
        }));

        SubscriptionHandle {
            stop: Some(stop),
            task,
            status,
        }
    }

    /// Runs until `shutdown` resolves or a fatal error occurs.
    ///
    /// It checks the categories, creates the subscription row if needed (at [`Builder::start`]), then takes
    /// the lease, standing by while another instance holds it. As the lease holder it reads batches after
    /// the cursor, hands them to the handler, and checkpoints. Once caught up it waits for its waker.
    ///
    /// On shutdown it finishes the batch in hand, checkpoints it, releases the lease so a standby takes
    /// over at once, and returns `Ok`. A batch waiting to be retried after a transient handler error is not
    /// retried: the run stops at once and the batch is delivered again on the next start.
    ///
    /// Database and waker errors are retried every [`Builder::retry_interval`], shown as
    /// [`State::Recovering`]; a lost lease returns to standby. Every other error releases the lease and ends
    /// the run.
    ///
    /// The status becomes [`State::Stopped`] when the run returns, and also when its future is dropped
    /// before that (a panicking handler, an aborted task). A dropped run does not release its lease: a
    /// standby takes over once it lapses.
    pub async fn run_until(
        self,
        shutdown: impl Future<Output = ()> + Send + 'static,
    ) -> Result<(), SubscribeError> {
        let mut shutdown: Shutdown = Box::pin(shutdown.fuse());
        let _stopped = MarkStopped(self.status.clone());

        self.run_inner(&mut shutdown).await
    }

    async fn run_inner(self, shutdown: &mut Shutdown) -> Result<(), SubscribeError> {
        let Subscription {
            setup,
            delivery,
            waker,
            status,
        } = self;
        if setup.shards > Shards::ONE {
            return shard::run(setup, delivery, waker, status, shutdown).await;
        }

        let mut runner = Runner {
            ctx: Ctx::new(&setup),
            delivery,
            waker,
            status,
            retry: setup.retry,
            retry_interval: setup.retry_interval,
            max_idle: setup.max_idle,
            replica: setup.replica(),
        };

        let (mut reader, requested) = loop {
            match prepare(&runner.ctx, &setup).await {
                Ok(prepared) => break prepared,
                Err(error) if error.is_retried() => {
                    runner.recovering(error);
                    if sleep_or_shutdown(setup.retry_interval, shutdown).await == Wait::Shutdown {
                        return Ok(());
                    }
                }
                Err(error) => return Err(error),
            }
        };

        loop {
            let mut term = match runner.ctx.acquire().await {
                Ok(Some(term)) => term,
                Ok(None) => {
                    runner.set_state(State::Standby);
                    if sleep_or_shutdown(setup.acquire_interval, shutdown).await == Wait::Shutdown {
                        return Ok(());
                    }
                    continue;
                }
                Err(error) if error.is_retried() => {
                    runner.recovering(error);
                    if sleep_or_shutdown(setup.retry_interval, shutdown).await == Wait::Shutdown {
                        return Ok(());
                    }
                    continue;
                }
                Err(error) => return Err(error),
            };

            match runner
                .lead(&setup, &mut term, &mut reader, &requested, shutdown)
                .await?
            {
                Ended::Shutdown => return Ok(()),
                Ended::LostLease => runner.set_state(State::Standby),
                Ended::Retry | Ended::Redefined => {}
            }
        }
    }
}

impl fmt::Debug for Subscription {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Subscription")
            .field("name", &self.setup.name)
            .field("categories", &self.setup.categories)
            .finish_non_exhaustive()
    }
}

/// A subscription running on its own task, from [`Subscription::spawn`]. Dropping the handle shuts the
/// subscription down without waiting for it.
#[derive(Debug)]
pub struct SubscriptionHandle {
    stop: Option<oneshot::Sender<()>>,
    task: JoinHandle<Result<(), SubscribeError>>,
    status: watch::Receiver<Status>,
}

impl SubscriptionHandle {
    /// The current status.
    pub fn status(&self) -> Status {
        self.status.borrow().clone()
    }

    /// A receiver of status changes.
    pub fn status_changes(&self) -> watch::Receiver<Status> {
        self.status.clone()
    }

    /// Whether the subscription has stopped, after a shutdown or a fatal error.
    pub fn is_finished(&self) -> bool {
        self.task.is_finished()
    }

    /// Finishes the current batch, checkpoints, releases the lease and waits for the task. Returns the
    /// fatal error that stopped the subscription earlier, if one did.
    pub async fn shutdown(mut self) -> Result<(), SubscribeError> {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }

        (&mut self.task)
            .await
            .map_err(|_| SubscribeError::Aborted)?
    }

    /// Waits for the subscription to stop by itself, which it does only on a fatal error.
    pub async fn join(mut self) -> Result<(), SubscribeError> {
        let result = (&mut self.task)
            .await
            .map_err(|_| SubscribeError::Aborted)?;
        self.stop.take();

        result
    }
}

/// Where a new subscription starts.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[non_exhaustive]
pub enum Start {
    /// Before the first event.
    #[default]
    Beginning,
    /// After the store's newest event when the subscription is created.
    Now,
    /// After this position.
    Position(position::Position),
}

/// How a batch is retried after [`handler::HandlerError::Transient`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub struct RetryPolicy {
    /// The wait before the second attempt. It doubles for each further attempt, up to 30 s.
    pub backoff: interval::Interval,
    /// Attempts in total, the first included. When they run out the subscription stops with
    /// [`SubscribeError::HandlerFailed`].
    pub max_attempts: NonZeroU32,
}

impl RetryPolicy {
    /// A policy with this first backoff and number of attempts.
    pub fn new(backoff: interval::Interval, max_attempts: NonZeroU32) -> Self {
        Self {
            backoff,
            max_attempts,
        }
    }

    /// The wait after failed attempt `attempt` (1-based).
    pub(crate) fn wait(&self, attempt: u32) -> Duration {
        let factor = 1_u32 << attempt.saturating_sub(1).min(16);

        self.backoff.get().saturating_mul(factor).min(MAX_BACKOFF)
    }
}

impl Default for RetryPolicy {
    /// 100 ms backoff, 5 attempts.
    fn default() -> Self {
        Self {
            backoff: pges_core::interval!(100 ms),
            max_attempts: DEFAULT_MAX_ATTEMPTS,
        }
    }
}

/// Attempts of [`RetryPolicy::default`].
const DEFAULT_MAX_ATTEMPTS: NonZeroU32 = match NonZeroU32::new(5) {
    Some(attempts) => attempts,
    None => NonZeroU32::MIN,
};

/// The event types of a tuple of [`event::Event`]s, for [`Builder::categories_of`]. Implemented for tuples of
/// one to eight event types.
pub trait Categories {
    /// The categories of the tuple's event types, in order.
    fn names() -> non_empty::NonEmpty<category::CategoryName>;
}

macro_rules! categories_for_tuple {
    ($first:ident $(, $rest:ident)*) => {
        impl<$first: event::Event $(, $rest: event::Event)*> Categories for ($first, $($rest,)*) {
            fn names() -> non_empty::NonEmpty<category::CategoryName> {
                non_empty::NonEmpty::new($first::CATEGORY, [$($rest::CATEGORY),*])
            }
        }
    };
}

categories_for_tuple!(A);
categories_for_tuple!(A, B);
categories_for_tuple!(A, B, C);
categories_for_tuple!(A, B, C, D);
categories_for_tuple!(A, B, C, D, E);
categories_for_tuple!(A, B, C, D, E, F);
categories_for_tuple!(A, B, C, D, E, F, G);
categories_for_tuple!(A, B, C, D, E, F, G, H);

/// What a subscription is doing, and how far it is.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct Status {
    /// What it is doing.
    pub state: State,
    /// The cursor: the position of the last event handled (or skipped past) and checkpointed.
    pub position: position::Position,
    /// The store's newest position when the subscription last read the log.
    pub high_water: position::Position,
    /// The shards this instance holds, for a [sharded](Builder::sharded) subscription; empty otherwise.
    /// `position` is then the lowest cursor among them.
    pub shards: Vec<Shard>,
}

impl Status {
    /// How many positions the cursor is behind the last high-water mark read. Positions of other
    /// categories count too.
    pub fn lag(&self) -> u64 {
        self.high_water.get().saturating_sub(self.position.get())
    }
}

/// One shard of a [sharded](Builder::sharded) subscription, from 0 to the number of shards less one.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Shard(u16);

impl Shard {
    /// The shard's number.
    pub fn get(self) -> u16 {
        self.0
    }
}

impl fmt::Display for Shard {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

/// What a subscription is doing.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum State {
    /// Checking its definition.
    Starting,
    /// Another instance holds the lease; this one waits to take it over.
    Standby,
    /// It holds the lease and is reading or waiting for new events.
    Running,
    /// It is retrying after this error.
    Recovering(Arc<SubscribeError>),
    /// It has stopped: the run returned, or its future was dropped (a panicking handler, an aborted task).
    Stopped,
}

/// A subscription could not start or stopped with an error.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum SubscribeError {
    /// The store's pool allows fewer connections than the subscription holds at once, so it could not renew
    /// its lease while a handler runs. `run_pg` needs 2 (the handler's transaction and a side renewal), `run`
    /// needs 1, and a waker adds the connections it keeps ([`waker::Waker::store_connections`]; 1 for a
    /// `NotifyWaker` on the store's pool). Other users of the pool need their own on top; see
    /// [`Builder`], Pool size.
    #[error(
        "the store's pool allows {max_connections} connections; the subscription needs at least {needed}"
    )]
    PoolTooSmall {
        /// The pool's `max_connections`.
        max_connections: u32,
        /// What the subscription and its waker hold at once.
        needed: u32,
    },
    /// A requested category is not registered in the store.
    #[error("category {0} is not registered in the store")]
    UnknownCategory(category::CategoryName),
    /// The subscription exists with other categories, and [`Builder::redefine`] was not given.
    #[error("subscription categories changed from {stored:?} to {requested:?}")]
    DefinitionChanged {
        /// The stored category ids.
        stored: Vec<category::CategoryId>,
        /// The requested category ids.
        requested: Vec<category::CategoryId>,
    },
    /// The subscription exists with another number of shards, and [`Builder::redefine`] was not given.
    #[error("subscription shards changed from {stored} to {requested}")]
    ShardsChanged {
        /// The stored number of shards.
        stored: u16,
        /// The requested number of shards.
        requested: u16,
    },
    /// The cursor is behind a category's retention horizon: events after it were removed. Rebuild the read
    /// model from snapshots or another projection, then restart from a later position.
    #[error("cursor {cursor} is behind the retention horizon {horizon} of category {category}")]
    CursorBeforeHorizon {
        /// The category.
        category: category::CategoryId,
        /// The subscription's cursor.
        cursor: position::Position,
        /// The category's horizon.
        horizon: position::Position,
    },
    /// The handler failed with a fatal error, or a transient one on every attempt. Nothing of the batch was
    /// checkpointed.
    #[error("handler failed on the batch from position {first} to {last}: {source}")]
    HandlerFailed {
        /// The batch's first position.
        first: position::Position,
        /// The batch's last position (its cut).
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
        /// The commit's last position.
        position: position::Position,
        /// What is wrong.
        reason: &'static str,
    },
    /// A migration raised the shared or the store's `min_compatible` above this crate's schema version while
    /// the subscription ran: the database no longer supports this release. Found on the next lease renewal or
    /// attempt to take a lease, which the database then refuses: a holder releases its lease, and a standby
    /// does not take one.
    #[error(
        "scope {scope} requires schema version {min_compatible} or newer; this crate knows {crate_version}"
    )]
    CrateTooOld {
        /// `pges` or the store's name.
        scope: String,
        /// The oldest schema version the scope still supports.
        min_compatible: migrate::SchemaVersion,
        /// The newest schema version this crate knows for the scope.
        crate_version: migrate::SchemaVersion,
    },
    /// The waker failed. Retried.
    #[error(transparent)]
    Wake(waker::WakeError),
    /// A database error. Retried.
    #[error(transparent)]
    Database(pges_postgres::error::DbError),
    /// The subscription's task panicked or was cancelled.
    #[error("the subscription task panicked or was cancelled")]
    Aborted,
}

impl SubscribeError {
    /// Whether the subscription retries after this error instead of stopping.
    pub fn is_retried(&self) -> bool {
        matches!(self, Self::Database(_) | Self::Wake(_))
    }
}

impl From<sqlx::Error> for SubscribeError {
    fn from(error: sqlx::Error) -> Self {
        Self::Database(error.into())
    }
}

/// The longest wait between handler attempts.
const MAX_BACKOFF: Duration = Duration::from_secs(30);

const DEFAULT_LEASE: interval::Interval = pges_core::interval!(15 s);
const DEFAULT_ACQUIRE_INTERVAL: interval::Interval = pges_core::interval!(1 s);
const DEFAULT_RETRY_INTERVAL: interval::Interval = pges_core::interval!(1 s);
const DEFAULT_MAX_IDLE: interval::Interval = pges_core::interval!(30 s);
const DEFAULT_BATCH_SIZE: NonZeroU32 = match NonZeroU32::new(1024) {
    Some(size) => size,
    None => NonZeroU32::MIN,
};

/// A failed side renewal is tried again after the lease divided by this.
const RENEW_RETRY_FRACTION: u32 = 8;

type Shutdown = Pin<Box<dyn Future<Output = ()> + Send>>;

struct Setup {
    store: store::Store,
    name: SubscriptionName,
    categories: Vec<category::CategoryName>,
    scope: read::Scope,
    shards: Shards,
    start: Start,
    redefine: Option<Start>,
    lease: Duration,
    batch_size: u32,
    on_unknown: event::OnUnknown,
    retry: RetryPolicy,
    acquire_interval: Duration,
    retry_interval: Duration,
    max_idle: Duration,
    /// The fewest connections the store's pool must allow.
    pool_needs: u32,
    replica: Option<replica::Replica>,
}

impl Setup {
    /// A new replica reader for one term loop: an unsharded subscription, or one shard of a sharded one.
    /// Each starts without a target, so its first read waits for the primary's insert position.
    fn replica(&self) -> Option<replica::Reader> {
        self.replica.clone().map(replica::Reader::new)
    }
}

enum Delivery {
    Pg(Box<dyn handler::DynPgHandler>),
    External(Box<dyn handler::DynHandler>),
    /// One handler for the shards an instance holds, called by one shard at a time.
    Shared(Shared),
}

#[derive(Clone)]
enum Shared {
    Pg(Arc<tokio::sync::Mutex<Box<dyn handler::DynPgHandler>>>),
    External(Arc<tokio::sync::Mutex<Box<dyn handler::DynHandler>>>),
}

impl Delivery {
    /// The store connections a batch holds at once: `run_pg` keeps the handler's transaction open while a
    /// side renewal takes another; `run` needs one, for reads, checkpoints and renewals in turn.
    fn store_connections(&self) -> u32 {
        match self {
            Self::Pg(_) | Self::Shared(Shared::Pg(_)) => 2,
            Self::External(_) | Self::Shared(Shared::External(_)) => 1,
        }
    }

    /// The handler, to share between the shards of an instance.
    fn into_shared(self) -> Shared {
        match self {
            Self::Pg(handler) => Shared::Pg(Arc::new(tokio::sync::Mutex::new(handler))),
            Self::External(handler) => Shared::External(Arc::new(tokio::sync::Mutex::new(handler))),
            Self::Shared(shared) => shared,
        }
    }
}

/// The subscription rows' statements, bound to one name and lease owner.
#[derive(Clone)]
struct Ctx {
    pool: PgPool,
    sql: Arc<sql::Statements>,
    store: pges_core::store::StoreName,
    name: String,
    owner: Uuid,
    /// The requested number of shards.
    shards: i16,
    lease: Duration,
    interval: PgInterval,
}

/// One leadership term of one shard: from taking its lease to losing or releasing it.
struct Term {
    shard: i16,
    fence: i64,
    cursor: position::Position,
    /// When the lease expires, measured from before the statement that set it was sent.
    deadline: Instant,
    stored: Vec<category::CategoryId>,
    stored_shards: i16,
    cursor_state: CursorState,
}

/// Whether `Term::cursor` is known to match the stored cursor.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CursorState {
    Confirmed,
    /// A statement failed after it may have committed (a COMMIT whose reply was lost moved the stored
    /// cursor). Read it again before the next batch, or that batch would be delivered twice.
    Unconfirmed,
}

enum Ended {
    Shutdown,
    LostLease,
    /// A retried error before the term started; the lease was released.
    Retry,
    /// The definition was replaced, which released every lease; take one again.
    Redefined,
}

enum Step {
    More,
    CaughtUp(high_water::HighWater),
    LostLease,
    Shutdown,
}

enum Waited {
    Woken,
    LostLease,
    Shutdown,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Wait {
    Elapsed,
    Shutdown,
}

/// The outcome of one checkpoint statement.
enum Checkpoint {
    /// The cursor moved; the lease was renewed if a new deadline is given.
    Moved(Option<Instant>),
    /// No row matched: the fence moved on (another instance took the lease), or the stored cursor is not
    /// `Term::cursor` (a COMMIT whose reply was lost moved it). Read the stored cursor to tell which.
    Refused,
}

enum Delivered {
    Checkpointed,
    /// The checkpoint was refused ([`Checkpoint::Refused`]) and the batch rolled back, if it could be.
    Refused,
    LostLease,
    /// Shutdown came while a failed batch waited for its retry.
    Shutdown,
}

/// Whether the lease was still held after renewals on the side connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Lease {
    Held,
    /// A renewal found the fence moved on: another instance took over.
    Lost,
}

enum Failure {
    Handler(handler::HandlerError),
    Database(pges_postgres::error::DbError),
}

/// A handler left its transaction aborted: a statement failed and the handler ignored the error.
#[derive(Debug, thiserror::Error)]
#[error("the handler left the transaction aborted (a statement failed and its error was ignored)")]
pub(crate) struct AbortedTransaction;

/// A handler's transaction ended under it: a savepoint begin on it failed or was dropped, and sqlx rolled
/// back the whole transaction.
#[derive(Debug, thiserror::Error)]
#[error(
    "the handler's transaction ended (a savepoint begin on it failed or was dropped, which rolls back the \
     whole transaction)"
)]
pub(crate) struct EndedTransaction;

struct Runner {
    ctx: Ctx,
    delivery: Delivery,
    waker: Box<dyn waker::DynWaker>,
    status: watch::Sender<Status>,
    retry: RetryPolicy,
    retry_interval: Duration,
    max_idle: Duration,
    replica: Option<replica::Reader>,
}

impl Runner {
    /// Leads one term: reads, delivers and checkpoints until shutdown, a lost lease or a fatal error.
    async fn lead(
        &mut self,
        setup: &Setup,
        term: &mut Term,
        reader: &mut read::Reader,
        requested: &[category::CategoryId],
        shutdown: &mut Shutdown,
    ) -> Result<Ended, SubscribeError> {
        if term.stored != requested || term.stored_shards != self.ctx.shards {
            match setup.redefine {
                Some(start) => match self.ctx.redefine(start, requested).await {
                    Ok(()) => return Ok(Ended::Redefined),
                    Err(error) if error.is_retried() => {
                        self.ctx.release(term).await;
                        self.recovering(error);
                        if sleep_or_shutdown(self.retry_interval, shutdown).await == Wait::Shutdown
                        {
                            return Ok(Ended::Shutdown);
                        }
                        return Ok(Ended::Retry);
                    }
                    Err(error) => {
                        self.ctx.release(term).await;
                        return Err(error);
                    }
                },
                None if term.stored != requested => {
                    self.ctx.release(term).await;
                    return Err(SubscribeError::DefinitionChanged {
                        stored: term.stored.clone(),
                        requested: requested.to_vec(),
                    });
                }
                None => {
                    self.ctx.release(term).await;
                    return Err(SubscribeError::ShardsChanged {
                        stored: u16::try_from(term.stored_shards).unwrap_or_default(),
                        requested: u16::try_from(self.ctx.shards).unwrap_or_default(),
                    });
                }
            }
        }

        self.status.send_modify(|status| {
            status.state = State::Running;
            status.position = term.cursor;
        });

        loop {
            if shutdown.as_mut().now_or_never().is_some() {
                self.ctx.release(term).await;
                return Ok(Ended::Shutdown);
            }

            let outcome = match self.step(term, reader, shutdown).await {
                Ok(Step::More) => Ok(Waited::Woken),
                Ok(Step::CaughtUp(known)) => {
                    self.running();
                    self.wait(term, &known, shutdown).await
                }
                Ok(Step::LostLease) => return self.ctx.lost_lease(term).await,
                Ok(Step::Shutdown) => Ok(Waited::Shutdown),
                Err(error) => Err(error),
            };

            match outcome {
                Ok(Waited::Woken) => self.running(),
                Ok(Waited::LostLease) => return self.ctx.lost_lease(term).await,
                Ok(Waited::Shutdown) => {
                    self.ctx.release(term).await;
                    return Ok(Ended::Shutdown);
                }
                Err(error) if error.is_retried() => {
                    term.cursor_state = CursorState::Unconfirmed;
                    if let Some(replica) = &mut self.replica {
                        replica.reset();
                    }
                    self.recovering(error);
                    if sleep_or_shutdown(self.retry_interval, shutdown).await == Wait::Shutdown {
                        self.ctx.release(term).await;
                        return Ok(Ended::Shutdown);
                    }
                }
                Err(error) => {
                    self.ctx.release(term).await;
                    return Err(error);
                }
            }
        }
    }

    /// Reads one page, delivers it, and checkpoints.
    async fn step(
        &mut self,
        term: &mut Term,
        reader: &mut read::Reader,
        shutdown: &mut Shutdown,
    ) -> Result<Step, SubscribeError> {
        if term.cursor_state == CursorState::Unconfirmed
            && let Step::LostLease = self.reread_cursor(term).await?
        {
            return Ok(Step::LostLease);
        }

        let page = match &mut self.replica {
            None => reader.read(term.cursor).await?,
            Some(replica) => {
                let (ready, lease) =
                    renewing(&self.ctx, term, replica.ready(&self.ctx.pool, shutdown)).await;
                if lease == Lease::Lost {
                    return Ok(Step::LostLease);
                }
                match ready? {
                    replica::Ready::Replica => {
                        reader.read_from(replica.pool(), term.cursor).await?
                    }
                    replica::Ready::Primary(_permit) => {
                        reader.read_from(&self.ctx.pool, term.cursor).await?
                    }
                    replica::Ready::Shutdown => return Ok(Step::Shutdown),
                }
            }
        };
        self.status
            .send_if_modified(|status| replace(&mut status.high_water, page.head));

        let cut = page.batch.last_position();
        if !page.batch.is_empty() {
            match self.deliver(term, &page.batch, shutdown).await? {
                Delivered::Checkpointed => {}
                Delivered::Refused => return self.refused(term).await,
                Delivered::LostLease => return Ok(Step::LostLease),
                Delivered::Shutdown => return Ok(Step::Shutdown),
            }
        } else if cut > term.cursor {
            let mut conn = self.ctx.pool.acquire().await?;
            match self
                .ctx
                .checkpoint(&mut conn, term, cut, Instant::now())
                .await?
            {
                Checkpoint::Moved(renewed) => renew(term, renewed),
                Checkpoint::Refused => return self.refused(term).await,
            }
        }

        if cut > term.cursor {
            term.cursor = cut;
            self.status
                .send_if_modified(|status| replace(&mut status.position, cut));
        }
        if !page.is_caught_up() {
            return Ok(Step::More);
        }

        // The cut, not `H`: a cursor can be ahead of `H` (Start::Position past the log), and a level-triggered
        // waker given `H` would fire again at once.
        Ok(Step::CaughtUp(reader.known(cut)))
    }

    /// After a refused checkpoint ([`Checkpoint::Refused`]), reads the stored cursor into `term` to read on
    /// from it ([`Step::More`]), if a COMMIT whose reply was lost moved it. If the fence moved on, or the
    /// cursor is still `term.cursor`, the lease is lost ([`Step::LostLease`]): with the fence and the cursor
    /// in place, a renewing checkpoint was refused because a migration dropped support for this crate, which
    /// [`Ctx::lost_lease`] reports. Reading on from the same cursor would read the same page and be refused
    /// again, forever.
    async fn refused(&mut self, term: &mut Term) -> Result<Step, SubscribeError> {
        let held = term.cursor;
        match self.reread_cursor(term).await? {
            Step::More if term.cursor == held => Ok(Step::LostLease),
            step => Ok(step),
        }
    }

    /// Reads the stored cursor into `term`: [`Step::More`] to read on from it, or [`Step::LostLease`] if the
    /// fence moved on.
    async fn reread_cursor(&mut self, term: &mut Term) -> Result<Step, SubscribeError> {
        let Some(stored) = self.ctx.stored_cursor(term).await? else {
            return Ok(Step::LostLease);
        };
        term.cursor = stored;
        term.cursor_state = CursorState::Confirmed;
        self.status
            .send_if_modified(|status| replace(&mut status.position, stored));
        Ok(Step::More)
    }

    /// Hands the batch to the handler and checkpoints, retrying transient handler errors. The lease is
    /// renewed while the handler runs and while a retry waits; shutdown ends the wait.
    async fn deliver(
        &mut self,
        term: &mut Term,
        batch: &batch::Batch,
        shutdown: &mut Shutdown,
    ) -> Result<Delivered, SubscribeError> {
        let mut attempt = 1_u32;
        loop {
            let outcome = match &mut self.delivery {
                Delivery::Pg(handler) => deliver_pg(&self.ctx, handler.as_mut(), term, batch).await,
                Delivery::External(handler) => {
                    deliver_external(&self.ctx, handler.as_mut(), term, batch).await
                }
                Delivery::Shared(Shared::Pg(handler)) => {
                    let (mut handler, lease) = renewing(&self.ctx, term, handler.lock()).await;
                    match lease {
                        Lease::Held => deliver_pg(&self.ctx, handler.as_mut(), term, batch).await,
                        Lease::Lost => Ok(Delivered::LostLease),
                    }
                }
                Delivery::Shared(Shared::External(handler)) => {
                    let (mut handler, lease) = renewing(&self.ctx, term, handler.lock()).await;
                    match lease {
                        Lease::Held => {
                            deliver_external(&self.ctx, handler.as_mut(), term, batch).await
                        }
                        Lease::Lost => Ok(Delivered::LostLease),
                    }
                }
            };

            let error = match outcome {
                Ok(delivered) => return Ok(delivered),
                Err(Failure::Database(error)) => return Err(SubscribeError::Database(error)),
                Err(Failure::Handler(handler::HandlerError::Transient(source)))
                    if attempt < self.retry.max_attempts.get() =>
                {
                    drop(source);
                    let backoff = sleep_or_shutdown(self.retry.wait(attempt), shutdown);
                    // Shutdown first: the shutdown future is fused, so a resolved one never fires again.
                    match renewing(&self.ctx, term, backoff).await {
                        (Wait::Shutdown, _) => return Ok(Delivered::Shutdown),
                        (Wait::Elapsed, Lease::Lost) => return Ok(Delivered::LostLease),
                        (Wait::Elapsed, Lease::Held) => {}
                    }
                    attempt += 1;
                    continue;
                }
                Err(Failure::Handler(
                    handler::HandlerError::Transient(source) | handler::HandlerError::Fatal(source),
                )) => source,
            };

            return Err(SubscribeError::HandlerFailed {
                first: batch.first_position().unwrap_or(batch.last_position()),
                last: batch.last_position(),
                source: error,
            });
        }
    }

    /// Waits for the waker, renewing the lease when it is due, and for at most `max_idle`.
    async fn wait(
        &mut self,
        term: &mut Term,
        known: &high_water::HighWater,
        shutdown: &mut Shutdown,
    ) -> Result<Waited, SubscribeError> {
        let idle_until = Instant::now() + self.max_idle;

        loop {
            let renew_at = self.ctx.renew_at(term);

            let woken = tokio::select! {
                woken = waker::DynWaker::changed(&mut *self.waker, known) => Some(woken),
                () = tokio::time::sleep_until(renew_at) => None,
                () = tokio::time::sleep_until(idle_until) => {
                    if let Some(replica) = &mut self.replica {
                        replica.woken(None);
                    }
                    return Ok(Waited::Woken);
                }
                () = shutdown.as_mut() => return Ok(Waited::Shutdown),
            };

            match woken {
                Some(Ok(high)) => {
                    if let Some(replica) = &mut self.replica {
                        replica.woken(high.lsn());
                    }
                    return Ok(Waited::Woken);
                }
                Some(Err(error)) => return Err(SubscribeError::Wake(error)),
                None => match self.ctx.renew(term.shard, term.fence).await? {
                    Some(deadline) => term.deadline = deadline,
                    None => return Ok(Waited::LostLease),
                },
            }
        }
    }

    fn set_state(&self, state: State) {
        self.status.send_modify(|status| status.state = state);
    }

    fn recovering(&self, error: SubscribeError) {
        self.set_state(State::Recovering(Arc::new(error)));
    }

    fn running(&self) {
        self.status.send_if_modified(|status| {
            if matches!(status.state, State::Running) {
                return false;
            }

            status.state = State::Running;
            true
        });
    }
}

impl Ctx {
    fn new(setup: &Setup) -> Self {
        Self {
            pool: setup.store.pool().clone(),
            sql: Arc::new(sql::Statements::new(pges_postgres::sql::store_name(
                &setup.store,
            ))),
            store: setup.store.name().clone(),
            name: setup.name.as_str().to_owned(),
            owner: Uuid::now_v7(),
            shards: setup.shards.to_i16(),
            lease: setup.lease,
            interval: interval(setup.lease),
        }
    }

    /// Stores the requested definition in every shard row, adds missing shards, moves every cursor to
    /// `start`, bumps every fence and clears every lease, so instances of the old definition stand down. A
    /// definition that is already stored is left alone, so a second instance redefining at the same time
    /// does not move the cursors again.
    async fn redefine(
        &self,
        start: Start,
        requested: &[category::CategoryId],
    ) -> Result<(), SubscribeError> {
        let position = self.start_position(start).await?;
        let ids: Vec<i32> = requested.iter().map(|id| id.get()).collect();
        self.sql
            .redefine
            .bind(sql::statement::redefine::Params {
                name: &self.name,
                position: read::to_i64(position),
                shards: self.shards,
                category_ids: &ids,
            })
            .execute(&self.pool)
            .await?;

        Ok(())
    }

    async fn start_position(&self, start: Start) -> Result<position::Position, SubscribeError> {
        match start {
            Start::Beginning => Ok(position::Position::BEGINNING),
            Start::Position(position) => Ok(position),
            Start::Now => {
                let head = self
                    .sql
                    .head
                    .bind(sql::statement::head::Params {})
                    .fetch_one(&self.pool)
                    .await?
                    .head;
                read::to_position(head)
            }
        }
    }

    /// Takes the lease of the lowest shard whose lease is free or lapsed. `None`: every shard is leased.
    /// Fails with [`SubscribeError::CrateTooOld`], taking nothing, once a migration dropped support for this
    /// crate.
    async fn acquire(&self) -> Result<Option<Term>, SubscribeError> {
        let sent = Instant::now();
        let row = self
            .sql
            .acquire
            .bind(sql::statement::acquire::Params {
                name: &self.name,
                owner: self.owner,
                lease: self.interval,
                shards: self.shards,
                shared_version: migrate::latest_version(migrate::SHARED).get(),
                store: self.store.as_str(),
                store_version: migrate::latest_version(migrate::STORE).get(),
                outbox_version: migrate::latest_version(migrate::OUTBOX).get(),
            })
            .fetch_one(&self.pool)
            .await?;
        if row.too_old == Some(true) {
            let mut conn = self.pool.acquire().await?;
            let too_old = migrate::crate_too_old(&mut conn, migrate::Scopes::OneStore(&self.store))
                .await
                .map_err(SubscribeError::Database)?;
            return match too_old {
                Some(too_old) => Err(SubscribeError::CrateTooOld {
                    scope: too_old.scope,
                    min_compatible: too_old.min_compatible,
                    crate_version: too_old.crate_version,
                }),
                None => Ok(None),
            };
        }

        let Some(shard) = row.shard else {
            return Ok(None);
        };
        let (Some(fence), Some(position), Some(shards), Some(category_ids)) =
            (row.fence, row.position, row.shards, row.category_ids)
        else {
            return Err(SubscribeError::Corrupt {
                position: position::Position::BEGINNING,
                reason: "a taken subscription lease came back with a NULL column",
            });
        };

        let mut stored: Vec<category::CategoryId> = category_ids
            .into_iter()
            .map(category::CategoryId::new)
            .collect();
        stored.sort_unstable();

        Ok(Some(Term {
            shard,
            fence,
            cursor: position::Position::from_i64(position).unwrap_or(position::Position::BEGINNING),
            deadline: sent + self.lease,
            stored,
            stored_shards: shards,
            cursor_state: CursorState::Confirmed,
        }))
    }

    /// The stored cursor, or `None` if the fence moved on.
    async fn stored_cursor(
        &self,
        term: &Term,
    ) -> Result<Option<position::Position>, SubscribeError> {
        let row = self
            .sql
            .cursor
            .bind(sql::statement::cursor::Params {
                name: &self.name,
                shard: term.shard,
                fence: term.fence,
            })
            .fetch_optional(&self.pool)
            .await?;
        let Some(row) = row else {
            return Ok(None);
        };

        Ok(Some(read::to_position(row.position)?))
    }

    /// Moves the cursor to `to` on `conn`, renewing the lease when less than half of it is left.
    ///
    /// `began` is an instant before `conn`'s transaction began. The renewal's `now()` is the transaction's
    /// start time, which in `run_pg` is before the handler ran, so the new deadline counts from there. A
    /// renewal that would not move the deadline (a side renewal during the handler went further) is skipped,
    /// so the checkpoint never shortens the lease.
    async fn checkpoint(
        &self,
        conn: &mut PgConnection,
        term: &Term,
        to: position::Position,
        began: Instant,
    ) -> Result<Checkpoint, sqlx::Error> {
        let renewed = began + self.lease;
        let due = term.deadline.saturating_duration_since(Instant::now()) < self.lease / 2
            && renewed > term.deadline;
        let updated = if due {
            self.sql
                .checkpoint_renew
                .bind(sql::statement::checkpoint_renew::Params {
                    name: &self.name,
                    shard: term.shard,
                    position: read::to_i64(to),
                    fence: term.fence,
                    lease: self.interval,
                    shared_version: migrate::latest_version(migrate::SHARED).get(),
                    store: self.store.as_str(),
                    store_version: migrate::latest_version(migrate::STORE).get(),
                    outbox_version: migrate::latest_version(migrate::OUTBOX).get(),
                    from: read::to_i64(term.cursor),
                })
                .execute(&mut *conn)
                .await?
        } else {
            self.sql
                .checkpoint
                .bind(sql::statement::checkpoint::Params {
                    name: &self.name,
                    shard: term.shard,
                    position: read::to_i64(to),
                    fence: term.fence,
                    from: read::to_i64(term.cursor),
                })
                .execute(&mut *conn)
                .await?
        };
        if updated.rows_affected() == 0 {
            return Ok(Checkpoint::Refused);
        }

        Ok(Checkpoint::Moved(due.then_some(renewed)))
    }

    /// Renews the lease in autocommit on a pool connection. Returns the new deadline, or `None` if the fence
    /// moved on.
    async fn renew(&self, shard: i16, fence: i64) -> Result<Option<Instant>, sqlx::Error> {
        let sent = Instant::now();
        let updated = self
            .sql
            .renew
            .bind(sql::statement::renew::Params {
                name: &self.name,
                shard,
                fence,
                lease: self.interval,
                shared_version: migrate::latest_version(migrate::SHARED).get(),
                store: self.store.as_str(),
                store_version: migrate::latest_version(migrate::STORE).get(),
                outbox_version: migrate::latest_version(migrate::OUTBOX).get(),
            })
            .execute(&self.pool)
            .await?;
        if updated.rows_affected() == 0 {
            return Ok(None);
        }

        Ok(Some(sent + self.lease))
    }

    /// When the lease is due for renewal: once less than half of it is left.
    fn renew_at(&self, term: &Term) -> Instant {
        term.deadline
            .checked_sub(self.lease / 2)
            .unwrap_or_else(Instant::now)
    }

    /// Ends a term whose lease renewal or fenced checkpoint was refused. Renewals are also refused once a
    /// migration dropped support for this crate; that stops the subscription with
    /// [`SubscribeError::CrateTooOld`] instead of standing by. If the check itself fails, the term ends as a
    /// lost lease and the next renewal checks again.
    async fn lost_lease(&self, term: &Term) -> Result<Ended, SubscribeError> {
        let too_old = match self.pool.acquire().await {
            Ok(mut conn) => {
                migrate::crate_too_old(&mut conn, migrate::Scopes::OneStore(&self.store)).await
            }
            Err(error) => Err(error.into()),
        };
        let Ok(Some(too_old)) = too_old else {
            return Ok(Ended::LostLease);
        };

        self.release(term).await;
        Err(SubscribeError::CrateTooOld {
            scope: too_old.scope,
            min_compatible: too_old.min_compatible,
            crate_version: too_old.crate_version,
        })
    }

    /// Gives the lease up, so a standby takes over at once. Best effort: if it fails, the lease lapses.
    async fn release(&self, term: &Term) {
        let _ = self
            .sql
            .release
            .bind(sql::statement::release::Params {
                name: &self.name,
                shard: term.shard,
                fence: term.fence,
            })
            .execute(&self.pool)
            .await;
    }
}

/// Checks the definition, loads the categories and their event types, and creates the subscription row
/// if it does not exist.
async fn prepare(
    ctx: &Ctx,
    setup: &Setup,
) -> Result<(read::Reader, Vec<category::CategoryId>), SubscribeError> {
    let max_connections = ctx.pool.options().get_max_connections();
    if max_connections < setup.pool_needs {
        return Err(SubscribeError::PoolTooSmall {
            max_connections,
            needed: setup.pool_needs,
        });
    }

    let reading = read::Reading {
        scope: setup.scope,
        limit: setup.batch_size,
        on_unknown: setup.on_unknown,
        shards: ctx.shards,
        before_horizon: read::BeforeHorizon::Fail,
    };
    let store = pges_postgres::sql::store_name(&setup.store);
    let watched = match setup.scope {
        read::Scope::Listed => {
            read::resolve(
                &ctx.pool,
                &ctx.sql,
                store,
                &setup.categories,
                reading.sharding(),
            )
            .await?
        }
        read::Scope::All => Vec::new(),
    };
    let requested: Vec<category::CategoryId> = watched.iter().map(|watched| watched.id).collect();

    let mut reader = read::Reader::new(ctx.pool.clone(), ctx.sql.clone(), store, watched, reading);
    reader.load_types().await?;

    let start = ctx.start_position(setup.start).await?;
    let ids: Vec<i32> = requested.iter().map(|id| id.get()).collect();
    let created = ctx
        .sql
        .create
        .bind(sql::statement::create::Params {
            position: read::to_i64(start),
            name: &ctx.name,
            shards: ctx.shards,
            category_ids: &ids,
        })
        .execute(&ctx.pool)
        .await;
    match created {
        Ok(_) => {}
        // Another instance created the subscription at the same moment; the definitions are compared when
        // a lease is taken.
        Err(error) if is_unique_violation(&error) => {}
        Err(error) => return Err(error.into()),
    }

    Ok((reader, requested))
}

/// Exactly once: the handler's writes and the checkpoint commit in one transaction.
async fn deliver_pg(
    ctx: &Ctx,
    handler: &mut dyn handler::DynPgHandler,
    term: &mut Term,
    batch: &batch::Batch,
) -> Result<Delivered, Failure> {
    let began = Instant::now();
    let mut conn = ctx.pool.acquire().await.map_err(database)?;
    let mut tx = conn.begin().await.map_err(database)?;
    let (handled, lease) = renewing(ctx, term, handler.handle(&mut tx, batch)).await;
    if lease == Lease::Lost {
        let _ = tx.rollback().await;
        return Ok(Delivered::LostLease);
    }
    if let Err(error) = handled {
        let _ = tx.rollback().await;
        return Err(Failure::Handler(error));
    }
    // Without this, the checkpoint would move in autocommit and the commit below do nothing.
    if !tx.is_in_transaction() {
        return Err(Failure::Handler(handler::HandlerError::Transient(
            Box::new(EndedTransaction),
        )));
    }

    let renewed = match ctx
        .checkpoint(&mut tx, term, batch.last_position(), began)
        .await
    {
        Ok(Checkpoint::Moved(renewed)) => renewed,
        Ok(Checkpoint::Refused) => {
            let _ = tx.rollback().await;
            return Ok(Delivered::Refused);
        }
        Err(error) if is_aborted_transaction(&error) => {
            let _ = tx.rollback().await;
            return Err(Failure::Handler(handler::HandlerError::Transient(
                Box::new(AbortedTransaction),
            )));
        }
        Err(error) if is_refused(&error) => {
            let _ = tx.rollback().await;
            return Err(Failure::Handler(refused(error)));
        }
        Err(error) => return Err(database(error)),
    };
    // A refused COMMIT (a deferred constraint, a serialization failure) rolled the batch back for sure:
    // it follows the retry policy ([`refused`]). Any other error may hide a commit.
    if let Err(error) = tx.commit().await {
        return Err(if is_refused(&error) {
            Failure::Handler(refused(error))
        } else {
            database(error)
        });
    }
    renew(term, renewed);

    Ok(Delivered::Checkpointed)
}

/// At least once: the checkpoint follows the handler in its own transaction.
async fn deliver_external(
    ctx: &Ctx,
    handler: &mut dyn handler::DynHandler,
    term: &mut Term,
    batch: &batch::Batch,
) -> Result<Delivered, Failure> {
    let (handled, lease) = renewing(ctx, term, handler.handle(batch)).await;
    if lease == Lease::Lost {
        return Ok(Delivered::LostLease);
    }
    handled.map_err(Failure::Handler)?;

    let mut conn = ctx.pool.acquire().await.map_err(database)?;
    match ctx
        .checkpoint(&mut conn, term, batch.last_position(), Instant::now())
        .await
        .map_err(database)?
    {
        Checkpoint::Moved(renewed) => {
            renew(term, renewed);
            Ok(Delivered::Checkpointed)
        }
        Checkpoint::Refused => Ok(Delivered::Refused),
    }
}

/// Runs `work` while renewing the lease on another pool connection whenever less than half of it is left,
/// so a slow handler or a long retry backoff does not hand the lease to a standby.
///
/// A renewal still in flight when `work` finishes is dropped: the deadline then stays the earlier one, which
/// only makes the next renewal come sooner. Dropping it also keeps a pool of one connection from waiting on
/// itself. After a renewal finds the fence moved, `work` runs to the end without further renewals and the
/// result says [`Lease::Lost`].
async fn renewing<T>(ctx: &Ctx, term: &mut Term, work: impl Future<Output = T>) -> (T, Lease) {
    let mut work = std::pin::pin!(work);
    let (shard, fence) = (term.shard, term.fence);
    let mut retry_at = None;

    loop {
        let due = match retry_at {
            Some(retry_at) => ctx.renew_at(term).max(retry_at),
            None => ctx.renew_at(term),
        };
        let renewal = async {
            tokio::time::sleep_until(due).await;
            ctx.renew(shard, fence).await
        };

        tokio::select! {
            output = &mut work => return (output, Lease::Held),
            renewed = renewal => match renewed {
                Ok(Some(deadline)) => {
                    term.deadline = deadline;
                    retry_at = None;
                }
                Ok(None) => return (work.await, Lease::Lost),
                // Tried again soon; if the lease lapses meanwhile, the fenced checkpoint keeps it safe.
                Err(_) => retry_at = Some(Instant::now() + ctx.lease / RENEW_RETRY_FRACTION),
            },
        }
    }
}

/// Sets the status to [`State::Stopped`] when the run ends, also when its future is dropped mid-run.
struct MarkStopped(watch::Sender<Status>);

impl Drop for MarkStopped {
    fn drop(&mut self) {
        self.0.send_modify(|status| status.state = State::Stopped);
    }
}

fn database(error: sqlx::Error) -> Failure {
    Failure::Database(error.into())
}

fn is_unique_violation(error: &sqlx::Error) -> bool {
    error
        .as_database_error()
        .and_then(|error| error.code())
        .is_some_and(|code| code == "23505")
}

pub(crate) fn is_aborted_transaction(error: &sqlx::Error) -> bool {
    error
        .as_database_error()
        .and_then(|error| error.code())
        .is_some_and(|code| code == "25P02")
}

/// Whether the server answered the statement with an ERROR: it failed, and in a transaction block the
/// transaction can only roll back, while the session goes on. A FATAL error or a lost connection is not
/// refused: a COMMIT may have taken effect before it.
fn is_refused(error: &sqlx::Error) -> bool {
    error
        .as_database_error()
        .and_then(|error| error.try_downcast_ref::<PgDatabaseError>())
        .is_some_and(|error| matches!(error.severity(), PgSeverity::Error))
}

/// The handler failure for an ERROR the server returned for `run_pg`'s checkpoint or COMMIT, which rolled the
/// batch back. Transient, so the batch is retried and the [`RetryPolicy`] still stops a batch that can never
/// commit once its attempts run out: most such errors are not the batch's fault and pass (a lock or statement
/// timeout behind a lost COMMIT's row lock, a serialization failure, a read-only transaction during a
/// failover). Fatal for an integrity constraint violation (class 23, such as a deferred unique constraint)
/// or a data exception (class 22, such as a deferred trigger's cast) at the COMMIT: the same batch fails
/// the same way on every attempt.
fn refused(error: sqlx::Error) -> handler::HandlerError {
    let error = pges_postgres::error::DbError::from(error);
    let never_commits = error
        .code()
        .is_some_and(|code| code.starts_with("23") || code.starts_with("22"));
    if never_commits {
        handler::HandlerError::Fatal(Box::new(error))
    } else {
        handler::HandlerError::Transient(Box::new(error))
    }
}

fn renew(term: &mut Term, renewed: Option<Instant>) {
    if let Some(deadline) = renewed {
        term.deadline = deadline;
    }
}

fn replace(slot: &mut position::Position, value: position::Position) -> bool {
    if *slot == value {
        return false;
    }

    *slot = value;
    true
}

async fn sleep_or_shutdown(duration: Duration, shutdown: &mut Shutdown) -> Wait {
    tokio::select! {
        () = tokio::time::sleep(duration) => Wait::Elapsed,
        () = shutdown.as_mut() => Wait::Shutdown,
    }
}

/// `^[a-z][a-z0-9_.-]{0,99}$`, the CHECK on `subscriptions.name` and `backfills.name`. Names are bound, never
/// interpolated; the type gives a typed error instead of a constraint violation.
pub(crate) const fn validate_name(name: &str) -> Result<(), SubscriptionNameError> {
    let bytes = name.as_bytes();
    if bytes.is_empty() {
        return Err(SubscriptionNameError::Empty);
    }
    if bytes.len() > SubscriptionName::MAX_LEN {
        return Err(SubscriptionNameError::TooLong);
    }
    if !bytes[0].is_ascii_lowercase() {
        return Err(SubscriptionNameError::InvalidStart);
    }

    let mut i = 1;
    while i < bytes.len() {
        match bytes[i] {
            b'a'..=b'z' | b'0'..=b'9' | b'_' | b'.' | b'-' => {}
            _ => return Err(SubscriptionNameError::InvalidCharacter),
        }
        i += 1;
    }

    Ok(())
}

fn interval(duration: Duration) -> PgInterval {
    PgInterval {
        months: 0,
        days: 0,
        microseconds: i64::try_from(duration.as_micros()).unwrap_or(i64::MAX),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retry_waits_double_up_to_the_cap() {
        let policy = RetryPolicy::new(pges_core::interval!(100 ms), NonZeroU32::MIN);

        assert_eq!(policy.wait(1), Duration::from_millis(100));
        assert_eq!(policy.wait(2), Duration::from_millis(200));
        assert_eq!(policy.wait(4), Duration::from_millis(800));
        assert_eq!(policy.wait(40), MAX_BACKOFF);
    }

    #[test]
    fn lag_counts_positions_behind_the_high_water_mark() {
        let status = Status {
            state: State::Running,
            position: pges_core::position!(7),
            high_water: pges_core::position!(10),
            shards: Vec::new(),
        };

        assert_eq!(status.lag(), 3);
    }

    #[test]
    fn accepts_subscription_names_only() {
        let long: &'static str = "a".repeat(100).leak();
        let too_long: &'static str = "a".repeat(101).leak();

        assert!(SubscriptionName::new("jobs-board.v3").is_ok());
        assert!(SubscriptionName::new("a").is_ok());
        assert!(SubscriptionName::new(long).is_ok());
        assert_eq!(
            SubscriptionName::new(too_long),
            Err(SubscriptionNameError::TooLong)
        );
        assert_eq!(SubscriptionName::new(""), Err(SubscriptionNameError::Empty));
        assert_eq!(
            SubscriptionName::new("Jobs"),
            Err(SubscriptionNameError::InvalidStart)
        );
        assert_eq!(
            SubscriptionName::new("1jobs"),
            Err(SubscriptionNameError::InvalidStart)
        );
        assert_eq!(
            SubscriptionName::parse("jobs board"),
            Err(SubscriptionNameError::InvalidCharacter)
        );
        assert_eq!(
            "jobs-board.v3".parse::<SubscriptionName>(),
            Ok(crate::subscription_name!("jobs-board.v3"))
        );
    }

    #[test]
    fn shards_are_between_one_and_the_maximum() {
        assert_eq!(Shards::new(0), None);
        assert_eq!(Shards::new(1), Some(Shards::ONE));
        assert_eq!(Shards::new(1024), Some(Shards::MAX));
        assert_eq!(Shards::new(1025), None);
        assert_eq!(crate::shards!(8).get(), 8);
    }
}
