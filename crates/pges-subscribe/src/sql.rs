//! SQL statements. This module supports the pges crates and is not a stable API: it can change in any
//! release.
//!
//! The crate keeps its SQL in `.sql` files under its `sql/` directory, declares every statement in one
//! registry ([`statement`]), and renders them once per subscription, backfill or waker with the store's
//! validated name, and once per category with names read from the `categories` table and validated again.

pub mod statement;

use pges_core::{category, store};
use pges_postgres::sql::{
    template,
    typed::{Query as _, Rendered},
};

/// Statements for one store.
#[derive(Debug)]
pub(crate) struct Statements {
    /// `H`, the store's newest position, 0 for an empty log.
    pub(crate) head: Rendered<statement::head::Query>,
    /// Category ids by name.
    pub(crate) categories: Rendered<statement::categories::Query>,
    /// Category names by id.
    pub(crate) categories_by_id: Rendered<statement::categories_by_id::Query>,
    /// Every event type of some categories.
    pub(crate) types: Rendered<statement::event_types::Query>,
    /// The retention horizon of some categories.
    pub(crate) horizons: Rendered<statement::horizons::Query>,
    /// The category with the highest retention horizon.
    pub(crate) highest_horizon: Rendered<statement::highest_horizon::Query>,
    /// A page of one category's log, checked against its retention horizon.
    pub(crate) read_category_log: Rendered<statement::read_category_log::Query>,
    /// A page of the log of every category, checked against the highest retention horizon.
    pub(crate) read_log: Rendered<statement::read_log::Query>,
    /// Which of some commits `delete_stream` deleted.
    pub(crate) deleted_commits: Rendered<statement::deleted_commits::Query>,
    /// Creates every shard row of the subscription unless the subscription exists.
    pub(crate) create: Rendered<statement::create::Query>,
    /// Takes over the lapsed lease of the lowest free shard and bumps its fence.
    pub(crate) acquire: Rendered<statement::acquire::Query>,
    /// Replaces the stored definition and cursor of every shard (`redefine`).
    pub(crate) redefine: Rendered<statement::redefine::Query>,
    /// Locks the shard's row for a `run_pg` batch, fenced.
    pub(crate) hold: Rendered<statement::hold::Query>,
    /// Moves the cursor at the end of a `run_pg` batch and renews the lease, fenced.
    pub(crate) checkpoint_held: Rendered<statement::checkpoint_held::Query>,
    /// Moves the cursor, fenced.
    pub(crate) checkpoint: Rendered<statement::checkpoint::Query>,
    /// Moves the cursor and renews the lease, fenced.
    pub(crate) checkpoint_renew: Rendered<statement::checkpoint_renew::Query>,
    /// Renews the lease, fenced.
    pub(crate) renew: Rendered<statement::renew::Query>,
    /// Gives the lease up, fenced.
    pub(crate) release: Rendered<statement::release::Query>,
    /// The stored cursor, fenced: re-read after an error that may have hidden a commit.
    pub(crate) cursor: Rendered<statement::cursor::Query>,
    /// Records an instance of a sharded subscription as alive and counts the live instances.
    pub(crate) heartbeat: Rendered<statement::heartbeat::Query>,
    /// Removes an instance of a sharded subscription.
    pub(crate) leave: Rendered<statement::leave::Query>,
}

impl Statements {
    pub(crate) fn new(store: &store::StoreName) -> Self {
        let values = template::Values::new().store(store);

        Self {
            head: head(store),
            categories: statement::categories::Query::render(&values),
            categories_by_id: statement::categories_by_id::Query::render(&values),
            types: statement::event_types::Query::render(&values),
            horizons: statement::horizons::Query::render(&values),
            highest_horizon: statement::highest_horizon::Query::render(&values),
            read_category_log: statement::read_category_log::Query::render(&values),
            read_log: statement::read_log::Query::render(&values),
            deleted_commits: statement::deleted_commits::Query::render(&values),
            create: statement::create::Query::render(&values),
            acquire: statement::acquire::Query::render(&values),
            redefine: statement::redefine::Query::render(&values),
            hold: statement::hold::Query::render(&values),
            checkpoint_held: statement::checkpoint_held::Query::render(&values),
            checkpoint: statement::checkpoint::Query::render(&values),
            checkpoint_renew: statement::checkpoint_renew::Query::render(&values),
            renew: statement::renew::Query::render(&values),
            release: statement::release::Query::render(&values),
            cursor: statement::cursor::Query::render(&values),
            heartbeat: statement::heartbeat::Query::render(&values),
            leave: statement::leave::Query::render(&values),
        }
    }
}

/// A backfill's statements for one store.
#[derive(Debug)]
pub(crate) struct Backfill {
    /// Creates the lanes unless they exist.
    pub(crate) create: Rendered<statement::backfill_create::Query>,
    /// The lanes, with their definition and cursors.
    pub(crate) lanes: Rendered<statement::backfill_lanes::Query>,
    /// Moves one lane's cursor in the handler's transaction, if it is still where the run left it.
    pub(crate) checkpoint: Rendered<statement::backfill_checkpoint::Query>,
    /// Moves the cursors of lanes that had no events in a page.
    pub(crate) advance: Rendered<statement::backfill_advance::Query>,
    /// A subscription's cursor, for `Until::Subscription`.
    pub(crate) subscription: Rendered<statement::backfill_subscription::Query>,
}

impl Backfill {
    pub(crate) fn new(store: &store::StoreName) -> Self {
        let values = template::Values::new().store(store);

        Self {
            create: statement::backfill_create::Query::render(&values),
            lanes: statement::backfill_lanes::Query::render(&values),
            checkpoint: statement::backfill_checkpoint::Query::render(&values),
            advance: statement::backfill_advance::Query::render(&values),
            subscription: statement::backfill_subscription::Query::render(&values),
        }
    }
}

/// `H`, the store's newest position, 0 for an empty log.
pub(crate) fn head(store: &store::StoreName) -> Rendered<statement::head::Query> {
    statement::head::Query::render(&template::Values::new().store(store))
}

/// The commits of one category named by a page of the log; for a shard, other shards' payloads are NULL.
#[derive(Debug, Clone)]
pub(crate) enum FetchCommits {
    /// Every stream's payloads.
    Whole(Rendered<statement::fetch_commits::Query>),
    /// One shard's payloads.
    Sharded(Rendered<statement::fetch_commits_sharded::Query>),
}

impl FetchCommits {
    pub(crate) fn new(
        store: &store::StoreName,
        category: category::CategoryName,
        sharding: Sharding,
    ) -> Self {
        let values = template::Values::new().store(store).category(category);
        match sharding {
            Sharding::Whole => Self::Whole(statement::fetch_commits::Query::render(&values)),
            Sharding::Sharded => {
                Self::Sharded(statement::fetch_commits_sharded::Query::render(&values))
            }
        }
    }
}

/// Whether reads keep only one shard's streams.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Sharding {
    Whole,
    Sharded,
}

/// The newest log entry of each category in `$1`, through `log_by_category`.
#[cfg(feature = "wake-poll")]
pub(crate) fn newest_by_category(
    store: &store::StoreName,
) -> Rendered<statement::newest_by_category::Query> {
    statement::newest_by_category::Query::render(&template::Values::new().store(store))
}
