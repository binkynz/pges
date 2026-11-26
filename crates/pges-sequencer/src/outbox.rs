//! The outbox source: commits queued in each outbox store's `pending` table.

use std::{fmt, num::NonZeroUsize, time::Duration};

use futures_util::future::BoxFuture;
use sqlx::{
    PgConnection, PgPool,
    postgres::{PgConnectOptions, PgListener, PgNotification, PgPoolOptions},
};
use tokio::time::Instant;

use pges_core::{category, commit, interval, store};
use pges_postgres::sql::{
    template,
    typed::{self, Query as _},
};

use crate::{log, source, source_kind, sql::statement};

/// The channel outbox stores notify on: `pges__outbox`, shared by all outbox stores of a database.
///
/// A transaction that queued commits in a store's `pending` table notifies once per store when it commits,
/// however many commits it queued there. The payload is the store's schema name. An append that queued
/// nothing does not notify.
///
/// [`OutboxSource`] listens on it. Listen on it yourself to observe outbox commits as the source sees them.
pub const CHANNEL: &str = "pges__outbox";

/// Sequences the stores created with the outbox option (`pges.stores.outbox`).
///
/// Appends to such a store also insert a row into its `pending` table, numbered by an uncached sequence at
/// the end of the append, so if one append saw another's commit, its number is higher. The source drains
/// `pending` inside the sequencer's batch transaction (`DELETE … RETURNING`, then sorted by number), so the
/// rows disappear exactly when their log rows commit, and a rolled-back batch leaves them in place. It needs
/// no token and no replication; its costs are an extra insert and delete per commit, vacuum churn on
/// `pending`, and a notification per committing transaction.
///
/// # Wake-ups
///
/// The source does not poll. A trigger on `pending` sends a notification on the channel [`CHANNEL`]
/// when an append commits, and the source listens on a connection of its own, opened with the pool's
/// connect options; it does not hold one of the pool's connections. On a notification it drains until a
/// drain returns less than its budget, then waits for the next one. It listens before its first drain after
/// opening, and after it lost the listening connection it listens again and drains at once, so no commit
/// falls between a drain and the notifications. While the sequencer stands by, nothing listens.
///
/// [`OutboxConfig::recheck_interval`] is a fallback, not the live path: after that long without a
/// notification the source checks the queues with one small query, in case a notification was lost. The
/// query runs on the listening connection, so one that died silently (no reset reached the source, as when
/// a firewall drops an idle connection) is found and replaced: if the query fails or takes longer than the
/// recheck interval (at least a second), the source listens again and drains.
///
/// A notification from a store the source does not know yet reads the list of outbox stores again; the
/// list is also read every [`OutboxConfig::store_refresh`].
///
/// `LISTEN` needs a session of its own, so the pool's connect options must not point at a pooler in
/// transaction mode.
///
/// # Cost at high write rates
///
/// PostgreSQL serialises the commits of transactions that sent a notification: each takes a lock shared by
/// the whole server at commit and holds it until the commit is done, WAL flush included. Appends to outbox
/// stores therefore commit one at a time, which caps their rate well below what the server sustains
/// otherwise. At high write rates use the logical source (`logical::LogicalSource`, the default, with the
/// `source-logical` feature), which adds nothing to the appends' commits.
///
/// A database should be sequenced by one kind of source. Stores without the outbox option are not drained
/// by this source.
#[derive(Debug)]
pub struct OutboxSource {
    pool: PgPool,
    config: OutboxConfig,
    stores: Vec<OutboxStore>,
    probe: typed::Rendered<statement::outbox_probe::Query>,
    refreshed_at: Option<Instant>,
    first_store: usize,
    backlog: Backlog,
    listener: Option<Listener>,
    recheck_at: Option<Instant>,
    recheck_answer_by: Option<Instant>,
    connecting: Option<Connecting>,
}

impl OutboxSource {
    /// An outbox source reading through `pool`. It listens for wake-ups on a connection of its own, made
    /// with the pool's connect options.
    pub fn new(pool: PgPool, config: OutboxConfig) -> Self {
        Self {
            pool,
            config,
            stores: Vec::new(),
            probe: probe_sql(&[]),
            refreshed_at: None,
            first_store: 0,
            backlog: Backlog::Drained,
            listener: None,
            recheck_at: None,
            recheck_answer_by: None,
            connecting: None,
        }
    }

    async fn refresh_stores(&mut self) -> Result<(), source::SourceError> {
        let rows = statement::outbox_stores::Query::fixed()
            .bind(statement::outbox_stores::Params {})
            .fetch_all(&self.pool)
            .await?;

        let mut stores = Vec::with_capacity(rows.len());
        for statement::outbox_stores::Row {
            store_id: id,
            schema_name: name,
        } in rows
        {
            let Ok(schema) = store::StoreName::parse(name.as_str()) else {
                return Err(source::SourceError::Invalid(format!(
                    "store {id} has an invalid schema name {name:?}"
                )));
            };
            stores.push(OutboxStore::new(store::StoreId::new(id), schema));
        }

        // A store new to this source may hold commits whose notifications it never saw.
        let known = |outbox: &OutboxStore| self.stores.iter().any(|old| old.id == outbox.id);
        if !stores.iter().all(known) {
            self.backlog = Backlog::Unknown;
        }
        self.probe = probe_sql(&stores);
        self.stores = stores;
        self.refreshed_at = Some(Instant::now());

        Ok(())
    }

    fn refresh_due(&self) -> bool {
        self.refreshed_at
            .is_none_or(|at| at.elapsed() >= self.config.store_refresh.get())
    }

    /// Probes the queue heads through the listening connection, which also shows that it still answers.
    async fn recheck(&mut self) -> Recheck {
        let Some(listener) = self.listener.as_mut() else {
            return Recheck::ListenerLost;
        };
        let probe = &self.probe;
        // Kept across calls: the sequencer drops `ready` to renew its lease, more often than this deadline
        // may be, and the recheck that follows must not start the clock again.
        let answer_by = *self.recheck_answer_by.get_or_insert_with(|| {
            Instant::now() + self.config.recheck_interval.get().max(MIN_PROBE_TIMEOUT)
        });

        let probed = tokio::time::timeout_at(
            answer_by,
            probe
                .bind(statement::outbox_probe::Params {})
                .fetch_optional(&mut listener.listener),
        )
        .await;
        self.recheck_answer_by = None;
        match probed {
            Ok(Ok(Some(_))) => Recheck::Pending,
            Ok(Ok(None)) => Recheck::Empty,
            // A real problem with the queues shows again in the drain that follows a new listener.
            Ok(Err(error)) => {
                tracing::debug!(%error, "the outbox source's recheck failed");
                Recheck::ListenerLost
            }
            Err(_) => {
                tracing::debug!(
                    "the outbox source's listening connection did not answer its recheck"
                );
                Recheck::ListenerLost
            }
        }
    }

    fn connect_options(&self) -> PgConnectOptions {
        (*self.pool.connect_options()).clone()
    }

    /// Records a notification. Its payload names the store; an unknown one reads the store list again.
    fn notified(&mut self, notification: &PgNotification) {
        self.backlog = Backlog::Unknown;
        let store = notification.payload();
        if !self
            .stores
            .iter()
            .any(|outbox| outbox.schema.as_str() == store)
        {
            self.refreshed_at = None;
        }
    }
}

impl source::Source for OutboxSource {
    fn kind(&self) -> source::SourceKind {
        source_kind!("outbox")
    }

    async fn open(&mut self, _token: Option<&[u8]>) -> Result<(), source::SourceError> {
        self.backlog = Backlog::Unknown;
        self.recheck_at = None;
        self.recheck_answer_by = None;
        if let Some(listener) = self.listener.take() {
            listener.close().await;
        }

        // Before the first drain: a commit after this point is notified, one before it is drained.
        self.connecting = None;
        self.listener = Some(listen(self.connect_options()).await?);
        self.refresh_stores().await
    }

    async fn ready(&mut self) -> Result<(), source::SourceError> {
        // Cancel safety: each await's outcome is stored before the next await, and repeating one is harmless.
        loop {
            if self.listener.is_none() {
                // Kept across calls: the sequencer drops `ready` to renew its lease, perhaps more often than
                // connecting takes.
                let options = self.connect_options();
                let connecting = self
                    .connecting
                    .get_or_insert_with(|| Connecting(Box::pin(listen(options))));
                let connected = (&mut connecting.0).await;
                self.connecting = None;
                let listener = connected?;
                // Commits made while nobody listened notified no one.
                self.backlog = Backlog::Unknown;
                self.listener = Some(listener);
            }
            if self.refresh_due() {
                self.refresh_stores().await?;
            }
            if self.backlog != Backlog::Drained {
                self.recheck_at = None;
                self.recheck_answer_by = None;
                return Ok(());
            }

            let now = Instant::now();
            let recheck_at = *self
                .recheck_at
                .get_or_insert(now + self.config.recheck_interval.get());
            let refresh_at = self
                .refreshed_at
                .map_or(now, |at| at + self.config.store_refresh.get());
            let Some(listener) = self.listener.as_mut() else {
                continue;
            };

            let woke = tokio::select! {
                received = listener.listener.try_recv() => Woke::Received(received),
                () = tokio::time::sleep_until(recheck_at.min(refresh_at)) => Woke::Timer,
            };
            match woke {
                Woke::Received(Ok(Some(notification))) => {
                    let mut notifications = vec![notification];
                    notifications.extend(std::iter::from_fn(|| listener.listener.next_buffered()));
                    for notification in &notifications {
                        self.notified(notification);
                    }
                }
                // The connection is gone (`None`), or broken in a way the listener does not recognise as
                // lost. Either way notifications may have been missed: listen on a new one, then drain.
                Woke::Received(Ok(None) | Err(_)) => {
                    tracing::debug!("the outbox source lost its listening connection");
                    self.listener = None;
                }
                Woke::Timer if Instant::now() >= recheck_at => {
                    match self.recheck().await {
                        Recheck::Empty => {}
                        Recheck::Pending => self.backlog = Backlog::Unknown,
                        // A connection that died silently (no reset reaches us) delivers nothing, and only
                        // a query on it shows that. The next turn listens again and drains.
                        Recheck::ListenerLost => self.listener = None,
                    }
                    self.recheck_at = None;
                }
                // The store list is due; it is read at the top of the loop.
                Woke::Timer => {}
            }
        }
    }

    async fn next(
        &mut self,
        tx: &mut PgConnection,
        limit: NonZeroUsize,
    ) -> Result<source::SourceBatch, source::SourceError> {
        let mut commits = Vec::new();
        let mut backlog = Backlog::Drained;

        let count = self.stores.len();
        for offset in 0..count {
            let Some(budget) = NonZeroUsize::new(limit.get().saturating_sub(commits.len())) else {
                backlog = Backlog::Unknown;
                break;
            };
            let outbox = &self.stores[(self.first_store + offset) % count];

            let drained = drain(tx, outbox, budget).await?;
            if drained.len() == budget.get() {
                backlog = Backlog::Unknown;
            }
            commits.extend(drained);
        }

        // Start the next drain at another store, so one busy store cannot starve the others.
        self.first_store = (self.first_store + 1) % count.max(1);
        self.backlog = backlog;

        Ok(source::SourceBatch::new(commits, Vec::new()))
    }

    async fn ack(&mut self, _token: &[u8]) -> Result<(), source::SourceError> {
        Ok(())
    }

    /// Stops listening, so a standby holds no connection and buffers no notifications.
    async fn close(&mut self) {
        self.connecting = None;
        if let Some(listener) = self.listener.take() {
            listener.close().await;
        }
    }
}

/// How the [`OutboxSource`] waits for work.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct OutboxConfig {
    /// A fallback for lost notifications, not the live path: after this long without one, the source checks
    /// the queues with one small query on its listening connection. It bounds how long a commit whose
    /// notification was lost waits, and how soon a listening connection that died silently is replaced.
    /// Default 1 s.
    pub recheck_interval: interval::Interval,
    /// How often the list of outbox stores is read again. A notification from a store the source does not
    /// know reads it at once. Default 10 s.
    pub store_refresh: interval::Interval,
}

impl Default for OutboxConfig {
    fn default() -> Self {
        Self {
            recheck_interval: interval!(1 s),
            store_refresh: interval!(10 s),
        }
    }
}

/// The least time the listening connection gets to answer a recheck; it gets the recheck interval if that
/// is longer.
const MIN_PROBE_TIMEOUT: Duration = Duration::from_secs(1);

/// The connection listening on [`CHANNEL`], with the one-connection pool it came from.
#[derive(Debug)]
struct Listener {
    pool: PgPool,
    listener: PgListener,
}

impl Listener {
    /// Closes the connection, waiting at most a second.
    async fn close(self) {
        let Self { pool, listener } = self;
        let closing = pool.close();
        // Dropping the listener runs `UNLISTEN *` and hands the connection back to the closed pool, which
        // closes it.
        drop(listener);
        let _ = tokio::time::timeout(Duration::from_secs(1), closing).await;
    }
}

/// A connection being made by [`listen`], in flight while [`source::Source::ready`] is dropped and called
/// again.
struct Connecting(BoxFuture<'static, Result<Listener, sqlx::Error>>);

impl fmt::Debug for Connecting {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Connecting")
    }
}

enum Woke {
    Received(Result<Option<PgNotification>, sqlx::Error>),
    Timer,
}

/// What a recheck found.
enum Recheck {
    /// No outbox store has a pending commit.
    Empty,
    /// Some outbox store has a pending commit: a notification was lost.
    Pending,
    /// The listening connection failed or did not answer in time.
    ListenerLost,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Backlog {
    /// The last drain took everything it saw.
    Drained,
    /// More rows may be waiting: the last drain was full, or the source was just opened.
    Unknown,
}

#[derive(Debug)]
struct OutboxStore {
    id: store::StoreId,
    schema: store::StoreName,
    drain: typed::Rendered<statement::outbox_drain::Query>,
}

impl OutboxStore {
    fn new(id: store::StoreId, schema: store::StoreName) -> Self {
        let drain = statement::outbox_drain::Query::render(&template::Values::new().store(&schema));

        Self { id, schema, drain }
    }
}

async fn drain(
    tx: &mut PgConnection,
    outbox: &OutboxStore,
    limit: NonZeroUsize,
) -> Result<Vec<source::CommitRef>, source::SourceError> {
    let limit = i64::try_from(limit.get()).unwrap_or(i64::MAX);
    let rows = outbox
        .drain
        .bind(statement::outbox_drain::Params { limit })
        .fetch_all(&mut *tx)
        .await?;

    rows.into_iter()
        .map(|row| {
            let statement::outbox_drain::Row {
                commit_id,
                category_id,
                event_count,
            } = row;

            let commit_id = commit::CommitId::from_uuid(commit_id).ok_or_else(|| {
                source::SourceError::Invalid(format!("commit id {commit_id} is not a UUIDv7"))
            })?;
            let event_count = log::event_count_from_sql(event_count).ok_or_else(|| {
                source::SourceError::Invalid(format!("commit {commit_id} has {event_count} events"))
            })?;

            Ok(source::CommitRef::new(
                outbox.id,
                commit_id,
                category::CategoryId::new(category_id),
                event_count,
            ))
        })
        .collect()
}

/// Opens a connection with `options` and listens on [`CHANNEL`].
async fn listen(options: PgConnectOptions) -> Result<Listener, sqlx::Error> {
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .min_connections(0)
        .max_lifetime(None)
        .idle_timeout(None)
        .connect_lazy_with(options);
    let mut listener = PgListener::connect_with(&pool).await?;
    listener.eager_reconnect(false);
    listener.listen(CHANNEL).await?;

    Ok(Listener { pool, listener })
}

/// One statement that returns a row if any outbox store has pending commits: `outbox/probe_none` when there
/// is no outbox store.
fn probe_sql(stores: &[OutboxStore]) -> typed::Rendered<statement::outbox_probe::Query> {
    if stores.is_empty() {
        return typed::Rendered::variant::<statement::outbox_probe_none::Query>(
            &template::Values::new(),
        );
    }

    let none = template::Values::new;
    let mut parts = vec![(&statement::OUTBOX_PROBE_HEAD, none())];
    for (i, outbox) in stores.iter().enumerate() {
        if i > 0 {
            parts.push((&statement::OUTBOX_PROBE_UNION, none()));
        }
        parts.push((&statement::OUTBOX_PROBE_STORE, none().store(&outbox.schema)));
    }
    parts.push((&statement::OUTBOX_PROBE_TAIL, none()));

    typed::Rendered::compose(parts)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn outbox(id: i32, name: &str) -> OutboxStore {
        OutboxStore::new(
            store::StoreId::new(id),
            store::StoreName::parse(name).unwrap(),
        )
    }

    #[test]
    fn the_probe_is_the_registered_statement_composed_per_store() {
        let registered = statement::OUTBOX_PROBE
            .render(&template::Values::new().store(&pges_core::store_name!("ops")));

        assert_eq!(probe_sql(&[]).as_str(), statement::OUTBOX_PROBE_NONE.sql());
        assert_eq!(
            probe_sql(&[outbox(1, "ops"), outbox(2, "ops")]).as_str(),
            registered.as_str()
        );
        let three = probe_sql(&[outbox(1, "a"), outbox(2, "b"), outbox(3, "c")])
            .as_str()
            .to_owned();
        assert_eq!(three.matches("UNION ALL").count(), 2);
        assert!(three.contains("b.pending"));
    }
}
