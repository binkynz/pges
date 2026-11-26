//! The `LISTEN/NOTIFY` notifier: after a batch's log rows commit, one `NOTIFY pges_<store>` per store that
//! changed, sent on its own pool connection, alongside the batch loop in the same task, so a slow or failing
//! notification never holds up the log.

use std::{
    collections::{BTreeMap, BTreeSet},
    convert::Infallible,
    sync::{Arc, Mutex, PoisonError},
};

use pges_postgres::sql::typed::Query as _;
use sqlx::PgPool;

use pges_core::{category, high_water, store};

use crate::sql::statement;

/// Notifications waiting to be sent, merged per store, and the signal that wakes the sender.
#[derive(Debug, Clone, Default)]
pub(crate) struct Notifier {
    inner: Arc<Inner>,
}

impl Notifier {
    /// Queues a notification for `store` (channel `pges_<schema>`). A notification still waiting for the same
    /// store is replaced by the newer high-water mark, keeping the categories both touched.
    pub(crate) fn queue(
        &self,
        store: store::StoreId,
        schema: &str,
        high_water: high_water::HighWater,
        touched: &BTreeSet<category::CategoryId>,
    ) {
        let mut waiting = self.waiting();
        let entry = waiting.entry(store).or_insert_with(|| Waiting {
            channel: channel(schema),
            high_water: high_water.clone(),
            touched: BTreeSet::new(),
        });
        entry.high_water = high_water;
        entry.touched.extend(touched.iter().copied());
        drop(waiting);

        self.inner.wake.notify_one();
    }

    /// Sends queued notifications until dropped. Failures are logged and the notifications dropped: they are
    /// hints, and the next batch sends newer ones.
    pub(crate) async fn run(&self, pool: &PgPool) -> Infallible {
        loop {
            self.inner.wake.notified().await;
            let waiting = std::mem::take(&mut *self.waiting());
            if waiting.is_empty() {
                continue;
            }

            if let Err(error) = send(pool, waiting.into_values()).await {
                tracing::warn!(%error, "NOTIFY failed; subscribers wake on the next batch");
            }
        }
    }

    fn waiting(&self) -> std::sync::MutexGuard<'_, BTreeMap<store::StoreId, Waiting>> {
        self.inner
            .waiting
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }
}

#[derive(Debug, Default)]
struct Inner {
    waiting: Mutex<BTreeMap<store::StoreId, Waiting>>,
    wake: tokio::sync::Notify,
}

#[derive(Debug)]
struct Waiting {
    channel: String,
    high_water: high_water::HighWater,
    touched: BTreeSet<category::CategoryId>,
}

/// The channel a store's notifications go to. Store names are at most 40 characters, so it stays within
/// PostgreSQL's 63-byte identifier limit.
fn channel(schema: &str) -> String {
    format!("pges_{schema}")
}

/// One statement, so the notifications commit together in a transaction of their own.
async fn send(pool: &PgPool, waiting: impl Iterator<Item = Waiting>) -> Result<(), sqlx::Error> {
    let (channels, payloads): (Vec<String>, Vec<String>) = waiting
        .map(|waiting| {
            let notice = high_water::Notice::new(&waiting.high_water, waiting.touched);
            (waiting.channel, notice.encode())
        })
        .unzip();

    statement::notify::Query::fixed()
        .bind(statement::notify::Params { channels, payloads })
        .execute(pool)
        .await?;

    Ok(())
}
