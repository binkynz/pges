//! In-process announcements: each store's high-water mark and the sequencer's status.

use std::{
    collections::HashMap,
    sync::{Arc, Mutex, PoisonError},
};

use tokio::time::Instant;

use pges_core::{high_water, store};

use crate::sequencer;

/// Hands out `tokio::sync::watch` receivers for what a [`Sequencer`](crate::sequencer::Sequencer)
/// announces. Cheap to clone; every clone sees the same channels.
///
/// The values are level-triggered hints. A receiver sees the newest value, not every value, and a value can
/// be behind the log, for example while no sequencer leads. Subscribers read the log itself on start and
/// after a reconnect, and use the watch only to learn that something new may be there.
///
/// The gRPC watch server and the `LISTEN/NOTIFY` notifier are built on the same channels.
#[derive(Debug, Clone)]
pub struct Watch {
    inner: Arc<Inner>,
}

impl Watch {
    /// The store's high-water mark. Starts at zero until a leader announces the store, which it does when
    /// its term starts and after every batch that wrote to the store's log.
    ///
    /// The channel of a store no leader announced (a store with no events yet, or no store at all) lasts
    /// only while someone holds a receiver of it, so asking for made-up store ids does not grow the watch.
    pub fn high_water(
        &self,
        store: store::StoreId,
    ) -> tokio::sync::watch::Receiver<high_water::HighWater> {
        let mut stores = self.stores();
        if let Some(channel) = stores.get(&store) {
            return channel.sender.subscribe();
        }

        stores.retain(|_, channel| {
            channel.origin == Origin::Announced || channel.sender.receiver_count() > 0
        });
        let sender = tokio::sync::watch::Sender::new(high_water::HighWater::default());
        let receiver = sender.subscribe();
        stores.insert(
            store,
            Channel {
                sender,
                origin: Origin::Asked,
            },
        );

        receiver
    }

    /// The sequencer's status.
    pub fn status(&self) -> tokio::sync::watch::Receiver<sequencer::Status> {
        self.inner.status.subscribe()
    }

    pub(crate) fn new() -> Self {
        Self {
            inner: Arc::new(Inner {
                stores: Mutex::new(HashMap::new()),
                status: tokio::sync::watch::Sender::new(sequencer::Status::Standby),
                lease: tokio::sync::watch::Sender::new(None),
            }),
        }
    }

    /// When the current term's lease may expire unless renewed: a leader whose sequencing hangs keeps saying
    /// [`sequencer::Status::Leading`], but stops being the leader once this passes.
    #[cfg(feature = "watch-grpc")]
    pub(crate) fn lease_deadline(&self) -> tokio::sync::watch::Receiver<Option<Instant>> {
        self.inner.lease.subscribe()
    }

    pub(crate) fn set_lease_deadline(&self, deadline: Instant) {
        self.inner.lease.send_replace(Some(deadline));
    }

    pub(crate) fn publish(&self, store: store::StoreId, high_water: high_water::HighWater) {
        let mut stores = self.stores();
        let channel = stores.entry(store).or_insert_with(|| Channel {
            sender: tokio::sync::watch::Sender::new(high_water::HighWater::default()),
            origin: Origin::Announced,
        });
        channel.origin = Origin::Announced;
        channel.sender.send_if_modified(|current| {
            if *current == high_water {
                return false;
            }

            *current = high_water;
            true
        });
    }

    pub(crate) fn set_status(&self, status: sequencer::Status) {
        self.inner.status.send_replace(status);
    }

    /// How many store channels exist.
    #[cfg(test)]
    fn channels(&self) -> usize {
        self.stores().len()
    }

    fn stores(&self) -> std::sync::MutexGuard<'_, HashMap<store::StoreId, Channel>> {
        self.inner
            .stores
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }
}

#[derive(Debug)]
struct Inner {
    stores: Mutex<HashMap<store::StoreId, Channel>>,
    status: tokio::sync::watch::Sender<sequencer::Status>,
    lease: tokio::sync::watch::Sender<Option<Instant>>,
}

/// One store's high-water channel.
#[derive(Debug)]
struct Channel {
    sender: tokio::sync::watch::Sender<high_water::HighWater>,
    origin: Origin,
}

/// Who created a store's channel.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Origin {
    /// A leader announced the store: it exists, and the channel stays.
    Announced,
    /// Only a watcher asked for it: the channel goes once nobody holds a receiver.
    Asked,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn channels_of_unannounced_stores_last_only_while_watched() {
        let watch = Watch::new();
        let announced = store::StoreId::new(1);
        watch.publish(announced, high_water::HighWater::default());

        for id in 100..1100 {
            drop(watch.high_water(store::StoreId::new(id)));
        }
        assert_eq!(watch.channels(), 2);

        let held: Vec<_> = (2000..2010)
            .map(|id| watch.high_water(store::StoreId::new(id)))
            .collect();
        drop(watch.high_water(store::StoreId::new(3000)));
        assert_eq!(watch.channels(), 12);

        // A store a watcher asked for first keeps its channel once a leader announces it.
        let early = watch.high_water(store::StoreId::new(4000));
        watch.publish(store::StoreId::new(4000), high_water::HighWater::default());
        drop((early, held));
        drop(watch.high_water(store::StoreId::new(5000)));
        assert_eq!(watch.channels(), 3);
    }
}
