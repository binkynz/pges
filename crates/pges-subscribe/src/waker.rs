//! How a caught-up subscription learns that new positions may exist: the [`Waker`] trait.
//!
//! Every waker is a hint. The log table is the truth: a subscription always reads it after waking, and a
//! waker that fires too often costs a read, never correctness. A waker that misses a change delays the
//! subscription until the next one, so a subscription that must not stall combines a push waker with
#![cfg_attr(
    feature = "wake-poll",
    doc = " [`poll::PollWaker`] through [`Wakers`]."
)]
#![cfg_attr(
    not(feature = "wake-poll"),
    doc = " `poll::PollWaker` through [`Wakers`]."
)]
//!
//! | Waker | Feature | Mechanism |
//! |---|---|---|
//! | [`recheck::Recheck`] | none | a timer: reads the log again every interval; the default, 500 ms |
#![cfg_attr(
    feature = "wake-grpc",
    doc = " | [`grpc::GrpcWaker`] | `wake-grpc` (default) | the leading sequencer's gRPC watch stream (`pges.watch.v1`, its `watch-grpc` feature), for subscribers in other processes |"
)]
#![cfg_attr(
    not(feature = "wake-grpc"),
    doc = " | `grpc::GrpcWaker` | `wake-grpc` (default) | the leading sequencer's gRPC watch stream (`pges.watch.v1`, its `watch-grpc` feature), for subscribers in other processes |"
)]
#![cfg_attr(
    feature = "wake-inproc",
    doc = " | [`inproc::InprocWaker`] | `wake-inproc` | the sequencer's `tokio::sync::watch` channel, when it runs in the same process |"
)]
#![cfg_attr(
    not(feature = "wake-inproc"),
    doc = " | `inproc::InprocWaker` | `wake-inproc` | the sequencer's `tokio::sync::watch` channel, when it runs in the same process |"
)]
#![cfg_attr(
    feature = "wake-notify",
    doc = " | [`notify::NotifyWaker`] | `wake-notify` | `LISTEN pges_<store>`, fed by the sequencer's `NOTIFY` after each batch (its `watch-notify` feature) |"
)]
#![cfg_attr(
    not(feature = "wake-notify"),
    doc = " | `notify::NotifyWaker` | `wake-notify` | `LISTEN pges_<store>`, fed by the sequencer's `NOTIFY` after each batch (its `watch-notify` feature) |"
)]
#![cfg_attr(
    feature = "wake-poll",
    doc = " | [`poll::PollWaker`] | `wake-poll` | the newest `log_by_category` entry of each watched category, on an interval with backoff |"
)]
#![cfg_attr(
    not(feature = "wake-poll"),
    doc = " | `poll::PollWaker` | `wake-poll` | the newest `log_by_category` entry of each watched category, on an interval with backoff |"
)]

use std::{error, future::Future};

use futures_util::future::{self, BoxFuture};

use pges_core::{high_water, non_empty};

#[cfg(feature = "wake-grpc")]
pub mod grpc;
#[cfg(feature = "wake-inproc")]
pub mod inproc;
#[cfg(feature = "wake-notify")]
pub mod notify;
#[cfg(feature = "wake-poll")]
pub mod poll;
pub mod recheck;

/// Tells a caught-up subscriber when to read the log again.
///
/// `known` is what the subscriber has read up to. Its `categories` list the categories the subscriber
/// watches, each with the position it has read up to; an empty list means the whole store, compared through
/// `known.store()`. [`Waker::changed`] resolves once the high-water mark of a watched category may have
/// passed `known` ([`has_passed`]), with the newest high-water mark the waker knows. It may resolve early;
/// the subscriber then reads nothing new and waits again.
///
/// The high-water mark it resolves with is a hint, and may be false: a `NOTIFY` can come from any role that
/// can connect. A subscription never compares a later call with it. `known` is always what the subscription
/// read, and of the high-water mark it uses only the LSN, lowered to the primary's WAL insert position when
/// a replica has not replayed it. A waker whose input is untrusted should let each input wake at most one
/// call, so a false position ahead of every `known` cannot resolve every call at once.
///
/// The future must be cancel safe: the subscriber drops it to renew its lease or to shut down, and calls
/// again.
///
/// `Waker` uses an `impl Future` return, so it is not dyn-compatible; [`DynWaker`] is its object-safe twin.
pub trait Waker: Send {
    /// Resolves when a watched category may have passed `known`.
    fn changed(
        &mut self,
        known: &high_water::HighWater,
    ) -> impl Future<Output = Result<high_water::HighWater, WakeError>> + Send;

    /// How many connections of the store's pool the waker keeps while the subscriber is busy, such as a
    /// listening connection. A subscription adds them to its own needs when it checks that the store's pool
    /// is large enough ([`SubscribeError::PoolTooSmall`](crate::subscription::SubscribeError::PoolTooSmall)).
    /// Connections taken only during [`Waker::changed`] do not count. Default 0.
    fn store_connections(&self) -> u32 {
        0
    }
}

/// The object-safe twin of [`Waker`], for a waker chosen at run time (`Box<dyn DynWaker>`). Every [`Waker`]
/// implements it, and `Box<dyn DynWaker>` implements [`Waker`].
pub trait DynWaker: Send {
    /// See [`Waker::changed`].
    fn changed<'a>(
        &'a mut self,
        known: &'a high_water::HighWater,
    ) -> BoxFuture<'a, Result<high_water::HighWater, WakeError>>;

    /// See [`Waker::store_connections`].
    fn store_connections(&self) -> u32;
}

impl<T: Waker> DynWaker for T {
    fn changed<'a>(
        &'a mut self,
        known: &'a high_water::HighWater,
    ) -> BoxFuture<'a, Result<high_water::HighWater, WakeError>> {
        Box::pin(Waker::changed(self, known))
    }

    fn store_connections(&self) -> u32 {
        Waker::store_connections(self)
    }
}

impl Waker for Box<dyn DynWaker> {
    async fn changed(
        &mut self,
        known: &high_water::HighWater,
    ) -> Result<high_water::HighWater, WakeError> {
        DynWaker::changed(&mut **self, known).await
    }

    fn store_connections(&self) -> u32 {
        DynWaker::store_connections(&**self)
    }
}

/// Several wakers as one, at least one: resolves when any of them does.
///
/// A waker that fails is left out until the next call, so one broken waker (for example an in-process
/// channel whose sequencer stopped) does not stall the others. The call fails only when every waker failed,
/// with the last error.
///
/// ```no_run
/// # #[cfg(all(feature = "wake-notify", feature = "wake-poll"))]
/// # fn example(store: &pges_postgres::store::Store) {
/// use pges_subscribe::waker::{self, notify, poll};
///
/// let wakers = waker::Wakers::new(notify::NotifyWaker::new(store))
///     .or(poll::PollWaker::new(store, poll::PollConfig::default()));
/// # }
/// ```
pub struct Wakers {
    /// Never empty: every constructor takes at least one waker.
    wakers: Vec<Box<dyn DynWaker>>,
}

impl Wakers {
    /// Starts with `waker`; add more with [`Wakers::or`].
    pub fn new(waker: impl Waker + 'static) -> Self {
        Self {
            wakers: vec![Box::new(waker)],
        }
    }

    /// Adds `waker`.
    pub fn or(mut self, waker: impl Waker + 'static) -> Self {
        self.wakers.push(Box::new(waker));
        self
    }

    /// Combines `wakers`, for a list chosen at run time.
    pub fn any(wakers: non_empty::NonEmpty<Box<dyn DynWaker>>) -> Self {
        Self {
            wakers: wakers.into_vec(),
        }
    }
}

impl std::fmt::Debug for Wakers {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Wakers")
            .field("wakers", &self.wakers.len())
            .finish()
    }
}

impl Waker for Wakers {
    async fn changed(
        &mut self,
        known: &high_water::HighWater,
    ) -> Result<high_water::HighWater, WakeError> {
        let mut pending: Vec<_> = self
            .wakers
            .iter_mut()
            .map(|waker| DynWaker::changed(&mut **waker, known))
            .collect();

        loop {
            let (result, _, rest) = future::select_all(pending).await;
            match result {
                Ok(high) => return Ok(high),
                Err(error) if rest.is_empty() => return Err(error),
                Err(_) => pending = rest,
            }
        }
    }

    /// The sum of the wakers' connections.
    fn store_connections(&self) -> u32 {
        self.wakers
            .iter()
            .map(|waker| DynWaker::store_connections(&**waker))
            .fold(0, u32::saturating_add)
    }
}

/// Whether `high` is past `known` for a category `known` lists, or, when `known` lists none, for the store.
/// The test every waker applies; custom wakers use it too.
pub fn has_passed(high: &high_water::HighWater, known: &high_water::HighWater) -> bool {
    if known.categories().is_empty() {
        return high.store() > known.store();
    }

    known
        .categories()
        .iter()
        .any(|(category, position)| high.category(*category) > *position)
}

/// A boxed error from a waker.
pub type BoxError = Box<dyn error::Error + Send + Sync + 'static>;

/// A waker could not wait for changes.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum WakeError {
    /// The waker's source is gone, for example the sequencer that fed an in-process channel stopped.
    #[error("the waker's source closed")]
    Closed,
    /// A database error.
    #[error(transparent)]
    Database(pges_postgres::error::DbError),
    /// Any other failure, for custom wakers.
    #[error(transparent)]
    Other(BoxError),
}

impl From<sqlx::Error> for WakeError {
    fn from(error: sqlx::Error) -> Self {
        Self::Database(error.into())
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use pges_core::{category, position};

    use super::*;

    fn high(store: u64, categories: &[(i32, u64)]) -> high_water::HighWater {
        high_water::HighWater::new(
            position::Position::new(store).unwrap(),
            categories
                .iter()
                .map(|(id, p)| {
                    (
                        category::CategoryId::new(*id),
                        position::Position::new(*p).unwrap(),
                    )
                })
                .collect(),
            None,
        )
    }

    #[test]
    fn passes_only_for_watched_categories() {
        let known = high(10, &[(1, 10), (2, 10)]);

        assert!(!has_passed(&high(20, &[(1, 7), (3, 20)]), &known));
        assert!(has_passed(&high(20, &[(1, 7), (2, 11)]), &known));
        assert!(!has_passed(&high(20, &[]), &known));
    }

    #[test]
    fn passes_on_the_store_when_no_category_is_watched() {
        let known = high(10, &[]);

        assert!(!has_passed(&high(10, &[(1, 10)]), &known));
        assert!(has_passed(&high(11, &[]), &known));
    }

    struct Fails;

    impl Waker for Fails {
        async fn changed(
            &mut self,
            _known: &high_water::HighWater,
        ) -> Result<high_water::HighWater, WakeError> {
            Err(WakeError::Closed)
        }
    }

    struct After(Duration, u64);

    impl Waker for After {
        async fn changed(
            &mut self,
            _known: &high_water::HighWater,
        ) -> Result<high_water::HighWater, WakeError> {
            tokio::time::sleep(self.0).await;
            Ok(high(self.1, &[]))
        }
    }

    #[tokio::test]
    async fn any_skips_failed_wakers_and_returns_the_first_to_fire() {
        let mut wakers = Wakers::new(Fails)
            .or(After(Duration::from_millis(200), 1))
            .or(After(Duration::from_millis(10), 2));

        let woken = Waker::changed(&mut wakers, &high(0, &[])).await.unwrap();

        assert_eq!(woken.store(), position!(2));
    }

    #[tokio::test]
    async fn any_fails_when_every_waker_failed() {
        let failing: Box<dyn DynWaker> = Box::new(Fails);
        let mut wakers = Wakers::any(non_empty::NonEmpty::new(failing, [Box::new(Fails) as _]));

        let result = Waker::changed(&mut wakers, &high(0, &[])).await;

        assert!(matches!(result, Err(WakeError::Closed)));
    }
}
