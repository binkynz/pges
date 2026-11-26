//! Ambient request metadata: set once where a request enters, filled into every commit made inside it.
//!
//! [`scope`] runs a future with metadata (a correlation id, an actor, a trace) that every append made
//! inside it uses for the fields it leaves unset: [`store::Store::append`], [`store::Store::append_in`],
//! [`store::Store::append_many`], the appends of a [`unit_of_work::UnitOfWork`] and
//! [`store::Store::delete_stream`]'s `$stream-deleted` commit.
//!
//! A commit's metadata field by field, first set wins:
//!
//! 1. the metadata given to the append (`.metadata(..)`);
//! 2. the innermost [`scope`];
//! 3. with the `otel` feature, for the trace only: the current `tracing` span's OpenTelemetry context
//!    (`TraceContext::current` in `pges_core::metadata`);
//! 4. unset.
//!
//! An append reads the scope and the span when its future is first polled, not when it is built: inside
//! `ambient::scope(meta, store.append(..))` or `store.append(..).into_future().instrument(span)` it sees the
//! scope and the span.
//!
//! The scope is a Tokio task-local. A task started with `tokio::spawn` inside a scope does not inherit
//! it; pass it on with [`current`]:
//!
//! ```no_run
//! # async fn example(store: pges_postgres::store::Store) {
//! use pges_postgres::ambient;
//!
//! let work = async { /* appends */ };
//! tokio::spawn(ambient::scope(ambient::current(), work));
//! # }
//! ```
//!
//! Subscriptions do not set a scope around a handler: a batch holds events of many commits, each with its
//! own correlation and cause, so no single follow-up metadata fits a batch. A handler passes
//! `event.follow_up()` to the appends one event causes, as `.metadata(..)` or through a [`scope`].

use std::{
    future::{Future, IntoFuture},
    sync::Arc,
};

use pges_core::metadata;

#[cfg(doc)]
use crate::{store, unit_of_work};

/// Runs `future` with `metadata` as the ambient metadata of the appends inside it.
///
/// Fields `metadata` leaves unset are taken from the enclosing scope, if any, so an inner scope overrides
/// the fields it sets and keeps the others. The enclosing scope is read when `scope` is called.
///
/// ```no_run
/// use pges_core::{event, metadata, stream, version};
/// use pges_postgres::{ambient, append, store};
///
/// async fn create<E: event::Event>(
///     store: &store::Store,
///     events: &[E],
/// ) -> Result<append::AppendResult, append::AppendError> {
///     let mut request = metadata::Metadata::default();
///     request.correlation_id = Some(metadata::CorrelationId::new_v7());
///     request.actor = Some("user:7".into());
///
///     // Recorded with the request's correlation id and actor. The scope takes a future, an async block
///     // or an append.
///     let new_stream = stream::StreamId::new_v7();
///     ambient::scope(request, store.append(new_stream, version::ExpectedVersion::NoStream, events))
///         .await
/// }
/// ```
pub fn scope<F: IntoFuture>(
    metadata: metadata::Metadata,
    future: F,
) -> impl Future<Output = F::Output> {
    let mut metadata = metadata;
    if let Some(outer) = ambient() {
        metadata.fill_unset(&outer);
    }

    AMBIENT.scope(Arc::new(metadata), future.into_future())
}

/// The ambient metadata of the current task: the innermost [`scope`]'s, or empty outside any scope.
pub fn current() -> metadata::Metadata {
    ambient()
        .map(|ambient| metadata::Metadata::clone(&ambient))
        .unwrap_or_default()
}

/// The metadata an append records: `explicit`, with its unset fields filled from the ambient scope and
/// then, with the `otel` feature, the trace from the current span.
pub(crate) fn resolve(explicit: metadata::Metadata) -> metadata::Metadata {
    let mut metadata = explicit;
    if let Some(ambient) = ambient() {
        metadata.fill_unset(&ambient);
    }

    #[cfg(feature = "otel")]
    if metadata.trace.is_none() {
        metadata.trace = metadata::TraceContext::current();
    }

    metadata
}

tokio::task_local! {
    static AMBIENT: Arc<metadata::Metadata>;
}

fn ambient() -> Option<Arc<metadata::Metadata>> {
    AMBIENT.try_with(Arc::clone).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn actor(name: &str) -> metadata::Metadata {
        let mut meta = metadata::Metadata::default();
        meta.actor = Some(name.into());
        meta
    }

    #[test]
    fn outside_a_scope_nothing_is_filled() {
        assert_eq!(current(), metadata::Metadata::default());
        assert_eq!(resolve(metadata::Metadata::default()).actor, None);
    }

    #[tokio::test]
    async fn explicit_fields_win_over_the_scope() {
        let mut outer = actor("user:7");
        outer.correlation_id = Some(metadata::CorrelationId::new_v7());

        let resolved = scope(outer.clone(), async { resolve(actor("job-runner")) }).await;

        assert_eq!(resolved.actor.as_deref(), Some("job-runner"));
        assert_eq!(resolved.correlation_id, outer.correlation_id);
    }

    #[tokio::test]
    async fn inner_scopes_override_the_fields_they_set() {
        let mut outer = actor("user:7");
        outer.correlation_id = Some(metadata::CorrelationId::new_v7());
        let inner_correlation = metadata::CorrelationId::new_v7();
        let mut inner = metadata::Metadata::default();
        inner.correlation_id = Some(inner_correlation);

        let (in_inner, after_inner) = scope(outer.clone(), async {
            let in_inner = scope(inner, async { current() }).await;
            (in_inner, current())
        })
        .await;

        assert_eq!(in_inner.correlation_id, Some(inner_correlation));
        assert_eq!(in_inner.actor.as_deref(), Some("user:7"));
        assert_eq!(after_inner, outer);
        assert_eq!(current(), metadata::Metadata::default());
    }

    #[tokio::test]
    async fn spawned_tasks_inherit_only_when_passed_on() {
        let outer = actor("user:7");

        let (forgotten, passed_on) = scope(outer, async {
            let forgotten = tokio::spawn(async { current() }).await.unwrap();
            let passed_on = tokio::spawn(scope(current(), async { current() }))
                .await
                .unwrap();
            (forgotten, passed_on)
        })
        .await;

        assert_eq!(forgotten.actor, None);
        assert_eq!(passed_on.actor.as_deref(), Some("user:7"));
    }
}
