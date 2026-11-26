//! Commit metadata: explicit fields, the ambient scope, the current span's trace (with `otel`), and
//! follow-up metadata, as stored and read back.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

mod common;

use anyhow::Result;
use futures_util::TryStreamExt;

use pges_core::{frame, metadata, stream, version};
use pges_postgres::{ambient, append, delete, unit_of_work};

use common::{Fixture, JobEvent, OrderEvent, Placed};

async fn stored(f: &Fixture, id: stream::StreamId) -> Result<Vec<stream::StreamEvent<JobEvent>>> {
    Ok(f.store
        .read_stream::<JobEvent>(id, version::Version::FIRST)
        .try_collect()
        .await?)
}

async fn last_metadata(f: &Fixture, id: stream::StreamId) -> Result<metadata::Metadata> {
    Ok(metadata::Metadata::clone(
        &stored(f, id).await?.last().unwrap().metadata,
    ))
}

fn request(actor: &str) -> metadata::Metadata {
    let mut meta = metadata::Metadata::default();
    meta.correlation_id = Some(metadata::CorrelationId::new_v7());
    meta.actor = Some(actor.into());
    meta.trace = Some(metadata::TraceContext::new(
        "00-11111111111111111111111111111111-2222222222222222-01",
    ));
    meta
}

async fn create(f: &Fixture) -> Result<stream::StreamId> {
    let id = stream::StreamId::new_v7();
    f.store
        .append(
            id,
            version::ExpectedVersion::NoStream,
            &[JobEvent::created("a")],
        )
        .await?;

    Ok(id)
}

#[tokio::test]
async fn without_a_scope_only_explicit_metadata_is_stored() -> Result<()> {
    let f = Fixture::new().await?;
    let id = create(&f).await?;

    assert_eq!(last_metadata(&f, id).await?, metadata::Metadata::default());
    f.finish().await
}

#[tokio::test]
async fn explicit_fields_win_and_the_scope_fills_the_rest() -> Result<()> {
    let f = Fixture::new().await?;
    let scope = request("user:7");
    let mut explicit = metadata::Metadata::default();
    explicit.actor = Some("job-runner".into());

    let id = ambient::scope(scope.clone(), async {
        let id = create(&f).await?;
        f.store
            .append(id, version::ExpectedVersion::Any, &[JobEvent::assigned()])
            .metadata(explicit)
            .await?;
        anyhow::Ok(id)
    })
    .await?;

    let events = stored(&f, id).await?;
    assert_eq!(*events[0].metadata, scope);
    assert_eq!(events[1].metadata.actor.as_deref(), Some("job-runner"));
    assert_eq!(events[1].metadata.correlation_id, scope.correlation_id);
    assert_eq!(events[1].metadata.trace, scope.trace);
    f.finish().await
}

#[tokio::test]
async fn a_scope_takes_an_append_directly() -> Result<()> {
    let f = Fixture::new().await?;
    let scope = request("user:7");
    let id = stream::StreamId::new_v7();

    ambient::scope(
        scope.clone(),
        f.store.append(
            id,
            version::ExpectedVersion::NoStream,
            &[JobEvent::created("a")],
        ),
    )
    .await?;

    assert_eq!(last_metadata(&f, id).await?, scope);
    f.finish().await
}

#[tokio::test]
async fn nested_scopes_override_the_fields_they_set() -> Result<()> {
    let f = Fixture::new().await?;
    let outer = request("user:7");
    let mut inner = metadata::Metadata::default();
    inner.actor = Some("importer".into());

    let (in_inner, after_inner) = ambient::scope(outer.clone(), async {
        let in_inner = ambient::scope(inner, create(&f)).await?;
        let after_inner = create(&f).await?;
        anyhow::Ok((in_inner, after_inner))
    })
    .await?;

    let in_inner = last_metadata(&f, in_inner).await?;
    assert_eq!(in_inner.actor.as_deref(), Some("importer"));
    assert_eq!(in_inner.correlation_id, outer.correlation_id);
    assert_eq!(last_metadata(&f, after_inner).await?, outer);
    f.finish().await
}

#[tokio::test]
async fn append_many_units_of_work_and_deletes_use_the_scope() -> Result<()> {
    let f = Fixture::new().await?;
    let scope = request("user:7");
    let (job, other_job, uow_job, many_order, deleted) = ambient::scope(scope.clone(), async {
        let job = stream::StreamId::new_v7();
        let other_job = stream::StreamId::new_v7();
        let jobs = [JobEvent::created("a")];
        let orders = [OrderEvent::Placed(Placed { total: 3 })];
        let many_order = stream::StreamId::new_v7();
        f.store
            .append_many([
                append::AppendEntry::new(job, version::ExpectedVersion::NoStream, &jobs),
                append::AppendEntry::new(many_order, version::ExpectedVersion::NoStream, &orders),
            ])
            .await?;

        let uow_job = stream::StreamId::new_v7();
        let mut explicit = metadata::Metadata::default();
        explicit.actor = Some("job-runner".into());
        let mut uow = f.store.begin().await?;
        uow.append(uow_job, version::ExpectedVersion::NoStream, &jobs)
            .metadata(explicit.clone())
            .await?;
        uow.append_many([append::AppendEntry::new(
            other_job,
            version::ExpectedVersion::NoStream,
            &jobs,
        )])
        .await?;
        uow.commit().await?;

        let mut conn = f.pool.acquire().await?;
        let deleted = stream::StreamId::new_v7();
        let mut wrapped = unit_of_work::UnitOfWork::wrap(&f.store, &mut conn);
        wrapped
            .append(deleted, version::ExpectedVersion::NoStream, &jobs)
            .await?;
        wrapped.finish().await?;
        drop(conn);
        f.store
            .delete_stream::<JobEvent>(deleted, delete::DeleteExpected::Any)
            .await?;

        anyhow::Ok((job, other_job, uow_job, many_order, deleted))
    })
    .await?;

    assert_eq!(last_metadata(&f, job).await?, scope);
    assert_eq!(last_metadata(&f, other_job).await?, scope);
    let uow = last_metadata(&f, uow_job).await?;
    assert_eq!(uow.actor.as_deref(), Some("job-runner"));
    assert_eq!(uow.correlation_id, scope.correlation_id);
    let orders: Vec<stream::StreamEvent<OrderEvent>> = f
        .store
        .read_stream::<OrderEvent>(many_order, version::Version::FIRST)
        .try_collect()
        .await?;
    assert_eq!(*orders[0].metadata, scope);
    assert_eq!(tombstone_metadata(&f, deleted).await?, scope);
    f.finish().await
}

#[tokio::test]
async fn follow_ups_chain_correlation_and_causation() -> Result<()> {
    let f = Fixture::new().await?;
    let root = create(&f).await?;
    let root_event = stored(&f, root).await?.remove(0);

    let mut second_meta = root_event.follow_up();
    second_meta.actor = Some("worker".into());
    let second = stream::StreamId::new_v7();
    f.store
        .append(
            second,
            version::ExpectedVersion::NoStream,
            &[JobEvent::assigned()],
        )
        .metadata(second_meta)
        .await?;
    let second_event = stored(&f, second).await?.remove(0);

    let third = stream::StreamId::new_v7();
    ambient::scope(second_event.follow_up(), async {
        f.store
            .append(
                third,
                version::ExpectedVersion::NoStream,
                &[JobEvent::assigned()],
            )
            .await
    })
    .await?;
    let third_meta = last_metadata(&f, third).await?;

    let root_commit = root_event.commit_id;
    assert_eq!(
        second_event.metadata.correlation_id,
        Some(metadata::CorrelationId::from(root_commit))
    );
    assert_eq!(
        second_event.metadata.causation_id,
        Some(metadata::CausationId::from(root_commit))
    );
    assert_eq!(second_event.metadata.actor.as_deref(), Some("worker"));
    assert_eq!(
        third_meta.correlation_id,
        Some(metadata::CorrelationId::from(root_commit))
    );
    assert_eq!(
        third_meta.causation_id,
        Some(metadata::CausationId::from(second_event.commit_id))
    );
    assert_eq!(third_meta.actor, None, "the actor is not inherited");
    f.finish().await
}

/// The `$stream-deleted` commit's metadata, read from the payload: deleted streams cannot be read.
async fn tombstone_metadata(f: &Fixture, id: stream::StreamId) -> Result<metadata::Metadata> {
    let (format, event_count, payload): (i16, i16, Vec<u8>) =
        sqlx::query_as("SELECT format, event_count, payload FROM es.commits WHERE stream_id = $1")
            .bind(id.as_uuid())
            .fetch_one(&f.pool)
            .await?;
    let frame = frame::decode(
        frame::Format::from_code(format)?,
        &payload,
        u16::try_from(event_count)?,
    )?;

    Ok(frame.metadata)
}

#[cfg(feature = "otel")]
mod otel {
    use std::future::IntoFuture as _;

    use opentelemetry::trace::{TraceContextExt as _, TracerProvider as _};
    use opentelemetry_sdk::trace::SdkTracerProvider;
    use tracing::Instrument as _;
    use tracing_opentelemetry::OpenTelemetrySpanExt as _;
    use tracing_subscriber::layer::SubscriberExt as _;

    use super::*;

    fn subscriber() -> (SdkTracerProvider, impl tracing::Subscriber + Send + Sync) {
        let provider = SdkTracerProvider::builder().build();
        let subscriber = tracing_subscriber::registry()
            .with(tracing_opentelemetry::layer().with_tracer(provider.tracer("test")));
        (provider, subscriber)
    }

    fn traceparent_of(span: &tracing::Span) -> String {
        let context = span.context();
        let otel_span = context.span();
        let span_context = otel_span.span_context();
        format!(
            "00-{}-{}-01",
            span_context.trace_id(),
            span_context.span_id()
        )
    }

    #[tokio::test]
    async fn the_trace_is_explicit_then_scope_then_span_then_none() -> Result<()> {
        let f = Fixture::new().await?;
        let (_provider, subscriber) = subscriber();
        let _default = tracing::subscriber::set_default(subscriber);
        let scope = request("user:7");
        let explicit =
            metadata::TraceContext::new("00-33333333333333333333333333333333-4444444444444444-01");
        let mut with_trace = metadata::Metadata::default();
        with_trace.trace = Some(explicit.clone());

        let span = tracing::info_span!("request");
        let mut without_trace = request("user:8");
        without_trace.trace = None;
        let (from_explicit, from_scope, from_span, span_in_scope) = async {
            let from_explicit = ambient::scope(scope.clone(), async {
                let id = stream::StreamId::new_v7();
                f.store
                    .append(
                        id,
                        version::ExpectedVersion::NoStream,
                        &[JobEvent::created("a")],
                    )
                    .metadata(with_trace)
                    .await?;
                anyhow::Ok(id)
            })
            .await?;
            let from_scope = ambient::scope(scope.clone(), create(&f)).await?;
            let from_span = create(&f).await?;
            let span_in_scope = ambient::scope(without_trace.clone(), create(&f)).await?;
            anyhow::Ok((from_explicit, from_scope, from_span, span_in_scope))
        }
        .instrument(span.clone())
        .await?;
        let from_none = create(&f).await?;
        // The span is read when the append first runs, so instrumenting its future works.
        let instrumented = stream::StreamId::new_v7();
        f.store
            .append(
                instrumented,
                version::ExpectedVersion::NoStream,
                &[JobEvent::created("a")],
            )
            .into_future()
            .instrument(span.clone())
            .await?;

        assert_eq!(
            last_metadata(&f, from_explicit).await?.trace,
            Some(explicit)
        );
        assert_eq!(last_metadata(&f, from_scope).await?.trace, scope.trace);
        let stored_span = last_metadata(&f, from_span).await?.trace.unwrap();
        assert_eq!(stored_span.traceparent, traceparent_of(&span));
        assert_eq!(stored_span.tracestate, None);
        let span_in_scope = last_metadata(&f, span_in_scope).await?;
        assert_eq!(span_in_scope.actor, without_trace.actor);
        assert_eq!(span_in_scope.correlation_id, without_trace.correlation_id);
        assert_eq!(
            span_in_scope.trace.map(|trace| trace.traceparent),
            Some(traceparent_of(&span)),
            "a scope without a trace leaves it to the span"
        );
        assert_eq!(last_metadata(&f, from_none).await?.trace, None);
        assert_eq!(
            last_metadata(&f, instrumented)
                .await?
                .trace
                .unwrap()
                .traceparent,
            traceparent_of(&span)
        );
        f.finish().await
    }

    #[tokio::test]
    async fn a_delivered_event_parents_the_handler_span() -> Result<()> {
        let f = Fixture::new().await?;
        let (_provider, subscriber) = subscriber();
        let _default = tracing::subscriber::set_default(subscriber);
        let request = tracing::info_span!("request");
        let id = create(&f).instrument(request.clone()).await?;
        let event = stored(&f, id).await?.remove(0);

        let handler = tracing::info_span!("handle");
        let _ = handler.set_parent(event.trace_parent().unwrap());

        let request_context = request.context();
        let handler_context = handler.context();
        assert_eq!(
            handler_context.span().span_context().trace_id(),
            request_context.span().span_context().trace_id()
        );
        assert_ne!(
            handler_context.span().span_context().span_id(),
            request_context.span().span_context().span_id()
        );
        f.finish().await
    }
}
