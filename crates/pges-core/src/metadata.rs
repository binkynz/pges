//! Per-commit metadata: who made a change, which request or workflow it belongs to, what caused it, and
//! the trace it ran in.
//!
//! The ids are distinct types ([`CorrelationId`], [`CausationId`], [`commit::CommitId`]), so storing a
//! correlation id as a causation id, or the other way round, does not compile.
//!
//! Commits caused by a delivered event take [`Metadata::follow_up`], which the event types offer with their
//! own commit id filled in (`StreamEvent::follow_up` in [`crate::stream`], and the subscription crate's
//! `RawEvent::follow_up`).
//!
//! With the `otel` feature, `TraceContext::current` reads the W3C trace context of the current `tracing`
//! span through `tracing-opentelemetry`, and `TraceContext::to_context` turns a stored one back into an
//! OpenTelemetry parent context.

use std::{collections::BTreeMap, fmt};

use uuid::Uuid;

use crate::commit;

/// Metadata recorded once per commit and shared by its events.
///
/// Stored as a protobuf message at the start of the payload frame, so fields can be added without breaking
/// older readers: they ignore what they do not know.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct Metadata {
    /// Groups the commits that one request or workflow produced.
    pub correlation_id: Option<CorrelationId>,
    /// The message, command or commit that caused this commit.
    pub causation_id: Option<CausationId>,
    /// Who or what made the change.
    pub actor: Option<String>,
    /// The W3C trace context of the operation that appended.
    pub trace: Option<TraceContext>,
    /// Anything else, as string pairs.
    pub extra: BTreeMap<String, String>,
}

impl Metadata {
    /// The metadata for commits caused by the commit `cause` that carries `self`.
    ///
    /// - correlation: carried over, or `cause` itself when `self` has none, so the cause starts the chain;
    /// - causation: `cause`;
    /// - trace: carried over;
    /// - actor: not inherited. It is whoever makes the new commit; set it, or let an ambient scope fill it;
    /// - extra: not carried over.
    ///
    /// Prefer the `follow_up` of a delivered event, which passes the event's own commit id.
    ///
    /// ```
    /// use pges_core::{commit, metadata};
    ///
    /// let cause = commit::CommitId::generate();
    /// let mut meta = metadata::Metadata::default();
    /// meta.actor = Some("user:7".into());
    ///
    /// let next = meta.follow_up(cause);
    /// assert_eq!(next.correlation_id, Some(metadata::CorrelationId::from(cause)));
    /// assert_eq!(next.causation_id, Some(metadata::CausationId::from(cause)));
    /// assert_eq!(next.actor, None);
    /// ```
    pub fn follow_up(&self, cause: commit::CommitId) -> Metadata {
        Metadata {
            correlation_id: Some(self.correlation_id.unwrap_or(CorrelationId::from(cause))),
            causation_id: Some(CausationId::from(cause)),
            actor: None,
            trace: self.trace.clone(),
            extra: BTreeMap::new(),
        }
    }

    /// Fills the fields `self` leaves unset from `fallback`: each `None` field takes `fallback`'s value, and
    /// `extra` takes the keys it does not have. Fields `self` sets are kept.
    ///
    /// ```
    /// use pges_core::metadata;
    ///
    /// let mut explicit = metadata::Metadata::default();
    /// explicit.actor = Some("job-runner".into());
    /// let mut ambient = metadata::Metadata::default();
    /// ambient.actor = Some("user:7".into());
    /// ambient.correlation_id = Some(metadata::CorrelationId::new_v7());
    ///
    /// explicit.fill_unset(&ambient);
    /// assert_eq!(explicit.actor.as_deref(), Some("job-runner"));
    /// assert_eq!(explicit.correlation_id, ambient.correlation_id);
    /// ```
    pub fn fill_unset(&mut self, fallback: &Metadata) {
        if self.correlation_id.is_none() {
            self.correlation_id = fallback.correlation_id;
        }
        if self.causation_id.is_none() {
            self.causation_id = fallback.causation_id;
        }
        if self.actor.is_none() {
            self.actor.clone_from(&fallback.actor);
        }
        if self.trace.is_none() {
            self.trace.clone_from(&fallback.trace);
        }
        for (key, value) in &fallback.extra {
            self.extra
                .entry(key.clone())
                .or_insert_with(|| value.clone());
        }
    }

    /// The stored trace context as an OpenTelemetry parent context, or `None` without one or when it is
    /// not a valid `traceparent`. See [`TraceContext::to_context`].
    #[cfg(feature = "otel")]
    pub fn trace_parent(&self) -> Option<opentelemetry::Context> {
        self.trace.as_ref().and_then(TraceContext::to_context)
    }
}

/// Groups the commits that one request or workflow produced.
///
/// A new chain starts with [`CorrelationId::new_v7`], or with the first commit's id
/// ([`Metadata::follow_up`] does that when the cause had none).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct CorrelationId(Uuid);

impl CorrelationId {
    /// A new time-ordered id.
    pub fn new_v7() -> Self {
        Self(Uuid::now_v7())
    }

    /// Wraps an id from elsewhere, such as a request id.
    pub const fn from_uuid(id: Uuid) -> Self {
        Self(id)
    }

    /// The underlying id.
    pub const fn as_uuid(&self) -> Uuid {
        self.0
    }
}

/// The commit that starts a chain correlates it.
impl From<commit::CommitId> for CorrelationId {
    fn from(id: commit::CommitId) -> Self {
        Self(id.as_uuid())
    }
}

impl fmt::Display for CorrelationId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

/// The message, command or commit that caused a commit.
///
/// For a commit caused by another commit it is that commit's id ([`Metadata::follow_up`]); for one caused
/// by an outside message, the message's id.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct CausationId(Uuid);

impl CausationId {
    /// Wraps an id from elsewhere, such as a command's or a message's.
    pub const fn from_uuid(id: Uuid) -> Self {
        Self(id)
    }

    /// The underlying id.
    pub const fn as_uuid(&self) -> Uuid {
        self.0
    }
}

/// A commit caused by another commit names it.
impl From<commit::CommitId> for CausationId {
    fn from(id: commit::CommitId) -> Self {
        Self(id.as_uuid())
    }
}

impl fmt::Display for CausationId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

/// A W3C trace context.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct TraceContext {
    /// The `traceparent` header value.
    pub traceparent: String,
    /// The `tracestate` header value, if any.
    pub tracestate: Option<String>,
}

impl TraceContext {
    /// A context with no `tracestate`.
    pub fn new(traceparent: impl Into<String>) -> Self {
        Self {
            traceparent: traceparent.into(),
            tracestate: None,
        }
    }
}

#[cfg(feature = "otel")]
impl TraceContext {
    /// The trace context of the current `tracing` span, or `None` when it has no valid OpenTelemetry span
    /// context (no `tracing-opentelemetry` layer, or no span).
    pub fn current() -> Option<Self> {
        Self::from_span(&tracing::Span::current())
    }

    /// The trace context of `span`, as the W3C `traceparent` and `tracestate` headers, or `None` when the
    /// span has no valid OpenTelemetry span context.
    ///
    /// The headers are written as `opentelemetry_sdk`'s `TraceContextPropagator` writes them (version 00,
    /// only the sampled flag), without depending on the SDK or on the global propagator's configuration.
    pub fn from_span(span: &tracing::Span) -> Option<Self> {
        use opentelemetry::trace::{TraceContextExt as _, TraceFlags};
        use tracing_opentelemetry::OpenTelemetrySpanExt as _;

        let context = span.context();
        let otel_span = context.span();
        let span_context = otel_span.span_context();
        if !span_context.is_valid() {
            return None;
        }

        let traceparent = format!(
            "00-{}-{}-{:02x}",
            span_context.trace_id(),
            span_context.span_id(),
            span_context.trace_flags() & TraceFlags::SAMPLED
        );
        let tracestate = span_context.trace_state().header();

        Some(Self {
            traceparent,
            tracestate: (!tracestate.is_empty()).then_some(tracestate),
        })
    }

    /// An OpenTelemetry context whose remote parent is this trace context, for a span that continues the
    /// trace, or `None` when `traceparent` is not valid. An invalid `tracestate` is dropped.
    ///
    /// Parsing follows `opentelemetry_sdk`'s `TraceContextPropagator`.
    ///
    /// ```
    /// use tracing_opentelemetry::OpenTelemetrySpanExt as _;
    ///
    /// use pges_core::metadata;
    ///
    /// let trace = metadata::TraceContext::new("00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01");
    /// let span = tracing::info_span!("handle");
    /// if let Some(parent) = trace.to_context() {
    ///     let _ = span.set_parent(parent);
    /// }
    /// ```
    pub fn to_context(&self) -> Option<opentelemetry::Context> {
        use std::str::FromStr as _;

        use opentelemetry::trace::{
            SpanContext, SpanId, TraceContextExt as _, TraceFlags, TraceId, TraceState,
        };

        let parts: Vec<&str> = self.traceparent.trim().split_terminator('-').collect();
        let [version, trace_id, span_id, flags, ..] = parts[..] else {
            return None;
        };
        let version = u8::from_str_radix(version, 16).ok()?;
        if version == u8::MAX || (version == 0 && parts.len() != 4) {
            return None;
        }
        let lowercase = |part: &str| !part.chars().any(|c| c.is_ascii_uppercase());
        if !(lowercase(trace_id) && lowercase(span_id) && lowercase(flags)) {
            return None;
        }

        let trace_id = TraceId::from_hex(trace_id).ok()?;
        let span_id = SpanId::from_hex(span_id).ok()?;
        let flags = TraceFlags::new(u8::from_str_radix(flags, 16).ok()?) & TraceFlags::SAMPLED;
        let state = self
            .tracestate
            .as_deref()
            .and_then(|state| TraceState::from_str(state).ok())
            .unwrap_or_default();
        let span_context = SpanContext::new(trace_id, span_id, flags, true, state);
        if !span_context.is_valid() {
            return None;
        }

        Some(opentelemetry::Context::new().with_remote_span_context(span_context))
    }
}

/// Metadata in a payload frame could not be decoded.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum MetadataError {
    /// The bytes are not a valid protobuf message.
    #[error("metadata is not a valid protobuf message: {0}")]
    Protobuf(#[from] prost::DecodeError),
    /// An id field does not hold 16 bytes.
    #[error("metadata field {field} is not a 16-byte UUID")]
    InvalidUuid {
        /// The field's name.
        field: &'static str,
    },
}

impl Metadata {
    pub(crate) fn encode(&self) -> Vec<u8> {
        use prost::Message;

        let wire = Wire {
            correlation_id: self.correlation_id.map(|id| id.0.as_bytes().to_vec()),
            causation_id: self.causation_id.map(|id| id.0.as_bytes().to_vec()),
            actor: self.actor.clone(),
            traceparent: self.trace.as_ref().map(|t| t.traceparent.clone()),
            tracestate: self.trace.as_ref().and_then(|t| t.tracestate.clone()),
            extra: self.extra.clone(),
        };

        wire.encode_to_vec()
    }

    pub(crate) fn decode(bytes: &[u8]) -> Result<Self, MetadataError> {
        use prost::Message;

        let wire = Wire::decode(bytes)?;

        Ok(Self {
            correlation_id: uuid_field(wire.correlation_id, "correlation_id")?.map(CorrelationId),
            causation_id: uuid_field(wire.causation_id, "causation_id")?.map(CausationId),
            actor: wire.actor,
            trace: wire.traceparent.map(|traceparent| TraceContext {
                traceparent,
                tracestate: wire.tracestate,
            }),
            extra: wire.extra,
        })
    }
}

/// Field numbers are part of the stored format: never reuse or renumber one.
#[derive(Clone, PartialEq, prost::Message)]
struct Wire {
    #[prost(bytes = "vec", optional, tag = "1")]
    correlation_id: Option<Vec<u8>>,
    #[prost(bytes = "vec", optional, tag = "2")]
    causation_id: Option<Vec<u8>>,
    #[prost(string, optional, tag = "3")]
    actor: Option<String>,
    #[prost(string, optional, tag = "4")]
    traceparent: Option<String>,
    #[prost(string, optional, tag = "5")]
    tracestate: Option<String>,
    #[prost(btree_map = "string, string", tag = "6")]
    extra: BTreeMap<String, String>,
}

fn uuid_field(bytes: Option<Vec<u8>>, field: &'static str) -> Result<Option<Uuid>, MetadataError> {
    bytes
        .map(|b| Uuid::from_slice(&b).map_err(|_| MetadataError::InvalidUuid { field }))
        .transpose()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn full() -> Metadata {
        Metadata {
            correlation_id: Some(CorrelationId::new_v7()),
            causation_id: Some(CausationId::from_uuid(Uuid::now_v7())),
            actor: Some("user:7".into()),
            trace: Some(TraceContext::new(
                "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01",
            )),
            extra: BTreeMap::from([("tenant".into(), "t1".into())]),
        }
    }

    #[test]
    fn follow_up_carries_correlation_and_trace_and_names_the_cause() {
        let cause = commit::CommitId::generate();
        let meta = full();

        let next = meta.follow_up(cause);

        assert_eq!(next.correlation_id, meta.correlation_id);
        assert_eq!(next.causation_id, Some(CausationId::from(cause)));
        assert_eq!(next.trace, meta.trace);
        assert_eq!(next.actor, None);
        assert!(next.extra.is_empty());
    }

    #[test]
    fn follow_up_of_an_uncorrelated_commit_correlates_by_the_cause() {
        let cause = commit::CommitId::generate();

        let next = Metadata::default().follow_up(cause);

        assert_eq!(next.correlation_id, Some(CorrelationId::from(cause)));
        assert_eq!(next.causation_id, Some(CausationId::from(cause)));
        assert_eq!(next.trace, None);
    }

    #[test]
    fn a_chain_keeps_its_first_correlation() {
        let first = commit::CommitId::generate();
        let second = commit::CommitId::generate();

        let after_first = Metadata::default().follow_up(first);
        let after_second = after_first.follow_up(second);

        assert_eq!(
            after_second.correlation_id,
            Some(CorrelationId::from(first))
        );
        assert_eq!(after_second.causation_id, Some(CausationId::from(second)));
    }

    #[test]
    fn fill_unset_keeps_set_fields() {
        let ambient = full();
        let mut explicit = Metadata {
            actor: Some("job-runner".into()),
            extra: BTreeMap::from([("tenant".into(), "t2".into())]),
            ..Metadata::default()
        };

        explicit.fill_unset(&ambient);

        assert_eq!(explicit.actor.as_deref(), Some("job-runner"));
        assert_eq!(explicit.correlation_id, ambient.correlation_id);
        assert_eq!(explicit.causation_id, ambient.causation_id);
        assert_eq!(explicit.trace, ambient.trace);
        assert_eq!(explicit.extra["tenant"], "t2");
    }

    #[test]
    fn round_trips_through_the_wire_format() {
        let meta = full();

        assert_eq!(Metadata::decode(&meta.encode()).unwrap(), meta);
        assert_eq!(
            Metadata::decode(&Metadata::default().encode()).unwrap(),
            Metadata::default()
        );
    }

    #[cfg(feature = "otel")]
    mod otel {
        use std::collections::HashMap;

        use opentelemetry::{
            propagation::TextMapPropagator as _,
            trace::{TraceContextExt as _, TracerProvider as _},
        };
        use opentelemetry_sdk::{propagation::TraceContextPropagator, trace::SdkTracerProvider};
        use tracing_subscriber::layer::SubscriberExt as _;

        use super::*;

        fn sdk_extract(trace: &TraceContext) -> opentelemetry::Context {
            let mut headers =
                HashMap::from([("traceparent".to_owned(), trace.traceparent.clone())]);
            if let Some(state) = &trace.tracestate {
                headers.insert("tracestate".to_owned(), state.clone());
            }
            TraceContextPropagator::new().extract(&headers)
        }

        #[test]
        fn no_span_context_without_a_layer() {
            let span = tracing::info_span!("plain");

            assert_eq!(TraceContext::from_span(&span), None);
            assert_eq!(TraceContext::current(), None);
        }

        #[test]
        fn reads_the_current_span_as_the_sdk_propagator_writes_it() {
            let provider = SdkTracerProvider::builder().build();
            let subscriber = tracing_subscriber::registry()
                .with(tracing_opentelemetry::layer().with_tracer(provider.tracer("test")));
            let _default = tracing::subscriber::set_default(subscriber);

            let span = tracing::info_span!("request");
            let _entered = span.enter();
            let trace = TraceContext::current().unwrap();

            let mut injected = HashMap::new();
            {
                use tracing_opentelemetry::OpenTelemetrySpanExt as _;
                TraceContextPropagator::new().inject_context(&span.context(), &mut injected);
            }
            assert_eq!(Some(&trace.traceparent), injected.get("traceparent"));
            assert_eq!(trace.tracestate, None);
        }

        #[test]
        fn parses_like_the_sdk_propagator() {
            let cases = [
                (
                    "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01",
                    None,
                ),
                (
                    "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-00",
                    Some("a=1,b=2"),
                ),
                (
                    "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-ff",
                    None,
                ),
                (
                    "01-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01-extra",
                    None,
                ),
                (
                    "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01-extra",
                    None,
                ),
                (
                    "ff-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01",
                    None,
                ),
                (
                    "00-0AF7651916CD43DD8448EB211C80319C-b7ad6b7169203331-01",
                    None,
                ),
                (
                    "00-00000000000000000000000000000000-b7ad6b7169203331-01",
                    None,
                ),
                (
                    "00-0af7651916cd43dd8448eb211c80319c-0000000000000000-01",
                    None,
                ),
                ("00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331", None),
                (
                    "00-0af7651916cd43dd8448eb211c80319c-b7ad6b716920333-01",
                    None,
                ),
                ("garbage", None),
                ("", None),
            ];

            for (traceparent, tracestate) in cases {
                let trace = TraceContext {
                    traceparent: traceparent.to_owned(),
                    tracestate: tracestate.map(str::to_owned),
                };
                let ours = trace.to_context();
                let sdk = sdk_extract(&trace);
                let sdk_span = sdk.span();
                let sdk_context = sdk_span.span_context();

                match ours {
                    Some(ours) => {
                        let ours_span = ours.span();
                        assert_eq!(ours_span.span_context(), sdk_context, "{traceparent}");
                    }
                    None => assert!(!sdk_context.is_valid(), "{traceparent}"),
                }
            }
        }

        #[test]
        fn a_stored_context_parents_a_span() {
            let provider = SdkTracerProvider::builder().build();
            let subscriber = tracing_subscriber::registry()
                .with(tracing_opentelemetry::layer().with_tracer(provider.tracer("test")));
            let _default = tracing::subscriber::set_default(subscriber);
            let meta = Metadata {
                trace: Some(TraceContext::new(
                    "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01",
                )),
                ..Metadata::default()
            };

            let span = tracing::info_span!("handle");
            {
                use tracing_opentelemetry::OpenTelemetrySpanExt as _;
                let _ = span.set_parent(meta.trace_parent().unwrap());
            }
            let child = TraceContext::from_span(&span).unwrap();

            assert!(
                child
                    .traceparent
                    .starts_with("00-0af7651916cd43dd8448eb211c80319c-")
            );
            assert!(!child.traceparent.contains("b7ad6b7169203331"));
        }
    }
}
