//! Untrusted input never panics or hangs: payload frames (and the metadata inside them) read back from
//! `commits.payload`, W3C trace contexts from incoming requests (`otel`), and store and category names read
//! at run time. Each decoder gets arbitrary bytes and mutations of valid input; the validators also agree with
//! the documented name rules.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::{collections::BTreeMap, sync::mpsc, time::Duration};

use proptest::prelude::*;

use pges_core::{category, event, frame, metadata, store};

/// Generous: a run of the default 256 cases takes milliseconds. Exceeding it means a decoder loops. Raise
/// `PROPTEST_CASES` for a longer search.
const DEADLINE: Duration = Duration::from_secs(120);

#[test]
fn frame_decode_never_panics_on_arbitrary_bytes() {
    within_deadline(|| {
        proptest!(|(payload in proptest::collection::vec(any::<u8>(), 0..256), count in any::<u16>())| {
            let _ = frame::decode(frame::Format::V1, &payload, count);
        });
    });
}

#[test]
fn frame_decode_never_panics_on_mutated_frames() {
    within_deadline(|| {
        proptest!(|(mutation in mutation(), count_delta in -2i32..=2)| {
            let (payload, count) = valid_frame();
            let payload = mutation.apply(payload);
            let count = u16::try_from(i32::from(count) + count_delta).unwrap_or(1);

            let _ = frame::decode(frame::Format::V1, &payload, count);
        });
    });
}

/// The metadata is a protobuf message: arbitrary bytes, and messages whose fields carry the wrong wire type
/// or a bad length, inside an otherwise valid frame.
#[test]
fn metadata_decode_never_panics() {
    within_deadline(|| {
        proptest!(|(metadata in prop_oneof![
            proptest::collection::vec(any::<u8>(), 0..128),
            protobuf_fields(),
        ])| {
            let mut payload = Vec::new();
            varint(&mut payload, metadata.len() as u64);
            payload.extend_from_slice(&metadata);
            payload.extend_from_slice(&[1, 1, 0]);

            if let Ok(decoded) = frame::decode(frame::Format::V1, &payload, 1) {
                prop_assert_eq!(decoded.events.len(), 1);
            }
        });
    });
}

#[test]
fn store_names_follow_the_documented_rule() {
    within_deadline(|| {
        proptest!(|(name in name_text())| {
            match store::StoreName::parse(name.clone()) {
                Ok(parsed) => {
                    prop_assert!(identifier(&name, 40), "{name:?}");
                    prop_assert!(!name.starts_with("pg_"), "{name:?}");
                    prop_assert_eq!(parsed.as_str(), name.as_str());
                }
                Err(_) => prop_assert!(
                    !identifier(&name, 40)
                        || name.starts_with("pg_")
                        || ["pges", "public", "information_schema"].contains(&name.as_str()),
                    "{name:?}"
                ),
            }
        });
    });
}

#[test]
fn category_names_follow_the_documented_rule() {
    within_deadline(|| {
        proptest!(|(name in name_text())| {
            let expected = identifier(&name, 40) && !name.contains("__");
            match category::CategoryName::leak(name.clone()) {
                Ok(parsed) => {
                    prop_assert!(expected, "{name:?}");
                    prop_assert_eq!(parsed.as_str(), name.as_str());
                }
                Err(_) => prop_assert!(!expected, "{name:?}"),
            }
        });
    });
}

#[cfg(feature = "otel")]
#[test]
fn trace_context_parsing_never_panics() {
    within_deadline(|| {
        proptest!(|(traceparent in traceparent(), tracestate in proptest::option::of(header_text()))| {
            let mut trace = metadata::TraceContext::new(traceparent);
            trace.tracestate = tracestate;

            let _ = trace.to_context();
        });
    });
}

/// Runs `f` on its own thread and fails if it does not finish by [`DEADLINE`], so a decoder that loops fails
/// the test instead of hanging it. A panic in `f` fails the test with that panic.
fn within_deadline(f: impl FnOnce() + Send + 'static) {
    let (done, finished) = mpsc::channel();
    let worker = std::thread::spawn(move || {
        f();
        let _ = done.send(());
    });
    match finished.recv_timeout(DEADLINE) {
        Ok(()) => worker.join().unwrap(),
        Err(mpsc::RecvTimeoutError::Disconnected) => {
            if let Err(panic) = worker.join() {
                std::panic::resume_unwind(panic);
            }
        }
        Err(mpsc::RecvTimeoutError::Timeout) => {
            panic!("not finished after {DEADLINE:?}: a decoder loops")
        }
    }
}

/// `^[a-z][a-z0-9_]{0,max-1}$`.
fn identifier(name: &str, max: usize) -> bool {
    let bytes = name.as_bytes();
    !bytes.is_empty()
        && bytes.len() <= max
        && bytes[0].is_ascii_lowercase()
        && bytes[1..]
            .iter()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || *b == b'_')
}

/// Mostly near-valid names, so both outcomes are common, plus arbitrary text.
fn name_text() -> impl Strategy<Value = String> {
    prop_oneof![
        4 => "[a-z_0-9A-Z.\\-]{0,45}",
        1 => "(pg_|pges|public|information_schema)[a-z_]{0,3}",
        1 => any::<String>(),
    ]
}

#[cfg(feature = "otel")]
fn header_text() -> impl Strategy<Value = String> {
    prop_oneof!["[0-9a-zA-Z=,@*/ \\-]{0,80}", any::<String>(),]
}

/// Arbitrary text, and valid traceparents with parts changed, added or dropped.
#[cfg(feature = "otel")]
fn traceparent() -> impl Strategy<Value = String> {
    prop_oneof![
        any::<String>(),
        "[0-9a-fA-F]{0,3}-[0-9a-fA-F]{0,33}-[0-9a-fA-F]{0,17}-[0-9a-fA-F]{0,3}(-[0-9a-z]{0,4})?",
        (
            mutation(),
            Just("00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01")
        )
            .prop_map(|(mutation, valid)| String::from_utf8_lossy(
                &mutation.apply(valid.as_bytes().to_vec())
            )
            .into_owned()),
    ]
}

/// A sequence of protobuf fields with the metadata's field numbers (and a few others), each with an arbitrary
/// wire type and a body that may not match it.
fn protobuf_fields() -> impl Strategy<Value = Vec<u8>> {
    proptest::collection::vec(
        (
            0u64..10,
            0u64..8,
            proptest::collection::vec(any::<u8>(), 0..20),
        ),
        0..6,
    )
    .prop_map(|fields| {
        let mut out = Vec::new();
        for (field, wire_type, body) in fields {
            varint(&mut out, (field << 3) | wire_type);
            if wire_type == 2 {
                varint(&mut out, body.len() as u64);
            }
            out.extend_from_slice(&body);
        }
        out
    })
}

/// A frame with full metadata and three events.
fn valid_frame() -> (Vec<u8>, u16) {
    let mut metadata = metadata::Metadata::default();
    metadata.correlation_id = Some(metadata::CorrelationId::new_v7());
    metadata.actor = Some("user:42".into());
    metadata.trace = Some(metadata::TraceContext::new(
        "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01",
    ));
    metadata.extra = BTreeMap::from([("tenant".into(), "t1".into())]);
    let version = event::SchemaVersion::new(3).unwrap();
    let events = [
        frame::FrameEvent::new(frame::EventTypeId::new(1), version, b"{\"a\":1}"),
        frame::FrameEvent::new(frame::EventTypeId::new(300), version, b""),
        frame::FrameEvent::new(frame::EventTypeId::new(u32::MAX), version, &[0xff; 200]),
    ];

    (
        frame::encode(frame::Format::V1, &metadata, &events).unwrap(),
        3,
    )
}

/// A change to valid input: flipped, replaced, inserted or removed bytes, or a truncation.
#[derive(Debug, Clone)]
enum Mutation {
    Flip { at: usize, bits: u8 },
    Set { at: usize, byte: u8 },
    Insert { at: usize, bytes: Vec<u8> },
    Remove { at: usize, len: usize },
    Truncate { at: usize },
}

impl Mutation {
    /// `at` is taken modulo the input's length, so every mutation lands inside it.
    fn apply(&self, mut input: Vec<u8>) -> Vec<u8> {
        let len = input.len().max(1);
        match self {
            Self::Flip { at, bits } => {
                if let Some(byte) = input.get_mut(at % len) {
                    *byte ^= bits;
                }
            }
            Self::Set { at, byte } => {
                if let Some(slot) = input.get_mut(at % len) {
                    *slot = *byte;
                }
            }
            Self::Insert { at, bytes } => {
                let at = at % (input.len() + 1);
                input.splice(at..at, bytes.iter().copied());
            }
            Self::Remove { at, len: count } => {
                let at = at % len;
                let end = (at + count).min(input.len());
                if at < end {
                    input.drain(at..end);
                }
            }
            Self::Truncate { at } => input.truncate(at % len),
        }
        input
    }
}

/// One to four mutations, applied in turn.
fn mutation() -> impl Strategy<Value = Mutations> {
    proptest::collection::vec(single_mutation(), 1..4).prop_map(Mutations)
}

#[derive(Debug, Clone)]
struct Mutations(Vec<Mutation>);

impl Mutations {
    fn apply(&self, input: Vec<u8>) -> Vec<u8> {
        self.0
            .iter()
            .fold(input, |input, mutation| mutation.apply(input))
    }
}

fn single_mutation() -> impl Strategy<Value = Mutation> {
    prop_oneof![
        (any::<usize>(), 1u8..).prop_map(|(at, bits)| Mutation::Flip { at, bits }),
        (any::<usize>(), any::<u8>()).prop_map(|(at, byte)| Mutation::Set { at, byte }),
        (
            any::<usize>(),
            proptest::collection::vec(any::<u8>(), 1..12)
        )
            .prop_map(|(at, bytes)| Mutation::Insert { at, bytes }),
        (any::<usize>(), 1usize..12).prop_map(|(at, len)| Mutation::Remove { at, len }),
        any::<usize>().prop_map(|at| Mutation::Truncate { at }),
    ]
}

fn varint(out: &mut Vec<u8>, mut value: u64) {
    while value >= 0x80 {
        out.push((value as u8) | 0x80);
        value >>= 7;
    }
    out.push(value as u8);
}
