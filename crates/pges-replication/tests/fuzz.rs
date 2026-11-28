//! Untrusted input never panics or hangs: `pgoutput` messages from the server, and slot, publication and
//! snapshot names read at run time. The decoder gets arbitrary bytes, a valid tag followed by arbitrary bytes,
//! and mutations of valid messages of every kind; the name validators also agree with the documented rules.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::{sync::mpsc, time::Duration};

use bytes::{BufMut, Bytes, BytesMut};
use proptest::prelude::*;

use pges_replication::{name, pgoutput};

/// Generous: a run of the default 256 cases takes milliseconds. Exceeding it means the decoder loops. Raise
/// `PROPTEST_CASES` for a longer search.
const DEADLINE: Duration = Duration::from_secs(120);

/// Every tag the decoder knows, including the streaming and two-phase ones it rejects.
const TAGS: &[u8] = b"BCORYIUDTMSEcAbPKrp";

#[test]
fn decode_never_panics_on_arbitrary_bytes() {
    within_deadline(|| {
        proptest!(|(bytes in proptest::collection::vec(any::<u8>(), 0..256))| {
            let _ = pgoutput::Message::decode(Bytes::from(bytes));
        });
    });
}

#[test]
fn decode_never_panics_after_a_valid_tag() {
    within_deadline(|| {
        proptest!(|(tag in proptest::sample::select(TAGS), body in body())| {
            let mut bytes = vec![tag];
            bytes.extend_from_slice(&body);

            let _ = pgoutput::Message::decode(Bytes::from(bytes));
        });
    });
}

#[test]
fn decode_never_panics_on_mutated_messages() {
    within_deadline(|| {
        proptest!(|(seed in proptest::sample::select(seeds()), mutations in mutations())| {
            prop_assert!(pgoutput::Message::decode(Bytes::from(seed.clone())).is_ok());
            let mutated = mutations.iter().fold(seed, |input, mutation| mutation.apply(input));

            let _ = pgoutput::Message::decode(Bytes::from(mutated));
        });
    });
}

#[test]
fn slot_names_follow_the_documented_rule() {
    within_deadline(|| {
        proptest!(|(text in name_text())| {
            let expected = !text.is_empty()
                && text.len() <= 63
                && text.bytes().all(|b| matches!(b, b'a'..=b'z' | b'0'..=b'9' | b'_'));
            match name::SlotName::new(text.clone()) {
                Ok(slot) => {
                    prop_assert!(expected, "{text:?}");
                    prop_assert_eq!(slot.as_str(), text.as_str());
                }
                Err(_) => prop_assert!(!expected, "{text:?}"),
            }
        });
    });
}

#[test]
fn publication_and_snapshot_names_never_panic() {
    within_deadline(|| {
        proptest!(|(text in name_text())| {
            if let Ok(publication) = name::PublicationName::new(text.clone()) {
                prop_assert_eq!(publication.as_str(), text.as_str());
            }
            if let Ok(snapshot) = name::SnapshotName::new(text.clone()) {
                prop_assert_eq!(snapshot.as_str(), text.as_str());
            }
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
            panic!("not finished after {DEADLINE:?}: the decoder loops")
        }
    }
}

/// Arbitrary bytes, and bytes biased towards the values the decoder branches on: tuple and identity markers,
/// NUL terminators, and counts and lengths near zero or the maximum.
fn body() -> impl Strategy<Value = Vec<u8>> {
    let interesting = prop_oneof![
        any::<u8>(),
        proptest::sample::select(b"NKOntubdfi\0\x7f\x80\xff".as_slice()),
    ];
    prop_oneof![
        proptest::collection::vec(any::<u8>(), 0..128),
        proptest::collection::vec(interesting, 0..128),
    ]
}

fn name_text() -> impl Strategy<Value = String> {
    prop_oneof![
        4 => "[a-z_0-9A-Z.\\-]{0,70}",
        1 => any::<String>(),
    ]
}

/// One valid message of each kind the decoder accepts.
fn seeds() -> Vec<Vec<u8>> {
    let mut seeds = Vec::new();
    let mut message = |build: &dyn Fn(&mut BytesMut)| {
        let mut buf = BytesMut::new();
        build(&mut buf);
        seeds.push(buf.to_vec());
    };

    message(&|buf| {
        buf.put_u8(b'B');
        buf.put_u64(0x10);
        buf.put_i64(1_000_000);
        buf.put_u32(742);
    });
    message(&|buf| {
        buf.put_u8(b'C');
        buf.put_u8(0);
        buf.put_u64(0x10);
        buf.put_u64(0x38);
        buf.put_i64(1_000_000);
    });
    message(&|buf| {
        buf.put_u8(b'O');
        buf.put_u64(9);
        cstr(buf, "node_a");
    });
    message(&|buf| {
        buf.put_u8(b'R');
        buf.put_u32(16_400);
        cstr(buf, "s1");
        cstr(buf, "commits");
        buf.put_u8(b'd');
        buf.put_i16(2);
        buf.put_u8(1);
        cstr(buf, "commit_id");
        buf.put_u32(2950);
        buf.put_i32(-1);
        buf.put_u8(0);
        cstr(buf, "event_count");
        buf.put_u32(21);
        buf.put_i32(-1);
    });
    message(&|buf| {
        buf.put_u8(b'Y');
        buf.put_u32(77);
        cstr(buf, "public");
        cstr(buf, "mood");
    });
    message(&|buf| {
        buf.put_u8(b'I');
        buf.put_u32(16_400);
        buf.put_u8(b'N');
        tuple(buf);
    });
    message(&|buf| {
        buf.put_u8(b'U');
        buf.put_u32(1);
        buf.put_u8(b'O');
        tuple(buf);
        buf.put_u8(b'N');
        tuple(buf);
    });
    message(&|buf| {
        buf.put_u8(b'D');
        buf.put_u32(1);
        buf.put_u8(b'K');
        tuple(buf);
    });
    message(&|buf| {
        buf.put_u8(b'T');
        buf.put_u32(2);
        buf.put_u8(3);
        buf.put_u32(5);
        buf.put_u32(6);
    });
    message(&|buf| {
        buf.put_u8(b'M');
        buf.put_u8(1);
        buf.put_u64(9);
        cstr(buf, "pges");
        buf.put_u32(3);
        buf.put_slice(b"abc");
    });

    seeds
}

/// A tuple with one value of each kind.
fn tuple(buf: &mut BytesMut) {
    buf.put_i16(4);
    buf.put_u8(b't');
    buf.put_u32(1);
    buf.put_slice(b"7");
    buf.put_u8(b'b');
    buf.put_u32(2);
    buf.put_slice(&[0, 1]);
    buf.put_u8(b'n');
    buf.put_u8(b'u');
}

fn cstr(buf: &mut BytesMut, text: &str) {
    buf.put_slice(text.as_bytes());
    buf.put_u8(0);
}

/// A change to valid input: flipped, replaced, inserted or removed bytes, or a truncation. Positions are taken
/// modulo the input's length, so every mutation lands inside it.
#[derive(Debug, Clone)]
enum Mutation {
    Flip { at: usize, bits: u8 },
    Set { at: usize, byte: u8 },
    Insert { at: usize, bytes: Vec<u8> },
    Remove { at: usize, len: usize },
    Truncate { at: usize },
}

impl Mutation {
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
fn mutations() -> impl Strategy<Value = Vec<Mutation>> {
    let mutation = prop_oneof![
        (any::<usize>(), 1u8..).prop_map(|(at, bits)| Mutation::Flip { at, bits }),
        (any::<usize>(), any::<u8>()).prop_map(|(at, byte)| Mutation::Set { at, byte }),
        (
            any::<usize>(),
            proptest::collection::vec(any::<u8>(), 1..12)
        )
            .prop_map(|(at, bytes)| Mutation::Insert { at, bytes }),
        (any::<usize>(), 1usize..12).prop_map(|(at, len)| Mutation::Remove { at, len }),
        any::<usize>().prop_map(|at| Mutation::Truncate { at }),
    ];
    proptest::collection::vec(mutation, 1..4)
}
