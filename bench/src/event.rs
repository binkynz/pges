//! The event the benchmarks append, its payload, and an aggregate to load.

use pges::core::aggregate;

/// The only event type: a payload of a chosen size.
#[derive(Debug, Clone, PartialEq, pges::Event)]
#[event(category = "bench")]
pub enum BenchEvent {
    /// A recorded payload.
    #[event(name = "bench.recorded")]
    Recorded {
        /// Pseudo-random text, so compression gains little.
        data: String,
    },
}

impl BenchEvent {
    /// An event whose `data` is `size` bytes of text drawn from `rng`.
    pub fn sized(size: usize, rng: &mut Rng) -> Self {
        Self::Recorded {
            data: payload(size, rng),
        }
    }

    /// `count` events of `size` bytes each.
    pub fn batch(count: usize, size: usize, rng: &mut Rng) -> Vec<Self> {
        (0..count).map(|_| Self::sized(size, rng)).collect()
    }
}

/// Counts a stream's events, for `load`.
#[derive(Debug, Default)]
pub struct Counter {
    /// Events applied.
    pub events: u64,
}

impl aggregate::Aggregate for Counter {
    type Event = BenchEvent;

    fn apply(&mut self, _event: &BenchEvent) {
        self.events += 1;
    }
}

/// A small, fast pseudo-random generator (xorshift64*). Not for anything but picking streams and filling
/// payloads.
#[derive(Debug, Clone)]
pub struct Rng(u64);

impl Rng {
    /// A generator seeded with `seed`; equal seeds give equal sequences.
    pub fn new(seed: u64) -> Self {
        Self(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1)
    }

    /// The next value.
    pub fn next_u64(&mut self) -> u64 {
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        self.0.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }

    /// A value below `bound`, which must not be zero.
    pub fn below(&mut self, bound: usize) -> usize {
        (self.next_u64() % bound.max(1) as u64) as usize
    }
}

/// `size` bytes of URL-safe text drawn from `rng`.
fn payload(size: usize, rng: &mut Rng) -> String {
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";

    (0..size)
        .map(|_| char::from(ALPHABET[(rng.next_u64() >> 58) as usize]))
        .collect()
}
