//! Randomized concurrent workloads checked against end-to-end invariants.
//!
//! Writers append, append in batches, run units of work (some dropped part-way or cut short by a timeout),
//! delete streams and save snapshots over overlapping streams of three categories, on top of history written
//! two months back. Meanwhile maintenance and retention run (window and snapshot-and-truncate retention drop
//! the history, `prune_log` its log rows), real sequencers are crashed, stopped and replaced, subscriptions
//! (listed, store-wide, sharded, `run_pg` and `run`; the listed one appends a follow-up per jobs event in its
//! transaction) are crashed and restarted, and a backfill is cut short and resumed. Afterwards the store, the
//! log and every delivery are checked against a ledger of what the writers were told:
//!
//! - every acknowledged commit is in the log once; the log is gap-free; no commit the writers were told
//!   failed exists, and a commit whose outcome is unknown exists whole or not at all;
//! - each stream's versions are contiguous, its commits hold the acknowledged events, an append that started
//!   after another returned comes later in the stream and in the log, `load` and `load_snapshotted` equal a
//!   fold of `read_stream` (and of the dropped history), and a stored snapshot equals the fold up to its
//!   version;
//! - deleted streams have no commits, snapshot or readable events left, and their tombstone is in the log;
//! - each `run_pg` subscription and the backfill deliver every surviving event once and the `run`
//!   subscription at least once, at its log position, in position order (per stream when sharded or in
//!   lanes), a deleted stream's events at most once as a prefix of its versions, and its tombstone once; the
//!   follow-ups match the handled events once; no subscription or sequencer recovers from an error;
//! - retention drops only history of the categories whose policy allows it.
//!
//! Each case is seeded: the seed fixes every task's schedule of operations, not the interleaving the server
//! picks. The default run is two short cases (outbox and logical source). `PGES_INVARIANTS_SEEDS` (a count,
//! or a comma-separated list of seeds) and `PGES_INVARIANTS_MILLIS` (workload length per case) make a longer
//! run; a failure prints the seed to replay with `PGES_INVARIANTS_SEEDS=<seed>,`.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

mod common;

use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    marker::PhantomData,
    num::{NonZeroU16, NonZeroU32},
    str::FromStr,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering},
    },
    time::Duration,
};

use anyhow::{Context, Result, bail, ensure};
use futures_util::StreamExt;
use serde::{Deserialize, Serialize};
use sqlx::{Executor, PgConnection, PgPool, postgres::PgConnectOptions};
use tokio::{sync::oneshot, task::JoinHandle};
use uuid::Uuid;

use pges_core::{aggregate, commit, event, interval, stream, version};
use pges_postgres::{append, delete, limit, read, retention, store, unit_of_work};
use pges_sequencer::{outbox, sequencer};
use pges_subscribe::{
    backfill, batch, handler,
    subscription::{self, StoreExt},
    waker::recheck,
};

use common::{Cat, Fixture, JobEvent, OrderEvent, PartEvent};

const CATS: [Cat; 3] = [Cat::Jobs, Cat::Orders, Cat::Parts];
/// Live streams per category; a deleted stream's slot gets a new stream.
const SLOTS: usize = 4;
const WRITERS: u64 = 4;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Kind {
    Outbox,
    Logical,
}

#[test]
fn concurrent_workloads_keep_the_invariants() {
    let millis: u64 = std::env::var("PGES_INVARIANTS_MILLIS")
        .ok()
        .map(|v| v.parse().expect("PGES_INVARIANTS_MILLIS is a number"))
        .unwrap_or(2500);
    let seeds: Vec<u64> = match std::env::var("PGES_INVARIANTS_SEEDS") {
        Ok(v) if v.contains(',') => v
            .split(',')
            .filter(|s| !s.trim().is_empty())
            .map(|s| s.trim().parse().expect("a seed is a number"))
            .collect(),
        Ok(v) => {
            let count: u64 = v
                .parse()
                .expect("PGES_INVARIANTS_SEEDS is a count or a list");
            let base = rand_base();
            (0..count).map(|i| base.wrapping_add(i)).collect()
        }
        Err(_) => vec![1, 2],
    };
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap();
    for seed in seeds {
        // Odd seeds run the outbox, even seeds the logical source.
        let kind = if seed % 2 == 1 {
            Kind::Outbox
        } else {
            Kind::Logical
        };
        eprintln!("invariants: seed {seed} ({kind:?}, {millis} ms)");
        let result = runtime.block_on(run_case(seed, kind, Duration::from_millis(millis)));
        if let Err(error) = result {
            panic!("seed {seed} ({kind:?}) broke an invariant: {error:#}");
        }
    }
}

fn rand_base() -> u64 {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos() as u64;
    nanos % 1_000_000 * 1000
}

/// splitmix64.
#[derive(Debug, Clone)]
struct Rng(u64);

impl Rng {
    fn new(seed: u64, stream: u64) -> Self {
        Self(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) ^ stream.wrapping_mul(0xD1B5_4A32_D192_ED03))
    }

    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }

    fn range(&mut self, low: u64, high: u64) -> u64 {
        low + self.below(high - low)
    }

    fn chance(&mut self, percent: u64) -> bool {
        self.below(100) < percent
    }
}

// --- events and aggregates ---------------------------------------------------------------------------------

trait Noted: event::Event + Clone + std::fmt::Debug + PartialEq {
    fn with(n: u32) -> Self;
    fn n(&self) -> u32;
}

impl Noted for JobEvent {
    fn with(n: u32) -> Self {
        Self::Noted { n }
    }
    fn n(&self) -> u32 {
        let Self::Noted { n } = self;
        *n
    }
}

impl Noted for OrderEvent {
    fn with(n: u32) -> Self {
        Self::Noted { n }
    }
    fn n(&self) -> u32 {
        let Self::Noted { n } = self;
        *n
    }
}

impl Noted for PartEvent {
    fn with(n: u32) -> Self {
        Self::Noted { n }
    }
    fn n(&self) -> u32 {
        let Self::Noted { n } = self;
        *n
    }
}

/// A fold that depends on every event and their order.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(bound = "")]
struct Tally<E> {
    count: u64,
    sum: u64,
    hash: u64,
    #[serde(skip)]
    events: PhantomData<fn() -> E>,
}

impl<E> Default for Tally<E> {
    fn default() -> Self {
        Self {
            count: 0,
            sum: 0,
            hash: 0,
            events: PhantomData,
        }
    }
}

impl<E> Tally<E> {
    fn add(&mut self, n: u32) {
        self.count += 1;
        self.sum += u64::from(n);
        self.hash = self.hash.rotate_left(7) ^ u64::from(n).wrapping_mul(0x9E37_79B9_7F4A_7C15);
    }

    fn of(ns: &[u32]) -> Self {
        let mut tally = Self::default();
        for n in ns {
            tally.add(*n);
        }
        tally
    }
}

impl<E: Noted> aggregate::Aggregate for Tally<E> {
    type Event = E;

    fn apply(&mut self, event: &E) {
        self.add(event.n());
    }
}

impl<E: Noted> aggregate::Snapshotted for Tally<E> {
    const SNAPSHOT_VERSION: NonZeroU16 = NonZeroU16::MIN;
}

macro_rules! with_event {
    ($cat:expr, $e:ident => $body:expr) => {
        match $cat {
            Cat::Jobs => {
                type $e = JobEvent;
                $body
            }
            Cat::Orders => {
                type $e = OrderEvent;
                $body
            }
            Cat::Parts => {
                type $e = PartEvent;
                $body
            }
        }
    };
}

fn cat_index(cat: Cat) -> usize {
    match cat {
        Cat::Jobs => 0,
        Cat::Orders => 1,
        Cat::Parts => 2,
    }
}

// --- the ledger --------------------------------------------------------------------------------------------

/// What a writer was told about one commit (one stream's part of an append).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Fate {
    /// Acknowledged: it committed.
    Acked,
    /// It failed or was dropped before committing: it must not exist.
    Absent,
    /// The future was dropped while the outcome was being decided.
    Unknown,
}

#[derive(Debug, Clone)]
struct Planned {
    stream: stream::StreamId,
    cat: Cat,
    ns: Vec<u32>,
    fate: Fate,
    result: Option<Acked>,
    /// Ticks before the call started and after it returned.
    started: u64,
    ended: u64,
    /// Written months back before the workload, for retention to drop.
    old: bool,
}

#[derive(Debug, Clone, Copy)]
struct Acked {
    commit_id: commit::CommitId,
    first: i64,
    last: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DeleteFate {
    Deleted,
    Unknown,
}

#[derive(Debug, Clone)]
struct Deletion {
    stream: stream::StreamId,
    fate: DeleteFate,
    tombstone: Option<Acked>,
    started: u64,
    ended: u64,
}

#[derive(Default)]
struct Ledger {
    commits: Vec<Planned>,
    deletions: Vec<Deletion>,
    /// Every stream any writer used, with its category.
    streams: BTreeMap<stream::StreamId, Cat>,
    failures: Vec<String>,
    counts: BTreeMap<&'static str, u64>,
}

struct World {
    seed: u64,
    store: store::Store,
    slots: Mutex<Vec<Vec<stream::StreamId>>>,
    ledger: Mutex<Ledger>,
    next_n: AtomicU32,
    tick: AtomicU64,
    stop: AtomicBool,
}

impl World {
    fn tick(&self) -> u64 {
        self.tick.fetch_add(1, Ordering::SeqCst)
    }

    fn ns(&self, count: u32) -> Vec<u32> {
        let first = self.next_n.fetch_add(count, Ordering::SeqCst);
        (first..first + count).collect()
    }

    fn pick(&self, rng: &mut Rng, cat: Cat) -> stream::StreamId {
        let slots = self.slots.lock().unwrap();
        slots[cat_index(cat)][rng.below(SLOTS as u64) as usize]
    }

    fn replace(&self, cat: Cat, old: stream::StreamId) {
        let mut slots = self.slots.lock().unwrap();
        let new = stream::StreamId::new_v7();
        for slot in slots[cat_index(cat)].iter_mut() {
            if *slot == old {
                *slot = new;
            }
        }
        self.ledger.lock().unwrap().streams.insert(new, cat);
    }

    fn count(&self, what: &'static str) {
        *self.ledger.lock().unwrap().counts.entry(what).or_default() += 1;
    }

    fn fail(&self, what: String) {
        self.ledger.lock().unwrap().failures.push(what);
    }

    fn record(&self, planned: Vec<Planned>) {
        self.ledger.lock().unwrap().commits.extend(planned);
    }
}

fn acked(result: &append::AppendResult) -> Acked {
    Acked {
        commit_id: result.commit_id,
        first: result.versions.start().to_i64(),
        last: result.versions.end().to_i64(),
    }
}

/// Whether an append error is one the workload can cause: a conflict, a stream deleted under it, or a
/// retryable database error such as a deadlock between units of work.
fn expected_append_error(error: &append::AppendError) -> bool {
    match error {
        append::AppendError::Conflict { .. } | append::AppendError::StreamDeleted(_) => true,
        append::AppendError::Database(db) => db.is_retryable(),
        _ => false,
    }
}

// --- writers -----------------------------------------------------------------------------------------------

async fn writer(world: Arc<World>, index: u64) {
    let mut rng = Rng::new(world.seed, 100 + index);
    while !world.stop.load(Ordering::SeqCst) {
        let roll = rng.below(100);
        let op = match roll {
            0..=29 => "append",
            30..=39 => "append_exact",
            40..=57 => "append_many",
            58..=71 => "uow_commit",
            72..=76 => "uow_dropped",
            77..=81 => "uow_commit_timeout",
            82..=85 => "append_timeout",
            86..=88 => "delete",
            _ => "snapshot",
        };
        world.count(op);
        let result = match op {
            "append" => append_one(&world, &mut rng, None).await,
            "append_exact" => append_exact(&world, &mut rng).await,
            "append_many" => append_many(&world, &mut rng).await,
            "uow_commit" => unit_of_work(&world, &mut rng, UowEnd::Commit).await,
            "uow_dropped" => unit_of_work(&world, &mut rng, UowEnd::Drop).await,
            "uow_commit_timeout" => {
                let micros = rng.range(0, 3000);
                unit_of_work(&world, &mut rng, UowEnd::CommitWithin(micros)).await
            }
            "append_timeout" => {
                let micros = rng.range(0, 3000);
                append_one(&world, &mut rng, Some(micros)).await
            }
            "delete" => delete_one(&world, &mut rng).await,
            _ => snapshot(&world, &mut rng).await,
        };
        if let Err(error) = result {
            world.fail(format!("writer {index} {op}: {error:#}"));
        }
        if rng.chance(20) {
            tokio::time::sleep(Duration::from_millis(rng.range(1, 6))).await;
        }
    }
}

fn random_cat(rng: &mut Rng) -> Cat {
    CATS[rng.below(3) as usize]
}

async fn append_one(world: &World, rng: &mut Rng, within: Option<u64>) -> Result<()> {
    let cat = random_cat(rng);
    let stream_id = world.pick(rng, cat);
    let ns = world.ns(rng.range(1, 4) as u32);
    let started = world.tick();
    let outcome = with_event!(cat, E => {
        let events: Vec<E> = ns.iter().map(|n| E::with(*n)).collect();
        let call = world.store.append(stream_id, version::ExpectedVersion::Any, &events);
        match within {
            Some(micros) => {
                tokio::time::timeout(Duration::from_micros(micros), call.into_future()).await.ok()
            }
            None => Some(call.await),
        }
    });
    let ended = world.tick();
    let mut planned = Planned {
        stream: stream_id,
        cat,
        ns,
        fate: Fate::Unknown,
        result: None,
        started,
        ended,
        old: false,
    };
    match outcome {
        None => world.count("append_cut_short"),
        Some(Ok(result)) => {
            planned.fate = Fate::Acked;
            planned.result = Some(acked(&result));
        }
        Some(Err(error)) if expected_append_error(&error) => {
            planned.fate = if matches!(&error, append::AppendError::Database(_)) {
                Fate::Unknown
            } else {
                Fate::Absent
            };
        }
        Some(Err(error)) => {
            world.record(vec![planned]);
            bail!("append to {stream_id}: {error:?}");
        }
    }
    world.record(vec![planned]);
    Ok(())
}

async fn append_exact(world: &World, rng: &mut Rng) -> Result<()> {
    let cat = random_cat(rng);
    let stream_id = world.pick(rng, cat);
    let ns = world.ns(rng.range(1, 3) as u32);
    let snapshot_after = rng.chance(30);
    let started = world.tick();
    let outcome = with_event!(cat, E => {
        let loaded = match world.store.load::<Tally<E>>(stream_id).await {
            Ok(loaded) => loaded,
            Err(pges_postgres::load::LoadError::Read(read::ReadError::StreamDeleted(_) | read::ReadError::Truncated { .. })) => {
                return Ok(());
            }
            Err(error) => bail!("load before an exact append to {stream_id}: {error:?}"),
        };
        let events: Vec<E> = ns.iter().map(|n| E::with(*n)).collect();
        let call = world.store.append(stream_id, loaded.expected(), &events);
        if snapshot_after {
            let mut state = loaded.state().clone();
            for n in &ns {
                state.add(*n);
            }
            call.snapshot_after(&state).await
        } else {
            call.await
        }
    });
    let ended = world.tick();
    let mut planned = Planned {
        stream: stream_id,
        cat,
        ns,
        fate: Fate::Absent,
        result: None,
        started,
        ended,
        old: false,
    };
    match outcome {
        Ok(result) => {
            planned.fate = Fate::Acked;
            planned.result = Some(acked(&result));
        }
        Err(error) if expected_append_error(&error) => {
            if matches!(&error, append::AppendError::Database(_)) {
                planned.fate = Fate::Unknown;
            }
        }
        Err(error) => {
            world.record(vec![planned]);
            bail!("exact append to {stream_id}: {error:?}");
        }
    }
    world.record(vec![planned]);
    Ok(())
}

/// Two or three distinct streams, of any categories.
fn pick_distinct(world: &World, rng: &mut Rng, count: usize) -> Vec<(Cat, stream::StreamId)> {
    let mut picked: Vec<(Cat, stream::StreamId)> = Vec::new();
    while picked.len() < count {
        let cat = random_cat(rng);
        let stream_id = world.pick(rng, cat);
        if !picked.iter().any(|(_, s)| *s == stream_id) {
            picked.push((cat, stream_id));
        }
    }
    picked
}

async fn append_many(world: &World, rng: &mut Rng) -> Result<()> {
    let count = rng.range(2, 4) as usize;
    let targets = pick_distinct(world, rng, count);
    let parts: Vec<(Cat, stream::StreamId, Vec<u32>)> = targets
        .into_iter()
        .map(|(cat, stream_id)| (cat, stream_id, world.ns(rng.range(1, 4) as u32)))
        .collect();
    let jobs: Vec<Vec<JobEvent>> = events_of(&parts, Cat::Jobs);
    let orders: Vec<Vec<OrderEvent>> = events_of(&parts, Cat::Orders);
    let partss: Vec<Vec<PartEvent>> = events_of(&parts, Cat::Parts);
    let mut entries = Vec::new();
    let (mut j, mut o, mut p) = (0, 0, 0);
    for (cat, stream_id, _) in &parts {
        let any = version::ExpectedVersion::Any;
        entries.push(match cat {
            Cat::Jobs => {
                j += 1;
                append::AppendEntry::new(*stream_id, any, &jobs[j - 1])
            }
            Cat::Orders => {
                o += 1;
                append::AppendEntry::new(*stream_id, any, &orders[o - 1])
            }
            Cat::Parts => {
                p += 1;
                append::AppendEntry::new(*stream_id, any, &partss[p - 1])
            }
        });
    }
    let started = world.tick();
    let outcome = world.store.append_many(entries).await;
    let ended = world.tick();
    let mut planned: Vec<Planned> = parts
        .iter()
        .map(|(cat, stream_id, ns)| Planned {
            stream: *stream_id,
            cat: *cat,
            ns: ns.clone(),
            fate: Fate::Absent,
            result: None,
            started,
            ended,
            old: false,
        })
        .collect();
    match outcome {
        Ok(results) => {
            ensure!(
                results.len() == planned.len(),
                "append_many returned {} results",
                results.len()
            );
            for (planned, result) in planned.iter_mut().zip(&results) {
                ensure!(
                    result.stream_id == planned.stream,
                    "append_many results out of entry order"
                );
                planned.fate = Fate::Acked;
                planned.result = Some(acked(result));
            }
        }
        Err(error) if expected_append_error(&error) => {
            if matches!(&error, append::AppendError::Database(_)) {
                for planned in &mut planned {
                    planned.fate = Fate::Unknown;
                }
            }
        }
        Err(error) => {
            world.record(planned);
            bail!("append_many: {error:?}");
        }
    }
    world.record(planned);
    Ok(())
}

fn events_of<E: Noted>(parts: &[(Cat, stream::StreamId, Vec<u32>)], cat: Cat) -> Vec<Vec<E>> {
    parts
        .iter()
        .filter(|(c, _, _)| *c == cat)
        .map(|(_, _, ns)| ns.iter().map(|n| E::with(*n)).collect())
        .collect()
}

#[derive(Debug, Clone, Copy)]
enum UowEnd {
    Commit,
    /// Dropped before committing, possibly while an append is in flight.
    Drop,
    /// The commit is cut short after this many microseconds.
    CommitWithin(u64),
}

async fn unit_of_work(world: &World, rng: &mut Rng, end: UowEnd) -> Result<()> {
    let count = rng.range(1, 4) as usize;
    let targets = pick_distinct(world, rng, count);
    let started = world.tick();
    let mut uow = match world.store.begin().await {
        Ok(uow) => uow,
        Err(error) => bail!("begin: {error:?}"),
    };
    let mut planned: Vec<Planned> = Vec::new();
    // Drop: the unit of work is dropped after this many appends, the last of them possibly in flight.
    let drop_after = rng.below(count as u64 + 1) as usize;
    let cut_in_flight = rng.chance(50);
    // A conflict leaves the unit of work usable; a database error aborts it.
    let mut aborted = false;
    for (i, (cat, stream_id)) in targets.iter().enumerate() {
        let ns = world.ns(rng.range(1, 3) as u32);
        let mut part = Planned {
            stream: *stream_id,
            cat: *cat,
            ns: ns.clone(),
            fate: Fate::Absent,
            result: None,
            started,
            ended: 0,
            old: false,
        };
        let dropping_here = matches!(end, UowEnd::Drop) && i == drop_after;
        let outcome = with_event!(*cat, E => {
            let events: Vec<E> = ns.iter().map(|n| E::with(*n)).collect();
            let call = uow.append(*stream_id, version::ExpectedVersion::Any, &events);
            if dropping_here && cut_in_flight {
                let micros = rng.range(0, 2000);
                tokio::time::timeout(Duration::from_micros(micros), call.into_future()).await.ok()
            } else {
                Some(call.await)
            }
        });
        match outcome {
            None => {
                planned.push(part);
                break;
            }
            Some(Ok(result)) => {
                part.result = Some(acked(&result));
                planned.push(part);
            }
            Some(Err(error)) if expected_append_error(&error) => {
                aborted = matches!(&error, append::AppendError::Database(_));
                planned.push(part);
                break;
            }
            Some(Err(error)) => {
                planned.push(part);
                world.record(planned);
                bail!("unit of work append to {stream_id}: {error:?}");
            }
        }
        if dropping_here {
            break;
        }
    }

    let fate = match end {
        UowEnd::Drop => {
            drop(uow);
            world.count("uow_dropped_done");
            Fate::Absent
        }
        UowEnd::Commit => match uow.commit().await {
            Ok(()) if aborted => {
                world.record(planned);
                bail!("a unit of work committed after a database error aborted it");
            }
            Ok(()) => Fate::Acked,
            Err(unit_of_work::TransactionError::Aborted) => Fate::Absent,
            Err(unit_of_work::TransactionError::Database(error)) if error.is_retryable() => {
                Fate::Unknown
            }
            Err(error) => {
                world.record(planned);
                bail!("commit: {error:?}");
            }
        },
        UowEnd::CommitWithin(micros) => {
            match tokio::time::timeout(Duration::from_micros(micros), uow.commit()).await {
                Err(_) => {
                    world.count("commit_cut_short");
                    Fate::Unknown
                }
                Ok(Ok(())) if aborted => {
                    world.record(planned);
                    bail!("a unit of work committed after a database error aborted it");
                }
                Ok(Ok(())) => Fate::Acked,
                Ok(Err(unit_of_work::TransactionError::Aborted)) => Fate::Absent,
                Ok(Err(unit_of_work::TransactionError::Database(error)))
                    if error.is_retryable() =>
                {
                    Fate::Unknown
                }
                Ok(Err(error)) => {
                    world.record(planned);
                    bail!("commit: {error:?}");
                }
            }
        }
    };
    let ended = world.tick();
    for part in &mut planned {
        part.ended = ended;
        // A part whose append failed wrote nothing whatever the commit did.
        part.fate = match fate {
            _ if part.result.is_none() => Fate::Absent,
            other => other,
        };
        if part.fate == Fate::Absent {
            part.result = None;
        }
    }
    world.record(planned);
    Ok(())
}

async fn delete_one(world: &World, rng: &mut Rng) -> Result<()> {
    let cat = random_cat(rng);
    let stream_id = world.pick(rng, cat);
    let cut = rng.chance(25).then(|| rng.range(0, 3000));
    let started = world.tick();
    let outcome = with_event!(cat, E => {
        let call = world.store.delete_stream::<E>(stream_id, delete::DeleteExpected::Any);
        match cut {
            Some(micros) => tokio::time::timeout(Duration::from_micros(micros), call).await.ok(),
            None => Some(call.await),
        }
    });
    let ended = world.tick();
    let deletion = match outcome {
        None => Some(Deletion {
            stream: stream_id,
            fate: DeleteFate::Unknown,
            tombstone: None,
            started,
            ended,
        }),
        Some(Ok(result)) => Some(Deletion {
            stream: stream_id,
            fate: DeleteFate::Deleted,
            tombstone: Some(acked(&result)),
            started,
            ended,
        }),
        Some(Err(
            delete::DeleteStreamError::NotFound(_) | delete::DeleteStreamError::StreamDeleted(_),
        )) => None,
        Some(Err(delete::DeleteStreamError::Database(error))) if error.is_retryable() => {
            Some(Deletion {
                stream: stream_id,
                fate: DeleteFate::Unknown,
                tombstone: None,
                started,
                ended,
            })
        }
        Some(Err(error)) => bail!("delete {stream_id}: {error:?}"),
    };
    if let Some(deletion) = deletion {
        world.ledger.lock().unwrap().deletions.push(deletion);
        world.replace(cat, stream_id);
    }
    Ok(())
}

async fn snapshot(world: &World, rng: &mut Rng) -> Result<()> {
    let cat = random_cat(rng);
    let stream_id = world.pick(rng, cat);
    with_event!(cat, E => {
        let snapshotted = match world.store.load_snapshotted::<Tally<E>>(stream_id).await {
            Ok(loaded) => loaded,
            Err(pges_postgres::load::LoadError::Read(read::ReadError::StreamDeleted(_))) => {
                return Ok(());
            }
            Err(error) => bail!("load_snapshotted {stream_id}: {error:?}"),
        };
        let replayed = match world.store.load::<Tally<E>>(stream_id).await {
            Ok(loaded) => Some(loaded),
            Err(pges_postgres::load::LoadError::Read(read::ReadError::StreamDeleted(_) | read::ReadError::Truncated { .. })) => None,
            Err(error) => bail!("load {stream_id}: {error:?}"),
        };
        if let Some(replayed) = replayed
            && replayed.version() == snapshotted.version()
        {
            ensure!(
                replayed.state() == snapshotted.state(),
                "stream {stream_id} at {:?}: load_snapshotted {:?}, load {:?}",
                replayed.version(),
                snapshotted.state(),
                replayed.state()
            );
        }
        if let Err(error) = world.store.save_snapshot(&snapshotted).await {
            bail!("save_snapshot {stream_id}: {error:?}");
        }
    });
    Ok(())
}

// --- maintenance -------------------------------------------------------------------------------------------

async fn maintenance(world: Arc<World>, config: retention::RetentionConfig) {
    let mut rng = Rng::new(world.seed, 300);
    let admin = world.store.admin();
    while !world.stop.load(Ordering::SeqCst) {
        tokio::time::sleep(Duration::from_millis(rng.range(50, 250))).await;
        if let Err(error) = admin.maintain().await {
            world.fail(format!("maintain: {error:?}"));
        }
        match admin.apply_retention(&config).await {
            Ok(report) => {
                world.count("retention");
                for leaf in &report.dropped {
                    world.count("dropped_leaf");
                    if leaf.category.as_str() == "jobs" {
                        world.fail(format!(
                            "retention dropped {leaf:?} of a category kept forever"
                        ));
                    }
                }
            }
            Err(error) => world.fail(format!("apply_retention: {error:?}")),
        }
        for cat in CATS {
            let name = with_event!(cat, E => <E as event::Event>::CATEGORY);
            if let Err(error) = admin.prune_log(name).await {
                world.fail(format!("prune_log({name}): {error:?}"));
            }
        }
    }
}

// --- sequencers --------------------------------------------------------------------------------------------

fn sequencer_config(rng: &mut Rng) -> sequencer::SequencerConfig {
    let mut config = sequencer::SequencerConfig::default();
    config.lease = sequencer::Lease::with_renew(interval!(500 ms), interval!(100 ms)).unwrap();
    config.acquire_interval = interval!(20 ms);
    config.retry_interval = interval!(30 ms);
    config.token_interval = Duration::from_millis([0, 30, 200][rng.below(3) as usize]);
    config.batch_size = std::num::NonZeroUsize::new(rng.range(1, 64) as usize).unwrap();
    config
}

/// Every sequencer status change of the case, for a failure's report.
static SEQUENCER_HISTORY: Mutex<Vec<String>> = Mutex::new(Vec::new());
static STARTED_AT: Mutex<Option<std::time::Instant>> = Mutex::new(None);

fn started_at() -> std::time::Instant {
    *STARTED_AT
        .lock()
        .unwrap()
        .get_or_insert_with(std::time::Instant::now)
}

struct Seq {
    name: String,
    watch: pges_sequencer::watch::Watch,
    pool: PgPool,
    stop: oneshot::Sender<()>,
    task: JoinHandle<Result<(), sequencer::RunError>>,
    errors: Arc<Mutex<Vec<String>>>,
}

impl Seq {
    async fn start(
        f: &Fixture,
        kind: Kind,
        name: String,
        config: sequencer::SequencerConfig,
    ) -> Result<Self> {
        SEQUENCER_HISTORY.lock().unwrap().push(format!(
            "{}ms {name} starts: token every {:?}, batches of {}",
            started_at().elapsed().as_millis(),
            config.token_interval,
            config.batch_size
        ));
        let options = PgConnectOptions::from_str(f.db.url())?.application_name(&name);
        let pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(6)
            .connect_with(options)
            .await?;
        let sequencer = match kind {
            Kind::Outbox => {
                let source =
                    outbox::OutboxSource::new(pool.clone(), outbox::OutboxConfig::default());
                Box::new(sequencer::Sequencer::new(pool.clone(), source, config)) as Box<dyn SeqRun>
            }
            Kind::Logical => {
                let replication = pges_replication::config::Config::from_url(f.db.url())?;
                let mut logical = pges_sequencer::logical::LogicalConfig::default();
                logical.start_timeout = interval!(20 s);
                let source =
                    pges_sequencer::logical::LogicalSource::new(pool.clone(), replication, logical);
                Box::new(sequencer::Sequencer::new(pool.clone(), source, config)) as Box<dyn SeqRun>
            }
        };
        let watch = sequencer.watch();
        let errors = Arc::new(Mutex::new(Vec::new()));
        let seen = errors.clone();
        let mut status = watch.status();
        let label = name.clone();
        tokio::spawn(async move {
            loop {
                let now = status.borrow_and_update().clone();
                SEQUENCER_HISTORY.lock().unwrap().push(format!(
                    "{}ms {label} {now:?}",
                    started_at().elapsed().as_millis()
                ));
                if let sequencer::Status::Recovering(error) = &now {
                    seen.lock().unwrap().push(error.to_string());
                }
                if status.changed().await.is_err() {
                    return;
                }
            }
        });
        let (stop, stopped) = oneshot::channel::<()>();
        let task = tokio::spawn(sequencer.run(Box::pin(async {
            let _ = stopped.await;
        })));
        Ok(Self {
            name,
            watch,
            pool,
            stop,
            task,
            errors,
        })
    }

    async fn wait_leading(&self) -> Result<()> {
        let mut status = self.watch.status();
        tokio::time::timeout(
            Duration::from_secs(20),
            status.wait_for(|s| matches!(s, sequencer::Status::Leading(_))),
        )
        .await
        .context("waiting to lead")??;
        Ok(())
    }

    fn errors(&self) -> Vec<String> {
        self.errors
            .lock()
            .unwrap()
            .iter()
            .map(|e| format!("sequencer {}: {e}", self.name))
            .collect()
    }

    async fn stop(self) -> Result<Vec<String>> {
        let errors = self.errors();
        let _ = self.stop.send(());
        tokio::time::timeout(Duration::from_secs(15), self.task)
            .await
            .with_context(|| format!("sequencer {} did not stop", self.name))???;
        self.pool.close().await;
        Ok(errors)
    }

    /// The task is dropped wherever it is and the server ends its sessions, as a process death would.
    async fn crash(self, server: &PgPool) -> Result<Vec<String>> {
        let errors = self.errors();
        self.task.abort();
        let _ = self.task.await;
        sqlx::query(
            "SELECT pg_terminate_backend(pid) FROM pg_stat_activity
             WHERE application_name = $1 AND pid <> pg_backend_pid()",
        )
        .bind(&self.name)
        .execute(server)
        .await?;
        tokio::time::timeout(Duration::from_secs(10), self.pool.close())
            .await
            .context("closing a crashed sequencer's pool")?;
        Ok(errors)
    }
}

/// A sequencer of either source behind one type.
trait SeqRun: Send {
    fn watch(&self) -> pges_sequencer::watch::Watch;
    fn run(
        self: Box<Self>,
        shutdown: std::pin::Pin<Box<dyn Future<Output = ()> + Send>>,
    ) -> std::pin::Pin<Box<dyn Future<Output = Result<(), sequencer::RunError>> + Send>>;
}

impl<S: pges_sequencer::source::Source + 'static> SeqRun for sequencer::Sequencer<S> {
    fn watch(&self) -> pges_sequencer::watch::Watch {
        sequencer::Sequencer::watch(self)
    }

    fn run(
        self: Box<Self>,
        shutdown: std::pin::Pin<Box<dyn Future<Output = ()> + Send>>,
    ) -> std::pin::Pin<Box<dyn Future<Output = Result<(), sequencer::RunError>> + Send>> {
        Box::pin(sequencer::Sequencer::run(*self, shutdown))
    }
}

// --- subscriptions -----------------------------------------------------------------------------------------

/// One delivered record.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Delivery {
    position: i64,
    stream: Uuid,
    /// 0 for a tombstone.
    version: i64,
    commit: Uuid,
    batch: u64,
}

fn deliveries(batch: &batch::Batch, id: u64) -> Vec<Delivery> {
    batch
        .iter()
        .filter_map(|record| match record {
            batch::RawRecorded::Event(raw) => Some(Delivery {
                position: raw.position.get() as i64,
                stream: raw.stream_id.as_uuid(),
                version: raw.version.to_i64(),
                commit: raw.commit_id.as_uuid(),
                batch: id,
            }),
            batch::RawRecorded::System(batch::SystemEvent::StreamDeleted {
                position,
                stream_id,
                ..
            }) => Some(Delivery {
                position: position.get() as i64,
                stream: stream_id.as_uuid(),
                version: 0,
                commit: Uuid::nil(),
                batch: id,
            }),
            _ => None,
        })
        .collect()
}

static BATCHES: AtomicU64 = AtomicU64::new(1);

/// Records into `public.inv_delivered` in the batch's transaction.
#[derive(Clone)]
struct PgRecorder {
    name: &'static str,
    /// Appends a follow-up for each jobs event, in the batch's transaction.
    follow_ups: Option<store::Store>,
}

/// Follow-up events carry the position of the event they follow, above every writer's numbers.
const FOLLOW_UP_BASE: u32 = 1 << 31;

/// The parts streams follow-ups go to, by position.
fn follow_up_streams() -> [stream::StreamId; 3] {
    [1, 2, 3].map(|i| {
        stream::StreamId::from_uuid(Uuid::from_u128(
            0x0190_0000_0000_7000_8000_0000_0000_0000 | i,
        ))
    })
}

impl handler::PgHandler for PgRecorder {
    async fn handle(
        &mut self,
        tx: &mut PgConnection,
        batch: &batch::Batch,
    ) -> Result<(), handler::HandlerError> {
        let id = BATCHES.fetch_add(1, Ordering::SeqCst);
        let records = deliveries(batch, id);
        let mut columns = (Vec::new(), Vec::new(), Vec::new(), Vec::new());
        for d in &records {
            columns.0.push(d.position);
            columns.1.push(d.stream);
            columns.2.push(d.version);
            columns.3.push(d.commit);
        }
        sqlx::query(
            "INSERT INTO public.inv_delivered (subscriber, batch, position, stream_id, version, commit_id)
             SELECT $1, $2, p, s, v, c FROM unnest($3::int8[], $4::uuid[], $5::int8[], $6::uuid[])
               WITH ORDINALITY AS u(p, s, v, c, o) ORDER BY o",
        )
        .bind(self.name)
        .bind(id as i64)
        .bind(&columns.0)
        .bind(&columns.1)
        .bind(&columns.2)
        .bind(&columns.3)
        .execute(&mut *tx)
        .await?;

        if let Some(store) = &self.follow_ups {
            let streams = follow_up_streams();
            for record in batch.iter() {
                let batch::RawRecorded::Event(raw) = record else {
                    continue;
                };
                if raw.category.as_str() != "jobs" {
                    continue;
                }
                let position = raw.position.get();
                let events = [PartEvent::Noted {
                    n: FOLLOW_UP_BASE + position as u32,
                }];
                store
                    .append_in(
                        &mut *tx,
                        streams[(position % 3) as usize],
                        version::ExpectedVersion::Any,
                        &events,
                    )
                    .await
                    .map_err(|error| handler::HandlerError::Fatal(Box::new(error)))?;
            }
        }
        Ok(())
    }
}

/// Records in memory, for `run`: at least once.
#[derive(Clone, Default)]
struct MemRecorder {
    seen: Arc<Mutex<Vec<Delivery>>>,
}

impl handler::Handler for MemRecorder {
    async fn handle(&mut self, batch: &batch::Batch) -> Result<(), handler::HandlerError> {
        let id = BATCHES.fetch_add(1, Ordering::SeqCst);
        self.seen.lock().unwrap().extend(deliveries(batch, id));
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SubKind {
    /// jobs and orders, `run_pg`, one instance.
    Listed,
    /// every category, three shards over two instances, `run_pg`.
    Sharded,
    /// every category, `run`, one instance.
    AtLeastOnce,
    /// jobs and orders up to the head when it first ran, two lanes, cut short and resumed.
    Backfill,
}

impl SubKind {
    fn name(self) -> &'static str {
        match self {
            Self::Listed => "inv-listed",
            Self::Sharded => "inv-sharded",
            Self::AtLeastOnce => "inv-run",
            Self::Backfill => "inv-backfill",
        }
    }

    fn instances(self) -> usize {
        match self {
            Self::Sharded => 2,
            _ => 1,
        }
    }

    fn covers(self, cat: Cat) -> bool {
        !matches!(self, Self::Listed | Self::Backfill) || cat != Cat::Parts
    }
}

fn build(
    store: &store::Store,
    kind: SubKind,
    batch_size: u32,
    memory: &MemRecorder,
) -> subscription::Subscription {
    let name = subscription::SubscriptionName::parse(kind.name()).unwrap();
    let size = NonZeroU32::new(batch_size).unwrap();
    let tune = |b: subscription::Builder<subscription::Listed>| {
        b.batch_size(size)
            .lease(interval!(600 ms))
            .acquire_interval(interval!(20 ms))
            .retry_interval(interval!(20 ms))
            .waker(recheck::Recheck::every(interval!(5 ms)))
    };
    let tune_all = |b: subscription::Builder<subscription::AllCategories>| {
        b.batch_size(size)
            .lease(interval!(600 ms))
            .acquire_interval(interval!(20 ms))
            .retry_interval(interval!(20 ms))
            .waker(recheck::Recheck::every(interval!(5 ms)))
    };
    match kind {
        SubKind::Listed => tune(store.subscription(name).categories(pges_core::non_empty![
            pges_core::category!("jobs"),
            pges_core::category!("orders")
        ]))
        .run_pg(PgRecorder {
            name: kind.name(),
            follow_ups: Some(store.clone()),
        }),
        SubKind::Sharded => tune_all(
            store
                .subscription(name)
                .all_categories()
                .sharded(subscription::Shards::new(3).unwrap()),
        )
        .run_pg(PgRecorder {
            name: kind.name(),
            follow_ups: None,
        }),
        SubKind::AtLeastOnce => {
            tune_all(store.subscription(name).all_categories()).run(memory.clone())
        }
        SubKind::Backfill => unreachable!("a backfill is not a subscription (run_backfill)"),
    }
}

// --- the case ----------------------------------------------------------------------------------------------

async fn run_case(seed: u64, kind: Kind, length: Duration) -> Result<()> {
    let outbox = match kind {
        Kind::Outbox => store::Outbox::Enabled,
        Kind::Logical => store::Outbox::Disabled,
    };
    let f = fixture(outbox).await?;
    let result = run_on(&f, seed, kind, length).await;
    match (result, kind) {
        (Ok(()), Kind::Outbox) => f.finish().await,
        (Ok(()), Kind::Logical) => f.finish_with_slots().await,
        (Err(error), _) => {
            let _ = f.finish_with_slots().await;
            Err(error)
        }
    }
}

/// The shared fixture's store and categories, with monthly leaves from three months back, so commits can be
/// written in the past for retention to drop.
async fn fixture(outbox: store::Outbox) -> Result<Fixture> {
    let db = pges_testing::TestDb::new().await?;
    let mut conn = db.connect().await?;
    pges_testing::create_store(
        &mut conn,
        &common::STORE_NAME,
        &pges_testing::definition(outbox),
    )
    .await?;
    let mut ids = BTreeMap::new();
    for cat in CATS {
        let name = with_event!(cat, E => <E as event::Event>::CATEGORY);
        let id =
            pges_testing::register_category(&mut conn, &common::STORE_NAME, name, 3, 2).await?;
        ids.insert(name.as_str().to_owned(), id);
    }
    let pool = db.pool().await?;
    pges_postgres::admin::Admin::new(pool.clone(), common::STORE_NAME)
        .maintain()
        .await?;
    let store =
        store::Store::open(pool.clone(), store::StoreConfig::new(common::STORE_NAME)).await?;
    store.register::<JobEvent>().await?;
    store.register::<OrderEvent>().await?;
    store.register::<PartEvent>().await?;

    Ok(Fixture {
        db,
        pool,
        store,
        ids,
    })
}

const DAY: Duration = Duration::from_secs(86_400);

/// A commit id `days` days back, plus `millis`.
fn id_days_back(days: u64, millis: u64) -> commit::CommitId {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap();
    let at = now - DAY * days as u32 + Duration::from_millis(millis);
    let ts = uuid::Timestamp::from_unix(uuid::NoContext, at.as_secs(), at.subsec_nanos());
    commit::CommitId::from_uuid(Uuid::new_v7(ts)).unwrap()
}

/// Writes commits about two months back, through a store opened with a skew that allows them: one to each
/// jobs stream (kept forever), one or two to each parts stream with a snapshot after them (snapshot and
/// truncate), and to two orders streams no one writes to again (window). Returns them as acknowledged.
async fn write_history(
    store: &store::Store,
    slots: &[Vec<stream::StreamId>],
    next_n: &AtomicU32,
    tick: &AtomicU64,
) -> Result<(Vec<Planned>, Vec<(stream::StreamId, Cat)>)> {
    let mut planned = Vec::new();
    let mut streams = Vec::new();
    let mut millis = 0;
    let mut targets: Vec<(Cat, stream::StreamId, usize)> = Vec::new();
    for stream_id in &slots[cat_index(Cat::Jobs)] {
        targets.push((Cat::Jobs, *stream_id, 1));
    }
    for (i, stream_id) in slots[cat_index(Cat::Parts)].iter().enumerate() {
        targets.push((Cat::Parts, *stream_id, 1 + i % 2));
    }
    for _ in 0..2 {
        let stream_id = stream::StreamId::new_v7();
        streams.push((stream_id, Cat::Orders));
        targets.push((Cat::Orders, stream_id, 2));
    }
    for (cat, stream_id, commits) in targets {
        for _ in 0..commits {
            millis += 1;
            let first = next_n.fetch_add(2, Ordering::SeqCst);
            let ns = vec![first, first + 1];
            let started = tick.fetch_add(1, Ordering::SeqCst);
            let result = with_event!(cat, E => {
                let events: Vec<E> = ns.iter().map(|n| E::with(*n)).collect();
                store
                    .append(stream_id, version::ExpectedVersion::Any, &events)
                    .commit_id(id_days_back(62, millis))
                    .await
                    .with_context(|| format!("writing history to {stream_id}"))?
            });
            planned.push(Planned {
                stream: stream_id,
                cat,
                ns,
                fate: Fate::Acked,
                result: Some(acked(&result)),
                started,
                ended: tick.fetch_add(1, Ordering::SeqCst),
                old: true,
            });
        }
        if cat == Cat::Parts {
            let loaded = store
                .load_snapshotted::<Tally<PartEvent>>(stream_id)
                .await?;
            store.save_snapshot(&loaded).await?;
        }
    }
    Ok((planned, streams))
}

async fn run_on(f: &Fixture, seed: u64, kind: Kind, length: Duration) -> Result<()> {
    let mut rng = Rng::new(seed, 0);
    *STARTED_AT.lock().unwrap() = Some(std::time::Instant::now());
    SEQUENCER_HISTORY.lock().unwrap().clear();
    let mut errors: Vec<String> = Vec::new();

    f.pool
        .execute(
            "CREATE TABLE public.inv_delivered (
               seq        int8 GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
               subscriber text NOT NULL,
               batch      int8 NOT NULL,
               position   int8 NOT NULL,
               stream_id  uuid NOT NULL,
               version    int8 NOT NULL,
               commit_id  uuid NOT NULL
             )",
        )
        .await?;
    // Retention requires the writers' transactions to be capped at the store's max_txn_age.
    f.pool
        .execute(sqlx::AssertSqlSafe(format!(
            "ALTER ROLE CURRENT_USER IN DATABASE {} SET transaction_timeout = '1h'",
            f.db.name()
        )))
        .await?;
    let role: String = sqlx::query_scalar("SELECT current_user::text")
        .fetch_one(&f.pool)
        .await?;
    let retention_config = retention::RetentionConfig::new(limit::WriterRoles::one(role));
    let admin = f.store.admin();
    admin
        .set_retention(
            pges_core::category!("orders"),
            retention::Retention::Window {
                keep: Duration::from_secs(20 * 86_400),
                then: retention::AfterWindow::Drop,
            },
        )
        .await?;
    admin
        .set_retention(
            pges_core::category!("parts"),
            retention::Retention::SnapshotAndTruncate {
                keep: Duration::from_secs(20 * 86_400),
            },
        )
        .await?;

    let mut seq_count = 0;
    let mut next_seq_name = || {
        seq_count += 1;
        format!("inv_seq_{seq_count}")
    };
    let mut sequencers: Vec<Seq> = Vec::new();
    let first = Seq::start(f, kind, next_seq_name(), sequencer_config(&mut rng)).await?;
    // The logical source sees only commits after its slot exists.
    first.wait_leading().await?;
    sequencers.push(first);

    // History months back, written while the store allows that skew, which then goes back to the default.
    let slots: Vec<Vec<stream::StreamId>> = CATS
        .iter()
        .map(|_| (0..SLOTS).map(|_| stream::StreamId::new_v7()).collect())
        .collect();
    let next_n = AtomicU32::new(1);
    let tick = AtomicU64::new(1);
    admin
        .set_writer_limits(limit::WriterLimits::new(
            100 * DAY,
            interval!(pges_core::store::DEFAULT_MAX_TXN_AGE),
        ))
        .await?;
    let history_pool = f.db.pool().await?;
    let mut history_config = store::StoreConfig::new(common::STORE_NAME);
    history_config.max_clock_skew = Some(100 * DAY);
    let history_store = store::Store::open(history_pool.clone(), history_config).await?;
    let (history, history_streams) = write_history(&history_store, &slots, &next_n, &tick).await?;
    history_pool.close().await;
    sqlx::query("UPDATE pges.stores SET max_clock_skew = $1 WHERE schema_name = $2")
        .bind(sqlx::postgres::types::PgInterval {
            months: 0,
            days: 0,
            microseconds: pges_core::store::DEFAULT_MAX_CLOCK_SKEW.as_micros() as i64,
        })
        .bind(common::STORE)
        .execute(&f.pool)
        .await?;

    // The writers' store, on a pool of its own.
    let writer_pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(16)
        .connect(f.db.url())
        .await?;
    let store = store::Store::open(
        writer_pool.clone(),
        store::StoreConfig::new(common::STORE_NAME),
    )
    .await?;

    let world = Arc::new(World {
        seed,
        store,
        slots: Mutex::new(slots),
        ledger: Mutex::new(Ledger::default()),
        next_n,
        tick,
        stop: AtomicBool::new(false),
    });
    {
        let slots = world.slots.lock().unwrap();
        let mut ledger = world.ledger.lock().unwrap();
        for (i, cat) in CATS.iter().enumerate() {
            for stream_id in &slots[i] {
                ledger.streams.insert(*stream_id, *cat);
            }
        }
        for (stream_id, cat) in history_streams {
            ledger.streams.insert(stream_id, cat);
        }
        ledger.commits.extend(history);
    }

    let subs = [SubKind::Listed, SubKind::Sharded, SubKind::AtLeastOnce];
    let batch_size = rng.range(1, 9) as u32;
    let memory = MemRecorder::default();
    let mut instances: Vec<Vec<common::Crashable>> = Vec::new();
    for sub in subs {
        let mut list = Vec::new();
        for _ in 0..sub.instances() {
            let memory = memory.clone();
            list.push(common::Crashable::spawn(f, |s| build(s, sub, batch_size, &memory)).await?);
        }
        instances.push(list);
    }

    // Every subscription passes the history before retention may drop it, so none falls behind a horizon.
    wait_all_sequenced(&f.pool, kind).await?;
    let history_head = common::head(&f.pool).await? as i64;
    for sub in subs {
        wait_caught_up(&f.pool, sub.name(), history_head).await?;
    }
    let history_log = read_log(&f.pool).await?;

    let mut tasks = Vec::new();
    for index in 0..WRITERS {
        tasks.push(tokio::spawn(writer(world.clone(), index)));
    }
    let maintenance_task = tokio::spawn(maintenance(world.clone(), retention_config));
    let backfill_task = {
        let url = f.db.url().to_owned();
        let delay = Duration::from_millis(rng.range(0, length.as_millis() as u64 / 2 + 1));
        tokio::spawn(async move {
            tokio::time::sleep(delay).await;
            run_backfill(&url, seed).await
        })
    };

    // Chaos: crash, stop and start sequencers and subscription instances.
    let deadline = tokio::time::Instant::now() + length;
    let mut chaos_log: Vec<String> = Vec::new();
    let begun = started_at();
    while tokio::time::Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(rng.range(100, 400))).await;
        match rng.below(10) {
            0..=1 if !sequencers.is_empty() => {
                let i = rng.below(sequencers.len() as u64) as usize;
                let s = sequencers.remove(i);
                chaos_log.push(format!(
                    "{}ms crash {}",
                    begun.elapsed().as_millis(),
                    s.name
                ));
                errors.extend(s.crash(&f.pool).await?);
            }
            2 if !sequencers.is_empty() => {
                let i = rng.below(sequencers.len() as u64) as usize;
                let s = sequencers.remove(i);
                chaos_log.push(format!("{}ms stop {}", begun.elapsed().as_millis(), s.name));
                errors.extend(s.stop().await?);
            }
            3..=4 if sequencers.len() < 2 => {
                let s = Seq::start(f, kind, next_seq_name(), sequencer_config(&mut rng)).await?;
                chaos_log.push(format!(
                    "{}ms start {}",
                    begun.elapsed().as_millis(),
                    s.name
                ));
                sequencers.push(s);
            }
            5..=7 => {
                let k = rng.below(subs.len() as u64) as usize;
                let i = rng.below(instances[k].len() as u64) as usize;
                let old = instances[k].remove(i);
                chaos_log.push(format!(
                    "{}ms crash {} instance",
                    begun.elapsed().as_millis(),
                    subs[k].name()
                ));
                errors.extend(old.crash().await);
                let memory = memory.clone();
                let sub = subs[k];
                instances[k].push(
                    common::Crashable::spawn(f, |s| build(s, sub, batch_size, &memory)).await?,
                );
            }
            _ => {}
        }
        if sequencers.is_empty() {
            let s = Seq::start(f, kind, next_seq_name(), sequencer_config(&mut rng)).await?;
            chaos_log.push(format!(
                "{}ms start {}",
                begun.elapsed().as_millis(),
                s.name
            ));
            sequencers.push(s);
        }
    }

    world.stop.store(true, Ordering::SeqCst);
    for task in tasks {
        task.await?;
    }
    maintenance_task.await?;
    let backfill_until = match backfill_task.await? {
        Ok((until, attempts)) => {
            world
                .ledger
                .lock()
                .unwrap()
                .counts
                .insert("backfill_attempts", attempts);
            Some(until)
        }
        Err(error) => {
            world.fail(format!("backfill: {error:#}"));
            None
        }
    };

    // Quiesce: everything committed gets sequenced and delivered.
    // The listed subscription's follow-ups are new commits, so go round until the head stays put.
    let mut head = -1;
    loop {
        wait_all_sequenced(&f.pool, kind).await?;
        let now = common::head(&f.pool).await? as i64;
        if now == head {
            break;
        }
        head = now;
        for sub in subs {
            wait_caught_up(&f.pool, sub.name(), head).await?;
        }
    }
    for list in instances {
        for instance in list {
            errors.extend(instance.errors());
            errors.extend(instance.crash().await);
        }
    }
    for s in sequencers {
        errors.extend(s.stop().await?);
    }

    let ledger = std::mem::take(&mut *world.ledger.lock().unwrap());
    eprintln!(
        "invariants: seed {seed}: {} commits planned, {} deletions, log head {head}, ops {:?}; chaos: {}",
        ledger.commits.len(),
        ledger.deletions.len(),
        ledger.counts,
        chaos_log.join(", ")
    );
    let mut failures = ledger.failures.clone();
    failures.extend(
        errors
            .into_iter()
            .map(|e| format!("recovered from an error: {e}")),
    );
    let checked = check(
        f,
        &world.store,
        &ledger,
        &memory,
        &subs,
        &history_log,
        backfill_until,
    )
    .await;
    if let Err(error) = checked {
        failures.push(format!("{error:#}"));
    }
    writer_pool.close().await;
    if !failures.is_empty() {
        eprintln!(
            "invariants: seed {seed}: sequencer history:\n  {}",
            SEQUENCER_HISTORY.lock().unwrap().join("\n  ")
        );
        bail!("{} failures:\n  {}", failures.len(), failures.join("\n  "));
    }
    Ok(())
}

/// Runs the backfill to the end, cutting the first runs short by dropping them, as a crash would; each run
/// is on a pool of its own, closed after it. Returns where it ended and how many runs it took.
async fn run_backfill(url: &str, seed: u64) -> Result<(i64, u64)> {
    let mut rng = Rng::new(seed, 400);
    let batch_size = NonZeroU32::new(rng.range(1, 9) as u32).unwrap();
    let name = subscription::SubscriptionName::parse(SubKind::Backfill.name()).unwrap();
    let mut attempts = 0;
    loop {
        attempts += 1;
        let pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(4)
            .connect(url)
            .await?;
        let store =
            store::Store::open(pool.clone(), store::StoreConfig::new(common::STORE_NAME)).await?;
        let run = store
            .backfill(name.clone(), backfill::Until::Head)
            .categories(pges_core::non_empty![
                pges_core::category!("jobs"),
                pges_core::category!("orders")
            ])
            .parallelism(backfill::Lanes::new(2).unwrap())
            .batch_size(batch_size)
            .run_pg(PgRecorder {
                name: SubKind::Backfill.name(),
                follow_ups: None,
            });
        let outcome = if attempts < 6 && rng.chance(85) {
            let cut = Duration::from_millis(rng.range(1, 60));
            tokio::time::timeout(cut, run).await.ok()
        } else {
            Some(run.await)
        };
        pool.close().await;
        match outcome {
            None => {}
            Some(Ok(finished)) => return Ok((finished.until.get() as i64, attempts)),
            Some(Err(error)) => bail!("run {attempts}: {error:?}"),
        }
    }
}

async fn wait_all_sequenced(pool: &PgPool, kind: Kind) -> Result<()> {
    let outbox_pending = if kind == Kind::Outbox {
        "(SELECT count(*) FROM es.pending)"
    } else {
        "0::int8"
    };
    let sql = format!(
        "SELECT (SELECT count(*) FROM es.commits c
                  WHERE NOT EXISTS (SELECT 1 FROM es.log l WHERE l.commit_id = c.commit_id))
              + (SELECT count(*) FROM es.deleted_commits d
                  WHERE NOT EXISTS (SELECT 1 FROM es.log l WHERE l.commit_id = d.commit_id))
              + {outbox_pending}"
    );
    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            let missing: i64 = sqlx::query_scalar(sqlx::AssertSqlSafe(sql.clone()))
                .fetch_one(pool)
                .await?;
            if missing == 0 {
                return anyhow::Ok(());
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .context("waiting for every commit to be sequenced")?
}

async fn wait_caught_up(pool: &PgPool, name: &str, head: i64) -> Result<()> {
    let reached = tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            let lowest: Option<i64> = sqlx::query_scalar(
                "SELECT min(position) FROM es.subscriptions WHERE name = $1 AND shard < shards",
            )
            .bind(name)
            .fetch_one(pool)
            .await?;
            if lowest.is_some_and(|p| p >= head) {
                return anyhow::Ok(());
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await;
    match reached {
        Ok(result) => result,
        Err(_) => {
            let rows: Vec<(i16, i64)> = sqlx::query_as(
                "SELECT shard, position FROM es.subscriptions WHERE name = $1 ORDER BY shard",
            )
            .bind(name)
            .fetch_all(pool)
            .await?;
            bail!("{name} did not reach {head}: {rows:?}")
        }
    }
}

// --- the checks --------------------------------------------------------------------------------------------

#[derive(Debug, Clone, Copy)]
struct LogRow {
    last_position: i64,
    commit_id: Uuid,
    category_id: i32,
    event_count: i16,
}

async fn read_log(pool: &PgPool) -> Result<Vec<LogRow>> {
    Ok(sqlx::query_as::<_, (i64, Uuid, i32, i16)>(
        "SELECT last_position, commit_id, category_id, event_count FROM es.log ORDER BY last_position",
    )
    .fetch_all(pool)
    .await?
    .into_iter()
    .map(|(last_position, commit_id, category_id, event_count)| LogRow {
        last_position,
        commit_id,
        category_id,
        event_count,
    })
    .collect())
}

async fn check(
    f: &Fixture,
    store: &store::Store,
    ledger: &Ledger,
    memory: &MemRecorder,
    subs: &[SubKind],
    history_log: &[LogRow],
    backfill_until: Option<i64>,
) -> Result<()> {
    let mut problems: Vec<String> = Vec::new();

    // Commits and deleted commits.
    let commits: Vec<(Uuid, Uuid, i64, i16, i32)> = sqlx::query_as(
        "SELECT commit_id, stream_id, last_version, event_count, category_id FROM es.commits",
    )
    .fetch_all(&f.pool)
    .await?;
    let commit_rows: HashMap<Uuid, (Uuid, i64, i16, i32)> = commits
        .iter()
        .map(|(c, s, v, n, k)| (*c, (*s, *v, *n, *k)))
        .collect();
    // History retention may drop: not jobs, which keeps everything.
    let droppable: BTreeSet<Uuid> = ledger
        .commits
        .iter()
        .filter(|p| p.old && p.cat != Cat::Jobs)
        .filter_map(|p| p.result.map(|r| r.commit_id.as_uuid()))
        .collect();
    let history: BTreeSet<Uuid> = ledger
        .commits
        .iter()
        .filter(|p| p.old)
        .filter_map(|p| p.result.map(|r| r.commit_id.as_uuid()))
        .collect();

    // The log: gap-free, each commit once. prune_log may have removed the rows of commits retention
    // dropped, so the rows read before retention ran fill in for them.
    let final_log = read_log(&f.pool).await?;
    let final_positions: HashMap<i64, LogRow> =
        final_log.iter().map(|r| (r.last_position, *r)).collect();
    let mut log = final_log.clone();
    let mut pruned = 0;
    for row in history_log {
        match final_positions.get(&row.last_position) {
            Some(now) if now.commit_id == row.commit_id && now.event_count == row.event_count => {}
            Some(now) => problems.push(format!("log row {row:?} became {now:?}")),
            None if droppable.contains(&row.commit_id)
                && !commit_rows.contains_key(&row.commit_id) =>
            {
                pruned += 1;
                log.push(*row);
            }
            None => problems.push(format!(
                "log row {row:?} is gone, but its commit was not dropped"
            )),
        }
    }
    log.sort_by_key(|r| r.last_position);
    let dropped_history = droppable
        .iter()
        .filter(|c| !commit_rows.contains_key(c))
        .count();
    eprintln!(
        "invariants: retention dropped {dropped_history} of {} droppable history commits; prune_log removed \
         {pruned} log rows",
        droppable.len()
    );
    let mut previous = 0_i64;
    let mut by_commit: HashMap<Uuid, LogRow> = HashMap::new();
    for row in &log {
        if row.last_position - i64::from(row.event_count) != previous {
            problems.push(format!(
                "log gap or overlap: row ending at {} with {} events after {previous}",
                row.last_position, row.event_count
            ));
        }
        previous = row.last_position;
        if let Some(earlier) = by_commit.insert(row.commit_id, *row) {
            problems.push(format!(
                "commit {} is in the log twice: at {} and {} ({} events)",
                row.commit_id, earlier.last_position, row.last_position, row.event_count
            ));
        }
    }

    let deleted_commits: BTreeSet<Uuid> =
        sqlx::query_scalar::<_, Uuid>("SELECT commit_id FROM es.deleted_commits")
            .fetch_all(&f.pool)
            .await?
            .into_iter()
            .collect();
    for (commit_id, (_, _, count, category)) in &commit_rows {
        match by_commit.get(commit_id) {
            None => problems.push(format!("commit {commit_id} is not in the log")),
            Some(row) if row.event_count != *count || row.category_id != *category => {
                problems.push(format!("commit {commit_id}'s log row differs: {row:?}"))
            }
            Some(_) => {}
        }
    }
    for row in &log {
        if !commit_rows.contains_key(&row.commit_id)
            && !deleted_commits.contains(&row.commit_id)
            && !droppable.contains(&row.commit_id)
        {
            problems.push(format!("log row {row:?} names no commit"));
        }
    }
    for commit_id in &history {
        if !droppable.contains(commit_id)
            && !commit_rows.contains_key(commit_id)
            && !deleted_commits.contains(commit_id)
        {
            problems.push(format!(
                "history commit {commit_id} of a kept category is gone"
            ));
        }
    }

    // Stream states.
    let stream_rows: HashMap<Uuid, (i64, i64, i16)> = sqlx::query_as::<_, (Uuid, i64, i64, i16)>(
        "SELECT stream_id, version, floor_version, state FROM es.streams",
    )
    .fetch_all(&f.pool)
    .await?
    .into_iter()
    .map(|(s, v, floor, state)| (s, (v, floor, state)))
    .collect();
    let deleted: BTreeSet<Uuid> = stream_rows
        .iter()
        .filter(|(_, (_, _, state))| *state == 2)
        .map(|(s, _)| *s)
        .collect();
    let snapshots: HashMap<Uuid, (i64, Vec<u8>)> = sqlx::query_as::<_, (Uuid, i64, Vec<u8>)>(
        "SELECT stream_id, version, state FROM es.snapshots",
    )
    .fetch_all(&f.pool)
    .await?
    .into_iter()
    .map(|(s, v, state)| (s, (v, state)))
    .collect();

    // Deletions.
    for deletion in &ledger.deletions {
        let id = deletion.stream.as_uuid();
        let is_deleted = deleted.contains(&id);
        if deletion.fate == DeleteFate::Deleted && !is_deleted {
            problems.push(format!("acknowledged delete of {id} did not happen"));
        }
        if let Some(tombstone) = deletion.tombstone
            && !by_commit.contains_key(&tombstone.commit_id.as_uuid())
        {
            problems.push(format!("tombstone of {id} is not in the log"));
        }
    }
    let targeted: BTreeSet<Uuid> = ledger
        .deletions
        .iter()
        .map(|d| d.stream.as_uuid())
        .collect();
    for id in &deleted {
        if !targeted.contains(id) {
            problems.push(format!("stream {id} is deleted but no writer deleted it"));
        }
        if snapshots.contains_key(id) {
            problems.push(format!("deleted stream {id} still has a snapshot"));
        }
        let left: Vec<_> = commits
            .iter()
            .filter(|(_, s, _, _, _)| s == id)
            .map(|(c, _, v, n, _)| (*c, *v, *n))
            .collect();
        // Only the tombstone remains.
        if left.len() != 1 {
            problems.push(format!("deleted stream {id} still has commits {left:?}"));
        }
    }

    // Commits planned per stream.
    let mut planned_by_stream: BTreeMap<Uuid, Vec<&Planned>> = BTreeMap::new();
    for planned in &ledger.commits {
        planned_by_stream
            .entry(planned.stream.as_uuid())
            .or_default()
            .push(planned);
    }
    // Every acknowledged commit, deleted or not, is in the log exactly once (checked above for duplicates).
    for planned in &ledger.commits {
        if let (Fate::Acked, Some(result)) = (planned.fate, planned.result)
            && !by_commit.contains_key(&result.commit_id.as_uuid())
        {
            problems.push(format!(
                "acknowledged commit {} of {} ({:?}, {:?}) is not in the log",
                result.commit_id, planned.stream, planned.cat, planned.ns
            ));
        }
    }

    // Surviving streams: their events against the ledger, and loads against the fold.
    for (stream_id, cat) in &ledger.streams {
        let id = stream_id.as_uuid();
        if deleted.contains(&id) {
            let read = with_event!(*cat, E => read_all::<E>(store, *stream_id, version::Version::FIRST).await);
            if !matches!(read, Err(read::ReadError::StreamDeleted(_))) {
                problems.push(format!("reading deleted stream {id} gave {read:?}"));
            }
            // Acknowledged appends to it are gone, and none came after its tombstone.
            let tombstone = ledger
                .deletions
                .iter()
                .filter(|d| d.stream == *stream_id)
                .find_map(|d| d.tombstone);
            for planned in planned_by_stream.get(&id).into_iter().flatten() {
                if let (Fate::Acked, Some(result)) = (planned.fate, planned.result) {
                    if commit_rows.contains_key(&result.commit_id.as_uuid()) {
                        problems.push(format!(
                            "deleted stream {id} kept commit {}",
                            result.commit_id
                        ));
                    }
                    if let Some(t) = tombstone
                        && result.first > t.first
                    {
                        problems.push(format!(
                            "commit {} of deleted stream {id} at {} came after its tombstone at {}",
                            result.commit_id, result.first, t.first
                        ));
                    }
                }
            }
            continue;
        }
        let empty = Vec::new();
        let planned = planned_by_stream.get(&id).unwrap_or(&empty);
        let head = stream_rows.get(&id).copied();
        let floor = head.map_or(0, |(_, floor, _)| floor);
        // Below the floor, retention dropped the commits: only history, and the ledger says what they held.
        let mut truncated: Vec<&&Planned> = planned
            .iter()
            .filter(|p| p.result.is_some_and(|r| r.last <= floor))
            .collect();
        truncated.sort_by_key(|p| p.result.map(|r| r.first));
        if let Some(p) = truncated.iter().find(|p| !p.old || p.cat == Cat::Jobs) {
            problems.push(format!(
                "stream {id} was truncated to {floor} over a commit retention keeps: {:?}",
                p.result
            ));
        }
        let from = version::Version::from_i64(floor + 1).unwrap();
        let events = match with_event!(*cat, E => read_all::<E>(store, *stream_id, from).await) {
            Ok(events) => events,
            Err(error) => {
                problems.push(format!("reading stream {id} from {from}: {error:?}"));
                continue;
            }
        };
        let mut all_ns: Vec<u32> = truncated
            .iter()
            .flat_map(|p| p.ns.iter().copied())
            .collect();
        if all_ns.len() as i64 != floor {
            problems.push(format!(
                "stream {id} has floor {floor}, but the dropped history held {} events",
                all_ns.len()
            ));
        }
        all_ns.extend(events.iter().map(|e| e.2));
        if let Err(error) = check_stream(
            *stream_id,
            floor,
            &events,
            &all_ns,
            planned,
            &commit_rows,
            &by_commit,
            head,
            snapshots.get(&id),
        ) {
            problems.push(format!("{error:#}"));
        }
        let fold = Tally::<()>::of(&all_ns);
        let loads = with_event!(*cat, E => {
            let loaded = store.load::<Tally<E>>(*stream_id).await;
            let snapshotted = store.load_snapshotted::<Tally<E>>(*stream_id).await;
            let as_unit = |t: &Tally<E>| Tally::<()> { count: t.count, sum: t.sum, hash: t.hash, events: PhantomData };
            (
                loaded.map(|l| (l.version().map(|v| v.to_i64()), as_unit(l.state()))),
                snapshotted.map(|l| (l.version().map(|v| v.to_i64()), as_unit(l.state()))),
            )
        });
        let last = (!all_ns.is_empty()).then_some(all_ns.len() as i64);
        let want = (last, fold);
        let truncated_error = |r: &Result<_, pges_postgres::load::LoadError>| {
            matches!(
                r,
                Err(pges_postgres::load::LoadError::Read(
                    read::ReadError::Truncated { .. }
                ))
            )
        };
        match loads {
            (loaded, snapshotted) if floor > 0 => {
                if !truncated_error(&loaded) {
                    problems.push(format!(
                        "load of {id} below its floor {floor} gave {loaded:?}"
                    ));
                }
                // Snapshot and truncate keeps a snapshot at or above the floor; a window drops whole
                // streams, which then have none.
                match snapshotted {
                    Ok(snapshotted) if snapshotted != want => problems.push(format!(
                        "load_snapshotted of {id} is {snapshotted:?}, the fold of its history {want:?}"
                    )),
                    Ok(_) => {}
                    error if *cat == Cat::Orders && truncated_error(&error) => {}
                    error => problems.push(format!("load_snapshotted of {id}: {error:?}")),
                }
            }
            (Ok(loaded), Ok(snapshotted)) => {
                if loaded != want {
                    problems.push(format!(
                        "load of {id} is {loaded:?}, the fold of read_stream {want:?}"
                    ));
                }
                if snapshotted != want {
                    problems.push(format!(
                        "load_snapshotted of {id} is {snapshotted:?}, the fold of read_stream {want:?}"
                    ));
                }
            }
            other => problems.push(format!("loading {id}: {other:?}")),
        }
    }
    let follow_ups: BTreeSet<Uuid> = follow_up_streams().iter().map(|s| s.as_uuid()).collect();
    for id in stream_rows.keys() {
        if !follow_ups.contains(id) && !ledger.streams.keys().any(|s| s.as_uuid() == *id) {
            problems.push(format!("stream {id} exists but no writer used it"));
        }
    }

    // Follow-ups: one for each jobs event the listed subscription handled, committed with its batch.
    let mut follow_up_ns: Vec<u32> = Vec::new();
    for stream_id in follow_up_streams() {
        let id = stream_id.as_uuid();
        let events = match read_all::<PartEvent>(store, stream_id, version::Version::FIRST).await {
            Ok(events) => events,
            Err(error) => {
                problems.push(format!("reading follow-up stream {id}: {error:?}"));
                continue;
            }
        };
        if let Some((i, (v, _, _))) = events
            .iter()
            .enumerate()
            .find(|(i, (v, _, _))| *v != *i as i64 + 1)
        {
            problems.push(format!("follow-up stream {id}: event {i} has version {v}"));
        }
        let ns: Vec<u32> = events.iter().map(|e| e.2).collect();
        match store.load_snapshotted::<Tally<PartEvent>>(stream_id).await {
            Ok(loaded) => {
                let state = loaded.state();
                let fold = Tally::<()>::of(&ns);
                if (state.count, state.sum, state.hash) != (fold.count, fold.sum, fold.hash) {
                    problems.push(format!("follow-up stream {id}: load differs from the fold"));
                }
            }
            Err(error) => problems.push(format!("loading follow-up stream {id}: {error:?}")),
        }
        follow_up_ns.extend(ns);
    }
    follow_up_ns.sort_unstable();
    let jobs: BTreeSet<Uuid> = ledger
        .streams
        .iter()
        .filter(|(_, c)| **c == Cat::Jobs)
        .map(|(s, _)| s.as_uuid())
        .collect();
    let mut handled: Vec<u32> = sqlx::query_as::<_, (i64, Uuid, i64)>(
        "SELECT position, stream_id, version FROM public.inv_delivered WHERE subscriber = $1",
    )
    .bind(SubKind::Listed.name())
    .fetch_all(&f.pool)
    .await?
    .into_iter()
    .filter(|(_, s, v)| *v > 0 && jobs.contains(s))
    .map(|(p, _, _)| FOLLOW_UP_BASE + p as u32)
    .collect();
    handled.sort_unstable();
    if follow_up_ns != handled {
        let have: BTreeSet<u32> = follow_up_ns.iter().copied().collect();
        let want: BTreeSet<u32> = handled.iter().copied().collect();
        problems.push(format!(
            "follow-ups differ from the handled jobs events: {} follow-ups for {} events; without one {:?}, \
             unhandled {:?}",
            follow_up_ns.len(),
            handled.len(),
            want.difference(&have).take(10).map(|n| n - FOLLOW_UP_BASE).collect::<Vec<_>>(),
            have.difference(&want).take(10).map(|n| n - FOLLOW_UP_BASE).collect::<Vec<_>>(),
        ));
    }

    // Commit order: an append that started after another returned comes later in the log.
    let mut acked: Vec<(u64, u64, i64, Uuid)> = Vec::new();
    for planned in &ledger.commits {
        if let (Fate::Acked, Some(result)) = (planned.fate, planned.result)
            && let Some(row) = by_commit.get(&result.commit_id.as_uuid())
        {
            acked.push((
                planned.started,
                planned.ended,
                row.last_position,
                row.commit_id,
            ));
        }
    }
    for deletion in &ledger.deletions {
        if let Some(t) = deletion.tombstone
            && let Some(row) = by_commit.get(&t.commit_id.as_uuid())
        {
            acked.push((
                deletion.started,
                deletion.ended,
                row.last_position,
                row.commit_id,
            ));
        }
    }
    let mut by_end = acked.clone();
    by_end.sort_by_key(|a| a.1);
    let mut by_start = acked;
    by_start.sort_by_key(|a| a.0);
    let mut i = 0;
    let mut latest: Option<(i64, Uuid)> = None;
    for b in &by_start {
        while i < by_end.len() && by_end[i].1 < b.0 {
            if latest.is_none_or(|(p, _)| by_end[i].2 > p) {
                latest = Some((by_end[i].2, by_end[i].3));
            }
            i += 1;
        }
        if let Some((p, c)) = latest
            && p >= b.2
        {
            problems.push(format!(
                "commit {} at {} started after commit {c} at {p} returned, but comes before it",
                b.3, b.2
            ));
        }
    }

    // Subscriptions.
    let category_ids: HashMap<i32, Cat> = CATS.iter().map(|c| (f.id(*c).get(), *c)).collect();
    let mut checked: Vec<(SubKind, Option<i64>)> = subs.iter().map(|s| (*s, None)).collect();
    match backfill_until {
        Some(until) => checked.push((SubKind::Backfill, Some(until))),
        None => problems.push("the backfill did not finish".to_owned()),
    }
    for (sub, until) in &checked {
        let delivered: Vec<Delivery> = match sub {
            SubKind::AtLeastOnce => memory.seen.lock().unwrap().clone(),
            _ => sqlx::query_as::<_, (i64, Uuid, i64, Uuid, i64)>(
                "SELECT position, stream_id, version, commit_id, batch FROM public.inv_delivered
                 WHERE subscriber = $1 ORDER BY seq",
            )
            .bind(sub.name())
            .fetch_all(&f.pool)
            .await?
            .into_iter()
            .map(|(position, stream, version, commit, batch)| Delivery {
                position,
                stream,
                version,
                commit,
                batch: batch as u64,
            })
            .collect(),
        };
        if let Err(error) = check_subscription(
            *sub,
            &delivered,
            &log,
            &commit_rows,
            &category_ids,
            &deleted,
            ledger,
            *until,
        ) {
            problems.push(format!("{}: {error:#}", sub.name()));
        }
    }

    if problems.len() > 40 {
        let more = problems.len() - 40;
        problems.truncate(40);
        problems.push(format!("... and {more} more"));
    }
    ensure!(problems.is_empty(), "{}", problems.join("\n  "));
    Ok(())
}

/// (version, commit, n) of each event of a stream.
async fn read_all<E: Noted>(
    store: &store::Store,
    stream_id: stream::StreamId,
    from: version::Version,
) -> Result<Vec<(i64, Uuid, u32)>, read::ReadError> {
    let mut events = Vec::new();
    let mut stream = std::pin::pin!(store.read_stream::<E>(stream_id, from));
    while let Some(event) = stream.next().await {
        let event = event?;
        events.push((
            event.version.to_i64(),
            event.commit_id.as_uuid(),
            event.event.n(),
        ));
    }
    Ok(events)
}

/// `events` are the stream's events above `floor`; `all_ns` the numbers of every event since the first,
/// those below the floor taken from the ledger.
#[allow(clippy::too_many_arguments)]
fn check_stream(
    stream_id: stream::StreamId,
    floor: i64,
    events: &[(i64, Uuid, u32)],
    all_ns: &[u32],
    planned: &[&Planned],
    commit_rows: &HashMap<Uuid, (Uuid, i64, i16, i32)>,
    by_commit: &HashMap<Uuid, LogRow>,
    head: Option<(i64, i64, i16)>,
    snapshot: Option<&(i64, Vec<u8>)>,
) -> Result<()> {
    let id = stream_id.as_uuid();
    // Versions floor+1..=n, contiguous.
    for (i, (version, _, _)) in events.iter().enumerate() {
        ensure!(
            *version == floor + i as i64 + 1,
            "stream {id}: event {i} above floor {floor} has version {version}"
        );
    }
    match head {
        None => ensure!(events.is_empty(), "stream {id} has events but no row"),
        Some((version, _, _)) => {
            ensure!(
                version == floor + events.len() as i64,
                "stream {id} is at version {version} with {} events above floor {floor}",
                events.len()
            );
        }
    }

    // Events grouped by commit, in version order; each commit's log position rises with its versions.
    let mut groups: Vec<(Uuid, Vec<u32>, i64, i64)> = Vec::new();
    for (version, commit, n) in events {
        match groups.last_mut() {
            Some((c, ns, _, last)) if c == commit => {
                ns.push(*n);
                *last = *version;
            }
            _ => groups.push((*commit, vec![*n], *version, *version)),
        }
    }
    let mut seen = BTreeSet::new();
    let mut last_position = 0;
    for (commit, ns, first, last) in &groups {
        ensure!(
            seen.insert(*commit),
            "stream {id}: commit {commit} is split"
        );
        let (_, row_last, count, _) = commit_rows
            .get(commit)
            .with_context(|| format!("stream {id}: commit {commit} has no row"))?;
        ensure!(
            *row_last == *last && i64::from(*count) == last - first + 1,
            "stream {id}: commit {commit} row says {row_last}/{count}, events {first}..={last}"
        );
        let position = by_commit
            .get(commit)
            .with_context(|| format!("stream {id}: commit {commit} is not in the log"))?
            .last_position;
        ensure!(
            position > last_position,
            "stream {id}: commit {commit} (versions {first}..={last}) is at {position}, before the previous commit's {last_position}"
        );
        last_position = position;
        // Each commit holds exactly the events of one planned commit of this stream.
        let owner = planned.iter().find(|p| p.ns.first() == ns.first());
        let Some(owner) = owner else {
            bail!("stream {id}: commit {commit} holds events {ns:?} no writer appended to it");
        };
        ensure!(
            owner.ns == *ns,
            "stream {id}: commit {commit} holds {ns:?}, the writer appended {:?}",
            owner.ns
        );
        match (owner.fate, owner.result) {
            (Fate::Absent, _) => bail!(
                "stream {id}: commit {commit} holds {ns:?}, which the writer was told failed or never committed"
            ),
            (_, Some(result)) => ensure!(
                result.commit_id.as_uuid() == *commit
                    && result.first == *first
                    && result.last == *last,
                "stream {id}: appended {result:?}, stored {commit} {first}..={last}"
            ),
            (_, None) => {}
        }
    }
    let present: BTreeSet<u32> = events.iter().map(|e| e.2).collect();
    for p in planned {
        let dropped = p.result.is_some_and(|r| r.last <= floor);
        if p.fate == Fate::Acked && !dropped && !p.ns.iter().all(|n| present.contains(n)) {
            bail!("stream {id}: acknowledged events {:?} are missing", p.ns);
        }
    }

    // A started-after-returned append of this stream has higher versions.
    let mut acked: Vec<&&Planned> = planned.iter().filter(|p| p.fate == Fate::Acked).collect();
    acked.sort_by_key(|p| p.ended);
    for a in &acked {
        for b in &acked {
            if a.ended < b.started {
                let (ra, rb) = (a.result.unwrap(), b.result.unwrap());
                ensure!(
                    ra.last < rb.first,
                    "stream {id}: {:?} started after {:?} returned but has lower versions",
                    rb,
                    ra
                );
            }
        }
    }

    // A snapshot equals the fold up to its version.
    if let Some((version, state)) = snapshot {
        let stored: Tally<()> = serde_json::from_slice(state)
            .with_context(|| format!("stream {id}: snapshot does not decode"))?;
        ensure!(
            *version >= 1 && *version <= all_ns.len() as i64,
            "stream {id}: snapshot at {version} with {} events",
            all_ns.len()
        );
        let ns = &all_ns[..*version as usize];
        ensure!(
            stored == Tally::of(ns),
            "stream {id}: snapshot at {version} is {stored:?}, the fold is {:?}",
            Tally::<()>::of(ns)
        );
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn check_subscription(
    sub: SubKind,
    delivered: &[Delivery],
    log: &[LogRow],
    commit_rows: &HashMap<Uuid, (Uuid, i64, i16, i32)>,
    category_ids: &HashMap<i32, Cat>,
    deleted: &BTreeSet<Uuid>,
    ledger: &Ledger,
    until: Option<i64>,
) -> Result<()> {
    let at_least_once = sub == SubKind::AtLeastOnce;
    let mut streams: HashMap<Uuid, Cat> = ledger
        .streams
        .iter()
        .map(|(s, c)| (s.as_uuid(), *c))
        .collect();
    for id in follow_up_streams() {
        streams.insert(id.as_uuid(), Cat::Parts);
    }
    // The versions of acknowledged commits of deleted streams, whose rows are gone.
    let mut gone: HashMap<Uuid, (Uuid, i64)> = HashMap::new();
    // History, which every subscription passed before retention or a delete could remove it.
    let mut history: BTreeSet<Uuid> = BTreeSet::new();
    for p in &ledger.commits {
        if let Some(r) = p.result {
            gone.insert(r.commit_id.as_uuid(), (p.stream.as_uuid(), r.last));
            if p.old {
                history.insert(r.commit_id.as_uuid());
            }
        }
    }

    // What should arrive: every event of a surviving commit of the covered categories, at its position,
    // and every tombstone.
    let mut expected: BTreeMap<i64, (Uuid, i64)> = BTreeMap::new();
    let mut maybe: BTreeMap<i64, (Uuid, i64)> = BTreeMap::new();
    let mut tombstones: BTreeMap<Uuid, i64> = BTreeMap::new();
    let mut history_positions: BTreeSet<i64> = BTreeSet::new();
    for row in log {
        let Some(cat) = category_ids.get(&row.category_id) else {
            bail!("log row {row:?} of an unknown category");
        };
        if !sub.covers(*cat) || until.is_some_and(|until| row.last_position > until) {
            continue;
        }
        let first = row.last_position - i64::from(row.event_count) + 1;
        if let Some((stream, last_version, count, _)) = commit_rows.get(&row.commit_id) {
            let first_version = last_version - i64::from(*count) + 1;
            // A deleted stream keeps only its tombstone commit.
            if deleted.contains(stream) {
                tombstones.insert(*stream, row.last_position);
                continue;
            }
            for (i, p) in (first..=row.last_position).enumerate() {
                expected.insert(p, (*stream, first_version + i as i64));
            }
        } else if let Some((stream, last_version)) = gone.get(&row.commit_id) {
            let first_version = last_version - i64::from(row.event_count) + 1;
            let into = if history.contains(&row.commit_id) && sub != SubKind::Backfill {
                &mut expected
            } else {
                if history.contains(&row.commit_id) {
                    history_positions.extend(first..=row.last_position);
                }
                &mut maybe
            };
            for (i, p) in (first..=row.last_position).enumerate() {
                into.insert(p, (*stream, first_version + i as i64));
            }
        } else {
            // A deleted commit whose outcome its writer never learned.
            for p in first..=row.last_position {
                maybe.insert(p, (Uuid::nil(), 0));
            }
        }
    }

    // Order: within a batch always; across batches for run_pg, overall when unsharded and per stream when
    // sharded.
    let mut seen_events: HashMap<i64, usize> = HashMap::new();
    let mut seen_tombstones: HashMap<Uuid, usize> = HashMap::new();
    let mut last_overall = 0_i64;
    let mut last_per_stream: HashMap<Uuid, i64> = HashMap::new();
    let mut last_in_batch: HashMap<u64, i64> = HashMap::new();
    let mut deleted_versions: BTreeMap<Uuid, Vec<i64>> = BTreeMap::new();
    for d in delivered {
        let in_batch = last_in_batch.entry(d.batch).or_insert(0);
        ensure!(
            d.position > *in_batch,
            "position {} after {} in one batch",
            d.position,
            in_batch
        );
        *in_batch = d.position;
        if !at_least_once {
            if matches!(sub, SubKind::Sharded | SubKind::Backfill) {
                let last = last_per_stream.entry(d.stream).or_insert(0);
                ensure!(
                    d.position > *last,
                    "stream {} delivered {} after {}",
                    d.stream,
                    d.position,
                    last
                );
                *last = d.position;
            } else {
                ensure!(
                    d.position > last_overall,
                    "position {} delivered after {last_overall}",
                    d.position
                );
                last_overall = d.position;
            }
        }
        if let Some(cat) = streams.get(&d.stream) {
            ensure!(
                sub.covers(*cat),
                "delivered stream {} of an uncovered category",
                d.stream
            );
        } else {
            bail!("delivered stream {} that no writer used", d.stream);
        }
        if d.version == 0 {
            *seen_tombstones.entry(d.stream).or_default() += 1;
            ensure!(
                tombstones.get(&d.stream) == Some(&d.position),
                "tombstone of {} delivered at {}, the log has it at {:?}",
                d.stream,
                d.position,
                tombstones.get(&d.stream)
            );
            continue;
        }
        *seen_events.entry(d.position).or_default() += 1;
        if let Some((stream, version)) = expected.get(&d.position) {
            ensure!(
                *stream == d.stream && *version == d.version,
                "position {} delivered as {}@{}, the log has {}@{}",
                d.position,
                d.stream,
                d.version,
                stream,
                version
            );
            if deleted.contains(&d.stream) {
                deleted_versions
                    .entry(d.stream)
                    .or_default()
                    .push(d.version);
            }
        } else if let Some((stream, version)) = maybe.get(&d.position) {
            ensure!(
                deleted.contains(&d.stream) || history_positions.contains(&d.position),
                "position {} of surviving stream {} has no commit",
                d.position,
                d.stream
            );
            if *version != 0 {
                ensure!(
                    *stream == d.stream && *version == d.version,
                    "deleted event at {} delivered as {}@{}, the writer was told {}@{}",
                    d.position,
                    d.stream,
                    d.version,
                    stream,
                    version
                );
            }
            if deleted.contains(&d.stream) {
                deleted_versions
                    .entry(d.stream)
                    .or_default()
                    .push(d.version);
            }
        } else {
            bail!(
                "delivered position {} ({}@{}) that is no event of the log",
                d.position,
                d.stream,
                d.version
            );
        }
    }

    let mut missing = Vec::new();
    for (position, (stream, version)) in &expected {
        match seen_events.get(position).copied().unwrap_or(0) {
            0 => missing.push(format!("{position} ({stream}@{version})")),
            1 => {}
            n if !at_least_once => bail!("position {position} delivered {n} times"),
            _ => {}
        }
    }
    ensure!(
        missing.is_empty(),
        "{} events never delivered: {}",
        missing.len(),
        missing
            .iter()
            .take(10)
            .cloned()
            .collect::<Vec<_>>()
            .join(", ")
    );
    for position in maybe.keys() {
        let n = seen_events.get(position).copied().unwrap_or(0);
        ensure!(
            at_least_once || n <= 1,
            "deleted event at {position} delivered {n} times"
        );
    }
    for stream in tombstones.keys() {
        let n = seen_tombstones.get(stream).copied().unwrap_or(0);
        ensure!(
            n >= 1 && (at_least_once || n == 1),
            "tombstone of {stream} delivered {n} times"
        );
    }
    for (stream, versions) in &mut deleted_versions {
        versions.sort_unstable();
        versions.dedup();
        ensure!(
            versions.iter().enumerate().all(|(i, v)| *v == i as i64 + 1),
            "deleted stream {stream}: delivered versions {versions:?} are not a prefix"
        );
    }
    Ok(())
}
