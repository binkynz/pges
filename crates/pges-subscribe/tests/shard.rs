//! Sharded subscriptions (`Builder::sharded`): every event exactly once across shards, each stream in order,
//! shards shared fairly between instances and taken over from a dead one, and a redefine when the number of
//! shards changes.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

mod common;

use std::{
    collections::BTreeMap,
    num::NonZeroU32,
    sync::{Arc, Mutex},
    time::Duration,
};

use anyhow::{Context, Result, ensure};
use proptest::prelude::*;
use sha2::{Digest, Sha256};
use uuid::Uuid;

use pges_core::{category, interval, non_empty, stream};
use pges_subscribe::{
    batch, handler,
    subscription::{self, StoreExt},
    subscription_name,
    waker::recheck,
};

use common::{Cat, Fixture, Recorder};

const SHARDS: u16 = 4;

/// Shards of fixed ids out of 1024, by the documented hash. They must never change: a changed hash would
/// move every stream to another shard and break the order of each stream across a deploy.
const PINNED_SHARD: u64 = 978;
const PINNED_NIL_SHARD: u64 = 469;

fn builder(f: &Fixture, name: &str, shards: u16) -> subscription::Builder<subscription::Listed> {
    builder_on(&f.store, name, shards)
}

fn builder_on(
    store: &pges_postgres::store::Store,
    name: &str,
    shards: u16,
) -> subscription::Builder<subscription::Listed> {
    store
        .subscription(subscription::SubscriptionName::parse(name).unwrap())
        .categories(non_empty![category!("jobs"), category!("orders")])
        .sharded(subscription::Shards::new(shards).unwrap())
        .batch_size(NonZeroU32::new(3).unwrap())
        .lease(interval!(600 ms))
        .acquire_interval(interval!(20 ms))
        .retry_interval(interval!(20 ms))
        .waker(recheck::Recheck::every(interval!(5 ms)))
}

/// Appends `rounds` single-event commits to each stream, alternating jobs and orders, and sequences them.
async fn append(f: &Fixture, streams: &[stream::StreamId], rounds: u32) -> Result<()> {
    for _ in 0..rounds {
        for (i, stream_id) in streams.iter().enumerate() {
            let cat = if i % 2 == 0 { Cat::Jobs } else { Cat::Orders };
            f.append(cat, *stream_id, 1 + (i % 2) as u32).await?;
        }
        f.sequence(7).await?;
    }
    f.sequence_all().await
}

/// Waits until every shard's stored cursor reaches `head`.
async fn wait_shards(f: &Fixture, name: &str, head: u64) -> Result<()> {
    tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            let lowest: Option<i64> = sqlx::query_scalar(
                "SELECT min(position) FROM es.subscriptions WHERE name = $1 AND shard < shards",
            )
            .bind(name)
            .fetch_one(&f.pool)
            .await?;
            if lowest.is_some_and(|p| p >= head as i64) {
                return anyhow::Ok(());
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .with_context(|| format!("waiting for every shard of {name} to reach {head}"))?
}

/// Waits until an instance holds `shards` shards.
async fn wait_holding(
    mut status: tokio::sync::watch::Receiver<subscription::Status>,
    shards: usize,
) -> Result<()> {
    let reached = tokio::time::timeout(
        Duration::from_secs(20),
        status.wait_for(|s| s.shards.len() == shards),
    )
    .await
    .map(|result| result.map(|_| ()));
    match reached {
        Ok(result) => Ok(result?),
        Err(_) => anyhow::bail!("waiting to hold {shards} shards: {:?}", status.borrow()),
    }
}

/// Checks that `subscriber` recorded every event of jobs and orders exactly once, and each stream's events
/// in position order.
async fn check_sharded_delivery(f: &Fixture, subscriber: &str) -> Result<()> {
    let delivered: Vec<(i64, Uuid)> = sqlx::query_as(
        "SELECT d.position, c.stream_id FROM public.delivered d
         JOIN es.commits c ON c.commit_id = d.commit_id
         WHERE d.subscriber = $1 ORDER BY d.seq",
    )
    .bind(subscriber)
    .fetch_all(&f.pool)
    .await?;

    let mut by_stream: BTreeMap<Uuid, Vec<i64>> = BTreeMap::new();
    for (position, stream_id) in &delivered {
        by_stream.entry(*stream_id).or_default().push(*position);
    }
    for (stream_id, positions) in &by_stream {
        ensure!(
            positions.windows(2).all(|pair| pair[0] < pair[1]),
            "stream {stream_id} out of order: {positions:?}"
        );
    }

    let mut positions: Vec<i64> = delivered.iter().map(|(p, _)| *p).collect();
    positions.sort_unstable();
    let expected: Vec<i64> = f
        .expected(&[f.id(Cat::Jobs), f.id(Cat::Orders)])
        .await?
        .into_iter()
        .map(|(p, _)| p)
        .collect();
    ensure!(
        positions == expected,
        "delivered {} events, expected {}; duplicates or gaps",
        positions.len(),
        expected.len()
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn instances_share_the_shards_and_deliver_every_event_once() -> Result<()> {
    let f = Fixture::new(pges_postgres::store::Outbox::Disabled).await?;
    // Six streams in each of the 4 shards, so every shard has events whatever the random ids.
    let candidates: Vec<uuid::Uuid> = (0..400)
        .map(|_| stream::StreamId::new_v7().as_uuid())
        .collect();
    let shard_of: Vec<(uuid::Uuid, i64)> = sqlx::query_as(
        "SELECT id, (substring(sha256(uuid_send(id)) FROM 1 FOR 8)::int8 & 9223372036854775807) % 4
         FROM unnest($1::uuid[]) AS id",
    )
    .bind(&candidates)
    .fetch_all(&f.pool)
    .await?;
    let mut per_shard = [0usize; 4];
    let streams: Vec<stream::StreamId> = shard_of
        .into_iter()
        .filter(|(_, shard)| {
            let count = &mut per_shard[*shard as usize];
            *count += 1;
            *count <= 6
        })
        .map(|(id, _)| stream::StreamId::from_uuid(id))
        .collect();
    assert_eq!(
        streams.len(),
        24,
        "400 random ids cover every shard six times"
    );
    append(&f, &streams, 3).await?;

    // One instance takes every shard.
    let first = common::Running::spawn(builder(&f, "split", SHARDS).run_pg(Recorder::new("split")));
    wait_holding(first.handle.status_changes(), SHARDS.into()).await?;

    // A second one gets half of them from the first.
    let second =
        common::Running::spawn(builder(&f, "split", SHARDS).run_pg(Recorder::new("split")));
    wait_holding(first.handle.status_changes(), 2).await?;
    wait_holding(second.handle.status_changes(), 2).await?;

    append(&f, &streams, 3).await?;
    let head = common::head(&f.pool).await?;
    wait_shards(&f, "split", head).await?;
    first.wait_position(head).await?;
    second.wait_position(head).await?;

    let mut held: Vec<u16> = first
        .handle
        .status()
        .shards
        .iter()
        .chain(&second.handle.status().shards)
        .map(|shard| shard.get())
        .collect();
    held.sort_unstable();
    assert_eq!(held, [0, 1, 2, 3]);

    first.stop().await?;
    second.stop().await?;
    check_sharded_delivery(&f, "split").await?;

    // Every shard got its six streams.
    let per_shard: Vec<i64> = sqlx::query_scalar(
        "SELECT count(DISTINCT stream_id) FROM es.commits
         GROUP BY (substring(sha256(uuid_send(stream_id)) FROM 1 FOR 8)::int8
                   & 9223372036854775807) % 4",
    )
    .fetch_all(&f.pool)
    .await?;
    assert_eq!(per_shard.len(), 4, "{per_shard:?}");

    let members: i64 = sqlx::query_scalar("SELECT count(*) FROM es.subscription_members")
        .fetch_one(&f.pool)
        .await?;
    assert_eq!(members, 0, "instances leave on shutdown");

    f.finish().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_shards_of_a_dead_instance_move_to_the_others() -> Result<()> {
    let f = Fixture::new(pges_postgres::store::Outbox::Disabled).await?;
    let streams: Vec<stream::StreamId> = (0..16).map(|_| stream::StreamId::new_v7()).collect();
    append(&f, &streams, 2).await?;

    let doomed = common::Crashable::spawn(&f, |store| {
        builder_on(store, "takeover", SHARDS).run_pg(Recorder::new("takeover"))
    })
    .await?;
    wait_holding(doomed.status.clone(), SHARDS.into()).await?;
    let survivor =
        common::Running::spawn(builder(&f, "takeover", SHARDS).run_pg(Recorder::new("takeover")));
    wait_holding(survivor.handle.status_changes(), 2).await?;
    wait_holding(doomed.status.clone(), 2).await?;

    append(&f, &streams, 2).await?;
    let errors = doomed.crash().await;
    ensure!(errors.is_empty(), "recovered from {errors:?}");

    append(&f, &streams, 2).await?;
    wait_holding(survivor.handle.status_changes(), SHARDS.into()).await?;
    let head = common::head(&f.pool).await?;
    wait_shards(&f, "takeover", head).await?;
    survivor.wait_position(head).await?;
    survivor.stop().await?;

    check_sharded_delivery(&f, "takeover").await?;

    f.finish().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn changing_the_number_of_shards_needs_a_redefine() -> Result<()> {
    let f = Fixture::new(pges_postgres::store::Outbox::Disabled).await?;
    let streams: Vec<stream::StreamId> = (0..8).map(|_| stream::StreamId::new_v7()).collect();
    append(&f, &streams, 2).await?;
    let head = common::head(&f.pool).await?;

    let running = common::Running::spawn(builder(&f, "resharded", 2).run_pg(Recorder::new("two")));
    wait_shards(&f, "resharded", head).await?;
    running.stop().await?;
    check_sharded_delivery(&f, "two").await?;

    for shards in [3, 1] {
        let error = common::Running::spawn(
            builder(&f, "resharded", shards).run_pg(Recorder::new("refused")),
        )
        .stopped()
        .await?;
        assert!(
            matches!(
                error,
                subscription::SubscribeError::ShardsChanged {
                    stored: 2,
                    requested,
                } if requested == shards
            ),
            "{error:?}"
        );
    }
    assert!(f.delivered("refused").await?.is_empty());

    // Three shards from the beginning: every event again, once, through the new shards.
    let running = common::Running::spawn(
        builder(&f, "resharded", 3)
            .redefine(subscription::Start::Beginning)
            .run_pg(Recorder::new("three")),
    );
    wait_holding(running.handle.status_changes(), 3).await?;
    wait_shards(&f, "resharded", head).await?;
    running.stop().await?;
    check_sharded_delivery(&f, "three").await?;

    // Back to one shard, not sharded: the other rows stay as retired shards, so their fences keep growing.
    let running = common::Running::spawn(
        f.store
            .subscription(subscription_name!("resharded"))
            .categories(non_empty![category!("jobs"), category!("orders")])
            .redefine(subscription::Start::Beginning)
            .waker(recheck::Recheck::every(interval!(5 ms)))
            .run_pg(Recorder::new("one")),
    );
    running.wait_position(head).await?;
    running.stop().await?;
    common::check_delivery(
        &f.delivered("one").await?,
        &f.expected(&[f.id(Cat::Jobs), f.id(Cat::Orders)]).await?,
    )?;
    let rows: Vec<(i16, i16)> = sqlx::query_as(
        "SELECT shard, shards FROM es.subscriptions WHERE name = 'resharded' ORDER BY shard",
    )
    .fetch_all(&f.pool)
    .await?;
    assert_eq!(rows, [(0, 1), (1, 1), (2, 1)]);

    f.finish().await
}

/// The shard of a stream as `Builder::sharded` documents it, computed apart from the SQL that filters.
fn documented_shard(stream_id: Uuid, shards: u16) -> u64 {
    let digest = Sha256::digest(stream_id.as_bytes());
    let first: [u8; 8] = digest[..8].try_into().unwrap();
    (u64::from_be_bytes(first) & i64::MAX as u64) % u64::from(shards)
}

#[test]
fn the_documented_shard_of_a_stream_never_changes() {
    let id = Uuid::parse_str("01937a3c-5f00-7000-8000-000000000001").unwrap();
    assert_eq!(documented_shard(id, 1024), PINNED_SHARD);
    assert_eq!(documented_shard(Uuid::nil(), 1024), PINNED_NIL_SHARD);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn each_stream_goes_to_the_shard_its_id_hashes_to() -> Result<()> {
    let f = Fixture::new(pges_postgres::store::Outbox::Disabled).await?;
    let streams: Vec<stream::StreamId> = (0..16).map(|_| stream::StreamId::new_v7()).collect();
    append(&f, &streams, 2).await?;
    let head = common::head(&f.pool).await?;

    // Shard 0 is held by an instance that never reads, so this one leads shard 1 only.
    let mut ids = vec![f.id(Cat::Jobs).get(), f.id(Cat::Orders).get()];
    ids.sort_unstable();
    sqlx::query(
        "INSERT INTO es.subscriptions (position, shard, shards, lease_owner, lease_until, name, category_ids)
         VALUES (0, 0, 2, $1, now() + interval '1 hour', 'routed', $2),
                (0, 1, 2, NULL, NULL, 'routed', $2)",
    )
    .bind(Uuid::now_v7())
    .bind(&ids)
    .execute(&f.pool)
    .await?;

    let running = common::Running::spawn(builder(&f, "routed", 2).run_pg(Recorder::new("routed")));
    wait_holding(running.handle.status_changes(), 1).await?;
    running.wait_position(head).await?;
    let held: Vec<u16> = running
        .handle
        .status()
        .shards
        .iter()
        .map(|shard| shard.get())
        .collect();
    assert_eq!(held, [1]);
    running.stop().await?;

    let delivered: Vec<(i64, Uuid)> = sqlx::query_as(
        "SELECT d.position, c.stream_id FROM public.delivered d
         JOIN es.commits c ON c.commit_id = d.commit_id
         WHERE d.subscriber = 'routed' ORDER BY d.seq",
    )
    .fetch_all(&f.pool)
    .await?;
    let expected: Vec<(i64, Uuid)> = sqlx::query_as(
        "SELECT l.last_position - s.n, c.stream_id FROM es.log l
         JOIN es.commits c ON c.commit_id = l.commit_id
         CROSS JOIN LATERAL generate_series(0, l.event_count - 1) AS s(n)
         ORDER BY 1",
    )
    .fetch_all(&f.pool)
    .await?;
    let expected: Vec<(i64, Uuid)> = expected
        .into_iter()
        .filter(|(_, stream_id)| documented_shard(*stream_id, 2) == 1)
        .collect();
    let mut sorted = delivered.clone();
    sorted.sort_unstable();
    ensure!(!expected.is_empty(), "no stream hashed to shard 1");
    ensure!(
        sorted == expected,
        "shard 1 delivered {} events, the documented hash gives {}",
        sorted.len(),
        expected.len()
    );

    f.finish().await
}

type Seen = Arc<Mutex<Vec<(u64, Uuid)>>>;

/// At-least-once handler collecting the position and stream of every event of any category.
struct Collect(Seen);

impl handler::Handler for Collect {
    async fn handle(&mut self, batch: &batch::Batch) -> Result<(), handler::HandlerError> {
        let mut seen = self.0.lock().unwrap();
        for record in batch.iter() {
            if let batch::RawRecorded::Event(raw) = record {
                seen.push((raw.position.get(), raw.stream_id.as_uuid()));
            }
        }
        Ok(())
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn store_wide_shards_with_run_deliver_every_event() -> Result<()> {
    let f = Fixture::new(pges_postgres::store::Outbox::Disabled).await?;
    let streams: Vec<stream::StreamId> = (0..12).map(|_| stream::StreamId::new_v7()).collect();
    for _ in 0..3 {
        for (i, stream_id) in streams.iter().enumerate() {
            let cat = [Cat::Jobs, Cat::Orders, Cat::Parts][i % 3];
            f.append(cat, *stream_id, 2).await?;
        }
    }
    f.sequence_all().await?;
    let head = common::head(&f.pool).await?;

    let seen: Seen = Arc::default();
    let instances: Vec<common::Running> = (0..2)
        .map(|_| {
            common::Running::spawn(
                f.store
                    .subscription(subscription_name!("everything"))
                    .all_categories()
                    .sharded(pges_subscribe::shards!(3))
                    .batch_size(NonZeroU32::new(4).unwrap())
                    .lease(interval!(600 ms))
                    .acquire_interval(interval!(20 ms))
                    .waker(recheck::Recheck::every(interval!(5 ms)))
                    .run(Collect(seen.clone())),
            )
        })
        .collect();
    wait_shards(&f, "everything", head).await?;
    for instance in instances {
        instance.stop().await?;
    }

    let seen = seen.lock().unwrap().clone();
    let mut by_stream: BTreeMap<Uuid, Vec<u64>> = BTreeMap::new();
    for (position, stream_id) in &seen {
        by_stream.entry(*stream_id).or_default().push(*position);
    }
    for positions in by_stream.values() {
        assert!(positions.windows(2).all(|pair| pair[0] < pair[1]));
    }
    let mut positions: Vec<u64> = seen.iter().map(|(p, _)| *p).collect();
    positions.sort_unstable();
    assert_eq!(positions, (1..=head).collect::<Vec<_>>());

    f.finish().await
}

#[derive(Debug, Clone)]
enum Op {
    /// An append of `events` events to stream `stream` of jobs, orders or the unsubscribed parts.
    Append {
        cat: u8,
        stream: u8,
        events: u32,
    },
    /// A sequencer batch of up to `n` commits.
    Sequence(i64),
    /// Instance `k` shuts down gracefully, and a new one takes its place.
    Restart(usize),
    /// Instance `k` is dropped mid-flight, and a new one takes its place.
    Crash(usize),
    Pause(u64),
}

fn op() -> impl Strategy<Value = Op> {
    prop_oneof![
        6 => (0..3_u8, 0..6_u8, 1..4_u32)
            .prop_map(|(cat, stream, events)| Op::Append { cat, stream, events }),
        4 => (1..6_i64).prop_map(Op::Sequence),
        1 => (0..2_usize).prop_map(Op::Restart),
        1 => (0..2_usize).prop_map(Op::Crash),
        1 => (0..20_u64).prop_map(Op::Pause),
    ]
}

enum Instance {
    Graceful(common::Running),
    Crashable(common::Crashable),
}

impl Instance {
    async fn end(self, errors: &mut Vec<String>) -> Result<()> {
        match self {
            Self::Graceful(running) => {
                errors.extend(running.errors());
                running.handle.shutdown().await?;
            }
            Self::Crashable(crashable) => errors.extend(crashable.crash().await),
        }
        Ok(())
    }
}

async fn run_case(ops: Vec<Op>, batch_size: u32) -> Result<()> {
    let f = Arc::new(Fixture::new(pges_postgres::store::Outbox::Disabled).await?);
    let streams: Vec<stream::StreamId> = (0..18).map(|_| stream::StreamId::new_v7()).collect();
    let name = "shard-property";
    let instance = |store: &pges_postgres::store::Store| {
        builder_on(store, name, 3)
            .batch_size(NonZeroU32::new(batch_size).unwrap())
            .run_pg(Recorder::new(name))
    };
    let mut errors = Vec::new();

    let mut instances = vec![
        Some(Instance::Crashable(
            common::Crashable::spawn(&f, instance).await?,
        )),
        Some(Instance::Graceful(common::Running::spawn(instance(
            &f.store,
        )))),
    ];
    for op in ops {
        match op {
            Op::Append {
                cat,
                stream,
                events,
            } => {
                let cat = [Cat::Jobs, Cat::Orders, Cat::Parts][cat as usize];
                let stream_id = streams[cat as usize * 6 + stream as usize];
                f.append(cat, stream_id, events).await?;
            }
            Op::Sequence(n) => {
                f.sequence(n).await?;
            }
            Op::Restart(k) => {
                if let Some(old) = instances[k].take() {
                    old.end(&mut errors).await?;
                }
                instances[k] = Some(Instance::Graceful(common::Running::spawn(instance(
                    &f.store,
                ))));
            }
            Op::Crash(k) => {
                if let Some(old) = instances[k].take() {
                    old.end(&mut errors).await?;
                }
                instances[k] = Some(Instance::Crashable(
                    common::Crashable::spawn(&f, instance).await?,
                ));
            }
            Op::Pause(ms) => tokio::time::sleep(Duration::from_millis(ms)).await,
        }
    }

    f.sequence_all().await?;
    let head = common::head(&f.pool).await?;
    wait_shards(&f, name, head).await?;
    for instance in instances.into_iter().flatten() {
        instance.end(&mut errors).await?;
    }
    ensure!(
        errors.is_empty(),
        "the subscription recovered from errors: {errors:?}"
    );

    check_sharded_delivery(&f, name).await?;

    Arc::into_inner(f).unwrap().finish().await
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 12, ..ProptestConfig::default() })]

    /// Any interleaving of commits, sequencer batches, and restarts and crashes of two instances sharing
    /// three shards delivers every event once, each stream in order.
    #[test]
    fn interleavings_deliver_every_event_once_and_each_stream_in_order(
        ops in prop::collection::vec(op(), 1..40),
        batch_size in 1..4_u32,
    ) {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(4)
            .enable_all()
            .build()
            .unwrap();
        let result = runtime.block_on(run_case(ops, batch_size));
        prop_assert!(result.is_ok(), "{:?}", result);
    }
}
