//! Store-wide subscriptions (`Builder::all_categories`): every category in store position order, including
//! categories registered after the subscription started, read from the log alone, then each page's commits
//! from their categories' partitions.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

mod common;

use std::{
    num::NonZeroU32,
    sync::{Arc, Mutex},
    time::Duration,
};

use anyhow::{Context, Result};
use proptest::prelude::*;
use serde::{Deserialize, Serialize};
use sqlx::PgConnection;

use pges_core::{category, interval, non_empty, position, stream, version};
use pges_postgres::{admin, delete};
use pges_subscribe::{
    batch, handler,
    subscription::{self, StoreExt},
    subscription_name,
    waker::recheck,
};

use common::{Cat, Fixture, Recorder};

/// The category registered while subscriptions run.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, pges_derive::Event)]
#[event(category = "tasks")]
enum TaskEvent {
    #[event(name = "task.noted")]
    Noted { n: u32 },
}

/// Registers `tasks`, its partitions and its event type.
async fn register_tasks(f: &Fixture) -> Result<()> {
    let admin = admin::Admin::new(f.pool.clone(), common::STORE_NAME);
    admin
        .register_category(
            <TaskEvent as pges_core::event::Event>::CATEGORY,
            admin::CategoryConfig::default(),
        )
        .await?;
    admin.maintain().await?;
    f.store.register::<TaskEvent>().await?;
    Ok(())
}

async fn append_tasks(f: &Fixture, stream_id: stream::StreamId, events: u32) -> Result<()> {
    let events: Vec<_> = (0..events).map(|n| TaskEvent::Noted { n }).collect();
    f.store
        .append(stream_id, version::ExpectedVersion::Any, &events)
        .await?;
    Ok(())
}

/// Every category of the store, as registered now.
async fn all_categories(f: &Fixture) -> Result<Vec<category::CategoryId>> {
    let ids: Vec<i32> = sqlx::query_scalar("SELECT category_id FROM es.categories")
        .fetch_all(&f.pool)
        .await?;
    Ok(ids.into_iter().map(category::CategoryId::new).collect())
}

fn subscription(f: &Fixture, name: &str, batch_size: u32) -> subscription::Subscription {
    subscription_on(&f.store, name, batch_size)
}

fn subscription_on(
    store: &pges_postgres::store::Store,
    name: &str,
    batch_size: u32,
) -> subscription::Subscription {
    store
        .subscription(subscription::SubscriptionName::parse(name).unwrap())
        .all_categories()
        .batch_size(NonZeroU32::new(batch_size).unwrap())
        .lease(interval!(400 ms))
        .acquire_interval(interval!(20 ms))
        .retry_interval(interval!(20 ms))
        .waker(recheck::Recheck::every(interval!(5 ms)))
        .run_pg(Recorder::new(name))
}

#[tokio::test]
async fn delivers_every_category_in_position_order() -> Result<()> {
    let f = Fixture::new(pges_postgres::store::Outbox::Disabled).await?;
    let (job, order, part) = (
        stream::StreamId::new_v7(),
        stream::StreamId::new_v7(),
        stream::StreamId::new_v7(),
    );
    for (cat, stream_id, events) in [
        (Cat::Jobs, job, 1),
        (Cat::Parts, part, 3),
        (Cat::Orders, order, 2),
        (Cat::Jobs, job, 2),
        (Cat::Parts, part, 1),
    ] {
        f.append(cat, stream_id, events).await?;
    }
    f.sequence_all().await?;

    let recorder = Recorder::new("everything");
    let running = common::Running::spawn(
        f.store
            .subscription(subscription_name!("everything"))
            .all_categories()
            .batch_size(NonZeroU32::new(2).unwrap())
            .waker(recheck::Recheck::every(interval!(5 ms)))
            .run_pg(recorder.clone()),
    );
    running.wait_position(common::head(&f.pool).await?).await?;
    assert!(recorder.calls() >= 3, "two commits per batch");

    // A category registered after the start is read too, without a restart.
    register_tasks(&f).await?;
    let task = stream::StreamId::new_v7();
    append_tasks(&f, task, 2).await?;
    f.append(Cat::Orders, order, 1).await?;
    append_tasks(&f, task, 1).await?;
    f.sequence_all().await?;
    let head = common::head(&f.pool).await?;
    running.wait_position(head).await?;
    running.stop().await?;

    let expected = f.expected(&all_categories(&f).await?).await?;
    assert_eq!(expected.len() as u64, head);
    common::check_delivery(&f.delivered("everything").await?, &expected)?;
    assert_eq!(f.cursor("everything").await?, Some(head as i64));
    let stored: Vec<i32> =
        sqlx::query_scalar("SELECT category_ids FROM es.subscriptions WHERE name = 'everything'")
            .fetch_one(&f.pool)
            .await?;
    assert!(stored.is_empty(), "an empty category list means all");

    f.finish().await
}

#[tokio::test]
async fn categories_and_all_categories_are_different_definitions() -> Result<()> {
    let f = Fixture::new(pges_postgres::store::Outbox::Disabled).await?;

    // `all_categories` and listed categories in one subscription do not compile (`tests/ui/`); a stored
    // store-wide definition differs from a listed one.
    let running = common::Running::spawn(subscription(&f, "defined", 8));
    common::wait_state(running.handle.status_changes(), |s| {
        matches!(s, subscription::State::Running)
    })
    .await?;
    running.stop().await?;
    let listed = f
        .store
        .subscription(subscription_name!("defined"))
        .categories(non_empty![category!("jobs")])
        .run_pg(Recorder::new("x"))
        .run_until(std::future::pending())
        .await;
    assert!(matches!(
        listed,
        Err(subscription::SubscribeError::DefinitionChanged { stored, .. }) if stored.is_empty()
    ));

    f.finish().await
}

#[tokio::test]
async fn a_cursor_behind_any_horizon_stops_the_subscription() -> Result<()> {
    let f = Fixture::new(pges_postgres::store::Outbox::Disabled).await?;
    let (job, order) = (stream::StreamId::new_v7(), stream::StreamId::new_v7());
    for _ in 0..3 {
        f.append(Cat::Jobs, job, 1).await?;
        f.append(Cat::Orders, order, 1).await?;
    }
    f.sequence_all().await?;
    sqlx::query("UPDATE es.categories SET horizon_position = 4 WHERE category_id = $1")
        .bind(f.id(Cat::Orders).get())
        .execute(&f.pool)
        .await?;

    let error = common::Running::spawn(subscription(&f, "behind", 8))
        .stopped()
        .await?;
    match error {
        subscription::SubscribeError::CursorBeforeHorizon {
            category,
            cursor,
            horizon,
        } => {
            assert_eq!(category, f.id(Cat::Orders));
            assert_eq!(cursor, position::Position::BEGINNING);
            assert_eq!(horizon, pges_testing::position(4));
        }
        other => panic!("unexpected error {other:?}"),
    }
    assert!(f.delivered("behind").await?.is_empty());

    // From the highest horizon on, every category is read.
    let running = common::Running::spawn(
        f.store
            .subscription(subscription_name!("at-horizon"))
            .all_categories()
            .start(subscription::Start::Position(pges_testing::position(4)))
            .waker(recheck::Recheck::every(interval!(5 ms)))
            .run_pg(Recorder::new("at-horizon")),
    );
    running.wait_position(6).await?;
    running.stop().await?;
    assert_eq!(
        f.delivered("at-horizon")
            .await?
            .iter()
            .map(|d| d.0)
            .collect::<Vec<_>>(),
        [5, 6]
    );

    f.finish().await
}

/// What a handler saw: an event's position, or the position and stream of a `$stream-deleted`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Seen {
    Event(u64),
    Deleted(u64, stream::StreamId),
}

impl Seen {
    fn position(&self) -> u64 {
        match self {
            Self::Event(position) | Self::Deleted(position, _) => *position,
        }
    }
}

/// Collects every record of every batch, events and system events, for `run` and `run_pg`.
#[derive(Debug, Clone, Default)]
struct Collect(Arc<Mutex<Vec<Seen>>>);

impl Collect {
    fn push(&self, batch: &batch::Batch) {
        let mut seen = self.0.lock().unwrap();
        for record in batch.iter() {
            seen.push(match record {
                batch::RawRecorded::System(batch::SystemEvent::StreamDeleted {
                    position,
                    stream_id,
                    ..
                }) => Seen::Deleted(position.get(), *stream_id),
                other => Seen::Event(other.position().get()),
            });
        }
    }

    /// Everything seen, by position.
    fn sorted(&self) -> Vec<Seen> {
        let mut seen = self.0.lock().unwrap().clone();
        seen.sort_unstable_by_key(Seen::position);
        seen
    }
}

impl handler::Handler for Collect {
    async fn handle(&mut self, batch: &batch::Batch) -> Result<(), handler::HandlerError> {
        self.push(batch);
        Ok(())
    }
}

impl handler::PgHandler for Collect {
    async fn handle(
        &mut self,
        _tx: &mut PgConnection,
        batch: &batch::Batch,
    ) -> Result<(), handler::HandlerError> {
        self.push(batch);
        Ok(())
    }
}

/// An outbox store whose log holds the rows of deleted commits: `deleted` is deleted after its commits were
/// sequenced (positions 1-2 and 4), `late` before (6-7), so its pending rows are sequenced after the delete.
/// Returns what a reader of every category must see, up to position 11.
async fn deleted_mid_log(f: &Fixture) -> Result<Vec<Seen>> {
    let any = delete::DeleteExpected::Any;
    let (deleted, late, kept, order) = (
        stream::StreamId::new_v7(),
        stream::StreamId::new_v7(),
        stream::StreamId::new_v7(),
        stream::StreamId::new_v7(),
    );
    f.append(Cat::Jobs, deleted, 2).await?;
    f.append(Cat::Orders, order, 1).await?;
    f.append(Cat::Jobs, deleted, 1).await?;
    f.append(Cat::Jobs, kept, 1).await?;
    let sequencer = common::Sequencer::start(f).await?;
    common::wait_sequenced(&f.pool, 5).await?;
    sequencer.stop().await?;

    f.append(Cat::Parts, late, 2).await?;
    f.append(Cat::Orders, order, 1).await?;
    f.store
        .delete_stream::<common::JobEvent>(deleted, any)
        .await?;
    f.store
        .delete_stream::<common::PartEvent>(late, any)
        .await?;
    f.append(Cat::Jobs, kept, 1).await?;
    let sequencer = common::Sequencer::start(f).await?;
    common::wait_sequenced(&f.pool, 11).await?;
    sequencer.stop().await?;

    Ok(vec![
        Seen::Event(3),
        Seen::Event(5),
        Seen::Event(8),
        Seen::Deleted(9, deleted),
        Seen::Deleted(10, late),
        Seen::Event(11),
    ])
}

/// Waits until every shard of `name` has stored a cursor at `head` or past it.
async fn wait_every_shard(f: &Fixture, name: &str, head: u64) -> Result<()> {
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

#[tokio::test]
async fn a_deleted_stream_reaches_every_reader_as_its_system_event_only() -> Result<()> {
    let f = Fixture::new(pges_postgres::store::Outbox::Enabled).await?;
    let expected = deleted_mid_log(&f).await?;

    for batch_size in [1, 2, 64] {
        let name = format!("everything-{batch_size}");
        let collect = Collect::default();
        let running = common::Running::spawn(
            f.store
                .subscription(subscription::SubscriptionName::parse(name.as_str()).unwrap())
                .all_categories()
                .batch_size(NonZeroU32::new(batch_size).unwrap())
                .waker(recheck::Recheck::every(interval!(5 ms)))
                .run_pg(collect.clone()),
        );
        running.wait_position(11).await?;
        running.stop().await?;
        assert_eq!(collect.sorted(), expected, "batch size {batch_size}");
    }

    let collect = Collect::default();
    let running = common::Running::spawn(
        f.store
            .subscription(subscription_name!("listed"))
            .categories(non_empty![
                category!("jobs"),
                category!("orders"),
                category!("parts")
            ])
            .batch_size(NonZeroU32::new(2).unwrap())
            .waker(recheck::Recheck::every(interval!(5 ms)))
            .run(collect.clone()),
    );
    running.wait_position(11).await?;
    running.stop().await?;
    assert_eq!(collect.sorted(), expected, "listed categories");

    let collect = Collect::default();
    f.store
        .backfill(subscription_name!("replay"), pges_testing::position(11))
        .categories(non_empty![
            category!("jobs"),
            category!("orders"),
            category!("parts")
        ])
        .parallelism(pges_subscribe::lanes!(2))
        .batch_size(NonZeroU32::new(2).unwrap())
        .run_pg(collect.clone())
        .await?;
    assert_eq!(collect.sorted(), expected, "backfill");

    f.finish().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_deleted_stream_reaches_a_sharded_store_wide_subscription() -> Result<()> {
    let f = Fixture::new(pges_postgres::store::Outbox::Enabled).await?;
    let expected = deleted_mid_log(&f).await?;

    let collect = Collect::default();
    let instances: Vec<common::Running> = (0..2)
        .map(|_| {
            common::Running::spawn(
                f.store
                    .subscription(subscription_name!("sharded"))
                    .all_categories()
                    .sharded(pges_subscribe::shards!(3))
                    .batch_size(NonZeroU32::new(2).unwrap())
                    .lease(interval!(600 ms))
                    .acquire_interval(interval!(20 ms))
                    .waker(recheck::Recheck::every(interval!(5 ms)))
                    .run(collect.clone()),
            )
        })
        .collect();
    wait_every_shard(&f, "sharded", 11).await?;
    for instance in instances {
        instance.stop().await?;
    }
    assert_eq!(collect.sorted(), expected);

    f.finish().await
}

#[tokio::test]
async fn a_log_row_whose_commit_vanished_without_a_deletion_is_corrupt() -> Result<()> {
    let f = Fixture::new(pges_postgres::store::Outbox::Enabled).await?;
    deleted_mid_log(&f).await?;
    sqlx::query(
        "DELETE FROM es.commits
         WHERE commit_id = (SELECT commit_id FROM es.log WHERE last_position = 5)",
    )
    .execute(&f.pool)
    .await?;

    let error = common::Running::spawn(subscription(&f, "corrupt", 64))
        .stopped()
        .await?;
    assert!(
        matches!(
            error,
            subscription::SubscribeError::Corrupt { position, .. }
                if position == pges_testing::position(5)
        ),
        "{error:?}"
    );

    f.finish().await
}

#[derive(Debug, Clone)]
enum Op {
    /// An append of `events` events to stream `stream` of a category: jobs, orders, parts, or tasks, which
    /// is registered on its first append.
    Append {
        cat: u8,
        stream: u8,
        events: u32,
    },
    /// A sequencer batch of up to `n` commits.
    Sequence(i64),
    /// Graceful shutdown, then a new instance resumes from the checkpoint.
    Restart,
    /// The subscription's task is dropped mid-flight; a new instance waits for the lease to lapse.
    Crash,
    Pause(u64),
}

fn op() -> impl Strategy<Value = Op> {
    prop_oneof![
        6 => (0..4_u8, 0..3_u8, 1..4_u32)
            .prop_map(|(cat, stream, events)| Op::Append { cat, stream, events }),
        4 => (1..6_i64).prop_map(Op::Sequence),
        1 => Just(Op::Restart),
        1 => Just(Op::Crash),
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
    let streams: Vec<stream::StreamId> = (0..12).map(|_| stream::StreamId::new_v7()).collect();
    let name = "store-property";
    let mut errors = Vec::new();
    let mut tasks_registered = false;

    let mut instance = Instance::Crashable(
        common::Crashable::spawn(&f, |store| subscription_on(store, name, batch_size)).await?,
    );
    for op in ops {
        match op {
            Op::Append {
                cat,
                stream,
                events,
            } => {
                let stream_id = streams[cat as usize * 3 + stream as usize];
                match cat {
                    0..=2 => {
                        let cat = [Cat::Jobs, Cat::Orders, Cat::Parts][cat as usize];
                        f.append(cat, stream_id, events).await?;
                    }
                    _ => {
                        if !tasks_registered {
                            register_tasks(&f).await?;
                            tasks_registered = true;
                        }
                        append_tasks(&f, stream_id, events).await?;
                    }
                }
            }
            Op::Sequence(n) => {
                f.sequence(n).await?;
            }
            Op::Restart => {
                instance.end(&mut errors).await?;
                instance =
                    Instance::Graceful(common::Running::spawn(subscription(&f, name, batch_size)));
            }
            Op::Crash => {
                instance.end(&mut errors).await?;
                instance = Instance::Crashable(
                    common::Crashable::spawn(&f, |store| subscription_on(store, name, batch_size))
                        .await?,
                );
            }
            Op::Pause(ms) => tokio::time::sleep(Duration::from_millis(ms)).await,
        }
    }

    f.sequence_all().await?;
    let head = common::head(&f.pool).await?;
    let holds_lease = |s: &subscription::State| matches!(s, subscription::State::Running);
    match &instance {
        Instance::Graceful(running) => {
            common::wait_state(running.handle.status_changes(), holds_lease).await?;
            running.wait_position(head).await?;
        }
        Instance::Crashable(crashable) => {
            common::wait_state(crashable.status.clone(), holds_lease).await?;
            common::wait_position(crashable.status.clone(), head).await?;
        }
    }
    instance.end(&mut errors).await?;
    anyhow::ensure!(
        errors.is_empty(),
        "the subscription recovered from errors: {errors:?}"
    );

    let expected = f.expected(&all_categories(&f).await?).await?;
    anyhow::ensure!(expected.len() as u64 == head);
    common::check_delivery(&f.delivered(name).await?, &expected)?;
    anyhow::ensure!(f.cursor(name).await? == Some(head as i64));

    Arc::into_inner(f).unwrap().finish().await
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 16, ..ProptestConfig::default() })]

    /// Any interleaving of commits across four categories (one registered midway), sequencer batches, and
    /// subscription restarts and crashes delivers every event of the store once, in position order.
    #[test]
    fn interleavings_deliver_every_event_once_in_position_order(
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

/// Records each event's category and stream.
#[derive(Clone, Default)]
struct Streams(Arc<Mutex<Vec<(category::CategoryName, stream::StreamId, String)>>>);

impl handler::Handler for Streams {
    async fn handle(&mut self, batch: &batch::Batch) -> Result<(), handler::HandlerError> {
        let mut seen = self.0.lock().unwrap();
        for record in batch.iter() {
            if let batch::RawRecorded::Event(raw) = record {
                seen.push((raw.category, raw.stream_id, raw.type_name.to_string()));
            }
        }
        Ok(())
    }
}

#[tokio::test]
async fn categories_that_share_a_commit_id_keep_their_own_commits() -> Result<()> {
    let f = Fixture::new(pges_postgres::store::Outbox::Disabled).await?;
    let (job, order) = (stream::StreamId::new_v7(), stream::StreamId::new_v7());
    let shared = pges_core::commit::CommitId::generate();
    f.store
        .append(
            job,
            version::ExpectedVersion::Any,
            &[common::JobEvent::Noted { n: 1 }],
        )
        .commit_id(shared)
        .await?;
    f.store
        .append(
            order,
            version::ExpectedVersion::Any,
            &[common::OrderEvent::Noted { n: 1 }],
        )
        .commit_id(shared)
        .await?;
    f.sequence_all().await?;

    let seen = Streams::default();
    let running = common::Running::spawn(
        f.store
            .subscription(subscription_name!("shared-ids"))
            .all_categories()
            .waker(recheck::Recheck::every(interval!(5 ms)))
            .run(seen.clone()),
    );
    running.wait_position(2).await?;
    running.stop().await?;

    let seen = seen.0.lock().unwrap().clone();
    assert_eq!(
        seen,
        vec![
            (category!("jobs"), job, "job.noted".to_owned()),
            (category!("orders"), order, "order.noted".to_owned()),
        ]
    );
    f.finish().await
}
