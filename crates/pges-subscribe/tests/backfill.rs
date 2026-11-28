//! Backfills: a one-off replay of some categories up to a position, split into lanes by stream, with
//! resumable per-lane progress and exactly-once delivery per event.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

mod common;

use std::{
    collections::{BTreeMap, BTreeSet},
    num::NonZeroU32,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use anyhow::{Result, ensure};
use sqlx::{Connection, PgConnection, PgPool};
use uuid::Uuid;

use pges_core::{category, interval, non_empty, position, stream};
use pges_subscribe::{
    backfill, batch, handler,
    subscription::{self, StoreExt},
    subscription_name,
    waker::recheck,
};

use common::{Cat, Fixture, Recorder};

#[tokio::test]
async fn a_backfill_handles_each_event_up_to_its_end_once_in_stream_order() -> Result<()> {
    let f = Fixture::new(pges_postgres::store::Outbox::Disabled).await?;
    let streams = write(&f, 12, 3).await?;
    let until = common::head(&f.pool).await?;
    write_more(&f, &streams).await?;

    let finished = f
        .store
        .backfill(subscription_name!("all"), pges_testing::position(until))
        .categories(non_empty![category!("jobs"), category!("orders")])
        .parallelism(pges_subscribe::lanes!(4))
        .batch_size(NonZeroU32::new(5).unwrap())
        .run_pg(Recorder::new("all"))
        .await?;

    assert_eq!(finished.until, pges_testing::position(until));
    let expected = expected(&f, &[Cat::Jobs, Cat::Orders], until).await?;
    assert_eq!(finished.events, expected.len() as u64);
    check(&f, "all", &expected).await?;

    let again = f
        .store
        .backfill(subscription_name!("all"), pges_testing::position(until))
        .categories(non_empty![category!("jobs"), category!("orders")])
        .parallelism(pges_subscribe::lanes!(4))
        .run_pg(Recorder::new("all"))
        .await?;
    assert_eq!(again.events, 0, "a finished backfill handles nothing more");
    check(&f, "all", &expected).await?;

    f.finish().await
}

#[tokio::test]
async fn a_failed_run_resumes_without_handling_an_event_twice() -> Result<()> {
    let f = Fixture::new(pges_postgres::store::Outbox::Disabled).await?;
    write(&f, 10, 4).await?;
    let until = common::head(&f.pool).await?;
    let expected = expected(&f, &[Cat::Jobs, Cat::Orders], until).await?;
    let middle = expected.iter().nth(expected.len() / 2).copied().unwrap();

    let failing = Failing {
        recorder: Recorder::new("resumed"),
        fail_at: pges_testing::position(middle as u64),
    };
    let result = f
        .store
        .backfill(subscription_name!("resumed"), backfill::Until::Head)
        .category::<common::JobEvent>()
        .categories(non_empty![category!("orders")])
        .parallelism(pges_subscribe::lanes!(3))
        .batch_size(NonZeroU32::new(4).unwrap())
        .run_pg(failing)
        .await;
    let Err(backfill::BackfillError::HandlerFailed { .. }) = result else {
        panic!("expected a handler failure, got {result:?}");
    };
    let partial = f.delivered("resumed").await?.len();

    // Dropping a run mid-way is a crash too: its open transactions roll back.
    let slow = Slow {
        recorder: Recorder::new("resumed"),
    };
    let dropped = tokio::time::timeout(
        Duration::from_millis(150),
        f.store
            .backfill(subscription_name!("resumed"), backfill::Until::Head)
            .categories(non_empty![category!("jobs"), category!("orders")])
            .parallelism(pges_subscribe::lanes!(3))
            .batch_size(NonZeroU32::new(4).unwrap())
            .run_pg(slow),
    )
    .await;
    assert!(dropped.is_err(), "the slow run was not dropped");

    let finished = f
        .store
        .backfill(subscription_name!("resumed"), backfill::Until::Head)
        .categories(non_empty![category!("jobs"), category!("orders")])
        .parallelism(pges_subscribe::lanes!(3))
        .run_pg(Recorder::new("resumed"))
        .await?;

    assert_eq!(finished.until, pges_testing::position(until));
    ensure!(
        partial > 0 && partial < expected.len(),
        "the failed run handled {partial} of {} events",
        expected.len()
    );
    check(&f, "resumed", &expected).await?;

    f.finish().await
}

/// A handler whose savepoint begin fails, which ends its transaction, moves no lane: the batch is retried, and
/// a later run handles every event once.
#[tokio::test]
async fn a_handler_whose_savepoint_begin_fails_moves_no_lane() -> Result<()> {
    let f = Fixture::new(pges_postgres::store::Outbox::Disabled).await?;
    write(&f, 4, 2).await?;
    let until = common::head(&f.pool).await?;
    let expected = expected(&f, &[Cat::Jobs], until).await?;

    let failing = FailsASavepoint {
        recorder: Recorder::new("savepoint"),
    };
    let result = f
        .store
        .backfill(subscription_name!("savepoint"), backfill::Until::Head)
        .category::<common::JobEvent>()
        .on_error(subscription::RetryPolicy::new(
            interval!(1 ms),
            NonZeroU32::new(2).unwrap(),
        ))
        .run_pg(failing.clone())
        .await;
    let Err(backfill::BackfillError::HandlerFailed { source, .. }) = result else {
        panic!("expected a handler failure, got {result:?}");
    };
    assert!(source.to_string().contains("transaction ended"), "{source}");
    assert!(failing.recorder.calls() >= 2, "retried");
    assert!(f.delivered("savepoint").await?.is_empty());

    f.store
        .backfill(subscription_name!("savepoint"), backfill::Until::Head)
        .category::<common::JobEvent>()
        .run_pg(Recorder::new("savepoint"))
        .await?;
    check(&f, "savepoint", &expected).await?;

    f.finish().await
}

#[tokio::test]
async fn the_end_comes_from_a_subscription_and_the_definition_is_fixed() -> Result<()> {
    let f = Fixture::new(pges_postgres::store::Outbox::Disabled).await?;
    write(&f, 4, 2).await?;
    let live = common::Running::spawn(
        f.store
            .subscription(subscription_name!("live"))
            .categories(non_empty![category!("jobs")])
            .waker(recheck::Recheck::every(interval!(20 ms)))
            .run_pg(Recorder::new("live")),
    );
    let head = common::head(&f.pool).await?;
    live.wait_position(head).await?;
    live.stop().await?;
    f.append(Cat::Jobs, stream::StreamId::new_v7(), 2).await?;
    f.sequence_all().await?;

    let unknown = f
        .store
        .backfill(
            subscription_name!("from-live"),
            backfill::Until::Subscription(subscription_name!("nope")),
        )
        .categories(non_empty![category!("jobs")])
        .run_pg(Recorder::new("from-live"))
        .await;
    assert!(
        matches!(unknown, Err(backfill::BackfillError::UnknownSubscription(ref name)) if name.as_str() == "nope"),
        "{unknown:?}"
    );

    let finished = f
        .store
        .backfill(
            subscription_name!("from-live"),
            backfill::Until::Subscription(subscription_name!("live")),
        )
        .categories(non_empty![category!("jobs")])
        .parallelism(pges_subscribe::lanes!(2))
        .run_pg(Recorder::new("from-live"))
        .await?;
    assert_eq!(finished.until, pges_testing::position(head));
    check(&f, "from-live", &expected(&f, &[Cat::Jobs], head).await?).await?;

    for (parallelism, until) in [
        (3, backfill::Until::Head),
        (
            2,
            backfill::Until::Position(pges_testing::position(head - 1)),
        ),
    ] {
        let changed = f
            .store
            .backfill(subscription_name!("from-live"), until)
            .categories(non_empty![category!("jobs")])
            .parallelism(backfill::Lanes::new(parallelism).unwrap())
            .run_pg(Recorder::new("from-live"))
            .await;
        assert!(
            matches!(
                changed,
                Err(backfill::BackfillError::DefinitionChanged { .. })
            ),
            "{changed:?}"
        );
    }
    let changed = f
        .store
        .backfill(subscription_name!("from-live"), backfill::Until::Head)
        .categories(non_empty![category!("jobs"), category!("orders")])
        .parallelism(pges_subscribe::lanes!(2))
        .run_pg(Recorder::new("from-live"))
        .await;
    assert!(
        matches!(
            changed,
            Err(backfill::BackfillError::DefinitionChanged { .. })
        ),
        "{changed:?}"
    );

    let ahead = f
        .store
        .backfill(
            subscription_name!("ahead"),
            pges_testing::position(head + 100),
        )
        .categories(non_empty![category!("jobs")])
        .run_pg(Recorder::new("ahead"))
        .await;
    assert!(
        matches!(ahead, Err(backfill::BackfillError::UntilAhead { .. })),
        "{ahead:?}"
    );
    let corrected = f
        .store
        .backfill(subscription_name!("ahead"), pges_testing::position(head))
        .categories(non_empty![category!("jobs")])
        .run_pg(Recorder::new("ahead"))
        .await?;
    assert_eq!(
        corrected.until,
        pges_testing::position(head),
        "a refused end is not stored"
    );

    f.finish().await
}

#[tokio::test]
async fn the_end_from_a_sharded_subscription_is_its_lowest_shard() -> Result<()> {
    let f = Fixture::new(pges_postgres::store::Outbox::Disabled).await?;
    write(&f, 4, 3).await?;
    let head = common::head(&f.pool).await?;
    let lowest = head - 3;

    // Shards 0 and 1 of 2, and shard 2, retired when the subscription shrank from 3 shards: not counted.
    sqlx::query(
        "INSERT INTO es.subscriptions (position, shard, shards, name, category_ids)
         VALUES ($1, 0, 2, 'live', $3), ($2, 1, 2, 'live', $3), (1, 2, 2, 'live', $3)",
    )
    .bind(head as i64)
    .bind(lowest as i64)
    .bind(vec![f.id(Cat::Jobs).get()])
    .execute(&f.pool)
    .await?;

    let finished = f
        .store
        .backfill(
            subscription_name!("from-sharded"),
            backfill::Until::Subscription(subscription_name!("live")),
        )
        .categories(non_empty![category!("jobs")])
        .parallelism(pges_subscribe::lanes!(2))
        .run_pg(Recorder::new("from-sharded"))
        .await?;
    assert_eq!(finished.until, pges_testing::position(lowest));
    check(
        &f,
        "from-sharded",
        &expected(&f, &[Cat::Jobs], lowest).await?,
    )
    .await?;

    f.finish().await
}

#[tokio::test]
async fn an_end_inside_a_commit_stops_before_that_commit() -> Result<()> {
    let f = Fixture::new(pges_postgres::store::Outbox::Disabled).await?;
    write(&f, 4, 2).await?;
    let (straddled,): (i64,) = sqlx::query_as(
        "SELECT last_position FROM es.log WHERE event_count = 2 \
         ORDER BY last_position LIMIT 1 OFFSET 3",
    )
    .fetch_one(&f.pool)
    .await?;
    let until = straddled as u64 - 1;

    let finished = f
        .store
        .backfill(subscription_name!("inside"), pges_testing::position(until))
        .categories(non_empty![
            category!("jobs"),
            category!("orders"),
            category!("parts")
        ])
        .parallelism(pges_subscribe::lanes!(2))
        .batch_size(NonZeroU32::new(3).unwrap())
        .run_pg(Recorder::new("inside"))
        .await?;

    assert_eq!(finished.until, pges_testing::position(until));
    let mut expected = expected(&f, &[Cat::Jobs, Cat::Orders, Cat::Parts], until).await?;
    assert!(
        expected.remove(&(until as i64)),
        "the straddled commit's first event"
    );
    check(&f, "inside", &expected).await?;

    f.finish().await
}

/// A backfill without categories, a bad name or a lane count out of range does not compile
/// (`tests/ui/`); what is left is checked against the store.
#[tokio::test]
async fn invalid_backfills_fail_before_reading() -> Result<()> {
    let f = Fixture::new(pges_postgres::store::Outbox::Disabled).await?;

    let run =
        |builder: backfill::Builder<subscription::Listed>| builder.run_pg(Recorder::new("invalid"));
    let results = [
        run(f
            .store
            .backfill(subscription_name!("wide"), backfill::Until::Head)
            .categories(non_empty![category!("jobs")])
            .parallelism(pges_subscribe::lanes!(100)))
        .await,
        run(f
            .store
            .backfill(subscription_name!("unknown"), backfill::Until::Head)
            .categories(non_empty![category!("nope")]))
        .await,
    ];
    assert!(matches!(
        results[0],
        Err(backfill::BackfillError::PoolTooSmall { needed: 100, .. })
    ));
    assert!(
        matches!(
            results[1],
            Err(backfill::BackfillError::UnknownCategory(name)) if name.as_str() == "nope"
        ),
        "{:?}",
        results[1]
    );

    f.finish().await
}

#[tokio::test]
async fn a_second_run_that_moves_a_lane_overtakes_this_one() -> Result<()> {
    let f = Fixture::new(pges_postgres::store::Outbox::Disabled).await?;
    write(&f, 1, 3).await?;

    let overtaking = Overtaking {
        pool: f.pool.clone(),
        calls: Arc::new(AtomicUsize::new(0)),
    };
    let result = f
        .store
        .backfill(subscription_name!("overtaken"), backfill::Until::Head)
        .categories(non_empty![category!("jobs")])
        .run_pg(overtaking.clone())
        .await;

    assert!(
        matches!(result, Err(backfill::BackfillError::Overtaken)),
        "{result:?}"
    );
    assert_eq!(overtaking.calls.load(Ordering::SeqCst), 1);

    f.finish().await
}

#[tokio::test]
async fn a_backfill_reads_each_category_from_its_retention_horizon() -> Result<()> {
    let f = Fixture::new(pges_postgres::store::Outbox::Disabled).await?;
    write(&f, 6, 3).await?;
    let until = common::head(&f.pool).await?;
    // Retention removed jobs up to a commit boundary; orders keeps everything.
    let horizon: i64 = sqlx::query_scalar(
        "SELECT last_position FROM es.log ORDER BY last_position OFFSET 20 LIMIT 1",
    )
    .fetch_one(&f.pool)
    .await?;
    sqlx::query("UPDATE es.categories SET horizon_position = $2 WHERE category_id = $1")
        .bind(f.id(Cat::Jobs).get())
        .bind(horizon)
        .execute(&f.pool)
        .await?;

    let finished = f
        .store
        .backfill(subscription_name!("horizon"), pges_testing::position(until))
        .categories(non_empty![category!("jobs"), category!("orders")])
        .parallelism(pges_subscribe::lanes!(2))
        .batch_size(NonZeroU32::new(4).unwrap())
        .run_pg(Recorder::new("horizon"))
        .await?;

    let jobs = expected(&f, &[Cat::Jobs], until).await?;
    let orders = expected(&f, &[Cat::Orders], until).await?;
    assert!(orders.iter().any(|position| *position < horizon));
    let expected: BTreeSet<i64> = jobs
        .into_iter()
        .filter(|position| *position > horizon)
        .chain(orders)
        .collect();
    assert_eq!(finished.events, expected.len() as u64);
    check(&f, "horizon", &expected).await?;

    f.finish().await
}

/// A listed read fetches the commits of the rows at or below the merge cut only. With jobs a hundred times
/// busier than orders, each page is cut at the end of a full page of jobs, before most orders rows: the
/// orders commits are fetched once each, on the pages that deliver them, not once per page. Counted as
/// scans of the orders partition's leaves; every fetch is one scan of the leaf holding its ids.
#[tokio::test]
async fn a_sparse_category_fetches_only_the_commits_up_to_the_cut() -> Result<()> {
    const JOBS: usize = 300;
    const ORDERS: usize = 3;
    const BATCH: u32 = 10;

    let mut f = Fixture::new(pges_postgres::store::Outbox::Disabled).await?;
    // Write and sequence on a pool of their own, closed before counting, so their scans are reported.
    let setup = f.db.pool().await?;
    f.pool = setup.clone();
    f.store = pges_postgres::store::Store::open(
        setup.clone(),
        pges_postgres::store::StoreConfig::new(common::STORE_NAME),
    )
    .await?;
    let (job, order) = (stream::StreamId::new_v7(), stream::StreamId::new_v7());
    for n in 1..=JOBS {
        f.append(Cat::Jobs, job, 1).await?;
        if n % (JOBS / ORDERS) == JOBS / ORDERS / 2 {
            f.append(Cat::Orders, order, 1).await?;
        }
    }
    f.sequence_all().await?;
    let until = common::head(&setup).await?;
    let expected = expected(&f, &[Cat::Jobs, Cat::Orders], until).await?;
    setup.close().await;
    f.pool = f.db.pool().await?;
    let before = scans_of_commits(&f, "orders").await?;

    let reading = f.db.pool().await?;
    let store = pges_postgres::store::Store::open(
        reading.clone(),
        pges_postgres::store::StoreConfig::new(common::STORE_NAME),
    )
    .await?;
    let finished = store
        .backfill(subscription_name!("skewed"), pges_testing::position(until))
        .categories(non_empty![category!("jobs"), category!("orders")])
        .batch_size(NonZeroU32::new(BATCH).unwrap())
        .run_pg(Recorder::new("skewed"))
        .await?;
    reading.close().await;
    let scans = scans_of_commits(&f, "orders").await? - before;

    assert_eq!(finished.events, (JOBS + ORDERS) as u64);
    check(&f, "skewed", &expected).await?;
    let pages = JOBS as i64 / i64::from(BATCH);
    println!("{scans} scans of the orders commits over {pages} pages, for {ORDERS} commits");
    assert!(
        scans <= ORDERS as i64,
        "{scans} scans of the orders commits over {pages} pages, for {ORDERS} commits"
    );

    f.finish().await
}

/// The scans (sequential and index) of the leaves of `es.commits_<category>`, once the backends that ran
/// them have exited and reported them: the same in three reads in a row.
async fn scans_of_commits(f: &Fixture, category: &str) -> Result<i64> {
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    let (mut previous, mut same) = (-1, 0);
    loop {
        let mut conn = f.db.connect().await?;
        let scans: i64 = sqlx::query_scalar(
            "SELECT coalesce(sum(coalesce(seq_scan, 0) + coalesce(idx_scan, 0)), 0)::int8
             FROM pg_stat_user_tables WHERE schemaname = 'es' AND relname LIKE $1",
        )
        .bind(format!("commits\\_{category}\\_\\_%"))
        .fetch_one(&mut conn)
        .await?;
        conn.close().await?;
        same = if scans == previous { same + 1 } else { 0 };
        if same == 2 {
            return Ok(scans);
        }
        assert!(
            std::time::Instant::now() < deadline,
            "the scans of commits_{category} kept changing"
        );

        previous = scans;
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// Appends `streams` streams in each of jobs, orders and parts, `commits` single and double event commits
/// each, interleaved, and sequences them. Returns the streams.
async fn write(f: &Fixture, streams: usize, commits: u32) -> Result<Vec<(Cat, stream::StreamId)>> {
    let streams: Vec<(Cat, stream::StreamId)> = (0..streams)
        .flat_map(|_| {
            [Cat::Jobs, Cat::Orders, Cat::Parts].map(|cat| (cat, stream::StreamId::new_v7()))
        })
        .collect();
    for round in 0..commits {
        for (cat, id) in &streams {
            f.append(*cat, *id, 1 + round % 2).await?;
        }
    }
    f.sequence_all().await?;

    Ok(streams)
}

/// One more commit on every stream, sequenced, after the backfill's end.
async fn write_more(f: &Fixture, streams: &[(Cat, stream::StreamId)]) -> Result<()> {
    for (cat, id) in streams {
        f.append(*cat, *id, 1).await?;
    }
    f.sequence_all().await
}

/// The event positions of `cats` up to `until`.
async fn expected(f: &Fixture, cats: &[Cat], until: u64) -> Result<BTreeSet<i64>> {
    let ids: Vec<_> = cats.iter().map(|cat| f.id(*cat)).collect();

    Ok(f.expected(&ids)
        .await?
        .into_iter()
        .map(|(position, _)| position)
        .filter(|position| *position <= until as i64)
        .collect())
}

/// Every expected position was recorded exactly once, and each stream's positions in increasing order.
async fn check(f: &Fixture, subscriber: &str, expected: &BTreeSet<i64>) -> Result<()> {
    let rows = delivered_with_streams(&f.pool, subscriber).await?;
    let positions: Vec<i64> = rows.iter().map(|(position, _)| *position).collect();
    let unique: BTreeSet<i64> = positions.iter().copied().collect();
    ensure!(
        unique.len() == positions.len(),
        "some events were handled twice: {positions:?}"
    );
    ensure!(
        &unique == expected,
        "handled {unique:?}, expected {expected:?}"
    );

    let mut last: BTreeMap<Uuid, i64> = BTreeMap::new();
    for (position, stream_id) in rows {
        if let Some(previous) = last.insert(stream_id, position) {
            ensure!(
                previous < position,
                "stream {stream_id}: {position} handled after {previous}"
            );
        }
    }

    Ok(())
}

/// What a recorder recorded, in order, with each event's stream.
async fn delivered_with_streams(pool: &PgPool, subscriber: &str) -> Result<Vec<(i64, Uuid)>> {
    Ok(sqlx::query_as(
        "SELECT d.position, c.stream_id FROM public.delivered d
         JOIN es.commits c ON c.commit_id = d.commit_id
         WHERE d.subscriber = $1 ORDER BY d.seq",
    )
    .bind(subscriber)
    .fetch_all(pool)
    .await?)
}

/// Records like [`Recorder`], but fails fatally on a batch that holds `fail_at`, after recording it.
#[derive(Clone)]
struct Failing {
    recorder: Recorder,
    fail_at: position::Position,
}

impl handler::PgHandler for Failing {
    async fn handle(
        &mut self,
        tx: &mut PgConnection,
        batch: &batch::Batch,
    ) -> Result<(), handler::HandlerError> {
        self.recorder.record(tx, batch).await?;
        if batch.iter().any(|record| record.position() == self.fail_at) {
            return Err(handler::HandlerError::Fatal("injected failure".into()));
        }
        Ok(())
    }
}

/// Records like [`Recorder`], then begins a savepoint on its aborted transaction, which fails and makes sqlx
/// end the whole transaction, and returns `Ok`.
#[derive(Clone)]
struct FailsASavepoint {
    recorder: Recorder,
}

impl handler::PgHandler for FailsASavepoint {
    async fn handle(
        &mut self,
        tx: &mut PgConnection,
        batch: &batch::Batch,
    ) -> Result<(), handler::HandlerError> {
        *self.recorder.calls.lock().unwrap() += 1;
        self.recorder.record(tx, batch).await?;
        let _ = sqlx::query("SELECT 1 / 0").execute(&mut *tx).await;
        let _ = tx.begin().await;
        Ok(())
    }
}

/// Records like [`Recorder`], slowly.
#[derive(Clone)]
struct Slow {
    recorder: Recorder,
}

impl handler::PgHandler for Slow {
    async fn handle(
        &mut self,
        tx: &mut PgConnection,
        batch: &batch::Batch,
    ) -> Result<(), handler::HandlerError> {
        self.recorder.record(tx, batch).await?;
        tokio::time::sleep(Duration::from_millis(40)).await;
        Ok(())
    }
}

/// Plays a second run of the same backfill: moves every lane's cursor on another connection before
/// returning.
#[derive(Clone)]
struct Overtaking {
    pool: PgPool,
    calls: Arc<AtomicUsize>,
}

impl handler::PgHandler for Overtaking {
    async fn handle(
        &mut self,
        _tx: &mut PgConnection,
        batch: &batch::Batch,
    ) -> Result<(), handler::HandlerError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        sqlx::query("UPDATE es.backfills SET position = $1 WHERE name = 'overtaken'")
            .bind(batch.last_position().get() as i64)
            .execute(&self.pool)
            .await?;
        Ok(())
    }
}
