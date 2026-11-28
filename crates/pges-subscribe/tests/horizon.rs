//! The retention horizon guard: a cursor behind a category's horizon stops the subscription with
//! `CursorBeforeHorizon`, checked inside the read statement.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

mod common;

use std::num::NonZeroU32;

use anyhow::Result;
use sqlx::Executor as _;

use pges_core::{category, interval, non_empty, position, stream};
use pges_subscribe::{
    subscription::{self, StoreExt},
    subscription_name,
    waker::recheck,
};

use common::{Cat, Fixture, Recorder};

async fn set_horizon(f: &Fixture, cat: Cat, horizon: i64) -> Result<()> {
    sqlx::query("UPDATE es.categories SET horizon_position = $2 WHERE category_id = $1")
        .bind(f.id(cat).get())
        .bind(horizon)
        .execute(&f.pool)
        .await?;
    Ok(())
}

async fn stopped_with(
    subscription: subscription::Subscription,
) -> Result<subscription::SubscribeError> {
    common::Running::spawn(subscription).stopped().await
}

#[tokio::test]
async fn a_cursor_behind_the_horizon_stops_the_subscription() -> Result<()> {
    let f = Fixture::new(pges_postgres::store::Outbox::Disabled).await?;
    let (job, order) = (stream::StreamId::new_v7(), stream::StreamId::new_v7());
    for _ in 0..3 {
        f.append(Cat::Jobs, job, 1).await?;
        f.append(Cat::Orders, order, 1).await?;
    }
    f.sequence_all().await?;
    // Retention removed jobs up to position 3.
    set_horizon(&f, Cat::Jobs, 3).await?;

    let error = stopped_with(
        f.store
            .subscription(subscription_name!("behind"))
            .categories(non_empty![category!("jobs")])
            .run_pg(Recorder::new("behind")),
    )
    .await?;
    match error {
        subscription::SubscribeError::CursorBeforeHorizon {
            category,
            cursor,
            horizon,
        } => {
            assert_eq!(category, f.id(Cat::Jobs));
            assert_eq!(cursor, position::Position::BEGINNING);
            assert_eq!(horizon, pges_testing::position(3));
        }
        other => panic!("unexpected error {other:?}"),
    }

    // Over several categories the orders rows are not delivered either: the batch fails as a whole.
    let error = stopped_with(
        f.store
            .subscription(subscription_name!("behind-both"))
            .categories(non_empty![category!("orders"), category!("jobs")])
            .run_pg(Recorder::new("behind-both")),
    )
    .await?;
    assert!(
        matches!(error, subscription::SubscribeError::CursorBeforeHorizon { category, .. }
            if category == f.id(Cat::Jobs)),
        "{error:?}"
    );
    assert!(f.delivered("behind-both").await?.is_empty());

    // A subscription starting at the horizon reads what is left.
    let running = common::Running::spawn(
        f.store
            .subscription(subscription_name!("at-horizon"))
            .categories(non_empty![category!("jobs")])
            .start(subscription::Start::Position(pges_testing::position(3)))
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
        [5]
    );

    f.finish().await
}

#[tokio::test]
async fn a_horizon_that_passes_a_running_subscription_stops_it() -> Result<()> {
    let f = Fixture::new(pges_postgres::store::Outbox::Disabled).await?;
    let job = stream::StreamId::new_v7();
    f.append(Cat::Jobs, job, 1).await?;
    f.sequence_all().await?;

    let running = common::Running::spawn(
        f.store
            .subscription(subscription_name!("overtaken"))
            .categories(non_empty![category!("jobs")])
            .waker(recheck::Recheck::every(interval!(5 ms)))
            .run_pg(Recorder::new("overtaken")),
    );
    running.wait_position(1).await?;

    // New events, then retention passes the cursor before the subscription reads them.
    for _ in 0..3 {
        f.append(Cat::Jobs, job, 1).await?;
    }
    set_horizon(&f, Cat::Jobs, 3).await?;
    f.sequence_all().await?;

    let error = running.stopped().await?;
    assert!(
        matches!(error, subscription::SubscribeError::CursorBeforeHorizon { cursor, .. }
            if cursor == pges_testing::position(1)),
        "{error:?}"
    );
    assert_eq!(f.cursor("overtaken").await?, Some(1));

    f.finish().await
}

/// A category read takes two statements: the log page, with the horizon check, then the page's commits.
/// Retention that raises the horizon and removes the commits in between stops the subscription; it does not
/// skip the missing commits as deleted. A lock on the category's commits holds the subscription between the
/// two statements.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn retention_between_the_log_page_and_its_commits_stops_the_subscription() -> Result<()> {
    let f = Fixture::new(pges_postgres::store::Outbox::Disabled).await?;
    let job = stream::StreamId::new_v7();
    for _ in 0..3 {
        f.append(Cat::Jobs, job, 1).await?;
    }
    f.sequence_all().await?;

    let mut retention = f.pool.begin().await?;
    sqlx::query("LOCK TABLE es.commits_jobs IN ACCESS EXCLUSIVE MODE")
        .execute(&mut *retention)
        .await?;
    let running = common::Running::spawn(
        f.store
            .subscription(subscription_name!("raced"))
            .categories(non_empty![category!("jobs")])
            .run_pg(Recorder::new("raced")),
    );
    tokio::time::timeout(std::time::Duration::from_secs(20), async {
        loop {
            let waiting: i64 = sqlx::query_scalar(
                "SELECT count(*) FROM pg_stat_activity
                 WHERE datname = current_database() AND wait_event_type = 'Lock'
                   AND query LIKE '%commits_jobs%'",
            )
            .fetch_one(&f.pool)
            .await?;
            if waiting > 0 {
                return anyhow::Ok(());
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await??;

    // As apply_retention does: the horizon first, then the commits go.
    sqlx::query("UPDATE es.categories SET horizon_position = 3 WHERE category_id = $1")
        .bind(f.id(Cat::Jobs).get())
        .execute(&mut *retention)
        .await?;
    sqlx::query("DELETE FROM es.commits_jobs")
        .execute(&mut *retention)
        .await?;
    retention.commit().await?;

    let error = running.stopped().await?;
    match error {
        subscription::SubscribeError::CursorBeforeHorizon {
            category,
            cursor,
            horizon,
        } => {
            assert_eq!(category, f.id(Cat::Jobs));
            assert_eq!(cursor, position::Position::BEGINNING);
            assert_eq!(horizon, pges_testing::position(3));
        }
        other => panic!("unexpected error {other:?}"),
    }
    assert!(f.delivered("raced").await?.is_empty());

    f.finish().await
}

/// Retention can keep a commit positioned below the horizon: one that committed before the dropped leaf's
/// last commit. A subscription created at `Start::Retained` reads every commit retention kept, listed or
/// store-wide, and skips the dropped ones; one from the beginning still fails.
#[tokio::test]
async fn a_subscription_started_at_retained_reads_commits_kept_below_the_horizon() -> Result<()> {
    let f = Fixture::new(pges_postgres::store::Outbox::Disabled).await?;
    let (job, order) = (stream::StreamId::new_v7(), stream::StreamId::new_v7());
    for _ in 0..3 {
        f.append(Cat::Jobs, job, 1).await?;
        f.append(Cat::Orders, order, 1).await?;
    }
    f.sequence_all().await?;
    // As apply_retention records it: jobs at position 3 is dropped, jobs at position 1 is kept.
    sqlx::query(
        "UPDATE es.categories SET horizon_position = 3, start_position = 0 WHERE category_id = $1",
    )
    .bind(f.id(Cat::Jobs).get())
    .execute(&f.pool)
    .await?;
    sqlx::query(
        "DELETE FROM es.commits_jobs WHERE commit_id = (SELECT commit_id FROM es.log WHERE last_position = 3)",
    )
    .execute(&f.pool)
    .await?;

    let error = stopped_with(
        f.store
            .subscription(subscription_name!("beginning"))
            .categories(non_empty![category!("jobs")])
            .run_pg(Recorder::new("beginning")),
    )
    .await?;
    assert!(
        matches!(
            error,
            subscription::SubscribeError::CursorBeforeHorizon { .. }
        ),
        "{error:?}"
    );

    for (name, all) in [("listed", false), ("all", true)] {
        let builder = f
            .store
            .subscription(subscription::SubscriptionName::parse(name)?)
            .start(subscription::Start::Retained)
            .waker(recheck::Recheck::every(interval!(5 ms)));
        let subscription = if all {
            builder.all_categories().run_pg(Recorder::new(name))
        } else {
            builder
                .categories(non_empty![category!("jobs"), category!("orders")])
                .run_pg(Recorder::new(name))
        };
        let running = common::Running::spawn(subscription);
        running.wait_position(6).await?;
        running.stop().await?;
        assert_eq!(
            f.delivered(name)
                .await?
                .iter()
                .map(|d| d.0)
                .collect::<Vec<_>>(),
            [1, 2, 4, 5, 6],
            "{name}"
        );
    }

    f.finish().await
}

/// `redefine(Start::Retained)` moves a subscription to the beginning and stores its `skip_through`, so the
/// redefined subscription reads every commit retention kept in its new categories, also one below the
/// horizon, and skips the dropped one.
#[tokio::test]
async fn a_subscription_redefined_at_retained_reads_commits_kept_below_the_horizon() -> Result<()> {
    tokio::time::timeout(std::time::Duration::from_secs(60), redefined_at_retained()).await?
}

async fn redefined_at_retained() -> Result<()> {
    let f = Fixture::new(pges_postgres::store::Outbox::Disabled).await?;
    let (job, order) = (stream::StreamId::new_v7(), stream::StreamId::new_v7());
    for _ in 0..3 {
        f.append(Cat::Jobs, job, 1).await?;
        f.append(Cat::Orders, order, 1).await?;
    }
    f.sequence_all().await?;
    // As apply_retention records it: jobs at position 3 is dropped, jobs at position 1 is kept.
    sqlx::query(
        "UPDATE es.categories SET horizon_position = 3, start_position = 0 WHERE category_id = $1",
    )
    .bind(f.id(Cat::Jobs).get())
    .execute(&f.pool)
    .await?;
    sqlx::query(
        "DELETE FROM es.commits_jobs WHERE commit_id = (SELECT commit_id FROM es.log WHERE last_position = 3)",
    )
    .execute(&f.pool)
    .await?;

    let builder = || {
        f.store
            .subscription(subscription_name!("redefined"))
            .waker(recheck::Recheck::every(interval!(5 ms)))
    };
    let running = common::Running::spawn(
        builder()
            .categories(non_empty![category!("orders")])
            .run_pg(Recorder::new("before")),
    );
    running.wait_position(6).await?;
    running.stop().await?;
    let skip_through: i64 =
        sqlx::query_scalar("SELECT skip_through FROM es.subscriptions WHERE name = 'redefined'")
            .fetch_one(&f.pool)
            .await?;
    assert_eq!(
        skip_through, 0,
        "a subscription from the beginning skips nothing"
    );

    let running = common::Running::spawn(
        builder()
            .categories(non_empty![category!("jobs"), category!("orders")])
            .redefine(subscription::Start::Retained)
            .run_pg(Recorder::new("after")),
    );
    if let Err(waiting) = running.wait_position(6).await {
        let error = running.stopped().await?;
        anyhow::bail!("{waiting:#}: the subscription stopped with {error:?}");
    }
    running.stop().await?;
    assert_eq!(
        f.delivered("after")
            .await?
            .iter()
            .map(|d| d.0)
            .collect::<Vec<_>>(),
        [1, 2, 4, 5, 6]
    );
    let skip_through: i64 =
        sqlx::query_scalar("SELECT skip_through FROM es.subscriptions WHERE name = 'redefined'")
            .fetch_one(&f.pool)
            .await?;
    assert_eq!(skip_through, 6, "the head when it was redefined");

    f.finish().await
}

/// A store-wide subscription started at `Start::Retained` does not page through the log rows of commits
/// retention dropped below a category's start: the log statement leaves them out, so the first page holds
/// the commits that are left, and the cursor moves once.
#[tokio::test]
async fn a_store_wide_retained_subscription_skips_dead_rows_in_one_page() -> Result<()> {
    let f = Fixture::new(pges_postgres::store::Outbox::Disabled).await?;
    let (job, order) = (stream::StreamId::new_v7(), stream::StreamId::new_v7());
    for _ in 0..10 {
        f.append(Cat::Jobs, job, 1).await?;
    }
    for _ in 0..2 {
        f.append(Cat::Orders, order, 1).await?;
    }
    f.sequence_all().await?;
    // As apply_retention records it after dropping every jobs commit, before prune_log deletes their rows.
    sqlx::query(
        "UPDATE es.categories SET horizon_position = 10, start_position = 10 WHERE category_id = $1",
    )
    .bind(f.id(Cat::Jobs).get())
    .execute(&f.pool)
    .await?;
    sqlx::query("DELETE FROM es.commits_jobs")
        .execute(&f.pool)
        .await?;
    // Every cursor the subscription stores, in order.
    f.pool
        .execute(
            "CREATE TABLE es.moved (seq int8 GENERATED ALWAYS AS IDENTITY, position int8 NOT NULL);
             CREATE FUNCTION es.record_move() RETURNS trigger LANGUAGE plpgsql AS
               $$ BEGIN INSERT INTO es.moved (position) VALUES (NEW.position); RETURN NEW; END $$;
             CREATE TRIGGER record_move AFTER UPDATE OF position ON es.subscriptions FOR EACH ROW
               WHEN (OLD.position IS DISTINCT FROM NEW.position) EXECUTE FUNCTION es.record_move();",
        )
        .await?;

    let running = common::Running::spawn(
        f.store
            .subscription(subscription_name!("retained"))
            .all_categories()
            .start(subscription::Start::Retained)
            .batch_size(NonZeroU32::new(2).unwrap())
            .waker(recheck::Recheck::every(interval!(5 ms)))
            .run_pg(Recorder::new("retained")),
    );
    running.wait_position(12).await?;
    running.stop().await?;

    assert_eq!(
        f.delivered("retained")
            .await?
            .iter()
            .map(|d| d.0)
            .collect::<Vec<_>>(),
        [11, 12]
    );
    let moved: Vec<i64> = sqlx::query_scalar("SELECT position FROM es.moved ORDER BY seq")
        .fetch_all(&f.pool)
        .await?;
    assert_eq!(moved, [12], "one page, not one per two dead rows");

    f.finish().await
}
