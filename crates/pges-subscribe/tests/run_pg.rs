//! `run_pg`: the handler's writes and the checkpoint commit together. Failures roll both back; leases fence
//! out a deposed instance; definitions and start positions.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

mod common;

use std::{
    num::NonZeroU32,
    sync::{Arc, Mutex},
    time::Duration,
};

use anyhow::{Context, Result};
use sqlx::{Connection, PgConnection, PgPool};
use tokio::sync::Notify;

use pges_core::{category, high_water, interval, non_empty, stream};
use pges_subscribe::{
    batch, handler,
    subscription::{self, StoreExt},
    subscription_name,
    waker::{self, recheck},
};
use pges_testing::{cancel, proxy};

use common::{Cat, Fixture, Recorder};

/// Records, then fails on the calls listed in `fail` (1-based) with the given kind.
struct Failing {
    recorder: Recorder,
    fail: Vec<usize>,
    transient: bool,
}

impl handler::PgHandler for Failing {
    async fn handle(
        &mut self,
        tx: &mut PgConnection,
        batch: &batch::Batch,
    ) -> Result<(), handler::HandlerError> {
        let call = {
            let mut calls = self.recorder.calls.lock().unwrap();
            *calls += 1;
            *calls
        };
        self.recorder.record(tx, batch).await?;
        if self.fail.contains(&call) {
            let error = format!("injected failure on call {call}").into();
            return Err(if self.transient {
                handler::HandlerError::Transient(error)
            } else {
                handler::HandlerError::Fatal(error)
            });
        }
        Ok(())
    }
}

async fn jobs(f: &Fixture, commits: usize) -> Result<stream::StreamId> {
    let id = stream::StreamId::new_v7();
    for _ in 0..commits {
        f.append(Cat::Jobs, id, 1).await?;
    }
    f.sequence_all().await?;
    Ok(id)
}

fn retry(attempts: u32) -> subscription::RetryPolicy {
    subscription::RetryPolicy::new(interval!(1 ms), NonZeroU32::new(attempts).unwrap())
}

#[tokio::test]
async fn a_failing_handler_rolls_back_its_writes_and_the_checkpoint() -> Result<()> {
    let f = Fixture::new(pges_postgres::store::Outbox::Disabled).await?;
    jobs(&f, 6).await?;

    let error = common::Running::spawn(
        f.store
            .subscription(subscription_name!("fatal"))
            .categories(non_empty![category!("jobs")])
            .batch_size(NonZeroU32::new(2).unwrap())
            .run_pg(Failing {
                recorder: Recorder::new("fatal"),
                fail: vec![2],
                transient: false,
            }),
    )
    .stopped()
    .await?;
    match error {
        subscription::SubscribeError::HandlerFailed { first, last, .. } => {
            assert_eq!(
                (first, last),
                (pges_testing::position(3), pges_testing::position(4))
            );
        }
        other => panic!("unexpected error {other:?}"),
    }
    // The second batch's rows rolled back with its checkpoint; the first batch's stayed.
    assert_eq!(
        f.delivered("fatal").await?,
        f.expected(&[f.id(Cat::Jobs)]).await?[..2]
    );
    assert_eq!(f.cursor("fatal").await?, Some(2));
    let leased: Option<Option<uuid::Uuid>> =
        sqlx::query_scalar("SELECT lease_owner FROM es.subscriptions WHERE name = 'fatal'")
            .fetch_optional(&f.pool)
            .await?;
    assert_eq!(
        leased,
        Some(None),
        "a stopping subscription releases its lease"
    );

    // A fixed handler resumes from the checkpoint: every event recorded once.
    let running = common::Running::spawn(
        f.store
            .subscription(subscription_name!("fatal"))
            .categories(non_empty![category!("jobs")])
            .batch_size(NonZeroU32::new(2).unwrap())
            .run_pg(Recorder::new("fatal")),
    );
    running.wait_position(6).await?;
    running.stop().await?;
    common::check_delivery(
        &f.delivered("fatal").await?,
        &f.expected(&[f.id(Cat::Jobs)]).await?,
    )?;

    f.finish().await
}

#[tokio::test]
async fn transient_errors_are_retried_and_applied_once() -> Result<()> {
    let f = Fixture::new(pges_postgres::store::Outbox::Disabled).await?;
    jobs(&f, 5).await?;

    let recorder = Recorder::new("transient");
    let running = common::Running::spawn(
        f.store
            .subscription(subscription_name!("transient"))
            .categories(non_empty![category!("jobs")])
            .batch_size(NonZeroU32::new(2).unwrap())
            .on_error(retry(3))
            .run_pg(Failing {
                recorder: recorder.clone(),
                fail: vec![1, 2, 4],
                transient: true,
            }),
    );
    running.wait_position(5).await?;
    running.stop().await?;

    common::check_delivery(
        &f.delivered("transient").await?,
        &f.expected(&[f.id(Cat::Jobs)]).await?,
    )?;
    assert_eq!(recorder.calls(), 6, "3 batches, 3 retried attempts");

    f.finish().await
}

/// Runs a failing statement and ignores its error: PostgreSQL would turn the COMMIT into a ROLLBACK.
struct IgnoresErrors;

impl handler::PgHandler for IgnoresErrors {
    async fn handle(
        &mut self,
        tx: &mut PgConnection,
        _batch: &batch::Batch,
    ) -> Result<(), handler::HandlerError> {
        let _ = sqlx::query("SELECT 1 / 0").execute(&mut *tx).await;
        Ok(())
    }
}

#[tokio::test]
async fn a_handler_that_leaves_its_transaction_aborted_is_not_checkpointed() -> Result<()> {
    let f = Fixture::new(pges_postgres::store::Outbox::Disabled).await?;
    jobs(&f, 2).await?;

    let error = common::Running::spawn(
        f.store
            .subscription(subscription_name!("aborted"))
            .categories(non_empty![category!("jobs")])
            .on_error(retry(2))
            .run_pg(IgnoresErrors),
    )
    .stopped()
    .await?;
    assert!(
        matches!(&error, subscription::SubscribeError::HandlerFailed { source, .. }
            if source.to_string().contains("aborted")),
        "{error:?}"
    );
    assert_eq!(f.cursor("aborted").await?, Some(0));

    f.finish().await
}

/// Records, then begins a savepoint on its aborted transaction, which fails and makes sqlx end the whole
/// transaction, and returns `Ok`.
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

#[tokio::test]
async fn a_handler_whose_savepoint_begin_fails_is_not_checkpointed() -> Result<()> {
    let f = Fixture::new(pges_postgres::store::Outbox::Disabled).await?;
    jobs(&f, 2).await?;
    let recorder = Recorder::new("savepoint");

    let error = common::Running::spawn(
        f.store
            .subscription(subscription_name!("savepoint"))
            .categories(non_empty![category!("jobs")])
            .on_error(retry(2))
            .run_pg(FailsASavepoint {
                recorder: recorder.clone(),
            }),
    )
    .stopped()
    .await?;
    assert!(
        matches!(&error, subscription::SubscribeError::HandlerFailed { source, .. }
            if source.to_string().contains("transaction ended")),
        "{error:?}"
    );
    assert_eq!(recorder.calls(), 2, "retried");
    assert_eq!(f.cursor("savepoint").await?, Some(0));
    assert!(f.delivered("savepoint").await?.is_empty());

    f.finish().await
}

/// Records, then drops a savepoint begin on its transaction while the begin is on the server, which makes
/// sqlx end the whole transaction, and returns `Ok`. Its store's pool goes through `proxy`.
struct DropsASavepoint {
    recorder: Recorder,
    proxy: Arc<proxy::Proxy>,
    side: PgPool,
}

impl handler::PgHandler for DropsASavepoint {
    async fn handle(
        &mut self,
        tx: &mut PgConnection,
        batch: &batch::Batch,
    ) -> Result<(), handler::HandlerError> {
        self.recorder.record(tx, batch).await?;
        let mut side = self.side.acquire().await?;
        self.proxy.hold(proxy::Direction::ToClient);
        let sent = cancel::wait_until_waiting(&mut side, "SAVEPOINT _sqlx_savepoint_1");
        let dropped = cancel::poll_check_drop(tx.begin(), 1, sent).await;
        self.proxy.release();
        match dropped {
            Ok(None) => Ok(()),
            Ok(Some(_)) => Err(handler::HandlerError::Fatal(
                "the savepoint completed".into(),
            )),
            Err(error) => Err(handler::HandlerError::Fatal(error.into())),
        }
    }
}

#[tokio::test]
async fn a_handler_that_drops_a_savepoint_begin_is_not_checkpointed() -> Result<()> {
    let f = Fixture::new(pges_postgres::store::Outbox::Disabled).await?;
    jobs(&f, 2).await?;
    let proxy = Arc::new(proxy::Proxy::start(pges_testing::server().await?).await?);
    let proxied = sqlx::postgres::PgPoolOptions::new()
        .max_connections(4)
        .connect(&proxy.url(&f.db)?)
        .await?;
    let store = pges_postgres::store::Store::open(
        proxied.clone(),
        pges_postgres::store::StoreConfig::new(common::STORE_NAME),
    )
    .await?;

    // One attempt: a retry could find the first attempt's connection still waiting after its savepoint.
    let error = common::Running::spawn(
        store
            .subscription(subscription_name!("dropped"))
            .categories(non_empty![category!("jobs")])
            .on_error(retry(1))
            .run_pg(DropsASavepoint {
                recorder: Recorder::new("dropped"),
                proxy: Arc::clone(&proxy),
                side: f.pool.clone(),
            }),
    )
    .stopped()
    .await?;
    assert!(
        matches!(&error, subscription::SubscribeError::HandlerFailed { source, .. }
            if source.to_string().contains("transaction ended")),
        "{error:?}"
    );
    assert_eq!(f.cursor("dropped").await?, Some(0));
    assert!(f.delivered("dropped").await?.is_empty());

    proxied.close().await;
    f.finish().await
}

/// Panics on every batch: a bug in the handler.
struct Panics;

impl handler::PgHandler for Panics {
    async fn handle(
        &mut self,
        _tx: &mut PgConnection,
        _batch: &batch::Batch,
    ) -> Result<(), handler::HandlerError> {
        panic!("injected handler panic");
    }
}

#[tokio::test]
async fn a_panicking_handler_leaves_the_status_stopped() -> Result<()> {
    let f = Fixture::new(pges_postgres::store::Outbox::Disabled).await?;
    jobs(&f, 2).await?;

    let handle = f
        .store
        .subscription(subscription_name!("panics"))
        .categories(non_empty![category!("jobs")])
        .run_pg(Panics)
        .spawn();
    let status = handle.status_changes();
    let joined = tokio::time::timeout(Duration::from_secs(20), handle.join())
        .await
        .context("the subscription did not stop")?;

    assert!(
        matches!(joined, Err(subscription::SubscribeError::Aborted)),
        "{joined:?}"
    );
    assert!(
        matches!(status.borrow().state, subscription::State::Stopped),
        "a dead subscription reports {:?}",
        status.borrow().state
    );
    assert_eq!(f.cursor("panics").await?, Some(0));

    f.finish().await
}

/// Writes two equal keys into a table whose unique constraint is deferred: every COMMIT is refused.
struct BreaksADeferredConstraint;

impl handler::PgHandler for BreaksADeferredConstraint {
    async fn handle(
        &mut self,
        tx: &mut PgConnection,
        _batch: &batch::Batch,
    ) -> Result<(), handler::HandlerError> {
        sqlx::query("INSERT INTO public.deferred VALUES (1), (1)")
            .execute(&mut *tx)
            .await?;
        Ok(())
    }
}

/// A refused COMMIT is the handler's failure: it stops the run with `HandlerFailed`, instead of being retried
/// for good as a database error.
#[tokio::test]
async fn a_refused_commit_is_a_handler_failure() -> Result<()> {
    let f = Fixture::new(pges_postgres::store::Outbox::Disabled).await?;
    jobs(&f, 1).await?;
    sqlx::query("CREATE TABLE public.deferred (k int UNIQUE DEFERRABLE INITIALLY DEFERRED)")
        .execute(&f.pool)
        .await?;

    let error = common::Running::spawn(
        f.store
            .subscription(subscription_name!("deferred"))
            .categories(non_empty![category!("jobs")])
            .retry_interval(interval!(10 ms))
            .on_error(retry(2))
            .run_pg(BreaksADeferredConstraint),
    )
    .stopped()
    .await?;
    assert!(
        matches!(&error, subscription::SubscribeError::HandlerFailed { source, .. }
            if source.to_string().contains("duplicate key")),
        "{error:?}"
    );
    assert_eq!(f.cursor("deferred").await?, Some(0));

    f.finish().await
}

/// Takes its snapshot in REPEATABLE READ, then records after a pause longer than half the lease, so a side
/// renewal updates the subscription's row after the snapshot.
struct RepeatableRead {
    recorder: Recorder,
    pause: Duration,
}

impl handler::PgHandler for RepeatableRead {
    async fn handle(
        &mut self,
        tx: &mut PgConnection,
        batch: &batch::Batch,
    ) -> Result<(), handler::HandlerError> {
        *self.recorder.calls.lock().unwrap() += 1;
        sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
            .execute(&mut *tx)
            .await?;
        sqlx::query("SELECT count(*) FROM public.delivered")
            .execute(&mut *tx)
            .await?;
        tokio::time::sleep(self.pause).await;
        self.recorder.record(tx, batch).await?;
        Ok(())
    }
}

/// The checkpoint's serialization failure follows the retry policy: the run stops once it runs out of
/// attempts, instead of retrying the batch for good as a database error.
#[tokio::test]
async fn a_checkpoint_serialization_failure_follows_the_retry_policy() -> Result<()> {
    let f = Fixture::new(pges_postgres::store::Outbox::Disabled).await?;
    jobs(&f, 1).await?;
    let recorder = Recorder::new("repeatable");

    let error = common::Running::spawn(
        f.store
            .subscription(subscription_name!("repeatable"))
            .categories(non_empty![category!("jobs")])
            .lease(interval!(400 ms))
            .retry_interval(interval!(10 ms))
            .on_error(retry(2))
            .run_pg(RepeatableRead {
                recorder: recorder.clone(),
                pause: Duration::from_millis(400),
            }),
    )
    .stopped()
    .await?;
    assert!(
        matches!(&error, subscription::SubscribeError::HandlerFailed { source, .. }
            if source.to_string().contains("could not serialize")),
        "{error:?}"
    );
    assert_eq!(recorder.calls(), 2, "retried per the policy");
    assert_eq!(f.cursor("repeatable").await?, Some(0));
    assert!(f.delivered("repeatable").await?.is_empty());

    f.finish().await
}

#[tokio::test]
async fn a_standby_takes_over_from_the_checkpoint() -> Result<()> {
    let f = Fixture::new(pges_postgres::store::Outbox::Disabled).await?;
    let id = jobs(&f, 3).await?;

    let subscription = |f: &Fixture| {
        f.store
            .subscription(subscription_name!("shared"))
            .categories(non_empty![category!("jobs")])
            .batch_size(NonZeroU32::new(2).unwrap())
            .acquire_interval(interval!(20 ms))
            .waker(recheck::Recheck::every(interval!(5 ms)))
            .run_pg(Recorder::new("shared"))
    };
    let first = common::Running::spawn(subscription(&f));
    first.wait_position(3).await?;
    let second = common::Running::spawn(subscription(&f));
    common::wait_state(second.handle.status_changes(), |s| {
        matches!(s, subscription::State::Standby)
    })
    .await?;

    for _ in 0..3 {
        f.append(Cat::Jobs, id, 1).await?;
    }
    f.sequence_all().await?;
    first.wait_position(6).await?;
    first.stop().await?;

    for _ in 0..3 {
        f.append(Cat::Jobs, id, 2).await?;
    }
    f.sequence_all().await?;
    second.wait_position(12).await?;
    second.stop().await?;

    common::check_delivery(
        &f.delivered("shared").await?,
        &f.expected(&[f.id(Cat::Jobs)]).await?,
    )?;

    f.finish().await
}

/// Records, but holds its first call until the test opens the gate.
struct Gated {
    recorder: Recorder,
    entered: Arc<Notify>,
    gate: Arc<Notify>,
    first: Arc<Mutex<bool>>,
}

impl handler::PgHandler for Gated {
    async fn handle(
        &mut self,
        tx: &mut PgConnection,
        batch: &batch::Batch,
    ) -> Result<(), handler::HandlerError> {
        let first = std::mem::replace(&mut *self.first.lock().unwrap(), false);
        if first {
            self.entered.notify_one();
            self.gate.notified().await;
        }
        self.recorder.record(tx, batch).await?;
        Ok(())
    }
}

#[tokio::test]
async fn a_deposed_instance_is_fenced_out() -> Result<()> {
    let f = Fixture::new(pges_postgres::store::Outbox::Disabled).await?;
    jobs(&f, 4).await?;

    let (entered, gate) = (Arc::new(Notify::new()), Arc::new(Notify::new()));
    let running = common::Running::spawn(
        f.store
            .subscription(subscription_name!("fenced"))
            .categories(non_empty![category!("jobs")])
            .lease(interval!(300 ms))
            .acquire_interval(interval!(20 ms))
            .run_pg(Gated {
                recorder: Recorder::new("fenced"),
                entered: entered.clone(),
                gate: gate.clone(),
                first: Arc::new(Mutex::new(true)),
            }),
    );
    tokio::time::timeout(Duration::from_secs(20), entered.notified())
        .await
        .context("the handler was not called")?;

    // Another instance took over in the meantime: the in-flight batch must not commit.
    sqlx::query("UPDATE es.subscriptions SET fence = fence + 1 WHERE name = 'fenced'")
        .execute(&f.pool)
        .await?;
    let mut status = running.handle.status_changes();
    gate.notify_one();
    tokio::time::timeout(
        Duration::from_secs(20),
        status.wait_for(|s| matches!(s.state, subscription::State::Standby)),
    )
    .await
    .context("the deposed instance did not stand down")??;
    assert!(f.delivered("fenced").await?.is_empty());
    assert_eq!(f.cursor("fenced").await?, Some(0));

    // Once the lease lapses, it takes the lease back and delivers everything once.
    running.wait_position(4).await?;
    running.stop().await?;
    common::check_delivery(
        &f.delivered("fenced").await?,
        &f.expected(&[f.id(Cat::Jobs)]).await?,
    )?;

    f.finish().await
}

#[tokio::test]
async fn a_changed_definition_needs_redefine() -> Result<()> {
    let f = Fixture::new(pges_postgres::store::Outbox::Disabled).await?;
    let order = stream::StreamId::new_v7();
    jobs(&f, 2).await?;
    f.append(Cat::Orders, order, 1).await?;
    f.sequence_all().await?;

    let running = common::Running::spawn(
        f.store
            .subscription(subscription_name!("board"))
            .categories(non_empty![category!("jobs")])
            .run_pg(Recorder::new("v1")),
    );
    running.wait_position(3).await?;
    running.stop().await?;

    let changed = f
        .store
        .subscription(subscription_name!("board"))
        .categories(non_empty![category!("jobs"), category!("orders")])
        .run_pg(Recorder::new("v2"))
        .run_until(std::future::pending())
        .await;
    match changed {
        Err(subscription::SubscribeError::DefinitionChanged { stored, requested }) => {
            assert_eq!(stored, [f.id(Cat::Jobs)]);
            assert_eq!(requested, [f.id(Cat::Jobs), f.id(Cat::Orders)]);
        }
        other => panic!("unexpected result {other:?}"),
    }

    let running = common::Running::spawn(
        f.store
            .subscription(subscription_name!("board"))
            .categories(non_empty![category!("orders"), category!("jobs")])
            .redefine(subscription::Start::Beginning)
            .run_pg(Recorder::new("v2")),
    );
    running.wait_position(3).await?;
    running.stop().await?;
    common::check_delivery(
        &f.delivered("v2").await?,
        &f.expected(&[f.id(Cat::Jobs), f.id(Cat::Orders)]).await?,
    )?;

    f.finish().await
}

#[tokio::test]
async fn new_subscriptions_start_where_asked() -> Result<()> {
    let f = Fixture::new(pges_postgres::store::Outbox::Disabled).await?;
    let id = jobs(&f, 4).await?;

    let now = common::Running::spawn(
        f.store
            .subscription(subscription_name!("from-now"))
            .categories(non_empty![category!("jobs")])
            .start(subscription::Start::Now)
            .waker(recheck::Recheck::every(interval!(5 ms)))
            .run_pg(Recorder::new("now")),
    );
    let at = common::Running::spawn(
        f.store
            .subscription(subscription_name!("from-two"))
            .categories(non_empty![category!("jobs")])
            .start(subscription::Start::Position(pges_testing::position(2)))
            .run_pg(Recorder::new("at")),
    );
    at.wait_position(4).await?;
    now.wait_position(4).await?;
    f.append(Cat::Jobs, id, 1).await?;
    f.sequence_all().await?;
    now.wait_position(5).await?;
    now.stop().await?;
    at.stop().await?;

    assert_eq!(f.delivered("now").await?, [(5, f.id(Cat::Jobs).get())]);
    assert_eq!(
        f.delivered("at")
            .await?
            .iter()
            .map(|d| d.0)
            .collect::<Vec<_>>(),
        [3, 4]
    );

    f.finish().await
}

/// A bad name, no categories or a zero batch size does not compile (`tests/ui/`); an unregistered category
/// is a fact of the store.
#[tokio::test]
async fn an_unknown_category_fails_at_start() -> Result<()> {
    let f = Fixture::new(pges_postgres::store::Outbox::Disabled).await?;

    let unknown = f
        .store
        .subscription(subscription_name!("unknown"))
        .categories(non_empty![category!("jobs"), category!("nope")])
        .run_pg(Recorder::new("x"))
        .run_until(std::future::pending())
        .await;
    assert!(
        matches!(
            unknown,
            Err(subscription::SubscribeError::UnknownCategory(name)) if name.as_str() == "nope"
        ),
        "{unknown:?}"
    );

    f.finish().await
}

/// A COMMIT can succeed while its reply is lost: the batch is applied and checkpointed, but the subscription
/// sees a database error. After any retried error it reads the stored cursor again, so the batch is not
/// applied twice. The test plays the lost reply: while reads fail, it applies the next batch and moves the
/// cursor with the instance's own fence, as that COMMIT would have.
#[tokio::test]
async fn after_a_database_error_the_stored_cursor_wins() -> Result<()> {
    let f = Fixture::new(pges_postgres::store::Outbox::Disabled).await?;
    let id = jobs(&f, 2).await?;

    // This test expects recoveries, so it stops the handle itself instead of `Running::stop`.
    let handle = f
        .store
        .subscription(subscription_name!("ambiguous"))
        .categories(non_empty![category!("jobs")])
        .retry_interval(interval!(50 ms))
        .waker(recheck::Recheck::every(interval!(5 ms)))
        .run_pg(Recorder::new("ambiguous"))
        .spawn();
    common::wait_position(handle.status_changes(), 2).await?;

    for _ in 0..2 {
        f.append(Cat::Jobs, id, 1).await?;
    }
    // The partition stays attached, so the sequencer still sees the commits; the subscription's read of
    // `commits_jobs` fails.
    sqlx::query("ALTER TABLE es.commits_jobs RENAME TO commits_jobs_hidden")
        .execute(&f.pool)
        .await?;
    f.sequence_all().await?;
    common::wait_state(handle.status_changes(), |s| {
        matches!(s, subscription::State::Recovering(_))
    })
    .await?;

    let expected = f.expected(&[f.id(Cat::Jobs)]).await?;
    let mut tx = f.pool.begin().await?;
    for (position, category_id) in &expected[2..] {
        sqlx::query(
            "INSERT INTO public.delivered (position, commit_id, category_id, subscriber)
             VALUES ($1, gen_random_uuid(), $2, 'ambiguous')",
        )
        .bind(position)
        .bind(category_id)
        .execute(&mut *tx)
        .await?;
    }
    sqlx::query("UPDATE es.subscriptions SET position = 4 WHERE name = 'ambiguous'")
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    sqlx::query("ALTER TABLE es.commits_jobs_hidden RENAME TO commits_jobs")
        .execute(&f.pool)
        .await?;

    f.append(Cat::Jobs, id, 1).await?;
    f.sequence_all().await?;
    common::wait_position(handle.status_changes(), 5).await?;
    handle.shutdown().await?;

    common::check_delivery(
        &f.delivered("ambiguous").await?,
        &f.expected(&[f.id(Cat::Jobs)]).await?,
    )?;

    f.finish().await
}

/// Records after a pause, as a slow projection would.
struct Slow {
    recorder: Recorder,
    pause: Duration,
}

impl handler::PgHandler for Slow {
    async fn handle(
        &mut self,
        tx: &mut PgConnection,
        batch: &batch::Batch,
    ) -> Result<(), handler::HandlerError> {
        tokio::time::sleep(self.pause).await;
        self.recorder.record(tx, batch).await?;
        Ok(())
    }
}

/// A lease renewed by a checkpoint inside `run_pg`'s transaction runs from the transaction's start (`now()`),
/// not from the checkpoint. The holder must count it the same way, or it renews too late after a slow batch
/// and a standby takes over while the holder is idle.
#[tokio::test]
async fn a_slow_batch_does_not_lose_the_lease_while_idle() -> Result<()> {
    let f = Fixture::new(pges_postgres::store::Outbox::Disabled).await?;
    jobs(&f, 1).await?;

    let subscription = |f: &Fixture| {
        f.store
            .subscription(subscription_name!("slow"))
            .categories(non_empty![category!("jobs")])
            .lease(interval!(600 ms))
            .acquire_interval(interval!(10 ms))
            .run_pg(Slow {
                recorder: Recorder::new("slow"),
                pause: Duration::from_millis(350),
            })
    };
    let first = common::Running::spawn(subscription(&f));
    common::wait_state(first.handle.status_changes(), |s| {
        matches!(s, subscription::State::Running)
    })
    .await?;
    let second = common::Running::spawn(subscription(&f));
    first.wait_position(1).await?;

    let mut status = second.handle.status_changes();
    let took_over = tokio::time::timeout(
        Duration::from_millis(1500),
        status.wait_for(|s| matches!(s.state, subscription::State::Running)),
    )
    .await;
    assert!(
        took_over.is_err(),
        "the standby took over from an idle holder"
    );

    second.stop().await?;
    first.stop().await?;
    f.finish().await
}

/// Resolves once the subscription behind `status` runs, if it ever does.
fn runs(
    mut status: tokio::sync::watch::Receiver<subscription::Status>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let _ = status
            .wait_for(|s| matches!(s.state, subscription::State::Running))
            .await;
    })
}

/// The lease is renewed on another connection while the handler runs, so a batch slower than the whole
/// lease does not hand over to a standby.
#[tokio::test]
async fn a_batch_slower_than_the_lease_keeps_it() -> Result<()> {
    let f = Fixture::new(pges_postgres::store::Outbox::Disabled).await?;
    jobs(&f, 2).await?;

    let subscription = |f: &Fixture| {
        f.store
            .subscription(subscription_name!("slower"))
            .categories(non_empty![category!("jobs")])
            .batch_size(NonZeroU32::new(1).unwrap())
            .lease(interval!(400 ms))
            .acquire_interval(interval!(10 ms))
            .run_pg(Slow {
                recorder: Recorder::new("slower"),
                pause: Duration::from_millis(1000),
            })
    };
    let first = common::Running::spawn(subscription(&f));
    common::wait_state(first.handle.status_changes(), |s| {
        matches!(s, subscription::State::Running)
    })
    .await?;
    let second = common::Running::spawn(subscription(&f));
    let took_over = runs(second.handle.status_changes());

    first.wait_position(2).await?;
    assert!(
        !took_over.is_finished(),
        "the standby took over from a busy holder"
    );
    took_over.abort();

    second.stop().await?;
    first.stop().await?;
    common::check_delivery(
        &f.delivered("slower").await?,
        &f.expected(&[f.id(Cat::Jobs)]).await?,
    )?;

    f.finish().await
}

/// The lease is renewed while a failed batch waits for its retry.
#[tokio::test]
async fn a_retry_backoff_longer_than_the_lease_keeps_it() -> Result<()> {
    let f = Fixture::new(pges_postgres::store::Outbox::Disabled).await?;
    jobs(&f, 1).await?;

    let recorder = Recorder::new("backoff");
    let subscription = |f: &Fixture, recorder: &Recorder| {
        f.store
            .subscription(subscription_name!("backoff"))
            .categories(non_empty![category!("jobs")])
            .lease(interval!(400 ms))
            .acquire_interval(interval!(10 ms))
            .on_error(subscription::RetryPolicy::new(
                interval!(1200 ms),
                NonZeroU32::new(2).unwrap(),
            ))
            .run_pg(Failing {
                recorder: recorder.clone(),
                fail: vec![1],
                transient: true,
            })
    };
    let first = common::Running::spawn(subscription(&f, &recorder));
    common::wait_state(first.handle.status_changes(), |s| {
        matches!(s, subscription::State::Running)
    })
    .await?;
    let second = common::Running::spawn(subscription(&f, &Recorder::new("backoff")));
    let took_over = runs(second.handle.status_changes());

    first.wait_position(1).await?;
    assert!(
        !took_over.is_finished(),
        "the standby took over during the backoff"
    );
    took_over.abort();
    assert_eq!(recorder.calls(), 2);

    second.stop().await?;
    first.stop().await?;
    common::check_delivery(
        &f.delivered("backoff").await?,
        &f.expected(&[f.id(Cat::Jobs)]).await?,
    )?;

    f.finish().await
}

/// Shutdown does not wait for a retry backoff: it releases the lease at once and leaves the batch for the
/// next start.
#[tokio::test]
async fn shutdown_ends_a_retry_backoff() -> Result<()> {
    let f = Fixture::new(pges_postgres::store::Outbox::Disabled).await?;
    jobs(&f, 1).await?;

    let recorder = Recorder::new("interrupted");
    let running = common::Running::spawn(
        f.store
            .subscription(subscription_name!("interrupted"))
            .categories(non_empty![category!("jobs")])
            .on_error(subscription::RetryPolicy::new(
                interval!(30 s),
                NonZeroU32::new(2).unwrap(),
            ))
            .run_pg(Failing {
                recorder: recorder.clone(),
                fail: vec![1],
                transient: true,
            }),
    );
    tokio::time::timeout(Duration::from_secs(20), async {
        while recorder.calls() == 0 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .context("the handler was not called")?;

    tokio::time::timeout(Duration::from_secs(2), running.stop())
        .await
        .context("shutdown waited for the backoff")??;
    assert_eq!(recorder.calls(), 1);
    assert_eq!(f.cursor("interrupted").await?, Some(0));
    assert!(f.delivered("interrupted").await?.is_empty());
    let leased: Option<uuid::Uuid> =
        sqlx::query_scalar("SELECT lease_owner FROM es.subscriptions WHERE name = 'interrupted'")
            .fetch_one(&f.pool)
            .await?;
    assert_eq!(leased, None, "shutdown releases the lease");

    f.finish().await
}

/// A renewal during the handler finds that another instance took the lease: the batch is not checkpointed,
/// the other instance's lease is left alone, and this one stands by until the other lets go.
#[tokio::test]
async fn a_renewal_that_finds_the_fence_moved_stops_the_checkpoint() -> Result<()> {
    let f = Fixture::new(pges_postgres::store::Outbox::Disabled).await?;
    jobs(&f, 3).await?;

    let (entered, gate) = (Arc::new(Notify::new()), Arc::new(Notify::new()));
    let running = common::Running::spawn(
        f.store
            .subscription(subscription_name!("taken"))
            .categories(non_empty![category!("jobs")])
            .lease(interval!(200 ms))
            .acquire_interval(interval!(20 ms))
            .run_pg(Gated {
                recorder: Recorder::new("taken"),
                entered: entered.clone(),
                gate: gate.clone(),
                first: Arc::new(Mutex::new(true)),
            }),
    );
    tokio::time::timeout(Duration::from_secs(20), entered.notified())
        .await
        .context("the handler was not called")?;

    // Another instance takes over and holds the lease for an hour; the next renewal sees the new fence.
    sqlx::query(
        "UPDATE es.subscriptions SET fence = fence + 1, lease_until = now() + interval '1 hour'
         WHERE name = 'taken'",
    )
    .execute(&f.pool)
    .await?;
    tokio::time::sleep(Duration::from_millis(400)).await;
    let mut status = running.handle.status_changes();
    gate.notify_one();
    tokio::time::timeout(
        Duration::from_secs(20),
        status.wait_for(|s| matches!(s.state, subscription::State::Standby)),
    )
    .await
    .context("the deposed instance did not stand down")??;
    assert!(f.delivered("taken").await?.is_empty());
    assert_eq!(f.cursor("taken").await?, Some(0));
    let long_lease: bool = sqlx::query_scalar(
        "SELECT lease_until > now() + interval '30 minutes' FROM es.subscriptions WHERE name = 'taken'",
    )
    .fetch_one(&f.pool)
    .await?;
    assert!(long_lease, "the deposed instance changed the new lease");

    // The other instance lets go: this one takes over and delivers everything once.
    sqlx::query("UPDATE es.subscriptions SET lease_until = NULL WHERE name = 'taken'")
        .execute(&f.pool)
        .await?;
    running.wait_position(3).await?;
    running.stop().await?;
    common::check_delivery(
        &f.delivered("taken").await?,
        &f.expected(&[f.id(Cat::Jobs)]).await?,
    )?;

    f.finish().await
}

/// A waker that never resolves, like a LISTEN connection that died without an error.
struct Silent;

impl waker::Waker for Silent {
    async fn changed(
        &mut self,
        _: &high_water::HighWater,
    ) -> Result<high_water::HighWater, waker::WakeError> {
        std::future::pending().await
    }
}

/// A caught-up subscription whose waker stays quiet still reads the log after `max_idle`, sharded or not,
/// while it renews its short lease. Before the backstop it renewed the lease and delivered nothing.
#[tokio::test]
async fn a_silent_waker_still_delivers_after_max_idle() -> Result<()> {
    let f = Fixture::new(pges_postgres::store::Outbox::Disabled).await?;
    let job = stream::StreamId::new_v7();
    f.append(Cat::Jobs, job, 1).await?;
    f.sequence_all().await?;

    let builder = |name: &str| {
        f.store
            .subscription(subscription::SubscriptionName::parse(name).unwrap())
            .categories(non_empty![category!("jobs")])
            .lease(interval!(400 ms))
            .max_idle(interval!(300 ms))
            .waker(Silent)
    };
    let single = common::Running::spawn(builder("silent").run_pg(Recorder::new("silent")));
    let sharded = common::Running::spawn(
        builder("silent_sharded")
            .sharded(pges_subscribe::shards!(2))
            .run_pg(Recorder::new("silent_sharded")),
    );
    delivered(&f, "silent", 1).await?;
    delivered(&f, "silent_sharded", 1).await?;

    // Both are caught up and waiting on the silent waker; only the backstop reads the new commits.
    tokio::time::sleep(Duration::from_millis(500)).await;
    for _ in 0..2 {
        f.append(Cat::Jobs, job, 1).await?;
    }
    f.sequence_all().await?;
    delivered(&f, "silent", 3).await?;
    delivered(&f, "silent_sharded", 3).await?;

    assert!(single.errors().is_empty(), "{:?}", single.errors());
    assert!(sharded.errors().is_empty(), "{:?}", sharded.errors());
    single.stop().await?;
    sharded.stop().await?;
    f.finish().await
}

/// Waits until `subscriber` has recorded `count` events.
async fn delivered(f: &Fixture, subscriber: &str, count: usize) -> Result<()> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let delivered = f.delivered(subscriber).await?.len();
        if delivered >= count {
            return Ok(());
        }
        anyhow::ensure!(
            tokio::time::Instant::now() < deadline,
            "{subscriber} delivered {delivered} of {count} events"
        );

        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// A migration that drops support for this crate while a subscription runs (the `min_compatible` of the
/// store, or of its outbox scope, raised past the crate's schema version) stops it at its next lease renewal with `CrateTooOld`, sharded or
/// not, and releases its leases. Another store's scope does not concern it.
#[tokio::test]
async fn a_migration_past_the_crate_stops_subscriptions_at_renewal() -> Result<()> {
    for (outbox, raised) in [
        (pges_postgres::store::Outbox::Disabled, "es"),
        (pges_postgres::store::Outbox::Enabled, "es.outbox"),
    ] {
        let f = Fixture::new(outbox).await?;
        f.append(Cat::Jobs, stream::StreamId::new_v7(), 1).await?;
        f.sequence_all().await?;
        sqlx::query(
        "INSERT INTO pges.scopes (schema_version, min_compatible, scope) VALUES (9, 9, 'other')",
    )
    .execute(&f.pool)
    .await?;

        let builder = |name: &str| {
            f.store
                .subscription(subscription::SubscriptionName::parse(name).unwrap())
                .categories(non_empty![category!("jobs")])
                .lease(interval!(400 ms))
        };
        let single = common::Running::spawn(builder("old").run_pg(Recorder::new("old")));
        let sharded = common::Running::spawn(
            builder("old_sharded")
                .sharded(pges_subscribe::shards!(2))
                .run_pg(Recorder::new("old_sharded")),
        );
        delivered(&f, "old", 1).await?;
        delivered(&f, "old_sharded", 1).await?;
        tokio::time::sleep(Duration::from_millis(500)).await;
        assert!(!single.handle.is_finished() && !sharded.handle.is_finished());

        let updated = sqlx::query(
            "UPDATE pges.scopes SET schema_version = 9, min_compatible = 9 WHERE scope = $1",
        )
        .bind(raised)
        .execute(&f.pool)
        .await?;
        assert_eq!(updated.rows_affected(), 1, "{raised}");
        for running in [single, sharded] {
            let error = running.stopped().await?;
            assert!(
                matches!(
                    &error,
                    subscription::SubscribeError::CrateTooOld { scope, min_compatible, crate_version }
                        if scope == raised && min_compatible.get() == 9 && crate_version.get() == 1
                ),
                "{error}"
            );
        }
        let leased: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM es.subscriptions WHERE lease_until IS NOT NULL",
        )
        .fetch_one(&f.pool)
        .await?;
        assert_eq!(leased, 0, "the leases were released");
        f.finish().await?;
    }
    Ok(())
}

/// A subscription of a release a migration no longer supports (the `min_compatible` of the store, or of its
/// outbox scope, raised past the crate's schema version) does not take a released lease, sharded or not: it stops with `CrateTooOld`
/// without recovering first, and every shard's fence and lease are left as they were.
#[tokio::test]
async fn a_subscription_past_its_crate_does_not_take_a_released_lease() -> Result<()> {
    for (outbox, raised) in [
        (pges_postgres::store::Outbox::Disabled, "es"),
        (pges_postgres::store::Outbox::Enabled, "es.outbox"),
    ] {
        let f = Fixture::new(outbox).await?;
        f.append(Cat::Jobs, stream::StreamId::new_v7(), 1).await?;
        f.sequence_all().await?;

        let builder = |name: &str| {
            f.store
                .subscription(subscription::SubscriptionName::parse(name).unwrap())
                .categories(non_empty![category!("jobs")])
                .lease(interval!(400 ms))
        };
        let single = || common::Running::spawn(builder("old").run_pg(Recorder::new("old")));
        let sharded = || {
            common::Running::spawn(
                builder("old_sharded")
                    .sharded(pges_subscribe::shards!(2))
                    .run_pg(Recorder::new("old_sharded")),
            )
        };
        let (first, first_sharded) = (single(), sharded());
        delivered(&f, "old", 1).await?;
        delivered(&f, "old_sharded", 1).await?;
        first.stop().await?;
        first_sharded.stop().await?;

        let leases = || {
            sqlx::query_as::<_, (String, i16, i64, Option<String>, bool)>(
            "SELECT name, shard, fence, lease_owner::text, lease_until IS NULL FROM es.subscriptions \
             ORDER BY name, shard",
        )
        .fetch_all(&f.pool)
        };
        let before = leases().await?;
        assert_eq!(before.len(), 3);
        assert!(
            before.iter().all(|row| row.4),
            "every lease was released: {before:?}"
        );

        let updated = sqlx::query(
            "UPDATE pges.scopes SET schema_version = 9, min_compatible = 9 WHERE scope = $1",
        )
        .bind(raised)
        .execute(&f.pool)
        .await?;
        assert_eq!(updated.rows_affected(), 1, "{raised}");
        for running in [single(), sharded()] {
            let error = running.stopped().await?;
            assert!(
                matches!(
                    &error,
                    subscription::SubscribeError::CrateTooOld { scope, min_compatible, crate_version }
                        if scope == raised && min_compatible.get() == 9 && crate_version.get() == 1
                ),
                "{error}"
            );
        }
        assert_eq!(leases().await?, before, "no lease was taken or changed");
        f.finish().await?;
    }
    Ok(())
}
