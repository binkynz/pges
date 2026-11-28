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
    // Fatal: the handler's statements after the rollback would have run in autocommit.
    assert_eq!(recorder.calls(), 1, "not retried");
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

/// Records, ends its transaction with a raw `ROLLBACK` or `COMMIT`, which sqlx does not see, and returns
/// `Ok`.
struct EndsItsTransaction {
    recorder: Recorder,
    end: &'static str,
}

impl handler::PgHandler for EndsItsTransaction {
    async fn handle(
        &mut self,
        tx: &mut PgConnection,
        batch: &batch::Batch,
    ) -> Result<(), handler::HandlerError> {
        *self.recorder.calls.lock().unwrap() += 1;
        self.recorder.record(tx, batch).await?;
        sqlx::query(sqlx::AssertSqlSafe(self.end))
            .execute(&mut *tx)
            .await?;
        Ok(())
    }
}

/// A raw `ROLLBACK` in the handler undoes its writes, and a raw `COMMIT` commits them alone; either way the
/// checkpoint would run in autocommit. It moves nothing, and the run stops at once with `HandlerFailed`.
#[tokio::test]
async fn a_handler_that_ends_its_transaction_is_not_checkpointed() -> Result<()> {
    let f = Fixture::new(pges_postgres::store::Outbox::Disabled).await?;
    jobs(&f, 2).await?;

    for (name, end, delivered) in [("rolled-back", "ROLLBACK", 0), ("committed", "COMMIT", 2)] {
        let recorder = Recorder::new(name);
        let error = common::Running::spawn(
            f.store
                .subscription(subscription::SubscriptionName::new(name).unwrap())
                .categories(non_empty![category!("jobs")])
                .on_error(retry(3))
                .run_pg(EndsItsTransaction {
                    recorder: recorder.clone(),
                    end,
                }),
        )
        .stopped()
        .await?;
        assert!(
            matches!(&error, subscription::SubscribeError::HandlerFailed { source, .. }
                if source.to_string().contains("ended the batch's transaction")),
            "{end}: {error:?}"
        );
        assert_eq!(recorder.calls(), 1, "{end}: misuse is not retried");
        assert_eq!(f.cursor(name).await?, Some(0), "{end}");
        assert_eq!(f.delivered(name).await?.len(), delivered, "{end}");
    }

    f.finish().await
}

/// On its first call: records, sends `end` raw (`COMMIT`, or `COMMIT; BEGIN`), then fails as `fail` says.
/// Later calls record and pass.
struct EndsThenFails {
    recorder: Recorder,
    end: &'static str,
    fail: Fail,
}

#[derive(Clone, Copy, Debug)]
enum Fail {
    /// Returns a transient error.
    ReturnsTransient,
    /// Runs a failing statement, ignores its error and returns `Ok`: the transaction it is in is aborted.
    IgnoresAnError,
}

impl handler::PgHandler for EndsThenFails {
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
        if call > 1 {
            return Ok(());
        }
        sqlx::raw_sql(sqlx::AssertSqlSafe(self.end))
            .execute(&mut *tx)
            .await?;
        match self.fail {
            Fail::ReturnsTransient => Err(handler::HandlerError::Transient(
                "a later step failed".into(),
            )),
            Fail::IgnoresAnError => {
                let _ = sqlx::query("SELECT 1 / 0").execute(&mut *tx).await;
                Ok(())
            }
        }
    }
}

/// A raw `COMMIT` is caught on the failure paths too: when the handler then returns a transient error, and
/// when it begins another transaction and leaves that one aborted (25P02 at the checkpoint, transient for
/// the batch's own transaction). Either way the batch's writes committed alone, so the run stops at once
/// with a fatal `HandlerFailed` instead of retrying them; the cursor does not move. Before, both were
/// retried and the batch applied twice.
#[tokio::test]
async fn a_handler_that_commits_then_fails_is_not_retried() -> Result<()> {
    let f = Fixture::new(pges_postgres::store::Outbox::Disabled).await?;
    jobs(&f, 1).await?;

    for (name, end, fail) in [
        ("commit-then-err", "COMMIT", Fail::ReturnsTransient),
        (
            "commit-begin-aborted",
            "COMMIT; BEGIN",
            Fail::IgnoresAnError,
        ),
    ] {
        let recorder = Recorder::new(name);
        let error = common::Running::spawn(
            f.store
                .subscription(subscription::SubscriptionName::new(name).unwrap())
                .categories(non_empty![category!("jobs")])
                .on_error(retry(3))
                .run_pg(EndsThenFails {
                    recorder: recorder.clone(),
                    end,
                    fail,
                }),
        )
        .stopped()
        .await?;
        assert!(
            matches!(&error, subscription::SubscribeError::HandlerFailed { source, .. }
                if source.to_string().contains("ended the batch's transaction")),
            "{name}: {error:?}"
        );
        assert_eq!(recorder.calls(), 1, "{name}: retried");
        assert_eq!(f.delivered(name).await?.len(), 1, "{name}: applied twice");
        assert_eq!(f.cursor(name).await?, Some(0), "{name}");
    }

    f.finish().await
}

/// Sets its transaction READ ONLY, which the level-setting check lets through.
struct SetsReadOnly {
    calls: Arc<Mutex<usize>>,
}

impl handler::PgHandler for SetsReadOnly {
    async fn handle(
        &mut self,
        tx: &mut PgConnection,
        _batch: &batch::Batch,
    ) -> Result<(), handler::HandlerError> {
        *self.calls.lock().unwrap() += 1;
        // The level the transaction has already passes.
        sqlx::query("SET TRANSACTION ISOLATION LEVEL READ COMMITTED")
            .execute(&mut *tx)
            .await?;
        sqlx::query("SET TRANSACTION READ ONLY")
            .execute(&mut *tx)
            .await?;
        Ok(())
    }
}

/// `SET TRANSACTION` fails only when it changes the level: setting the level the transaction has passes, and
/// `READ ONLY` passes, then fails the checkpoint (25006), which is transient and retried until the policy
/// stops the run.
#[tokio::test]
async fn a_read_only_handler_transaction_fails_at_the_checkpoint() -> Result<()> {
    let f = Fixture::new(pges_postgres::store::Outbox::Disabled).await?;
    jobs(&f, 1).await?;
    let calls = Arc::new(Mutex::new(0));

    let error = common::Running::spawn(
        f.store
            .subscription(subscription_name!("read-only"))
            .categories(non_empty![category!("jobs")])
            .on_error(retry(2))
            .run_pg(SetsReadOnly {
                calls: calls.clone(),
            }),
    )
    .stopped()
    .await?;
    assert!(
        matches!(&error, subscription::SubscribeError::HandlerFailed { source, .. }
            if source.to_string().contains("read-only transaction")),
        "{error:?}"
    );
    assert_eq!(*calls.lock().unwrap(), 2, "retried per the policy");
    assert_eq!(f.cursor("read-only").await?, Some(0));

    f.finish().await
}

/// A session's `tcp_keepalives_idle`, `_interval`, `_count`, `tcp_user_timeout`, and whether it is on a
/// Unix socket.
type TcpSettings = (String, String, String, String, bool);

/// Reads its session's TCP settings while it holds the subscription's row.
struct ReadsKeepalives {
    settings: Arc<Mutex<Vec<TcpSettings>>>,
}

impl handler::PgHandler for ReadsKeepalives {
    async fn handle(
        &mut self,
        tx: &mut PgConnection,
        _batch: &batch::Batch,
    ) -> Result<(), handler::HandlerError> {
        let row: TcpSettings = sqlx::query_as(
            "SELECT current_setting('tcp_keepalives_idle'), current_setting('tcp_keepalives_interval'),
                    current_setting('tcp_keepalives_count'), current_setting('tcp_user_timeout'),
                    inet_client_addr() IS NULL",
        )
        .fetch_one(&mut *tx)
        .await?;
        self.settings.lock().unwrap().push(row);
        Ok(())
    }
}

/// The session that holds a subscription's row has TCP keepalives derived from the lease (15 s: probes after
/// 5 s idle, every 2 s, 3 of them), so the server drops a vanished client's backend and its lock within about
/// one lease instead of the hours its defaults take. `tcp_user_timeout` stays the server's (0, off): it
/// would drop a healthy handler that reads slowly (`a_handler_reading_slowly_past_the_lease_completes`).
/// They are local to the batch's transaction: the pooled connection keeps its own settings afterwards. Over
/// a Unix socket the server ignores them and reports 0.
#[tokio::test]
async fn the_holding_session_has_keepalives_from_the_lease() -> Result<()> {
    let f = Fixture::new(pges_postgres::store::Outbox::Disabled).await?;
    jobs(&f, 1).await?;
    let settings = Arc::new(Mutex::new(Vec::new()));

    let running = common::Running::spawn(
        f.store
            .subscription(subscription_name!("keepalive"))
            .categories(non_empty![category!("jobs")])
            .lease(interval!(15 s))
            .run_pg(ReadsKeepalives {
                settings: settings.clone(),
            }),
    );
    running.wait_position(1).await?;
    running.stop().await?;

    let settings = settings.lock().unwrap().clone();
    let [(idle, interval, count, user_timeout, unix)] = settings.as_slice() else {
        panic!("one batch: {settings:?}");
    };
    if *unix {
        assert_eq!(
            (idle.as_str(), interval.as_str(), count.as_str()),
            ("0", "0", "0")
        );
    } else {
        assert_eq!(
            (
                idle.as_str(),
                interval.as_str(),
                count.as_str(),
                user_timeout.as_str()
            ),
            ("5", "2", "3", "0")
        );
    }

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

/// Takes its snapshot at the isolation level it asks for, then records after a pause longer than the lease.
/// It notes the level its transaction ran at.
struct Isolated {
    recorder: Recorder,
    isolation: handler::Isolation,
    pause: Duration,
    levels: Arc<Mutex<Vec<String>>>,
}

impl handler::PgHandler for Isolated {
    async fn handle(
        &mut self,
        tx: &mut PgConnection,
        batch: &batch::Batch,
    ) -> Result<(), handler::HandlerError> {
        let level: String = sqlx::query_scalar("SELECT current_setting('transaction_isolation')")
            .fetch_one(&mut *tx)
            .await?;
        self.levels.lock().unwrap().push(level);
        sqlx::query("SELECT count(*) FROM public.delivered")
            .execute(&mut *tx)
            .await?;
        tokio::time::sleep(self.pause).await;
        self.recorder.record(tx, batch).await?;
        Ok(())
    }

    fn isolation(&self) -> handler::Isolation {
        self.isolation
    }
}

/// A handler at READ COMMITTED (asked for, not the default), REPEATABLE READ or SERIALIZABLE whose batches
/// outlast the lease keeps its lease and is checkpointed: nothing renews the subscription's row after the
/// handler's snapshot, and a standby does not take over.
#[tokio::test]
async fn a_repeatable_read_or_serializable_batch_slower_than_the_lease_is_checkpointed()
-> Result<()> {
    let f = Fixture::new(pges_postgres::store::Outbox::Disabled).await?;
    jobs(&f, 2).await?;

    for (name, isolation, level) in [
        (
            "read-committed",
            handler::Isolation::ReadCommitted,
            "read committed",
        ),
        (
            "repeatable",
            handler::Isolation::RepeatableRead,
            "repeatable read",
        ),
        (
            "serializable",
            handler::Isolation::Serializable,
            "serializable",
        ),
    ] {
        let levels = Arc::new(Mutex::new(Vec::new()));
        let subscription = |f: &Fixture, levels: &Arc<Mutex<Vec<String>>>| {
            f.store
                .subscription(subscription::SubscriptionName::new(name).unwrap())
                .categories(non_empty![category!("jobs")])
                .batch_size(NonZeroU32::new(1).unwrap())
                .lease(interval!(400 ms))
                .acquire_interval(interval!(10 ms))
                .on_error(retry(1))
                .run_pg(Isolated {
                    recorder: Recorder::new(name),
                    isolation,
                    pause: Duration::from_millis(600),
                    levels: levels.clone(),
                })
        };
        let first = common::Running::spawn(subscription(&f, &levels));
        common::wait_state(first.handle.status_changes(), |s| {
            matches!(s, subscription::State::Running)
        })
        .await?;
        let second = common::Running::spawn(subscription(&f, &Arc::new(Mutex::new(Vec::new()))));
        let took_over = runs(second.handle.status_changes());

        first.wait_position(2).await?;
        assert!(
            !took_over.is_finished(),
            "{name}: the standby took over from a busy holder"
        );
        took_over.abort();
        assert_eq!(*levels.lock().unwrap(), [level, level], "{name}");

        second.stop().await?;
        first.stop().await?;
        common::check_delivery(
            &f.delivered(name).await?,
            &f.expected(&[f.id(Cat::Jobs)]).await?,
        )?;
    }

    f.finish().await
}

/// Sets its isolation level itself, too late: the subscription's statements came first.
struct SetsItsIsolation;

impl handler::PgHandler for SetsItsIsolation {
    async fn handle(
        &mut self,
        tx: &mut PgConnection,
        _batch: &batch::Batch,
    ) -> Result<(), handler::HandlerError> {
        sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
            .execute(&mut *tx)
            .await?;
        Ok(())
    }
}

/// `SET TRANSACTION ISOLATION LEVEL` in the handler fails, and stops the run: the level comes from
/// `PgHandler::isolation`.
#[tokio::test]
async fn a_handler_cannot_set_its_isolation_level_itself() -> Result<()> {
    let f = Fixture::new(pges_postgres::store::Outbox::Disabled).await?;
    jobs(&f, 1).await?;

    let error = common::Running::spawn(
        f.store
            .subscription(subscription_name!("set-isolation"))
            .categories(non_empty![category!("jobs")])
            .run_pg(SetsItsIsolation),
    )
    .stopped()
    .await?;
    assert!(
        matches!(&error, subscription::SubscribeError::HandlerFailed { source, .. }
            if source.to_string().contains("before any query")),
        "{error:?}"
    );
    assert_eq!(f.cursor("set-isolation").await?, Some(0));

    f.finish().await
}
/// Makes every COMMIT of a batch the subscription `subscriber` records run `body` first: a deferred constraint
/// trigger on `public.delivered`, which fires at the COMMIT, after the checkpoint.
async fn at_commit(f: &Fixture, subscriber: &str, body: &str) -> Result<()> {
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "CREATE FUNCTION public.at_commit_{subscriber}() RETURNS trigger LANGUAGE plpgsql AS $$
         BEGIN {body} RETURN NULL; END $$"
    )))
    .execute(&f.pool)
    .await?;
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "CREATE CONSTRAINT TRIGGER at_commit_{subscriber} AFTER INSERT ON public.delivered
         DEFERRABLE INITIALLY DEFERRED FOR EACH ROW
         WHEN (NEW.subscriber = '{subscriber}') EXECUTE FUNCTION public.at_commit_{subscriber}()"
    )))
    .execute(&f.pool)
    .await?;
    Ok(())
}

/// A serialization failure at the COMMIT (40001, as a SERIALIZABLE handler's conflict with another
/// transaction is reported there) follows the retry policy: transient, so the batch is retried, and the run
/// stops with `HandlerFailed` once it runs out of attempts, instead of retrying the batch for good as a
/// database error. The failure is raised by a deferred trigger, so every attempt meets it. With the
/// subscription's row held for the batch, the checkpoint itself no longer fails with 40001 under REPEATABLE
/// READ or SERIALIZABLE (`a_repeatable_read_or_serializable_batch_slower_than_the_lease_is_checkpointed`).
#[tokio::test]
async fn a_serialization_failure_at_the_commit_follows_the_retry_policy() -> Result<()> {
    let f = Fixture::new(pges_postgres::store::Outbox::Disabled).await?;
    jobs(&f, 1).await?;
    at_commit(
        &f,
        "serializable",
        "RAISE EXCEPTION 'could not serialize access' USING ERRCODE = 'serialization_failure';",
    )
    .await?;
    let levels = Arc::new(Mutex::new(Vec::new()));

    let error = common::Running::spawn(
        f.store
            .subscription(subscription_name!("serializable"))
            .categories(non_empty![category!("jobs")])
            .retry_interval(interval!(10 ms))
            .on_error(retry(2))
            .run_pg(Isolated {
                recorder: Recorder::new("serializable"),
                isolation: handler::Isolation::Serializable,
                pause: Duration::ZERO,
                levels: levels.clone(),
            }),
    )
    .stopped()
    .await?;
    assert!(
        matches!(&error, subscription::SubscribeError::HandlerFailed { source, .. }
            if source.to_string().contains("could not serialize")),
        "{error:?}"
    );
    assert_eq!(
        *levels.lock().unwrap(),
        ["serializable", "serializable"],
        "retried per the policy"
    );
    assert_eq!(f.cursor("serializable").await?, Some(0));
    assert!(f.delivered("serializable").await?.is_empty());

    f.finish().await
}

/// A serialization failure at the batch's hold (`subscription/hold.sql`), as a REPEATABLE READ or SERIALIZABLE
/// batch meets when another session updates the subscription's row while the hold waits on its lock (a
/// renewal dropped in flight, a racing acquire on a lapsed lease, a redefine), is no handler failure: the
/// handler never ran. The batch is rolled back and retried as after a database error, reading the stored
/// cursor and fence again, and a one-attempt retry policy still delivers it. The test updates the row's lease
/// as a renewal does and holds that transaction open until the batch waits on the row lock.
#[tokio::test]
async fn a_serialization_failure_at_the_hold_is_not_a_handler_failure() -> Result<()> {
    let f = Fixture::new(pges_postgres::store::Outbox::Disabled).await?;
    let id = jobs(&f, 1).await?;
    let levels = Arc::new(Mutex::new(Vec::new()));

    let running = common::Running::spawn(
        f.store
            .subscription(subscription_name!("hold-serialization"))
            .categories(non_empty![category!("jobs")])
            .retry_interval(interval!(10 ms))
            .waker(recheck::Recheck::every(interval!(5 ms)))
            .on_error(retry(1))
            .run_pg(Isolated {
                recorder: Recorder::new("hold-serialization"),
                isolation: handler::Isolation::RepeatableRead,
                pause: Duration::ZERO,
                levels: levels.clone(),
            }),
    );
    running.wait_position(1).await?;

    // Another session's renewal, committed while the next batch's hold waits on the row lock.
    let mut renewal = f.pool.begin().await?;
    sqlx::query(
        "UPDATE es.subscriptions SET lease_until = lease_until + interval '1 ms'
         WHERE name = 'hold-serialization'",
    )
    .execute(&mut *renewal)
    .await?;
    f.append(Cat::Jobs, id, 1).await?;
    f.sequence_all().await?;
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let waiting: bool = sqlx::query_scalar(
                "SELECT EXISTS (SELECT FROM pg_stat_activity
                                WHERE datname = current_database() AND wait_event_type = 'Lock'
                                  AND query LIKE '%FOR NO KEY UPDATE OF subscriptions%')",
            )
            .fetch_one(&f.pool)
            .await?;
            if waiting {
                return anyhow::Ok(());
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .context("the subscription's batch never waited on the renewal")??;
    renewal.commit().await?;

    if let Err(error) = running.wait_position(2).await {
        if running.handle.is_finished() {
            panic!(
                "the subscription stopped: {:?}",
                running.handle.join().await
            );
        }
        return Err(error);
    }
    assert_eq!(f.cursor("hold-serialization").await?, Some(2));
    assert_eq!(
        *levels.lock().unwrap(),
        ["repeatable read", "repeatable read"],
        "the handler ran once per batch"
    );
    let errors = running.errors();
    assert!(
        errors
            .iter()
            .any(|error| error.contains("could not serialize")),
        "the hold met no serialization failure: {errors:?}"
    );
    running.handle.shutdown().await?;
    common::check_delivery(
        &f.delivered("hold-serialization").await?,
        &f.expected(&[f.id(Cat::Jobs)]).await?,
    )?;

    f.finish().await
}
/// Records with a short `lock_timeout`, which the COMMIT in its transaction inherits.
struct ShortLockTimeout {
    recorder: Recorder,
}

impl handler::PgHandler for ShortLockTimeout {
    async fn handle(
        &mut self,
        tx: &mut PgConnection,
        batch: &batch::Batch,
    ) -> Result<(), handler::HandlerError> {
        *self.recorder.calls.lock().unwrap() += 1;
        sqlx::query("SET LOCAL lock_timeout = '100ms'")
            .execute(&mut *tx)
            .await?;
        self.recorder.record(tx, batch).await?;
        Ok(())
    }
}

/// A COMMIT that times out waiting for a lock (55P03, here in a deferred trigger, as behind a lock another
/// session holds a little too long) is no fault of the batch: it is transient and retried, and the batch is
/// applied once the lock is gone, instead of stopping the run with `HandlerFailed`. (The checkpoint itself no
/// longer waits on the subscription's row: the batch locked it before the handler ran.)
#[tokio::test]
async fn a_commit_lock_timeout_is_retried() -> Result<()> {
    let f = Fixture::new(pges_postgres::store::Outbox::Disabled).await?;
    jobs(&f, 1).await?;
    sqlx::query("CREATE TABLE public.gate (k int PRIMARY KEY)")
        .execute(&f.pool)
        .await?;
    sqlx::query("INSERT INTO public.gate VALUES (1)")
        .execute(&f.pool)
        .await?;
    at_commit(&f, "locked", "PERFORM FROM public.gate FOR UPDATE;").await?;
    let recorder = Recorder::new("locked");

    let mut lock = f.pool.begin().await?;
    sqlx::query("SELECT FROM public.gate FOR UPDATE")
        .execute(&mut *lock)
        .await?;
    let running = common::Running::spawn(
        f.store
            .subscription(subscription_name!("locked"))
            .categories(non_empty![category!("jobs")])
            .on_error(subscription::RetryPolicy::new(
                interval!(100 ms),
                NonZeroU32::new(20).unwrap(),
            ))
            .run_pg(ShortLockTimeout {
                recorder: recorder.clone(),
            }),
    );
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while recorder.calls() < 2 {
        assert!(
            tokio::time::Instant::now() < deadline,
            "the first attempt did not time out"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    lock.rollback().await?;

    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while f.cursor("locked").await? != Some(1) {
        if running.handle.is_finished() {
            panic!(
                "the subscription stopped: {:?}",
                running.handle.join().await
            );
        }
        assert!(tokio::time::Instant::now() < deadline, "not checkpointed");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(f.delivered("locked").await?.len(), 1, "applied once");
    running.stop().await?;

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

/// Another instance takes the shard over while the holder waits for new events, before its next renewal: the
/// holder's next batch finds the fence moved when it locks the row, runs no handler, and stands by.
#[tokio::test]
async fn a_deposed_instance_is_fenced_out() -> Result<()> {
    let f = Fixture::new(pges_postgres::store::Outbox::Disabled).await?;
    let id = jobs(&f, 2).await?;

    let recorder = Recorder::new("fenced");
    let running = common::Running::spawn(
        f.store
            .subscription(subscription_name!("fenced"))
            .categories(non_empty![category!("jobs")])
            .acquire_interval(interval!(20 ms))
            .waker(recheck::Recheck::every(interval!(20 ms)))
            .run_pg(recorder.clone()),
    );
    running.wait_position(2).await?;

    // The default lease is 15 s, so no renewal comes before the next batch.
    sqlx::query(
        "UPDATE es.subscriptions SET fence = fence + 1, lease_until = now() + interval '1 hour'
         WHERE name = 'fenced'",
    )
    .execute(&f.pool)
    .await?;
    let mut status = running.handle.status_changes();
    f.append(Cat::Jobs, id, 1).await?;
    f.append(Cat::Jobs, id, 1).await?;
    f.sequence_all().await?;
    tokio::time::timeout(
        Duration::from_secs(20),
        status.wait_for(|s| matches!(s.state, subscription::State::Standby)),
    )
    .await
    .context("the deposed instance did not stand down")??;
    assert_eq!(recorder.calls(), 1, "no handler ran after the takeover");
    assert_eq!(f.delivered("fenced").await?.len(), 2);
    assert_eq!(f.cursor("fenced").await?, Some(2));

    // The other instance lets go: this one takes the lease back and delivers everything once.
    sqlx::query("UPDATE es.subscriptions SET lease_until = NULL WHERE name = 'fenced'")
        .execute(&f.pool)
        .await?;
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

/// A COMMIT whose reply was lost can become visible only after the subscription read its cursor again: the
/// client saw an error while the server still finished the transaction. The batch read from the stale cursor
/// must not be applied on top of it. The test holds the lost COMMIT open until the subscription's next batch
/// waits on its row lock (the batch's lock on the subscription's row, `subscription/hold.sql`), then commits
/// it: the lock is taken on the moved row, the handler applies the stale batch, and the checkpoint finds the
/// cursor moved (`subscription/checkpoint_held.sql`), rolls the batch back and reads the cursor again.
#[tokio::test]
async fn a_lost_commit_landing_late_is_not_applied_twice() -> Result<()> {
    let f = Fixture::new(pges_postgres::store::Outbox::Disabled).await?;
    let id = jobs(&f, 2).await?;

    let handle = f
        .store
        .subscription(subscription_name!("late"))
        .categories(non_empty![category!("jobs")])
        .retry_interval(interval!(50 ms))
        .waker(recheck::Recheck::every(interval!(5 ms)))
        .run_pg(Recorder::new("late"))
        .spawn();
    common::wait_position(handle.status_changes(), 2).await?;

    for _ in 0..2 {
        f.append(Cat::Jobs, id, 1).await?;
    }
    sqlx::query("ALTER TABLE es.commits_jobs RENAME TO commits_jobs_hidden")
        .execute(&f.pool)
        .await?;
    f.sequence_all().await?;
    common::wait_state(handle.status_changes(), |s| {
        matches!(s, subscription::State::Recovering(_))
    })
    .await?;

    // The lost COMMIT's transaction: the batch applied and the cursor moved, not yet visible.
    let expected = f.expected(&[f.id(Cat::Jobs)]).await?;
    let mut lost = f.pool.begin().await?;
    for (position, category_id) in &expected[2..] {
        sqlx::query(
            "INSERT INTO public.delivered (position, commit_id, category_id, subscriber)
             VALUES ($1, gen_random_uuid(), $2, 'late')",
        )
        .bind(position)
        .bind(category_id)
        .execute(&mut *lost)
        .await?;
    }
    sqlx::query("UPDATE es.subscriptions SET position = 4 WHERE name = 'late'")
        .execute(&mut *lost)
        .await?;
    sqlx::query("ALTER TABLE es.commits_jobs_hidden RENAME TO commits_jobs")
        .execute(&f.pool)
        .await?;

    // The subscription reads the cursor again (still 2), reads 3 and 4 and its batch waits on the row lock.
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let waiting: bool = sqlx::query_scalar(
                "SELECT EXISTS (SELECT FROM pg_stat_activity
                                WHERE datname = current_database() AND wait_event_type = 'Lock'
                                  AND query LIKE '%FOR NO KEY UPDATE OF subscriptions%')",
            )
            .fetch_one(&f.pool)
            .await?;
            if waiting {
                return anyhow::Ok(());
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .context("the subscription's batch never waited on the lost COMMIT")??;
    lost.commit().await?;

    f.append(Cat::Jobs, id, 1).await?;
    f.sequence_all().await?;
    common::wait_position(handle.status_changes(), 5).await?;
    handle.shutdown().await?;

    common::check_delivery(
        &f.delivered("late").await?,
        &f.expected(&[f.id(Cat::Jobs)]).await?,
    )?;

    f.finish().await
}

/// `subscription/checkpoint_held.sql` moves the cursor only from where the instance holds it, as the other
/// checkpoints do: a stored cursor elsewhere (as a lost COMMIT landing late leaves it) updates no row, in the
/// batch's transaction and with the fence in place.
#[tokio::test]
async fn the_held_checkpoint_moves_only_a_cursor_it_holds() -> Result<()> {
    use pges_postgres::sql::{template, typed::Query as _};
    use pges_subscribe::sql::statement::checkpoint_held;

    let f = Fixture::new(pges_postgres::store::Outbox::Disabled).await?;
    jobs(&f, 2).await?;
    let handle = f
        .store
        .subscription(subscription_name!("held"))
        .categories(non_empty![category!("jobs")])
        .run_pg(Recorder::new("held"))
        .spawn();
    common::wait_position(handle.status_changes(), 2).await?;
    handle.shutdown().await?;
    let fence: i64 =
        sqlx::query_scalar("SELECT fence FROM es.subscriptions WHERE name = 'held' AND shard = 0")
            .fetch_one(&f.pool)
            .await?;

    let checkpoint =
        checkpoint_held::Query::render(&template::Values::new().store(&common::STORE_NAME));
    let mut tx = f.pool.begin().await?;
    let xact: i64 = sqlx::query_scalar("SELECT pg_current_xact_id()::text::int8")
        .fetch_one(&mut *tx)
        .await?;
    let params = |from| checkpoint_held::Params {
        name: "held",
        shard: 0,
        position: 5,
        fence,
        xact,
        lease: sqlx::postgres::types::PgInterval {
            months: 0,
            days: 0,
            microseconds: 1_000_000,
        },
        from,
    };

    let row = checkpoint.bind(params(1)).fetch_one(&mut *tx).await?;
    assert_eq!(
        (row.in_batch, row.moved),
        (Some(true), Some(false)),
        "moved a cursor the instance does not hold"
    );
    let row = checkpoint.bind(params(2)).fetch_one(&mut *tx).await?;
    assert_eq!((row.in_batch, row.moved), (Some(true), Some(true)));
    tx.rollback().await?;
    assert_eq!(f.cursor("held").await?, Some(2));

    f.finish().await
}

/// Streams a large result through the batch's transaction and pauses after its first row, as a handler that
/// does slow work per row would, then records.
struct SlowReader {
    recorder: Recorder,
    pause: Duration,
    results: Arc<Mutex<Vec<String>>>,
}

impl handler::PgHandler for SlowReader {
    async fn handle(
        &mut self,
        tx: &mut PgConnection,
        batch: &batch::Batch,
    ) -> Result<(), handler::HandlerError> {
        use futures_util::TryStreamExt as _;

        let read = async {
            // About 160 MB: far more than the socket buffers hold, so the server blocks sending it.
            let mut rows = sqlx::query_scalar::<_, String>(
                "SELECT repeat('x', 8000) FROM generate_series(1, 20000)",
            )
            .fetch(&mut *tx);
            let mut read = 0_usize;
            while rows.try_next().await?.is_some() {
                read += 1;
                if read == 1 {
                    tokio::time::sleep(self.pause).await;
                }
            }
            Ok::<_, sqlx::Error>(read)
        }
        .await;
        self.results.lock().unwrap().push(format!("{read:?}"));
        read?;
        self.recorder.record(tx, batch).await?;
        Ok(())
    }
}

/// A healthy handler that reads a large result slowly, pausing longer than the lease, completes: the
/// server waits on a full receive window meanwhile, and nothing the batch sets on its session gives up on
/// that. (`tcp_user_timeout` set to the lease did: on Linux it also bounds zero-window probing, so the
/// server dropped the connection and every attempt failed.)
#[tokio::test]
async fn a_handler_reading_slowly_past_the_lease_completes() -> Result<()> {
    let f = Fixture::new(pges_postgres::store::Outbox::Disabled).await?;
    jobs(&f, 1).await?;
    let results = Arc::new(Mutex::new(Vec::new()));

    let running = common::Running::spawn(
        f.store
            .subscription(subscription_name!("slow-reader"))
            .categories(non_empty![category!("jobs")])
            .lease(interval!(2 s))
            .on_error(retry(1))
            .run_pg(SlowReader {
                recorder: Recorder::new("slow-reader"),
                pause: Duration::from_secs(8),
                results: results.clone(),
            }),
    );
    let reached = tokio::time::timeout(Duration::from_secs(60), running.wait_position(1)).await;
    let results = results.lock().unwrap().clone();
    assert!(
        matches!(reached, Ok(Ok(_))),
        "not checkpointed: {reached:?}; reads: {results:?}"
    );
    assert_eq!(results, ["Ok(20000)"]);
    running.stop().await?;
    assert_eq!(f.delivered("slow-reader").await?.len(), 1);

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

/// A lease renewed by the checkpoint at the end of a `run_pg` batch counts from the checkpoint, not from the
/// transaction's start, which came before the handler ran. If it counted from the start, the holder would
/// renew too late after a slow batch and a standby would take over while the holder is idle.
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

/// The batch's row lock keeps the lease while the handler runs, so a batch slower than the whole lease does
/// not hand over to a standby.
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

/// The batch's transaction holds the subscription's row while the handler runs, past the lease: a standby
/// does not take over, a write to the row waits for the batch, and the batch is checkpointed.
#[tokio::test]
async fn a_batch_holds_the_subscription_row_until_it_commits() -> Result<()> {
    let f = Fixture::new(pges_postgres::store::Outbox::Disabled).await?;
    jobs(&f, 3).await?;

    let (entered, gate) = (Arc::new(Notify::new()), Arc::new(Notify::new()));
    let subscription = |f: &Fixture, entered: &Arc<Notify>, gate: &Arc<Notify>, first: bool| {
        f.store
            .subscription(subscription_name!("held"))
            .categories(non_empty![category!("jobs")])
            .lease(interval!(200 ms))
            .acquire_interval(interval!(20 ms))
            .run_pg(Gated {
                recorder: Recorder::new("held"),
                entered: entered.clone(),
                gate: gate.clone(),
                first: Arc::new(Mutex::new(first)),
            })
    };
    let running = common::Running::spawn(subscription(&f, &entered, &gate, true));
    tokio::time::timeout(Duration::from_secs(20), entered.notified())
        .await
        .context("the handler was not called")?;
    let standby = common::Running::spawn(subscription(
        &f,
        &Arc::new(Notify::new()),
        &Arc::new(Notify::new()),
        false,
    ));
    let took_over = runs(standby.handle.status_changes());
    tokio::time::sleep(Duration::from_millis(600)).await;

    let lapsed: bool =
        sqlx::query_scalar("SELECT lease_until < now() FROM es.subscriptions WHERE name = 'held'")
            .fetch_one(&f.pool)
            .await?;
    assert!(lapsed, "the stored lease lapsed while the handler ran");
    let mut tx = f.pool.begin().await?;
    sqlx::query("SET LOCAL lock_timeout = '100ms'")
        .execute(&mut *tx)
        .await?;
    let takeover = sqlx::query("UPDATE es.subscriptions SET fence = fence + 1 WHERE name = 'held'")
        .execute(&mut *tx)
        .await;
    assert!(
        matches!(&takeover, Err(sqlx::Error::Database(error)) if error.code().as_deref() == Some("55P03")),
        "a takeover did not wait for the batch: {takeover:?}"
    );
    tx.rollback().await?;

    gate.notify_one();
    running.wait_position(3).await?;
    assert!(
        !took_over.is_finished(),
        "the standby took over from a busy holder"
    );
    took_over.abort();

    standby.stop().await?;
    running.stop().await?;
    common::check_delivery(
        &f.delivered("held").await?,
        &f.expected(&[f.id(Cat::Jobs)]).await?,
    )?;

    f.finish().await
}

/// A `run` handler that holds its first call until the test opens the gate, and counts its calls.
struct GatedExternal {
    calls: Arc<Mutex<usize>>,
    entered: Arc<Notify>,
    gate: Arc<Notify>,
}

impl handler::Handler for GatedExternal {
    async fn handle(&mut self, _batch: &batch::Batch) -> Result<(), handler::HandlerError> {
        let call = {
            let mut calls = self.calls.lock().unwrap();
            *calls += 1;
            *calls
        };
        if call == 1 {
            self.entered.notify_one();
            self.gate.notified().await;
        }
        Ok(())
    }
}

/// `run`, for contrast: a side renewal during the handler finds that another instance took the lease, the
/// batch is not checkpointed, the other instance's lease is left alone, and this one stands by until the
/// other lets go.
#[tokio::test]
async fn a_renewal_beside_a_run_handler_that_finds_the_fence_moved_stops_the_checkpoint()
-> Result<()> {
    let f = Fixture::new(pges_postgres::store::Outbox::Disabled).await?;
    jobs(&f, 3).await?;

    let (entered, gate) = (Arc::new(Notify::new()), Arc::new(Notify::new()));
    let calls = Arc::new(Mutex::new(0));
    let running = common::Running::spawn(
        f.store
            .subscription(subscription_name!("taken"))
            .categories(non_empty![category!("jobs")])
            .lease(interval!(200 ms))
            .acquire_interval(interval!(20 ms))
            .run(GatedExternal {
                calls: calls.clone(),
                entered: entered.clone(),
                gate: gate.clone(),
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
    assert_eq!(f.cursor("taken").await?, Some(0));
    let long_lease: bool = sqlx::query_scalar(
        "SELECT lease_until > now() + interval '30 minutes' FROM es.subscriptions WHERE name = 'taken'",
    )
    .fetch_one(&f.pool)
    .await?;
    assert!(long_lease, "the deposed instance changed the new lease");

    // The other instance lets go: this one takes over and delivers the batch again.
    sqlx::query("UPDATE es.subscriptions SET lease_until = NULL WHERE name = 'taken'")
        .execute(&f.pool)
        .await?;
    running.wait_position(3).await?;
    running.stop().await?;
    assert_eq!(*calls.lock().unwrap(), 2);

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

/// Counts its calls; the first waits until the test lets it go. Implements both handler kinds.
#[derive(Clone)]
struct FirstWaits {
    calls: Arc<Mutex<usize>>,
    entered: Arc<Notify>,
    go: Arc<Notify>,
}

impl FirstWaits {
    fn new() -> Self {
        Self {
            calls: Arc::new(Mutex::new(0)),
            entered: Arc::new(Notify::new()),
            go: Arc::new(Notify::new()),
        }
    }

    fn calls(&self) -> usize {
        *self.calls.lock().unwrap()
    }

    async fn call(&self) {
        let call = {
            let mut calls = self.calls.lock().unwrap();
            *calls += 1;
            *calls
        };
        if call == 1 {
            self.entered.notify_one();
            self.go.notified().await;
        }
    }
}

impl handler::Handler for FirstWaits {
    async fn handle(&mut self, _batch: &batch::Batch) -> Result<(), handler::HandlerError> {
        self.call().await;
        Ok(())
    }
}

impl handler::PgHandler for FirstWaits {
    async fn handle(
        &mut self,
        _tx: &mut PgConnection,
        _batch: &batch::Batch,
    ) -> Result<(), handler::HandlerError> {
        self.call().await;
        Ok(())
    }
}

/// A migration that drops support for this crate between lease renewals stops the subscription with
/// `CrateTooOld` at the checkpoint that renews the lease, whether it checkpoints a delivered batch or an
/// empty page that only moves the cursor, for `run` and `run_pg`. The refused checkpoint leaves the cursor and
/// the fence where they were, so reading the cursor again tells nothing: reading the same page and being
/// refused again would go on forever, the handler called again each time. A `run_pg` batch is refused
/// earlier, by the lock on the subscription's row that begins its transaction (`subscription/hold.sql`,
/// which checks the crate's version as the renewal does), so its handler is not called at all.
///
/// The first batch's handler waits while the test raises `min_compatible` and locks the log, so the next page
/// is read only after more than half of the lease has gone: its checkpoint renews the lease, and no renewal
/// runs before it.
#[tokio::test]
async fn a_migration_past_the_crate_stops_a_subscription_at_a_renewing_checkpoint() -> Result<()> {
    for pg in [false, true] {
        for empty in [false, true] {
            let case = format!("run_pg {pg}, empty page {empty}");
            let f = Fixture::new(pges_postgres::store::Outbox::Disabled).await?;
            f.append(Cat::Jobs, stream::StreamId::new_v7(), 1).await?;
            let next = if empty { Cat::Orders } else { Cat::Jobs };
            f.append(next, stream::StreamId::new_v7(), 1).await?;
            f.sequence_all().await?;

            let handler = FirstWaits::new();
            let builder = f
                .store
                .subscription(subscription_name!("old"))
                .categories(non_empty![category!("jobs")])
                .batch_size(NonZeroU32::new(1).unwrap())
                .lease(interval!(400 ms));
            let running = common::Running::spawn(if pg {
                builder.run_pg(handler.clone())
            } else {
                builder.run(handler.clone())
            });

            tokio::time::timeout(Duration::from_secs(10), handler.entered.notified())
                .await
                .context("the first batch was not delivered")?;
            let updated = sqlx::query(
                "UPDATE pges.scopes SET schema_version = 9, min_compatible = 9 WHERE scope = 'es'",
            )
            .execute(&f.pool)
            .await?;
            assert_eq!(updated.rows_affected(), 1);
            let mut lock = f.pool.begin().await?;
            sqlx::query("LOCK TABLE es.log IN ACCESS EXCLUSIVE MODE")
                .execute(&mut *lock)
                .await?;
            handler.go.notify_one();
            tokio::time::sleep(Duration::from_millis(300)).await;
            lock.rollback().await?;

            let stopped = running.stopped().await;
            let calls = handler.calls();
            let error =
                stopped.with_context(|| format!("{case}: the handler was called {calls} times"))?;
            assert!(
                matches!(
                    &error,
                    subscription::SubscribeError::CrateTooOld { scope, min_compatible, .. }
                        if scope == "es" && min_compatible.get() == 9
                ),
                "{case}: {error}"
            );
            assert_eq!(
                calls,
                if empty || pg { 1 } else { 2 },
                "{case}: the refused batch was handled again, or a run batch not at all"
            );
            f.finish().await?;
        }
    }
    Ok(())
}
