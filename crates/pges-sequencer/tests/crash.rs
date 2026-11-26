//! Crash correctness (design.md, Spikes before building, spike 2): a sequencer killed at an exact point of
//! its batch loop, under concurrent appends, with the outbox and the logical source. After a restart, or a
//! standby taking over, no commit appears twice in the log, positions are gap-free, every commit is
//! sequenced exactly once, and the log keeps commit order in each category and in the store.
//!
//! The fault points come from a wrapper around the real source (`Faulty`), which stops the sequencer inside
//! `next`, `ack` or `ready`, so the kill lands at the same step every run:
//!
//! - `BeforeCommit`: the batch wrote its log rows, and the transaction has not committed. The wrapper sets
//!   `pges_test.fault` in the batch transaction, and a statement trigger on the log sleeps when it is set.
//! - `BeforeAck`: the batch, or the token write, committed; the source has not been acked.
//! - `AfterAck`: the source was acked; the next batch has not started.
//!
//! The killed sequencer's task is dropped and the server ends its sessions, as if the process had died.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

mod common;

use std::{num::NonZeroUsize, sync::Arc, time::Duration};

use anyhow::{Context, Result, ensure};
use proptest::prelude::*;
use sqlx::{Executor, PgConnection, PgPool};
use tokio::sync::oneshot;
use uuid::Uuid;

use pges_sequencer::{sequencer, source};

use common::{Fixture, Kind, Load, Running};

const WRITERS: usize = 4;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn outbox_killed_before_the_log_commit() -> Result<()> {
    crash_case(
        Kind::Outbox,
        Case::new(Point::BeforeCommit, 3, Takeover::Restart),
    )
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn outbox_killed_between_the_log_commit_and_the_ack() -> Result<()> {
    crash_case(
        Kind::Outbox,
        Case::new(Point::BeforeAck, 3, Takeover::Standby),
    )
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn outbox_killed_between_the_ack_and_the_next_batch() -> Result<()> {
    crash_case(
        Kind::Outbox,
        Case::new(Point::AfterAck, 3, Takeover::Restart),
    )
    .await
}

#[cfg(feature = "source-logical")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn logical_killed_before_the_log_commit() -> Result<()> {
    crash_case(
        Kind::Logical,
        Case::new(Point::BeforeCommit, 3, Takeover::Standby),
    )
    .await
}

/// The token is stored with every batch, so the stored token is ahead of the slot's confirmed position
/// when the sequencer dies.
#[cfg(feature = "source-logical")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn logical_killed_between_the_log_commit_and_the_ack() -> Result<()> {
    let mut case = Case::new(Point::BeforeAck, 3, Takeover::Restart);
    case.token_interval = Duration::ZERO;
    crash_case(Kind::Logical, case).await
}

#[cfg(feature = "source-logical")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn logical_killed_between_the_ack_and_the_next_batch() -> Result<()> {
    crash_case(
        Kind::Logical,
        Case::new(Point::AfterAck, 3, Takeover::Standby),
    )
    .await
}

/// Batches commit without a token for an hour, so the next leader replays every commit since the first
/// batch from the slot and drops those already in the log.
#[cfg(feature = "source-logical")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn logical_killed_before_the_log_commit_replays_from_an_old_token() -> Result<()> {
    let mut case = Case::new(Point::BeforeCommit, 5, Takeover::Restart);
    case.token_interval = Duration::from_secs(3600);
    crash_case(Kind::Logical, case).await
}

fn case() -> impl Strategy<Value = Case> {
    (
        prop_oneof![
            Just(Point::BeforeCommit),
            Just(Point::BeforeAck),
            Just(Point::AfterAck),
        ],
        1..5_usize,
        prop_oneof![Just(Takeover::Restart), Just(Takeover::Standby)],
        prop_oneof![
            Just(Duration::ZERO),
            Just(Duration::from_millis(30)),
            Just(Duration::from_secs(3600)),
        ],
        1..8_usize,
    )
        .prop_map(|(point, at, takeover, token_interval, batch_size)| {
            let mut case = Case::new(point, at, takeover);
            // Without a stored token nothing is acked, so an ack point would never be reached.
            case.token_interval = match point {
                Point::BeforeAck | Point::AfterAck if token_interval > Duration::from_secs(1) => {
                    Duration::from_millis(30)
                }
                _ => token_interval,
            };
            case.batch_size = NonZeroUsize::new(batch_size).unwrap();
            case
        })
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 6, ..ProptestConfig::default() })]

    #[test]
    fn outbox_survives_a_kill_at_any_point(case in case()) {
        let result = runtime().block_on(crash_case(Kind::Outbox, case));
        prop_assert!(result.is_ok(), "{:?}", result);
    }
}

#[cfg(feature = "source-logical")]
proptest! {
    #![proptest_config(ProptestConfig { cases: 6, ..ProptestConfig::default() })]

    #[test]
    fn logical_survives_a_kill_at_any_point(case in case()) {
        let result = runtime().block_on(crash_case(Kind::Logical, case));
        prop_assert!(result.is_ok(), "{:?}", result);
    }
}

/// Where the first sequencer stops before it is killed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Point {
    /// Inside a batch that brought commits: its log rows are written, its transaction is open.
    BeforeCommit,
    /// In `Source::ack`, before the source learns of the durable token.
    BeforeAck,
    /// In the first `Source::ready` after the source was acked.
    AfterAck,
}

/// Who sequences after the kill.
#[derive(Debug, Clone, Copy)]
enum Takeover {
    /// A new sequencer, started after the kill.
    Restart,
    /// A standby, started before the kill, that takes over once the lease expires.
    Standby,
}

#[derive(Debug, Clone)]
struct Case {
    point: Point,
    /// Which occurrence of the point stops the sequencer, from 1.
    at: usize,
    takeover: Takeover,
    token_interval: Duration,
    batch_size: NonZeroUsize,
}

impl Case {
    fn new(point: Point, at: usize, takeover: Takeover) -> Self {
        Self {
            point,
            at,
            takeover,
            token_interval: Duration::from_millis(30),
            batch_size: NonZeroUsize::new(4).unwrap(),
        }
    }

    fn config(&self) -> sequencer::SequencerConfig {
        let mut config = common::fast();
        config.token_interval = self.token_interval;
        config.batch_size = self.batch_size;
        config
    }
}

async fn crash_case(kind: Kind, case: Case) -> Result<()> {
    let f = Arc::new(Fixture::with(kind).await?);
    install_hang(&mut f.db.connect().await?).await?;

    let first = format!("t_crash_first_{}", f.db.name());
    let pool = f.named_pool(&first).await?;
    let (hit, reached) = oneshot::channel();
    let faulty = Faulty::new(f.source(pool.clone())?, case.point, case.at, hit);
    let leader = Running::start(pool, faulty, case.config())?;
    // The logical source only sees commits once its slot exists.
    leader.wait_leading().await?;
    let load = Load::start(&f, WRITERS);
    let standby = match case.takeover {
        Takeover::Standby => Some(f.start(case.config()).await?),
        Takeover::Restart => None,
    };

    let stopped_batch = tokio::time::timeout(Duration::from_secs(20), reached)
        .await
        .with_context(|| format!("{case:?} was never reached"))??;
    if case.point == Point::BeforeCommit {
        wait_hanging(&f.pool, &first).await?;
    }
    let errors = leader.errors();
    ensure!(
        errors.is_empty(),
        "the first sequencer recovered from {errors:?}"
    );
    leader.crash(&f.pool, &first).await?;
    // A standby may lead already; without one, the batch the kill cut short must have left no trace.
    if let Takeover::Restart = case.takeover {
        check_rolled_back(&f, &stopped_batch).await?;
    }

    let next = match standby {
        Some(standby) => standby,
        None => f.start(case.config()).await?,
    };
    next.wait_leading().await?;
    // Writers keep appending under the new leader.
    tokio::time::sleep(Duration::from_millis(300)).await;
    let appended = load.stop().await?;

    let commits = common::commit_count(&f.pool).await?;
    ensure!(
        commits == appended.len(),
        "{commits} commits, {} appends",
        appended.len()
    );
    common::wait_sequenced(&f.pool, commits).await?;
    let log = common::check_log(&f.pool).await?;
    common::check_every_commit_once(&f.pool).await?;
    common::check_commit_order(&log, &appended)?;
    next.stop().await?;

    Arc::into_inner(f).unwrap().finish().await
}

/// The commits of a batch killed before its log commit are not in the log, and the outbox still holds them.
async fn check_rolled_back(f: &Fixture, batch: &[Uuid]) -> Result<()> {
    let logged: i64 = sqlx::query_scalar("SELECT count(*) FROM es.log WHERE commit_id = ANY($1)")
        .bind(batch)
        .fetch_one(&f.pool)
        .await?;
    ensure!(
        logged == 0,
        "{logged} commits of the killed batch are in the log"
    );

    if f.kind == Kind::Outbox {
        let pending: i64 =
            sqlx::query_scalar("SELECT count(*) FROM es.pending WHERE commit_id = ANY($1)")
                .bind(batch)
                .fetch_one(&f.pool)
                .await?;
        ensure!(
            pending == i64::try_from(batch.len())?,
            "{pending} of the killed batch's {} commits are pending",
            batch.len()
        );
    }
    Ok(())
}

/// Makes a batch hang after its log rows are inserted, when the batch transaction set `pges_test.fault`.
async fn install_hang(conn: &mut PgConnection) -> Result<()> {
    conn.execute(
        "CREATE FUNCTION public.t_hang_batch() RETURNS trigger LANGUAGE plpgsql AS $$
         BEGIN
             IF current_setting('pges_test.fault', true) = 'hang' THEN
                 PERFORM pg_sleep(3600);
             END IF;
             RETURN NULL;
         END $$",
    )
    .await?;
    conn.execute(
        "CREATE TRIGGER t_hang_batch AFTER INSERT ON es.log
         FOR EACH STATEMENT EXECUTE FUNCTION public.t_hang_batch()",
    )
    .await?;
    Ok(())
}

/// Waits until a session of `application_name` sleeps in the log trigger.
async fn wait_hanging(pool: &PgPool, application_name: &str) -> Result<()> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let hanging: bool = sqlx::query_scalar(
            "SELECT EXISTS (SELECT 1 FROM pg_stat_activity
                            WHERE application_name = $1 AND wait_event = 'PgSleep')",
        )
        .bind(application_name)
        .fetch_one(pool)
        .await?;
        if hanging {
            return Ok(());
        }
        ensure!(
            tokio::time::Instant::now() < deadline,
            "the batch never reached the log trigger"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap()
}

/// The real source, stopped for good at the `at`-th occurrence of `point`. `hit` fires when it stops, with
/// the commits of the batch it stopped in before the log commit (none at an ack point).
struct Faulty {
    inner: Box<dyn source::DynSource>,
    point: Point,
    at: usize,
    seen: usize,
    hit: Option<oneshot::Sender<Vec<Uuid>>>,
    stopped: Stopped,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Stopped {
    No,
    Yes,
}

impl Faulty {
    fn new(
        inner: Box<dyn source::DynSource>,
        point: Point,
        at: usize,
        hit: oneshot::Sender<Vec<Uuid>>,
    ) -> Self {
        Self {
            inner,
            point,
            at,
            seen: 0,
            hit: Some(hit),
            stopped: Stopped::No,
        }
    }

    /// Counts an occurrence of `point`; true at the `at`-th.
    fn reached(&mut self, point: Point) -> bool {
        if point != self.point {
            return false;
        }
        self.seen += 1;
        self.seen == self.at
    }

    fn stop(&mut self, batch: &[source::CommitRef]) {
        self.stopped = Stopped::Yes;
        if let Some(hit) = self.hit.take() {
            let _ = hit.send(
                batch
                    .iter()
                    .map(|commit| commit.commit_id.as_uuid())
                    .collect(),
            );
        }
    }
}

impl source::Source for Faulty {
    fn kind(&self) -> source::SourceKind {
        source::Source::kind(&self.inner)
    }

    async fn open(&mut self, token: Option<&[u8]>) -> Result<(), source::SourceError> {
        source::Source::open(&mut self.inner, token).await
    }

    async fn ready(&mut self) -> Result<(), source::SourceError> {
        if self.stopped == Stopped::Yes {
            std::future::pending::<()>().await;
        }
        source::Source::ready(&mut self.inner).await
    }

    async fn next(
        &mut self,
        tx: &mut PgConnection,
        limit: NonZeroUsize,
    ) -> Result<source::SourceBatch, source::SourceError> {
        let batch = source::Source::next(&mut self.inner, tx, limit).await?;
        if !batch.commits.is_empty() && self.reached(Point::BeforeCommit) {
            tx.execute("SET LOCAL pges_test.fault = 'hang'").await?;
            self.stop(&batch.commits);
        }
        Ok(batch)
    }

    async fn ack(&mut self, token: &[u8]) -> Result<(), source::SourceError> {
        if self.reached(Point::BeforeAck) {
            self.stop(&[]);
            std::future::pending::<()>().await;
        }
        source::Source::ack(&mut self.inner, token).await?;
        if self.reached(Point::AfterAck) {
            self.stop(&[]);
        }
        Ok(())
    }

    async fn keep_alive(&mut self) -> Result<(), source::SourceError> {
        source::Source::keep_alive(&mut self.inner).await
    }

    async fn close(&mut self) {
        source::Source::close(&mut self.inner).await;
    }
}
