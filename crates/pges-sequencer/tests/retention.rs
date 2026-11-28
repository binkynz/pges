//! Retention (`pges-postgres`) after `LogicalSource::resequence`. Resequence gives the commits it recovers
//! positions in commit id order after the log tail, so a recovered commit can sit after rows of newer
//! commits. Retention must still see it when it drops the commit's leaf, must not advance its scan start
//! past rows of commits that still exist, and `prune_log` followed by another resequence must not sequence
//! a commit twice. A second resequence must not mistake the first one's rows for the newest sequenced
//! commit either.
//!
//! The recovered commit is written months in the past through a store whose recorded clock skew spans it.
//! The test then lowers the store's skew back to the default, so retention may drop leaves from months
//! ago. `Admin::set_writer_limits` refuses to lower it, so the test writes the column itself.

#![cfg(feature = "source-logical")]
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

mod common;

use std::time::Duration;

use anyhow::Result;
use sqlx::Executor;
use uuid::Uuid;

use pges_core::{commit, event::Event as _, stream, version};
use pges_postgres::{admin, limit, retention};
use pges_sequencer::{logical, sequencer, source};

use common::{Fixture, JobEvent, OrderEvent};

const DAY: Duration = Duration::from_secs(86_400);

/// The leaf holding the commit resequence recovered is dropped, though the recovered commit's position is
/// past the first row of a commit too new for that leaf.
#[tokio::test]
async fn retention_drops_a_leaf_whose_commits_were_resequenced() -> Result<()> {
    let f = Fixture::logical_with_history(3, 200 * DAY).await?;
    let admin = admin::Admin::new(f.pool.clone(), common::STORE_NAME);

    // Q, two months back, is sequenced first; P, three months back, is recovered after it.
    let q = id_at(&f, 2, 2, 1).await?;
    let p = id_at(&f, 3, 2, 1).await?;
    sequence_then_recover(&f, q, p).await?;
    let log = common::check_log(&f.pool).await?;
    assert_eq!(common::commit_order(&log), [q, p]);
    assert_eq!(resequenced_range(&f).await?, (2, 2));

    default_skew(&f).await?;
    window(&admin, 7 * DAY).await?;
    let report = admin
        .apply_retention(&retention::RetentionConfig::new(limit::WriterRoles::one(
            f.writer_role().await?,
        )))
        .await?;
    assert!(report.refused.is_empty(), "{report:?}");
    let dropped: Vec<&str> = report.dropped.iter().map(|d| d.leaf.as_str()).collect();
    assert_eq!(dropped[..2], [leaf(&f, 3).await?, leaf(&f, 2).await?]);
    // P's position is the horizon. The scan start stays below P: only Q's row is known to come before
    // every later leaf's commits.
    assert_eq!(horizon(&f).await?, (2, 1));

    f.finish().await
}

/// Retention drops the leaf of a recovered commit P that sits after Q, a commit of the next leaf that is
/// still kept. The scan start must stay below Q, so `prune_log` keeps Q's row and a later resequence does
/// not take Q for a commit without one.
#[tokio::test]
async fn prune_log_then_resequence_sequences_no_commit_twice() -> Result<()> {
    let f = Fixture::logical_with_history(3, 200 * DAY).await?;
    let admin = admin::Admin::new(f.pool.clone(), common::STORE_NAME);

    // Q opens the leaf two months back, too close to P's leaf for retention to stop its scan at Q.
    let q = id_at(&f, 2, 0, 1).await?;
    let p = id_at(&f, 3, 2, 1).await?;
    sequence_then_recover(&f, q, p).await?;

    // Keeps Q's leaf, not P's.
    let keep = keep_two_months_back(&f).await?;
    default_skew(&f).await?;
    window(&admin, keep).await?;
    let report = admin
        .apply_retention(&retention::RetentionConfig::new(limit::WriterRoles::one(
            f.writer_role().await?,
        )))
        .await?;
    let dropped: Vec<&str> = report.dropped.iter().map(|d| d.leaf.as_str()).collect();
    assert_eq!(dropped, [leaf(&f, 3).await?], "{report:?}");

    admin.prune_log(JobEvent::CATEGORY).await?;
    halt(&f).await?;
    let mut source = f.logical_source(f.pool.clone())?;
    let resequenced = source.resequence(&f.resequence_config().await?).await?;
    assert_eq!(
        resequenced.stores[0].commits, 0,
        "a sequenced commit was recovered again"
    );
    common::check_every_commit_once(&f.pool).await?;
    let positions: Vec<i64> =
        sqlx::query_scalar("SELECT last_position FROM es.log WHERE commit_id = $1")
            .bind(q.as_uuid())
            .fetch_all(&f.pool)
            .await?;
    assert_eq!(positions, [1], "prune_log deleted the row of a kept commit");
    // P's position is the horizon; the scan start stays below Q.
    assert_eq!(horizon(&f).await?, (2, 0));

    f.finish().await
}

/// A second resequence searches back from the newest sequenced commit. The first one's recovered row,
/// with an old id but a late position, does not count as the point after which every commit's row lies.
#[tokio::test]
async fn a_second_resequence_ignores_the_order_of_the_first() -> Result<()> {
    let f = Fixture::logical_with_history(3, 200 * DAY).await?;

    let q = commit::CommitId::generate();
    let p = id_at(&f, 3, 2, 1).await?;
    sequence_then_recover(&f, q, p).await?;
    default_skew(&f).await?;

    let running = f.start(common::fast()).await?;
    running.wait_leading().await?;
    let n = append(&f, commit::CommitId::generate()).await?;
    common::wait_sequenced(&f.pool, 3).await?;
    running.stop().await?;
    let log = common::check_log(&f.pool).await?;
    assert_eq!(common::commit_order(&log), [q, p, n]);

    halt(&f).await?;
    let mut source = f.logical_source(f.pool.clone())?;
    let resequenced = source.resequence(&f.resequence_config().await?).await?;
    assert_eq!(
        resequenced.stores[0].commits, 0,
        "a sequenced commit was recovered again"
    );
    common::check_log(&f.pool).await?;
    common::check_every_commit_once(&f.pool).await?;

    f.finish().await
}

/// A resequence that commits while a retention run is under way, after the run started but before it scans
/// the category. The run must read the positions resequence gave the category together with the log, or it
/// takes the recovered row P, positioned after Q of the kept leaf, for one in commit order and moves the
/// scan start past Q.
#[tokio::test]
async fn retention_sees_a_resequence_that_lands_during_its_run() -> Result<()> {
    let f = Fixture::logical_with_history(3, 200 * DAY).await?;
    let admin = admin::Admin::new(f.pool.clone(), common::STORE_NAME);

    // jobs: a leaf retention drops first, and waits on while the orders are resequenced.
    let j = id_at(&f, 3, 2, 1).await?;
    let q = id_at(&f, 2, 0, 1).await?;
    let p = id_at(&f, 3, 2, 2).await?;
    let running = f.start(common::fast()).await?;
    running.wait_leading().await?;
    append(&f, j).await?;
    append_order(&f, q).await?;
    common::wait_sequenced(&f.pool, 2).await?;
    running.stop().await?;
    f.drop_slot().await?;
    append_order(&f, p).await?;

    let keep = keep_two_months_back(&f).await?;
    default_skew(&f).await?;
    window_for(&admin, JobEvent::CATEGORY, keep).await?;
    window_for(&admin, OrderEvent::CATEGORY, keep).await?;

    let mut blocker = f.pool.begin().await?;
    blocker
        .execute("LOCK TABLE es.commits_jobs IN SHARE UPDATE EXCLUSIVE MODE")
        .await?;
    let config = retention::RetentionConfig::new(limit::WriterRoles::one(f.writer_role().await?));
    let run = tokio::spawn({
        let admin = admin.clone();
        async move { admin.apply_retention(&config).await }
    });
    // The run has scanned both categories' settings and waits to detach the jobs leaf.
    while !sqlx::query_scalar::<_, bool>(
        "SELECT EXISTS (SELECT 1 FROM pg_locks WHERE NOT granted
                        AND relation = 'es.commits_jobs'::regclass)",
    )
    .fetch_one(&f.pool)
    .await?
    {
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    // Resequence searches a month back for P only with the wide skew P was written under; the run read
    // the default one already.
    set_skew(&f, "200 days").await?;
    halt_on_missing_slot(&f).await?;
    let mut source = f.logical_source(f.pool.clone())?;
    let resequenced = source.resequence(&f.resequence_config().await?).await?;
    assert_eq!(resequenced.stores[0].commits, 1);
    default_skew(&f).await?;
    blocker.rollback().await?;

    let report = run.await??;
    let dropped: Vec<&str> = report.dropped.iter().map(|d| d.leaf.as_str()).collect();
    assert_eq!(
        dropped,
        [leaf(&f, 3).await?, leaf_of(&f, "orders", 3).await?],
        "{report:?}"
    );
    let log: Vec<uuid::Uuid> =
        sqlx::query_scalar("SELECT commit_id FROM es.log ORDER BY last_position")
            .fetch_all(&f.pool)
            .await?;
    assert_eq!(log, [j.as_uuid(), q.as_uuid(), p.as_uuid()]);
    // P's position is the orders horizon; the scan start stays below Q.
    let orders: (i64, i64) = sqlx::query_as(
        "SELECT horizon_position, retention_scan_from FROM es.categories WHERE name = 'orders'",
    )
    .fetch_one(&f.pool)
    .await?;
    assert_eq!(orders, (3, 0));

    f.finish().await
}

/// `prune_log` deletes the log rows of a category whose commits retention dropped, and those can be the
/// store's newest. A sequencer that starts afterwards must not hand out their positions again: subscribers
/// have passed them, and would skip the commits that got them a second time.
#[tokio::test]
async fn positions_continue_after_prune_log_removed_the_newest_rows() -> Result<()> {
    let f = Fixture::logical_with_history(3, 200 * DAY).await?;
    let admin = admin::Admin::new(f.pool.clone(), common::STORE_NAME);

    let running = f.start(common::fast()).await?;
    running.wait_leading().await?;
    append(&f, id_at(&f, 3, 2, 1).await?).await?;
    append(&f, id_at(&f, 3, 2, 2).await?).await?;
    common::wait_sequenced(&f.pool, 2).await?;
    running.stop().await?;

    default_skew(&f).await?;
    window(&admin, 7 * DAY).await?;
    let report = admin
        .apply_retention(&retention::RetentionConfig::new(limit::WriterRoles::one(
            f.writer_role().await?,
        )))
        .await?;
    assert!(report.refused.is_empty(), "{report:?}");
    admin.prune_log(JobEvent::CATEGORY).await?;
    let rows: i64 = sqlx::query_scalar("SELECT count(*) FROM es.log")
        .fetch_one(&f.pool)
        .await?;
    assert_eq!(rows, 0, "prune_log kept rows of dropped commits");

    let running = f.start(common::fast()).await?;
    running.wait_leading().await?;
    let next = append(&f, commit::CommitId::generate()).await?;
    let position = tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            let position: Option<i64> =
                sqlx::query_scalar("SELECT last_position FROM es.log WHERE commit_id = $1")
                    .bind(next.as_uuid())
                    .fetch_optional(&f.pool)
                    .await?;
            if let Some(position) = position {
                return anyhow::Ok(position);
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await??;
    running.stop().await?;
    assert_eq!(position, 3, "a position was handed out twice");

    f.finish().await
}

/// A leader that crashed before it stored its token leaves log rows above the coarse high-water mark. When
/// retention drops their commits, `prune_log` keeps those rows, so the next term continues after them and
/// does not hand their positions out again. The outbox source does not deliver a drained commit again, so a
/// new commit would take the position; the logical source replays the commit after its token and would give
/// it the same position again.
#[tokio::test]
async fn positions_continue_after_a_crash_before_the_mark_and_prune_log() -> Result<()> {
    for kind in [common::Kind::Outbox, common::Kind::Logical] {
        let f = Fixture::with_history(kind, 3, 200 * DAY).await?;
        let admin = admin::Admin::new(f.pool.clone(), common::STORE_NAME);

        let running = f.start(common::fast()).await?;
        running.wait_leading().await?;
        append(&f, id_at(&f, 3, 2, 1).await?).await?;
        common::wait_sequenced(&f.pool, 1).await?;
        running.stop().await?;

        // The token, and so the mark, would be stored only after an hour: the crash comes first.
        let mut slow = common::fast();
        slow.token_interval = Duration::from_secs(3600);
        let running = f.start(slow).await?;
        running.wait_leading().await?;
        let late = append(&f, id_at(&f, 3, 2, 2).await?).await?;
        common::wait_sequenced(&f.pool, 2).await?;
        running.kill().await;
        let mark: i64 =
            sqlx::query_scalar("SELECT last_position FROM pges.stores WHERE schema_name = $1")
                .bind(common::STORE)
                .fetch_one(&f.pool)
                .await?;
        assert_eq!(mark, 1, "{kind:?}: the crashed leader stored its mark");

        default_skew(&f).await?;
        window(&admin, 7 * DAY).await?;
        let report = admin
            .apply_retention(&retention::RetentionConfig::new(limit::WriterRoles::one(
                f.writer_role().await?,
            )))
            .await?;
        assert!(report.refused.is_empty(), "{kind:?}: {report:?}");
        assert_eq!(
            horizon(&f).await?.1,
            2,
            "{kind:?}: the scan reached the newest row"
        );
        admin.prune_log(JobEvent::CATEGORY).await?;
        let rows: Vec<i64> = sqlx::query_scalar("SELECT last_position FROM es.log")
            .fetch_all(&f.pool)
            .await?;
        assert_eq!(
            rows,
            [2],
            "{kind:?}: prune_log deleted a row above the mark"
        );

        let running = f.start(common::fast()).await?;
        running.wait_leading().await?;
        let next = append(&f, commit::CommitId::generate()).await?;
        let position = tokio::time::timeout(Duration::from_secs(20), async {
            loop {
                let position: Option<i64> =
                    sqlx::query_scalar("SELECT last_position FROM es.log WHERE commit_id = $1")
                        .bind(next.as_uuid())
                        .fetch_optional(&f.pool)
                        .await?;
                if let Some(position) = position {
                    return anyhow::Ok(position);
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await??;
        running.stop().await?;
        assert_eq!(position, 3, "{kind:?}: a position was handed out twice");
        let late_rows: Vec<i64> =
            sqlx::query_scalar("SELECT last_position FROM es.log WHERE commit_id = $1")
                .bind(late.as_uuid())
                .fetch_all(&f.pool)
                .await?;
        assert_eq!(
            late_rows,
            [2],
            "{kind:?}: the late commit was sequenced again"
        );

        f.finish().await?;
    }

    Ok(())
}

/// A resequence after `prune_log` removed the store's newest rows does not hand out their positions again
/// either.
#[tokio::test]
async fn resequence_continues_after_prune_log_removed_the_newest_rows() -> Result<()> {
    let f = Fixture::logical_with_history(3, 200 * DAY).await?;
    let admin = admin::Admin::new(f.pool.clone(), common::STORE_NAME);

    let running = f.start(common::fast()).await?;
    running.wait_leading().await?;
    append(&f, id_at(&f, 3, 2, 1).await?).await?;
    append(&f, id_at(&f, 3, 2, 2).await?).await?;
    common::wait_sequenced(&f.pool, 2).await?;
    running.stop().await?;

    default_skew(&f).await?;
    window(&admin, 7 * DAY).await?;
    let report = admin
        .apply_retention(&retention::RetentionConfig::new(limit::WriterRoles::one(
            f.writer_role().await?,
        )))
        .await?;
    assert!(report.refused.is_empty(), "{report:?}");
    admin.prune_log(JobEvent::CATEGORY).await?;

    f.drop_slot().await?;
    let lost = append(&f, commit::CommitId::generate()).await?;
    halt_on_missing_slot(&f).await?;
    let mut source = f.logical_source(f.pool.clone())?;
    let resequenced = source.resequence(&f.resequence_config().await?).await?;
    assert_eq!(resequenced.stores[0].commits, 1);
    let position: i64 = sqlx::query_scalar("SELECT last_position FROM es.log WHERE commit_id = $1")
        .bind(lost.as_uuid())
        .fetch_one(&f.pool)
        .await?;
    assert_eq!(position, 3, "a position was handed out twice");

    f.finish().await
}

/// A `keep` that retains the leaf two months back and drops the one before it.
async fn keep_two_months_back(f: &Fixture) -> Result<Duration> {
    let keep: f64 = sqlx::query_scalar(
        "SELECT extract(epoch FROM now() - (date_trunc('month', now() AT TIME ZONE 'UTC') AT TIME ZONE 'UTC'
                                            - interval '1 month'))::float8",
    )
    .fetch_one(&f.pool)
    .await?;
    Ok(Duration::from_secs_f64(keep) + DAY)
}

/// Sequences `q`, then loses the slot, appends `p` and resequences, so `p` is positioned after `q`.
async fn sequence_then_recover(
    f: &Fixture,
    q: commit::CommitId,
    p: commit::CommitId,
) -> Result<()> {
    let running = f.start(common::fast()).await?;
    running.wait_leading().await?;
    append(f, q).await?;
    common::wait_sequenced(&f.pool, 1).await?;
    running.stop().await?;

    f.drop_slot().await?;
    append(f, p).await?;
    halt_on_missing_slot(f).await?;
    let mut source = f.logical_source(f.pool.clone())?;
    let resequenced = source.resequence(&f.resequence_config().await?).await?;
    assert_eq!(resequenced.stores[0].commits, 1);

    Ok(())
}

/// Drops the slot and lets a sequencer find it missing, which halts sequencing.
async fn halt(f: &Fixture) -> Result<()> {
    f.drop_slot().await?;
    halt_on_missing_slot(f).await
}

async fn halt_on_missing_slot(f: &Fixture) -> Result<()> {
    let halting = f.start(common::fast()).await?;
    let result = halting.result().await;
    let Err(sequencer::RunError::Source(source::SourceError::Halt(error))) = &result else {
        panic!("expected a halt, got {result:?}");
    };
    assert!(
        matches!(
            error.downcast_ref::<logical::SlotLost>(),
            Some(logical::SlotLost::Missing { .. })
        ),
        "{error}"
    );
    Ok(())
}

/// Appends one event to a new jobs stream, with commit id `at`.
async fn append(f: &Fixture, at: commit::CommitId) -> Result<commit::CommitId> {
    let appended = f
        .store
        .append(
            stream::StreamId::new_v7(),
            version::ExpectedVersion::NoStream,
            &[JobEvent::Noted { n: 0 }],
        )
        .commit_id(at)
        .await?;
    Ok(appended.commit_id)
}

/// Appends one event to a new orders stream, with commit id `at`.
async fn append_order(f: &Fixture, at: commit::CommitId) -> Result<commit::CommitId> {
    let appended = f
        .store
        .append(
            stream::StreamId::new_v7(),
            version::ExpectedVersion::NoStream,
            &[OrderEvent::Noted { n: 0 }],
        )
        .commit_id(at)
        .await?;
    Ok(appended.commit_id)
}

/// Back to the default skew; the writer with the wide one is done.
async fn default_skew(f: &Fixture) -> Result<()> {
    set_skew(f, "5 seconds").await
}

async fn set_skew(f: &Fixture, skew: &str) -> Result<()> {
    sqlx::query("UPDATE pges.stores SET max_clock_skew = $2::interval WHERE schema_name = $1")
        .bind(common::STORE)
        .bind(skew)
        .execute(&f.pool)
        .await?;
    Ok(())
}

async fn window(admin: &admin::Admin, keep: Duration) -> Result<()> {
    window_for(admin, JobEvent::CATEGORY, keep).await
}

async fn window_for(
    admin: &admin::Admin,
    category: pges_core::category::CategoryName,
    keep: Duration,
) -> Result<()> {
    admin
        .set_retention(
            category,
            retention::Retention::Window {
                keep,
                then: retention::AfterWindow::Drop,
            },
        )
        .await?;
    Ok(())
}

/// A commit id `months_back` months before the start of the current month, plus `day` days and `ms`
/// milliseconds.
async fn id_at(f: &Fixture, months_back: i32, day: i32, ms: i64) -> Result<commit::CommitId> {
    let millis: i64 = sqlx::query_scalar(
        "SELECT (extract(epoch FROM date_trunc('month', now() AT TIME ZONE 'UTC') AT TIME ZONE 'UTC'
                 - make_interval(months => $1) + make_interval(days => $2)) * 1000)::int8 + $3",
    )
    .bind(months_back)
    .bind(day)
    .bind(ms)
    .fetch_one(&f.pool)
    .await?;
    let millis = u64::try_from(millis)?;
    let ts = uuid::Timestamp::from_unix(
        uuid::NoContext,
        millis / 1000,
        u32::try_from(millis % 1000)? * 1_000_000,
    );
    Ok(commit::CommitId::from_uuid(Uuid::new_v7(ts)).unwrap())
}

/// The jobs leaf `months_back` months before the current one.
async fn leaf(f: &Fixture, months_back: i32) -> Result<String> {
    leaf_of(f, "jobs", months_back).await
}

/// The `category` leaf `months_back` months before the current one.
async fn leaf_of(f: &Fixture, category: &str, months_back: i32) -> Result<String> {
    Ok(sqlx::query_scalar(
        "SELECT 'commits_' || $2 || '__' || to_char(date_trunc('month', now() AT TIME ZONE 'UTC')
                                                   - make_interval(months => $1), 'YYYYMM')",
    )
    .bind(months_back)
    .bind(category)
    .fetch_one(&f.pool)
    .await?)
}

/// The jobs category's `horizon_position` and `retention_scan_from`.
async fn horizon(f: &Fixture) -> Result<(i64, i64)> {
    Ok(sqlx::query_as(
        "SELECT horizon_position, retention_scan_from FROM es.categories WHERE name = 'jobs'",
    )
    .fetch_one(&f.pool)
    .await?)
}

/// The jobs category's `resequenced_from` and `resequenced_through`.
async fn resequenced_range(f: &Fixture) -> Result<(i64, i64)> {
    Ok(sqlx::query_as(
        "SELECT resequenced_from, resequenced_through FROM es.categories WHERE name = 'jobs'",
    )
    .fetch_one(&f.pool)
    .await?)
}
