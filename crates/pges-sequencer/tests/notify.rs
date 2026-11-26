//! `NOTIFY pges_<store>` after each batch: a `LISTEN` connection receives the store's high-water mark, the
//! LSN, and the positions of the categories the batch touched.

#![cfg(feature = "watch-notify")]
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

mod common;

use std::time::Duration;

use anyhow::{Context, Result};
use sqlx::postgres::PgListener;

use pges_core::{high_water, stream};
use pges_sequencer::sequencer;

use common::{Fixture, Target};

#[tokio::test]
async fn each_batch_notifies_its_store() -> Result<()> {
    let f = Fixture::new().await?;
    let mut listener = PgListener::connect(f.db.url()).await?;
    listener.listen("pges_es").await?;
    let running = f.start(common::fast()).await?;
    running.wait_leading().await?;

    f.append(Target::Job(stream::StreamId::new_v7()), 2).await?;
    let notice = wait_for_store(&mut listener, 2).await?;
    assert_eq!(
        *notice.categories(),
        high_water::NoticeCategories::Listed(vec![(f.jobs, pges_testing::position(2))])
    );
    let lsn = notice.lsn().context("no LSN")?;

    f.append(Target::Order(stream::StreamId::new_v7()), 1)
        .await?;
    let notice = wait_for_store(&mut listener, 3).await?;
    // Only the touched category, unless two batches were merged into one notification.
    let orders = (f.orders, pges_testing::position(3));
    let jobs = (f.jobs, pges_testing::position(2));
    assert!(
        *notice.categories() == high_water::NoticeCategories::Listed(vec![orders])
            || *notice.categories() == high_water::NoticeCategories::Listed(vec![jobs, orders]),
        "{notice:?}"
    );
    assert!(notice.lsn().context("no LSN")? > lsn);

    let high = running.watch.high_water(f.store_id).borrow().clone();
    assert_eq!(high.store(), notice.store());
    assert_eq!(high.lsn(), notice.lsn());

    running.stop().await?;
    f.finish().await
}

#[tokio::test]
async fn notifications_can_be_turned_off() -> Result<()> {
    let f = Fixture::new().await?;
    let mut listener = PgListener::connect(f.db.url()).await?;
    listener.listen("pges_es").await?;
    let mut config = common::fast();
    config.notify = sequencer::Notify::Disabled;
    let running = f.start(config).await?;
    running.wait_leading().await?;

    f.append(Target::Job(stream::StreamId::new_v7()), 1).await?;
    common::wait_sequenced(&f.pool, 1).await?;
    let received = tokio::time::timeout(Duration::from_millis(500), listener.recv()).await;
    assert!(received.is_err(), "{received:?}");

    running.stop().await?;
    f.finish().await
}

/// NOTIFY fails for every batch: the log keeps moving, nothing is retried as `Recovering`, and notifications
/// resume once NOTIFY works again.
#[tokio::test]
async fn a_failing_notify_does_not_hold_up_the_log() -> Result<()> {
    let f = Fixture::new().await?;
    // The sequencer's `pg_notify` resolves to a function that counts its calls and fails: one with the same
    // signature, earlier on the search path of new connections.
    sqlx::query("CREATE SEQUENCE public.notify_calls")
        .execute(&f.pool)
        .await?;
    sqlx::query(
        "CREATE FUNCTION public.pg_notify(text, text) RETURNS void LANGUAGE plpgsql AS $$
         BEGIN
             PERFORM nextval('public.notify_calls');
             RAISE EXCEPTION 'NOTIFY is broken';
         END $$",
    )
    .execute(&f.pool)
    .await?;
    sqlx::query(sqlx::AssertSqlSafe(format!(
        r#"ALTER DATABASE "{}" SET search_path = public, pg_catalog"#,
        f.db.name()
    )))
    .execute(&f.pool)
    .await?;
    let mut listener = PgListener::connect(f.db.url()).await?;
    listener.listen("pges_es").await?;
    let running = f.start(common::fast()).await?;
    running.wait_leading().await?;

    for n in 1..=3 {
        f.append(Target::Job(stream::StreamId::new_v7()), 1).await?;
        common::wait_sequenced(&f.pool, n).await?;
    }
    common::wait_high_water(&running.watch, f.store_id, 3).await?;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while notify_calls(&f).await? == 0 {
        assert!(
            tokio::time::Instant::now() < deadline,
            "the broken pg_notify was never called"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let received = tokio::time::timeout(Duration::from_millis(300), listener.recv()).await;
    assert!(received.is_err(), "{received:?}");

    sqlx::query("DROP FUNCTION public.pg_notify(text, text)")
        .execute(&f.pool)
        .await?;
    f.append(Target::Job(stream::StreamId::new_v7()), 1).await?;
    let notice = wait_for_store(&mut listener, 4).await?;
    assert_eq!(notice.store(), pges_testing::position(4));
    assert_eq!(common::check_log(&f.pool).await?.len(), 4);

    running.stop().await?;
    f.finish().await
}

async fn notify_calls(f: &Fixture) -> Result<i64> {
    let (last, called): (i64, bool) =
        sqlx::query_as("SELECT last_value, is_called FROM public.notify_calls")
            .fetch_one(&f.pool)
            .await?;
    Ok(if called { last } else { 0 })
}

/// Receives notifications until one reports the store at `store`.
async fn wait_for_store(listener: &mut PgListener, store: u64) -> Result<high_water::Notice> {
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let notification = listener.recv().await?;
            assert_eq!(notification.channel(), "pges_es");
            let notice = high_water::Notice::decode(notification.payload())?;
            if notice.store() == pges_testing::position(store) {
                return anyhow::Ok(notice);
            }
            assert!(notice.store() < pges_testing::position(store), "{notice:?}");
        }
    })
    .await
    .context("waiting for a notification")?
}
