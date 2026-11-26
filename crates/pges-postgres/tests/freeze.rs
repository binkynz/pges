//! Freezing closed partitions: `Admin::freeze_closed_partitions` freezes the time leaves no append can write
//! to and the full log partitions, once, and `Admin::partition_ages` reports every partition.
//!
//! Rows are written with SQL, a few at a time, so autovacuum never processes the tables during a test.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::time::Duration;

use anyhow::Result;
use sqlx::{Connection, PgPool};

use pges_postgres::{admin, freeze};

const STORE_NAME: pges_core::store::StoreName = pges_core::store_name!("es");

struct Fixture {
    db: pges_testing::TestDb,
    pool: PgPool,
    admin: admin::Admin,
}

impl Fixture {
    /// A store with `jobs`, monthly leaves from two months back to one ahead, ten commits in the oldest leaf
    /// and ten in the current one, and log partitions 0 (full) and 1.
    async fn new() -> Result<Self> {
        let db = pges_testing::TestDb::new().await?;
        let mut conn = db.connect().await?;
        pges_testing::create_store(
            &mut conn,
            &STORE_NAME,
            &pges_testing::definition(pges_postgres::store::Outbox::Disabled),
        )
        .await?;
        let jobs = pges_testing::register_category(
            &mut conn,
            &STORE_NAME,
            pges_core::category!("jobs"),
            2,
            1,
        )
        .await?;
        pges_testing::create_log_partition(&mut conn, &STORE_NAME, 0).await?;
        pges_testing::create_log_partition(&mut conn, &STORE_NAME, 1).await?;

        sqlx::query(
            "INSERT INTO es.commits
               (last_version, commit_id, stream_id, category_id, event_count, format, payload)
             SELECT 1, pges.uuidv7_floor(at + i * interval '1 millisecond'), gen_random_uuid(), $1, 1, 1,
                    '\\x00'
             FROM unnest(ARRAY[date_trunc('month', now() AT TIME ZONE 'UTC') AT TIME ZONE 'UTC'
                                 - interval '2 months' + interval '1 day',
                               now() - interval '1 minute']) AS at,
                  generate_series(0, 9) AS i",
        )
        .bind(jobs.get())
        .execute(&mut conn)
        .await?;
        sqlx::query(
            "INSERT INTO es.log (last_position, commit_id, category_id, event_count)
             SELECT p, gen_random_uuid(), $1, 1 FROM unnest(ARRAY[1, 99999999, 100000000]::int8[]) AS p",
        )
        .bind(jobs.get())
        .execute(&mut conn)
        .await?;

        let pool = db.pool().await?;
        let admin = admin::Admin::new(pool.clone(), STORE_NAME);

        Ok(Self { db, pool, admin })
    }

    async fn finish(self) -> Result<()> {
        self.pool.close().await;
        self.db.cleanup().await
    }

    /// The leaf of the month `months_back` before the current one.
    async fn leaf(&self, months_back: i32) -> Result<String> {
        Ok(sqlx::query_scalar(
            "SELECT 'commits_jobs_' || to_char(now() AT TIME ZONE 'UTC' - make_interval(months => $1), 'YYYYMM')",
        )
        .bind(months_back)
        .fetch_one(&self.pool)
        .await?)
    }

    async fn age(&self, partition: &str) -> Result<freeze::PartitionAge> {
        let ages = self.admin.partition_ages().await?;
        Ok(ages
            .into_iter()
            .find(|age| age.partition == partition)
            .unwrap_or_else(|| panic!("{partition} is reported")))
    }
}

#[tokio::test]
async fn closed_partitions_are_frozen_once() -> Result<()> {
    let f = Fixture::new().await?;
    let (old, current) = (f.leaf(2).await?, f.leaf(0).await?);
    for table in [old.as_str(), current.as_str(), "log_0000", "log_0001"] {
        let age = f.age(table).await?;
        assert!(age.pages > 0 && age.frozen_pages == 0, "{table}: {age:?}");
    }

    let report = f.admin.freeze_closed_partitions().await?;

    let frozen: Vec<&str> = report.frozen.iter().map(|p| p.partition.as_str()).collect();
    assert_eq!(frozen, [old.as_str(), "log_0000"], "{report:?}");
    assert!(report.busy.is_empty());
    for partition in &report.frozen {
        assert!(partition.unfrozen_pages > 0 && partition.unfrozen_pages <= partition.pages);
    }

    // The test server commits asynchronously, and vacuum leaves a row unfrozen until the commits around
    // its own are flushed; the next run freezes it then.
    let mut runs = 1;
    loop {
        let again = f.admin.freeze_closed_partitions().await?;
        if again == freeze::FreezeReport::default() {
            break;
        }
        for partition in &again.frozen {
            assert!(
                [old.as_str(), "log_0000"].contains(&partition.partition.as_str()),
                "{again:?}"
            );
        }
        runs += 1;
        assert!(
            runs < 50,
            "the closed partitions are never all frozen: {again:?}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    for table in [old.as_str(), "log_0000"] {
        let age = f.age(table).await?;
        assert_eq!(age.frozen_pages, age.pages, "{table}: {age:?}");
    }
    let open = f.age(&current).await?;
    assert_eq!(
        open.frozen_pages, 0,
        "the current leaf can still be written to"
    );
    let newest = f.age("log_0001").await?;
    assert_eq!(
        newest.frozen_pages, 0,
        "the newest log partition is still written to"
    );

    // A delete in a frozen leaf (delete_stream) clears its page's all-frozen bit, which the recorded
    // all-frozen pages do not show until a vacuum: its dead row makes the next run freeze the leaf again.
    // The deleting connection closes, which publishes its statistics.
    let mut deleter = f.db.connect().await?;
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "DELETE FROM es.{old} WHERE commit_id = (SELECT commit_id FROM es.{old} ORDER BY commit_id LIMIT 1)"
    )))
    .execute(&mut deleter)
    .await?;
    deleter.close().await?;
    let mut runs = 0;
    let refrozen = loop {
        let again = f.admin.freeze_closed_partitions().await?;
        if let Some(partition) = again.frozen.iter().find(|p| p.partition == old.as_str()) {
            break partition.clone();
        }
        runs += 1;
        assert!(runs < 100, "the leaf's dead row is never seen: {again:?}");
        tokio::time::sleep(Duration::from_millis(100)).await;
    };
    assert!(
        refrozen.dead_rows > 0 || refrozen.unfrozen_pages > 0,
        "{refrozen:?}"
    );

    f.finish().await
}

#[tokio::test]
async fn every_partition_is_reported() -> Result<()> {
    let f = Fixture::new().await?;
    let mut expected = vec!["log_0000".to_owned(), "log_0001".to_owned()];
    for months_back in -1..=2 {
        expected.push(f.leaf(months_back).await?);
    }
    for i in 0..4 {
        expected.push(format!("snapshots_p{i:03}"));
        expected.push(format!("streams_p{i:03}"));
    }
    expected.sort();

    let ages = f.admin.partition_ages().await?;

    let names: Vec<&str> = ages.iter().map(|age| age.partition.as_str()).collect();
    assert_eq!(names, expected);
    assert!(
        ages.iter()
            .all(|age| age.xid_age < 1_000_000 && age.multixact_age < 1_000_000),
        "{ages:?}"
    );
    let empty = ages
        .iter()
        .find(|age| age.partition == "streams_p000")
        .unwrap();
    assert_eq!((empty.pages, empty.frozen_pages), (0, 0));

    f.finish().await
}

#[tokio::test]
async fn ages_are_read_while_maintenance_holds_its_lock() -> Result<()> {
    let f = Fixture::new().await?;
    let mut maintainer = f.pool.acquire().await?;
    sqlx::query("SELECT pg_advisory_lock(hashtext('pges.maintain.' || $1))")
        .bind(STORE_NAME.as_str())
        .execute(&mut *maintainer)
        .await?;

    let ages = tokio::time::timeout(Duration::from_secs(10), f.admin.partition_ages())
        .await
        .expect("partition_ages does not wait for the maintenance lock")?;

    assert!(!ages.is_empty());
    maintainer.detach().close().await?;
    f.finish().await
}
