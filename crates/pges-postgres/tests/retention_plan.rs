//! The plans of retention's per-leaf statements: their cost must follow the leaf, whatever the planner
//! estimates. A leaf that can be dropped gives the checks no row to stop at, so a plan that pairs the leaf
//! with every `streams` row, or every leaf commit with every scanned log row, runs to the end: minutes to
//! hours per leaf at 100 million streams.
//!
//! The planner is pushed towards those plans by disabling the plans retention needs; the statements must
//! keep their shape anyway.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use anyhow::Result;
use sqlx::{Connection, Executor, PgConnection, postgres::types::PgInterval};
use uuid::Uuid;

use pges_postgres::sql::{statement, template};

const STORE_NAME: pges_core::store::StoreName = pges_core::store_name!("es");

struct Fixture {
    db: pges_testing::TestDb,
    conn: PgConnection,
    leaf_name: String,
    leaf: template::Values,
}

impl Fixture {
    /// A store with `jobs` and its leaf of the previous month.
    async fn new() -> Result<Self> {
        let db = pges_testing::TestDb::new().await?;
        let mut conn = db.connect().await?;
        pges_testing::create_store(
            &mut conn,
            &STORE_NAME,
            &pges_testing::definition(pges_postgres::store::Outbox::Disabled),
        )
        .await?;
        pges_testing::register_category(&mut conn, &STORE_NAME, pges_core::category!("jobs"), 1, 0)
            .await?;
        pges_testing::create_log_partition(&mut conn, &STORE_NAME, 0).await?;
        let leaf: String = sqlx::query_scalar(
            "SELECT 'commits_jobs__' || to_char(now() AT TIME ZONE 'UTC' - interval '1 month', 'YYYYMM')",
        )
        .fetch_one(&mut conn)
        .await?;
        let values = template::Values::new().store(&STORE_NAME).ident(
            template::IdentPlaceholder::Leaf,
            &template::Ident::new(leaf.clone()).unwrap(),
        );

        Ok(Self {
            db,
            conn,
            leaf_name: leaf,
            leaf: values,
        })
    }

    async fn finish(self) -> Result<()> {
        self.conn.close().await?;
        self.db.cleanup().await
    }
}

fn text(plan: &[String]) -> String {
    plan.join("\n")
}

fn hour() -> PgInterval {
    PgInterval {
        months: 0,
        days: 0,
        microseconds: 3_600_000_000,
    }
}

#[tokio::test]
async fn the_sequenced_check_is_a_set_difference() -> Result<()> {
    let mut f = Fixture::new().await?;
    let sql = statement::SCAN.render(&f.leaf);

    let mut tx = f.conn.begin().await?;
    tx.execute("SET LOCAL enable_hashagg = off; SET LOCAL enable_sort = off")
        .await?;
    let plan: Vec<String> = sqlx::query_scalar(sqlx::AssertSqlSafe(format!("EXPLAIN {sql}")))
        .bind(1_i32)
        .bind(0_i64)
        .bind(i64::MAX)
        .bind(Uuid::now_v7())
        .bind(hour())
        .bind(hour())
        .bind(0_i64)
        .bind(0_i64)
        .fetch_all(&mut *tx)
        .await?;
    tx.rollback().await?;

    let plan = text(&plan);
    assert!(plan.contains("SetOp"), "{plan}");
    assert!(!plan.contains("Anti Join"), "{plan}");

    f.finish().await
}

#[tokio::test]
async fn policy_checks_look_each_stream_up() -> Result<()> {
    let mut f = Fixture::new().await?;
    let live = statement::LIVE_STREAMS.render(&f.leaf);
    let missing = statement::MISSING_SNAPSHOTS.render(&f.leaf);

    let mut tx = f.conn.begin().await?;
    tx.execute("SET LOCAL enable_nestloop = off").await?;
    let live: Vec<String> = sqlx::query_scalar(sqlx::AssertSqlSafe(format!("EXPLAIN {live}")))
        .bind(hour())
        .bind(100_i64)
        .fetch_all(&mut *tx)
        .await?;
    let missing: Vec<String> =
        sqlx::query_scalar(sqlx::AssertSqlSafe(format!("EXPLAIN {missing}")))
            .bind(1_i32)
            .bind(100_i64)
            .fetch_all(&mut *tx)
            .await?;
    tx.rollback().await?;

    for plan in [text(&live), text(&missing)] {
        assert!(plan.contains("Nested Loop"), "{plan}");
        assert!(
            !plan.contains("Hash Join") && !plan.contains("Merge Join"),
            "{plan}"
        );
        assert!(!plan.contains("Seq Scan on streams"), "{plan}");
        assert!(plan.contains("Index Cond: (stream_id = "), "{plan}");
    }

    f.finish().await
}

#[tokio::test]
async fn floors_are_raised_stream_by_stream() -> Result<()> {
    let mut f = Fixture::new().await?;
    let sql = statement::RAISE_FLOORS.render(&f.leaf);

    let mut tx = f.conn.begin().await?;
    tx.execute("SET LOCAL enable_nestloop = off").await?;
    let unprobed: Vec<String> = sqlx::query_scalar(sqlx::AssertSqlSafe(format!("EXPLAIN {sql}")))
        .fetch_all(&mut *tx)
        .await?;
    tx.execute("SET LOCAL enable_nestloop = on").await?;
    tx.execute(statement::PROBE_JOINS.sql()).await?;
    let probed: Vec<String> = sqlx::query_scalar(sqlx::AssertSqlSafe(format!("EXPLAIN {sql}")))
        .fetch_all(&mut *tx)
        .await?;
    tx.execute(statement::PLAN_JOINS.sql()).await?;
    let restored: Vec<String> = sqlx::query_scalar("SHOW enable_hashjoin")
        .fetch_all(&mut *tx)
        .await?;
    tx.rollback().await?;

    let unprobed = text(&unprobed);
    assert!(
        unprobed.contains("Hash Join") || unprobed.contains("Merge Join"),
        "the check below can fail: {unprobed}"
    );
    let probed = text(&probed);
    assert!(probed.contains("Nested Loop"), "{probed}");
    // Driven from the leaf: each stream is found through its key, never by reading every streams row.
    assert!(!probed.contains("Seq Scan on streams"), "{probed}");
    assert!(
        probed.contains("Index Cond: (stream_id = d.stream_id)"),
        "{probed}"
    );
    assert!(
        !probed.contains("Hash Join") && !probed.contains("Merge Join"),
        "{probed}"
    );
    assert_eq!(restored, ["on"]);

    f.finish().await
}

/// The statements as they were before their plans were fixed, the reference for their answers.
mod before {
    pub const SCAN: &str = "WITH seen AS (
  SELECT l.last_position, l.commit_id FROM es.log l
  WHERE l.category_id = $1 AND l.last_position > $2 AND l.last_position <= $3
)
SELECT (SELECT max(last_position) FROM seen WHERE commit_id < $4) AS horizon,
       NOT EXISTS (SELECT 1 FROM es.{leaf} d
                   WHERE NOT EXISTS (SELECT 1 FROM seen s
                                     WHERE s.commit_id = d.commit_id)) AS sequenced,
       (SELECT max(last_position) FROM seen
        WHERE commit_id < pges.uuidv7_floor(uuid_extract_timestamp($4::uuid)
                                            - $5::interval - 2 * $6::interval)
          AND last_position NOT BETWEEN $7 AND $8)
         AS next_scan_from";

    pub const SCAN_STOP: &str = "SELECT last_position FROM es.log
WHERE category_id = $1 AND last_position > $2
  AND commit_id >= pges.uuidv7_floor(uuid_extract_timestamp($3::uuid) + $4::interval
                                    + 2 * $5::interval)
ORDER BY last_position
LIMIT 1";

    pub const LIVE_STREAMS: &str = "SELECT DISTINCT d.stream_id FROM es.{leaf} d
JOIN es.streams s ON s.stream_id = d.stream_id
WHERE s.state <> 2 AND s.last_commit_id >= pges.uuidv7_floor(now() - $1::interval)
ORDER BY d.stream_id
LIMIT $2";

    pub const MISSING_SNAPSHOTS: &str = "SELECT d.stream_id
FROM (SELECT stream_id, max(last_version) AS last_version FROM es.{leaf} GROUP BY stream_id) d
JOIN es.streams s ON s.stream_id = d.stream_id
WHERE s.state <> 2
  AND NOT EXISTS (SELECT 1 FROM es.snapshots n
                  WHERE n.stream_id = d.stream_id AND n.category_id = $1
                    AND n.version >= d.last_version)
ORDER BY d.stream_id
LIMIT $2";

    pub const RAISE_FLOORS: &str = "UPDATE es.streams s SET floor_version = d.last_version
FROM (SELECT stream_id, max(last_version) AS last_version FROM es.{leaf} GROUP BY stream_id) d
WHERE s.stream_id = d.stream_id AND s.floor_version < d.last_version";
}

/// Planner settings that each take away a plan the statements could use.
const PLANS: [&str; 7] = [
    "SET LOCAL enable_seqscan = on",
    "SET LOCAL enable_nestloop = off",
    "SET LOCAL enable_hashjoin = off; SET LOCAL enable_mergejoin = off",
    "SET LOCAL enable_hashagg = off",
    "SET LOCAL enable_sort = off",
    "SET LOCAL enable_indexscan = off; SET LOCAL enable_indexonlyscan = off; SET LOCAL enable_bitmapscan = off",
    "SET LOCAL enable_seqscan = off; SET LOCAL enable_material = off; SET LOCAL enable_memoize = off",
];

type ScanRow = (Option<i64>, bool, Option<i64>);

/// One answer of every retention statement, old or new, for a set of scan and policy parameters.
#[derive(Debug, PartialEq)]
struct Answers {
    scans: Vec<ScanRow>,
    stops: Vec<Option<i64>>,
    live: Vec<Vec<Uuid>>,
    missing: Vec<Vec<Uuid>>,
    floors: (u64, Vec<(Uuid, i64)>),
}

impl Fixture {
    /// 3000 commits of 900 streams in the leaf, up to four each; streams rows for most of them (some deleted,
    /// some written to within the hour, some with a floor already raised); snapshots for most, some of
    /// another category or below the stream's last version in the leaf; log rows for most commits, in commit
    /// id order, and some for commits of other leaves.
    async fn histories(&mut self) -> Result<(i32, Uuid)> {
        let jobs: i32 =
            sqlx::query_scalar("SELECT category_id FROM es.categories WHERE name = 'jobs'")
                .fetch_one(&mut self.conn)
                .await?;
        let start =
            "date_trunc('month', now() AT TIME ZONE 'UTC') AT TIME ZONE 'UTC' - interval '1 month'";
        let leaf = self.leaf_name.clone();
        self.conn
            .execute(sqlx::AssertSqlSafe(
                format!(
                    "INSERT INTO es.{leaf}
                       (last_version, commit_id, stream_id, category_id, event_count, format, payload)
                     SELECT i / 900 + 1, pges.uuidv7_floor({start} + interval '1 day' + i * interval '1 second'),
                            md5('s' || i % 900)::uuid, {jobs}, 1, 1, '\\x00'
                     FROM generate_series(0, 2999) AS i;
                     INSERT INTO es.streams
                       (version, floor_version, stream_id, first_commit_id, last_commit_id, category_id, state)
                     SELECT 10, s % 4, md5('s' || s)::uuid, gen_random_uuid(),
                            CASE WHEN s % 3 = 0 THEN pges.uuidv7_floor(now() - interval '1 hour')
                                 ELSE pges.uuidv7_floor(now() - interval '60 days') END,
                            {jobs}, CASE WHEN s % 11 = 0 THEN 2 WHEN s % 7 = 0 THEN 1 ELSE 0 END
                     FROM generate_series(0, 899) AS s WHERE s % 17 <> 0;
                     INSERT INTO es.snapshots
                       (version, stream_id, commit_id, category_id, state_version, format, state)
                     SELECT s % 6, md5('s' || s)::uuid, gen_random_uuid(),
                            CASE WHEN s % 13 = 0 THEN {jobs} + 1 ELSE {jobs} END, 1, 1, '\\x00'
                     FROM generate_series(0, 899) AS s WHERE s % 5 <> 0;
                     INSERT INTO es.log (last_position, commit_id, category_id, event_count)
                     SELECT row_number() OVER (ORDER BY commit_id), commit_id, category_id, 1
                     FROM (SELECT commit_id, category_id FROM es.{leaf} d
                           WHERE ('x' || substr(md5(d.commit_id::text), 1, 4))::bit(16)::int % 23 <> 0
                           UNION ALL
                           SELECT pges.uuidv7_floor({start} + interval '1 month' + i * interval '1 minute'),
                                  {jobs}
                           FROM generate_series(0, 299) AS i
                           UNION ALL
                           SELECT pges.uuidv7_floor({start} + interval '1 day' + i * interval '7 second'),
                                  {jobs} + 1
                           FROM generate_series(0, 99) AS i) c;
                     ANALYZE es.commits, es.streams, es.snapshots, es.log"
                ),
            ))
            .await?;
        let upper: Uuid = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
            "SELECT pges.uuidv7_floor({start} + interval '1 month')"
        )))
        .fetch_one(&mut self.conn)
        .await?;

        Ok((jobs, upper))
    }

    /// Every statement's answers, under the planner settings `plan`, old (`before`) or new.
    async fn answers(&mut self, jobs: i32, upper: Uuid, plan: &str, old: bool) -> Result<Answers> {
        let leaf = self.leaf_name.clone();
        let sql = |old_sql: &str, new: &template::Statement| {
            if old {
                old_sql.replace("{leaf}", &leaf)
            } else {
                new.render(&self.leaf)
            }
        };
        let (scan, stop, live, missing, floors) = (
            sql(before::SCAN, &statement::SCAN),
            sql(before::SCAN_STOP, &statement::SCAN_STOP),
            sql(before::LIVE_STREAMS, &statement::LIVE_STREAMS),
            sql(before::MISSING_SNAPSHOTS, &statement::MISSING_SNAPSHOTS),
            sql(before::RAISE_FLOORS, &statement::RAISE_FLOORS),
        );
        let minutes = |m: i64| PgInterval {
            months: 0,
            days: 0,
            microseconds: m * 60_000_000,
        };

        let mut tx = self.conn.begin().await?;
        tx.execute(sqlx::AssertSqlSafe(plan)).await?;
        let mut answers = Answers {
            scans: Vec::new(),
            stops: Vec::new(),
            live: Vec::new(),
            missing: Vec::new(),
            floors: (0, Vec::new()),
        };
        for (from, to, resequenced) in [
            (0, i64::MAX, (0, 0)),
            (0, 2900, (100, 200)),
            (40, 3100, (0, 0)),
            (0, 1500, (1000, 3400)),
            (3000, i64::MAX, (0, 0)),
        ] {
            for (txn_age, skew) in [(60, 1), (1, 0), (24 * 60, 60)] {
                let row: ScanRow = sqlx::query_as(sqlx::AssertSqlSafe(&*scan))
                    .bind(jobs)
                    .bind(from)
                    .bind(to)
                    .bind(upper)
                    .bind(minutes(txn_age))
                    .bind(minutes(skew))
                    .bind(resequenced.0)
                    .bind(resequenced.1)
                    .fetch_one(&mut *tx)
                    .await?;
                answers.scans.push(row);
            }
        }
        for (from, txn_age, skew) in [
            (0, 60, 1),
            (0, 24 * 60, 60),
            (2000, 1, 0),
            (0, 40 * 24 * 60, 0),
        ] {
            let row: Option<i64> = sqlx::query_scalar(sqlx::AssertSqlSafe(&*stop))
                .bind(jobs)
                .bind(from)
                .bind(upper)
                .bind(minutes(txn_age))
                .bind(minutes(skew))
                .fetch_optional(&mut *tx)
                .await?;
            answers.stops.push(row);
        }
        for (keep, limit) in [(120, 100), (120, 1000), (90 * 24 * 60, 1000), (1, 1000)] {
            let rows: Vec<Uuid> = sqlx::query_scalar(sqlx::AssertSqlSafe(&*live))
                .bind(minutes(keep))
                .bind(limit)
                .fetch_all(&mut *tx)
                .await?;
            answers.live.push(rows);
        }
        for (category, limit) in [(jobs, 100), (jobs, 1000), (jobs + 1, 1000)] {
            let rows: Vec<Uuid> = sqlx::query_scalar(sqlx::AssertSqlSafe(&*missing))
                .bind(category)
                .bind(limit)
                .fetch_all(&mut *tx)
                .await?;
            answers.missing.push(rows);
        }
        if !old {
            tx.execute(statement::PROBE_JOINS.sql()).await?;
        }
        let updated = sqlx::query(sqlx::AssertSqlSafe(&*floors))
            .execute(&mut *tx)
            .await?
            .rows_affected();
        let after: Vec<(Uuid, i64)> =
            sqlx::query_as("SELECT stream_id, floor_version FROM es.streams ORDER BY stream_id")
                .fetch_all(&mut *tx)
                .await?;
        answers.floors = (updated, after);
        tx.rollback().await?;

        Ok(answers)
    }
}

#[tokio::test]
async fn the_statements_answer_as_before_under_every_plan() -> Result<()> {
    let mut f = Fixture::new().await?;
    let (jobs, upper) = f.histories().await?;

    let expected = f.answers(jobs, upper, PLANS[0], true).await?;
    assert!(
        expected.scans.iter().all(|scan| !scan.1),
        "one in 23 commits has no log row"
    );
    assert!(
        expected
            .scans
            .iter()
            .any(|scan| scan.0.is_some() && scan.2.is_some())
    );
    assert!(
        expected.stops.iter().any(Option::is_some) && expected.stops.iter().any(Option::is_none)
    );
    assert!(
        expected.live[3].is_empty(),
        "no stream was written to in the last minute"
    );
    assert_eq!(expected.live[0].len(), 100);
    assert!(expected.live[1].len() > 100 && expected.live[1].len() < expected.live[2].len());
    assert_eq!(expected.missing[0].len(), 100);
    assert!(expected.missing[1].len() > 100);
    assert!(expected.floors.0 > 0);
    for plan in PLANS {
        assert_eq!(
            f.answers(jobs, upper, plan, true).await?,
            expected,
            "before, {plan}"
        );
        assert_eq!(
            f.answers(jobs, upper, plan, false).await?,
            expected,
            "after, {plan}"
        );
    }

    // Every commit of the leaf sequenced: the sequenced check now passes where the scan covers them.
    let leaf = f.leaf_name.clone();
    f.conn
        .execute(
            sqlx::AssertSqlSafe(format!(
                "INSERT INTO es.log (last_position, commit_id, category_id, event_count)
                 SELECT 10000 + row_number() OVER (ORDER BY commit_id), commit_id, category_id, 1
                 FROM es.{leaf} d
                 WHERE NOT EXISTS (SELECT 1 FROM es.log l
                                   WHERE l.category_id = d.category_id AND l.commit_id = d.commit_id)"
            ))
            ,
        )
        .await?;
    let expected = f.answers(jobs, upper, PLANS[0], true).await?;
    assert!(expected.scans[0].1, "{:?}", expected.scans);
    assert!(
        expected.scans.iter().any(|scan| !scan.1),
        "{:?}",
        expected.scans
    );
    for plan in PLANS {
        assert_eq!(
            f.answers(jobs, upper, plan, false).await?,
            expected,
            "after, {plan}"
        );
    }

    f.finish().await
}
