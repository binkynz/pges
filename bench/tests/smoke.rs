//! Runs every benchmark for about a second with small settings, so they keep compiling and working.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use clap::Parser;

async fn run(args: &[&str]) -> pges_bench::report::Report {
    let cli =
        pges_bench::Cli::try_parse_from(std::iter::once("pges-bench").chain(args.iter().copied()))
            .unwrap();
    pges_bench::run(cli).await.unwrap()
}

fn number(record: &serde_json::Value, field: &str) -> f64 {
    record[field]
        .as_f64()
        .unwrap_or_else(|| panic!("{field} missing in {record}"))
}

#[tokio::test(flavor = "multi_thread")]
async fn append_runs() {
    let report = run(&[
        "append",
        "--streams",
        "1000",
        "--concurrency",
        "4",
        "--duration",
        "1s",
        "--warmup",
        "200ms",
    ])
    .await;

    assert_eq!(report.records.len(), 4);
    for record in &report.records {
        assert!(number(record, "events") > 0.0, "{record}");
        assert_eq!(number(record, "errors"), 0.0, "{record}");
        assert!(record["wal"]["bytes"].as_i64().unwrap() > 0, "{record}");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn sequencer_runs() {
    let report = run(&[
        "sequencer",
        "--payloads",
        "100,4000",
        "--concurrency",
        "2",
        "--rate",
        "200",
        "--duration",
        "1s",
        "--backlog",
        "500",
        "--streams",
        "200",
    ])
    .await;

    assert_eq!(report.records.len(), 4);
    for record in &report.records {
        assert!(number(record, "events") > 0.0, "{record}");
        assert_eq!(
            record["lag"]["count"].as_u64(),
            record["events"].as_u64(),
            "{record}"
        );
        assert!(number(record, "drain_events_per_s") > 0.0, "{record}");
        assert_eq!(number(record, "recovering"), 0.0, "{record}");
        assert!(
            number(record, "announced_before_return") <= number(record, "events"),
            "{record}"
        );
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn outbox_notify_runs() {
    let report = run(&[
        "outbox-notify",
        "--concurrency",
        "1,4",
        "--duration",
        "500ms",
        "--streams",
        "100",
        "--warmup",
        "200ms",
    ])
    .await;

    assert_eq!(report.records.len(), 4);
    assert_eq!(report.table.rows.len(), 2);
    for record in &report.records {
        assert!(number(record, "appends") > 0.0, "{record}");
        assert_eq!(number(record, "errors"), 0.0, "{record}");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn read_runs() {
    let report = run(&[
        "read",
        "--events",
        "2000",
        "--lengths",
        "10,100",
        "--load-streams",
        "3",
        "--duration",
        "500ms",
        "--warmup",
        "200ms",
    ])
    .await;

    assert_eq!(report.records.len(), 3);
    assert_eq!(number(&report.records[0], "events"), 2000.0);
    for record in &report.records[1..] {
        assert!(number(record, "loads") > 0.0, "{record}");
        assert_eq!(number(record, "errors"), 0.0, "{record}");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn partitions_runs() {
    let report = run(&[
        "partitions",
        "--leaves",
        "3,10",
        "--events",
        "2000",
        "--streams",
        "50",
        "--readers",
        "2",
        "--duration",
        "500ms",
    ])
    .await;

    assert_eq!(report.records.len(), 14);
    for record in &report.records {
        assert!(number(record, "leaves") >= 3.0, "{record}");
        match record["what"].as_str().unwrap() {
            "catch-up category" | "catch-up store" => {
                assert_eq!(number(record, "events"), 2001.0, "{record}");
            }
            "concurrent category reads" | "concurrent category reads, LATERAL" => {
                assert!(number(record, "reads") > 0.0, "{record}");
                assert_eq!(number(record, "errors"), 0.0, "{record}");
            }
            _ => {
                let generic = &record["statement"]["generic"];
                assert!(number(generic, "relation_locks") > 0.0, "{record}");
            }
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn wake_runs() {
    let report = run(&[
        "wake",
        "--subscriptions",
        "2",
        "--rate",
        "200",
        "--concurrency",
        "4",
        "--duration",
        "1s",
        "--warmup",
        "200ms",
    ])
    .await;

    assert_eq!(report.records.len(), 2);
    for record in &report.records {
        assert!(number(record, "events") > 0.0, "{record}");
        assert_eq!(number(record, "errors"), 0.0, "{record}");
        assert_eq!(
            number(record, "delivered"),
            2.0 * number(record, "events"),
            "{record}"
        );
        assert_eq!(number(record, "recovering"), 0.0, "{record}");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn scale_runs() {
    let db = pges_testing::TestDb::new().await.unwrap();
    // Resequence and retention check that the writer role caps its transactions; set for this database only.
    let mut conn = db.connect().await.unwrap();
    let cap = format!(
        "ALTER ROLE current_user IN DATABASE {} SET transaction_timeout = '1h'",
        db.name()
    );
    sqlx::Executor::execute(&mut conn, sqlx::AssertSqlSafe(cap))
        .await
        .unwrap();
    sqlx::Connection::close(conn).await.unwrap();
    let scale = |action: &[&str]| -> Vec<String> {
        let mut args: Vec<String> = ["--database-url", db.url(), "scale", action[0]]
            .iter()
            .map(|arg| (*arg).to_owned())
            .collect();
        args.extend(["--database".to_owned(), db.name().to_owned()]);
        args.extend(action[1..].iter().map(|arg| (*arg).to_owned()));
        args
    };
    let run_scale = |action: &[&str]| {
        let args = scale(action);
        async move { run(&args.iter().map(String::as_str).collect::<Vec<_>>()).await }
    };

    let filled = run_scale(&[
        "fill",
        "--streams",
        "1000",
        "--months",
        "3",
        "--days",
        "5",
        "--chunk",
        "300",
        "--log-window",
        "700",
    ])
    .await;
    assert_eq!(filled.records.len(), 3);
    assert_eq!(number(&filled.records[0], "commits"), 1500.0);

    let appended = run_scale(&[
        "append",
        "--concurrency",
        "2",
        "--duration",
        "300ms",
        "--warmup",
        "100ms",
        "--sample",
        "50",
    ])
    .await;
    let created = run_scale(&[
        "append",
        "--streams",
        "new",
        "--concurrency",
        "2",
        "--duration",
        "300ms",
        "--warmup",
        "100ms",
    ])
    .await;
    for record in appended.records.iter().chain(&created.records) {
        assert!(number(record, "calls") > 0.0, "{record}");
        assert_eq!(number(record, "errors"), 0.0, "{record}");
    }
    let loaded = run_scale(&[
        "load",
        "--category",
        "audit",
        "--concurrency",
        "2",
        "--duration",
        "300ms",
        "--warmup",
        "100ms",
    ])
    .await;
    assert!(number(&loaded.records[0], "loads") > 0.0);
    assert_eq!(number(&loaded.records[0], "events_per_load"), 20.0);
    assert_eq!(number(&loaded.records[0], "errors"), 0.0);

    let caught_up = run_scale(&[
        "catch-up",
        "--scope",
        "store",
        "--from",
        "0",
        "--duration",
        "60s",
    ])
    .await;
    assert_eq!(
        caught_up.records[0]["reached"],
        caught_up.records[0]["newest"]
    );

    let drained = run_scale(&["drain", "--commits", "300", "--limit", "60s"]).await;
    assert!(number(&drained.records[0], "resequence_seconds") >= 0.0);

    let retained = run_scale(&["retention", "--category", "jobs", "--keep-days", "1"]).await;
    let dropped = retained.records[0]["dropped"].as_array().unwrap();
    assert!(!dropped.is_empty(), "{}", retained.records[0]);

    db.cleanup_with_slots().await.unwrap();
}
