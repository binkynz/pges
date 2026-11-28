//! Streaming against PostgreSQL 18: a store's `commits` table, its publication, and plain SQL inserts.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::time::Duration;

use anyhow::{Context, Result, bail, ensure};
use sqlx::{Connection, Executor, PgConnection};

use pges_core::lsn;
use pges_replication::{config, connection, error, name, pgoutput, stream};

const STORE: &str = "s1";
const STORE_NAME: pges_core::store::StoreName = pges_core::store_name!("s1");
const TIMEOUT: Duration = Duration::from_secs(20);

#[tokio::test]
async fn append_arrives_as_begin_relation_insert_commit() -> Result<()> {
    let mut f = Fixture::new().await?;
    let mut connection = f.replication().await?;
    let identity = connection.identify_system().await?;
    assert_eq!(identity.database.as_deref(), Some(f.db.name()));
    let created = connection
        .create_slot(&f.slot, connection::Failover::Disabled)
        .await?;
    assert_eq!(created.name, f.slot);
    assert!(created.consistent_point > lsn::Lsn::ZERO);

    let commit_id = f.append(&mut 0).await?;
    let mut stream = connection.start(&f.start_options()).await?;
    let messages = read_until_commits(&mut stream, 1).await?;

    let kinds: Vec<&str> = messages.iter().map(kind).collect();
    assert_eq!(kinds.first(), Some(&"begin"), "{kinds:?}");
    assert_eq!(kinds.last(), Some(&"commit"), "{kinds:?}");
    assert_eq!(
        kinds.iter().filter(|k| **k == "insert").count(),
        1,
        "{kinds:?}"
    );
    assert!(kinds.contains(&"relation"), "{kinds:?}");

    let (pgoutput::Message::Begin(begin), pgoutput::Message::Commit(commit)) =
        (&messages[0], &messages[messages.len() - 1])
    else {
        panic!("not framed by begin and commit: {kinds:?}");
    };
    assert_eq!(begin.final_lsn, commit.commit_lsn);
    assert!(commit.end_lsn > commit.commit_lsn);

    let mut relations = pgoutput::Relations::new();
    let mut names = Vec::new();
    let mut insert = None;
    for message in &messages {
        match message {
            pgoutput::Message::Relation(relation) => {
                names.push(relation.name.clone());
                relations.insert(relation.clone());
            }
            pgoutput::Message::Insert(i) => insert = Some(i.clone()),
            _ => {}
        }
    }
    let insert = insert.unwrap();

    // PG18 announces the leaf partition too, but publishes the Insert under the root: route by relation id.
    let root = f.oid(&format!("{STORE}.commits")).await?;
    assert_eq!(insert.relation, pgoutput::RelationId(root));
    let relation = relations.get(insert.relation).unwrap();
    assert_eq!(
        (relation.namespace.as_str(), relation.name.as_str()),
        (STORE, "commits")
    );
    assert!(
        names.iter().any(|n| n.starts_with("commits_jobs_")),
        "leaf Relation messages arrive as well: {names:?}"
    );

    // The publication's column list: no payload.
    let columns: Vec<&str> = relation.columns.iter().map(|c| c.name.as_str()).collect();
    assert_eq!(
        columns,
        [
            "last_version",
            "commit_id",
            "stream_id",
            "category_id",
            "event_count"
        ]
    );
    let commit_id_column = columns.iter().position(|c| *c == "commit_id").unwrap();
    assert_eq!(insert.new.text(commit_id_column), Some(commit_id.as_str()));

    let mut connection = stream.stop().await?;
    connection
        .drop_slot(&f.slot, connection::DropWait::NoWait)
        .await?;
    connection.close().await?;
    f.finish().await
}

#[tokio::test]
async fn commits_arrive_in_commit_order() -> Result<()> {
    let mut f = Fixture::new().await?;
    let mut connection = f.replication().await?;
    connection
        .create_slot(&f.slot, connection::Failover::Disabled)
        .await?;

    let mut a = f.writer().await?;
    let mut b = f.writer().await?;
    let mut version = 0;
    a.execute("BEGIN").await?;
    let id_a = append(&mut a, f.jobs, &mut version).await?;
    b.execute("BEGIN").await?;
    let id_b = append(&mut b, f.jobs, &mut version).await?;
    let id_c = f.append(&mut version).await?;
    a.execute("COMMIT").await?;
    b.execute("COMMIT").await?;

    let mut stream = connection.start(&f.start_options()).await?;
    let first = transactions(read_until_commits(&mut stream, 3).await?);
    let order: Vec<&str> = first.iter().map(|t| t.commit_ids[0].as_str()).collect();
    assert_eq!(order, [id_c.as_str(), id_a.as_str(), id_b.as_str()]);
    assert!(
        first
            .windows(2)
            .all(|w| w[0].commit.end_lsn <= w[1].commit.commit_lsn),
        "commit LSNs increase"
    );

    // Many concurrent writers: whatever order they commit in, the stream follows WAL commit order.
    let mut tasks = Vec::new();
    for _ in 0..8 {
        let mut writer = f.writer().await?;
        let jobs = f.jobs;
        tasks.push(tokio::spawn(async move {
            let mut version = 0;
            for _ in 0..5 {
                append(&mut writer, jobs, &mut version).await?;
            }
            writer.close().await?;
            anyhow::Ok(())
        }));
    }
    for task in tasks {
        task.await??;
    }
    let more = transactions(read_until_commits(&mut stream, 40).await?);
    assert_eq!(more.len(), 40);
    assert!(
        more.windows(2)
            .all(|w| w[0].commit.end_lsn <= w[1].commit.commit_lsn),
        "commit LSNs increase"
    );

    let mut connection = stream.stop().await?;
    connection
        .drop_slot(&f.slot, connection::DropWait::NoWait)
        .await?;
    connection.close().await?;
    a.close().await?;
    b.close().await?;
    f.finish().await
}

#[tokio::test]
async fn confirmed_transactions_are_not_resent() -> Result<()> {
    let mut f = Fixture::new().await?;
    let mut connection = f.replication().await?;
    connection
        .create_slot(&f.slot, connection::Failover::Disabled)
        .await?;

    let mut version = 0;
    let first = f.append(&mut version).await?;
    let second = f.append(&mut version).await?;
    let mut stream = connection.start(&f.start_options()).await?;
    let seen = transactions(read_until_commits(&mut stream, 2).await?);
    assert_eq!(ids(&seen), [first.as_str(), second.as_str()]);
    stream.confirm(seen[0].commit.end_lsn).await?;
    assert_eq!(stream.confirmed(), seen[0].commit.end_lsn);
    stream.stop().await?.close().await?;

    let third = f.append(&mut version).await?;
    let mut stream = f.replication().await?.start(&f.start_options()).await?;
    let seen = transactions(read_until_commits(&mut stream, 2).await?);
    assert_eq!(
        ids(&seen),
        [second.as_str(), third.as_str()],
        "the confirmed transaction is not sent again"
    );
    stream.confirm(seen[1].commit.end_lsn).await?;
    let mut connection = stream.stop().await?;
    let confirmed: String = sqlx::query_scalar(
        "SELECT confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name = $1",
    )
    .bind(f.slot.as_str())
    .fetch_one(&mut f.conn)
    .await?;
    assert_eq!(confirmed.parse::<lsn::Lsn>()?, seen[1].commit.end_lsn);

    connection
        .drop_slot(&f.slot, connection::DropWait::NoWait)
        .await?;
    connection.close().await?;
    f.finish().await
}

/// The exported snapshot sees exactly what the slot will not stream.
#[tokio::test]
async fn an_exported_snapshot_sees_what_the_slot_skips() -> Result<()> {
    let mut f = Fixture::new().await?;
    let mut version = 0;
    let before = f.append(&mut version).await?;
    let mut connection = f.replication().await?;
    let created = connection
        .create_slot_with(
            &f.slot,
            connection::Failover::Disabled,
            connection::SlotSnapshot::Export,
        )
        .await?;
    let snapshot = created.snapshot.context("no snapshot exported")?;
    let after = f.append(&mut version).await?;

    let mut reader = f.db.connect().await?;
    reader
        .execute("BEGIN ISOLATION LEVEL REPEATABLE READ")
        .await?;
    reader
        .execute(sqlx::AssertSqlSafe(format!(
            "SET TRANSACTION SNAPSHOT '{snapshot}'"
        )))
        .await?;
    let visible: Vec<String> = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
        "SELECT commit_id::text FROM {STORE}.commits ORDER BY last_version"
    )))
    .fetch_all(&mut reader)
    .await?;
    reader.execute("COMMIT").await?;
    assert_eq!(visible, std::slice::from_ref(&before));

    let mut stream = connection.start(&f.start_options()).await?;
    let seen = transactions(read_until_commits(&mut stream, 1).await?);
    assert_eq!(ids(&seen), [after.as_str()]);
    let mut connection = stream.stop().await?;
    connection
        .drop_slot(&f.slot, connection::DropWait::NoWait)
        .await?;
    connection.close().await?;
    reader.close().await?;
    f.finish().await
}

#[tokio::test]
async fn keepalive_answers_a_status_request() -> Result<()> {
    let f = Fixture::new().await?;
    let mut connection = f.replication().await?;
    connection
        .create_slot(&f.slot, connection::Failover::Disabled)
        .await?;
    let mut stream = connection.start(&f.start_options()).await?;

    stream.send_status(stream::Reply::Requested).await?;
    let keepalive = tokio::time::timeout(TIMEOUT, async {
        loop {
            if let Some(stream::ReplicationMessage::Keepalive(k)) = stream.next().await? {
                return anyhow::Ok(k);
            }
        }
    })
    .await??;
    assert!(keepalive.wal_end > lsn::Lsn::ZERO);

    let mut connection = stream.stop().await?;
    connection
        .drop_slot(&f.slot, connection::DropWait::NoWait)
        .await?;
    connection.close().await?;
    f.finish().await
}

#[tokio::test]
async fn cancelled_next_loses_nothing() -> Result<()> {
    let f = Fixture::new().await?;
    let mut connection = f.replication().await?;
    connection
        .create_slot(&f.slot, connection::Failover::Disabled)
        .await?;
    // Start first, so `next` waits on the socket while the writer appends.
    let mut stream = connection.start(&f.start_options()).await?;
    let mut writer = f.writer().await?;
    let jobs = f.jobs;
    let appends = tokio::spawn(async move {
        let mut version = 0;
        let mut ids = Vec::new();
        for _ in 0..20 {
            ids.push(append(&mut writer, jobs, &mut version).await?);
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
        writer.close().await?;
        anyhow::Ok(ids)
    });

    let mut messages = Vec::new();
    let mut commits = 0;
    let mut cancelled = 0;
    let deadline = tokio::time::Instant::now() + TIMEOUT;
    while commits < 20 && tokio::time::Instant::now() < deadline {
        // A zero timeout drops `next` at its first await point whenever no message is buffered.
        match tokio::time::timeout(Duration::ZERO, stream.next()).await {
            Err(_) => {
                cancelled += 1;
                tokio::task::yield_now().await;
            }
            Ok(message) => {
                if let Some(stream::ReplicationMessage::XLogData(data)) = message? {
                    if matches!(data.message, pgoutput::Message::Commit(_)) {
                        commits += 1;
                    }
                    messages.push(data.message);
                }
            }
        }
    }
    assert!(cancelled > 0, "some calls were cancelled");
    let expected = appends.await??;
    let seen = transactions(messages);
    assert_eq!(
        ids(&seen),
        expected.iter().map(String::as_str).collect::<Vec<_>>()
    );

    let mut connection = stream.stop().await?;
    connection
        .drop_slot(&f.slot, connection::DropWait::NoWait)
        .await?;
    connection.close().await?;
    f.finish().await
}

#[tokio::test]
async fn slot_errors_carry_sqlstate() -> Result<()> {
    let mut f = Fixture::new().await?;
    let mut connection = f.replication().await?;
    let missing = connection
        .drop_slot(&f.slot, connection::DropWait::NoWait)
        .await;
    assert_server_error(missing, error::SqlState::UNDEFINED_OBJECT);

    connection
        .create_slot(&f.slot, connection::Failover::Enabled)
        .await?;
    let failover: bool =
        sqlx::query_scalar("SELECT failover FROM pg_replication_slots WHERE slot_name = $1")
            .bind(f.slot.as_str())
            .fetch_one(&mut f.conn)
            .await?;
    assert!(failover);
    let duplicate = connection
        .create_slot(&f.slot, connection::Failover::Disabled)
        .await;
    assert_server_error(duplicate.map(drop), error::SqlState::DUPLICATE_OBJECT);

    // The connection stays usable after a command error.
    let stream = connection.start(&f.start_options()).await?;
    let busy = f.replication().await?.start(&f.start_options()).await;
    assert_server_error(busy.map(drop), error::SqlState::OBJECT_IN_USE);

    let mut connection = stream.stop().await?;
    connection
        .drop_slot(&f.slot, connection::DropWait::Wait)
        .await?;
    connection.close().await?;
    f.finish().await
}

/// PostgreSQL allows slot names that start with a digit, and so does `SlotName`; unquoted, the replication
/// command scanner reads the leading digits as a number and the command is a syntax error.
#[tokio::test]
async fn slot_names_starting_with_a_digit_work_in_every_command() -> Result<()> {
    let mut f = Fixture::new().await?;
    f.slot = name::SlotName::new(format!("0_{}", f.db.name()))?;
    let mut connection = f.replication().await?;
    connection
        .create_slot(&f.slot, connection::Failover::Disabled)
        .await?;
    connection
        .alter_slot_failover(&f.slot, connection::Failover::Enabled)
        .await?;

    f.append(&mut 0).await?;
    let mut stream = connection.start(&f.start_options()).await?;
    read_until_commits(&mut stream, 1).await?;
    let mut connection = stream.stop().await?;

    connection
        .drop_slot(&f.slot, connection::DropWait::NoWait)
        .await?;
    connection.close().await?;
    f.finish().await
}

#[tokio::test]
async fn alter_slot_failover_turns_it_on_and_off() -> Result<()> {
    let mut f = Fixture::new().await?;
    let mut connection = f.replication().await?;
    let missing = connection
        .alter_slot_failover(&f.slot, connection::Failover::Enabled)
        .await;
    assert_server_error(missing, error::SqlState::UNDEFINED_OBJECT);

    connection
        .create_slot(&f.slot, connection::Failover::Disabled)
        .await?;
    for (failover, expected) in [
        (connection::Failover::Enabled, true),
        (connection::Failover::Disabled, false),
    ] {
        connection.alter_slot_failover(&f.slot, failover).await?;
        let actual: bool =
            sqlx::query_scalar("SELECT failover FROM pg_replication_slots WHERE slot_name = $1")
                .bind(f.slot.as_str())
                .fetch_one(&mut f.conn)
                .await?;
        assert_eq!(actual, expected, "{failover:?}");
    }

    // A slot another process streams from is altered once that process lets go of it.
    let stream = f.replication().await?.start(&f.start_options()).await?;
    let mut altered =
        Box::pin(connection.alter_slot_failover(&f.slot, connection::Failover::Enabled));
    let waited = tokio::time::timeout(Duration::from_millis(500), altered.as_mut()).await;
    assert!(waited.is_err(), "altered a slot in use: {waited:?}");
    stream.stop().await?.close().await?;
    tokio::time::timeout(Duration::from_secs(10), altered).await??;

    connection
        .drop_slot(&f.slot, connection::DropWait::Wait)
        .await?;
    connection.close().await?;
    f.finish().await
}

#[tokio::test]
async fn terminated_walsenders_report_the_fatal_error() -> Result<()> {
    let mut f = Fixture::new().await?;
    let mut connection = f.replication().await?;
    connection
        .create_slot(&f.slot, connection::Failover::Disabled)
        .await?;
    let streaming_pid = connection.process_id();
    let mut stream = connection.start(&f.start_options()).await?;

    // A command blocked on the active slot, then terminated: the FATAL error, not a lost connection.
    let mut waiting = f.replication().await?;
    let waiting_pid = waiting.process_id();
    let slot = f.slot.clone();
    let drop = tokio::spawn(async move {
        waiting
            .drop_slot(&slot, connection::DropWait::Wait)
            .await
            .map(|()| waiting)
    });
    f.wait_for_event(waiting_pid, "ReplicationSlotDrop").await?;
    f.terminate(waiting_pid).await?;
    match drop.await? {
        Err(error::CommandError::Server(e)) => {
            assert_eq!(e.code, error::SqlState::ADMIN_SHUTDOWN, "{e}");
            assert_eq!(e.severity, error::Severity::Fatal, "{e}");
        }
        other => panic!("expected SQLSTATE 57P01, got {other:?}"),
    }

    f.terminate(streaming_pid).await?;
    let ended = tokio::time::timeout(TIMEOUT, async {
        loop {
            if let Err(e) = stream.next().await {
                return e;
            }
        }
    })
    .await?;
    match ended {
        error::StreamError::Server(e) => assert_eq!(e.code, error::SqlState::ADMIN_SHUTDOWN, "{e}"),
        other => panic!("expected SQLSTATE 57P01, got {other:?}"),
    }

    let mut connection = f.replication().await?;
    connection
        .drop_slot(&f.slot, connection::DropWait::Wait)
        .await?;
    connection.close().await?;
    f.finish().await
}

struct Fixture {
    db: pges_testing::TestDb,
    conn: PgConnection,
    jobs: i32,
    slot: name::SlotName,
}

impl Fixture {
    async fn new() -> Result<Self> {
        let db = pges_testing::TestDb::new().await?;
        let mut conn = db.connect().await?;
        let wal_level: String = sqlx::query_scalar("SHOW wal_level")
            .fetch_one(&mut conn)
            .await?;
        if wal_level != "logical" {
            bail!("the test server needs wal_level=logical, not {wal_level}");
        }

        pges_testing::create_store(
            &mut conn,
            &STORE_NAME,
            &pges_testing::definition(Default::default()),
        )
        .await?;
        let jobs = pges_testing::register_category(
            &mut conn,
            &STORE_NAME,
            pges_core::category!("jobs"),
            1,
            2,
        )
        .await?
        .get();
        // Slot names are server-wide; the database name is unique per test.
        let slot = name::SlotName::new(format!("s_{}", db.name()))?;
        Ok(Self {
            db,
            conn,
            jobs,
            slot,
        })
    }

    async fn replication(&self) -> Result<connection::ReplicationConnection> {
        let config = config::Config::from_url(self.db.url())?;
        Ok(connection::ReplicationConnection::connect(&config).await?)
    }

    fn start_options(&self) -> connection::StartOptions {
        connection::StartOptions::new(
            self.slot.clone(),
            pges_replication::publication_name!("pges"),
        )
    }

    /// A session whose commits are flushed before they return; decoding only sees flushed WAL.
    async fn writer(&self) -> Result<PgConnection> {
        let mut conn = self.db.connect().await?;
        conn.execute("SET synchronous_commit = on").await?;
        Ok(conn)
    }

    async fn append(&mut self, version: &mut i64) -> Result<String> {
        let mut writer = self.writer().await?;
        let id = append(&mut writer, self.jobs, version).await?;
        writer.close().await?;
        Ok(id)
    }

    async fn oid(&mut self, table: &str) -> Result<u32> {
        let oid: i64 = sqlx::query_scalar("SELECT $1::regclass::oid::int8")
            .bind(table)
            .fetch_one(&mut self.conn)
            .await?;
        Ok(u32::try_from(oid)?)
    }

    async fn wait_for_event(&mut self, pid: i32, event: &str) -> Result<()> {
        let deadline = tokio::time::Instant::now() + TIMEOUT;
        loop {
            let waiting: bool = sqlx::query_scalar(
                "SELECT EXISTS (SELECT FROM pg_stat_activity WHERE pid = $1 AND wait_event = $2)",
            )
            .bind(pid)
            .bind(event)
            .fetch_one(&mut self.conn)
            .await?;
            if waiting {
                return Ok(());
            }
            if tokio::time::Instant::now() > deadline {
                bail!("process {pid} never waited on {event}");
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    async fn terminate(&mut self, pid: i32) -> Result<()> {
        let terminated: bool = sqlx::query_scalar("SELECT pg_terminate_backend($1)")
            .bind(pid)
            .fetch_one(&mut self.conn)
            .await?;
        ensure!(terminated, "process {pid} was not terminated");
        Ok(())
    }

    async fn finish(self) -> Result<()> {
        self.conn.close().await?;
        self.db.cleanup().await
    }
}

/// Inserts one commit row directly; returns its commit id as text.
async fn append(conn: &mut PgConnection, category: i32, version: &mut i64) -> Result<String> {
    *version += 1;
    let id: String = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
        "INSERT INTO {STORE}.commits (last_version, commit_id, stream_id, category_id, event_count, format, payload) \
         VALUES ($1, uuidv7(), gen_random_uuid(), $2, 1, 1, '\\x00') RETURNING commit_id::text"
    )))
    .bind(*version)
    .bind(category)
    .fetch_one(conn)
    .await?;
    Ok(id)
}

/// Reads until `commits` Commit messages arrived; returns the pgoutput messages in order.
async fn read_until_commits(
    stream: &mut stream::ReplicationStream,
    commits: usize,
) -> Result<Vec<pgoutput::Message>> {
    let mut messages = Vec::new();
    let mut seen = 0;
    tokio::time::timeout(TIMEOUT, async {
        while seen < commits {
            let message = stream.next().await?.context("the server ended streaming")?;
            if let stream::ReplicationMessage::XLogData(data) = message {
                if matches!(data.message, pgoutput::Message::Commit(_)) {
                    seen += 1;
                }
                messages.push(data.message);
            }
        }
        anyhow::Ok(())
    })
    .await
    .with_context(|| format!("waiting for {commits} commits, saw {seen}"))??;
    Ok(messages)
}

struct Transaction {
    commit_ids: Vec<String>,
    commit: pgoutput::Commit,
}

/// Groups messages into transactions; the commit id is the second published column.
fn transactions(messages: Vec<pgoutput::Message>) -> Vec<Transaction> {
    let mut out = Vec::new();
    let mut commit_ids = Vec::new();
    for message in messages {
        match message {
            pgoutput::Message::Begin(_) => commit_ids.clear(),
            pgoutput::Message::Insert(insert) => {
                commit_ids.push(insert.new.text(1).unwrap().to_owned());
            }
            pgoutput::Message::Commit(commit) => out.push(Transaction {
                commit_ids: std::mem::take(&mut commit_ids),
                commit,
            }),
            _ => {}
        }
    }
    out
}

fn ids(transactions: &[Transaction]) -> Vec<&str> {
    transactions
        .iter()
        .flat_map(|t| t.commit_ids.iter().map(String::as_str))
        .collect()
}

fn kind(message: &pgoutput::Message) -> &'static str {
    match message {
        pgoutput::Message::Begin(_) => "begin",
        pgoutput::Message::Commit(_) => "commit",
        pgoutput::Message::Relation(_) => "relation",
        pgoutput::Message::Insert(_) => "insert",
        _ => "other",
    }
}

fn assert_server_error(result: Result<(), error::CommandError>, code: error::SqlState) {
    match result {
        Err(error::CommandError::Server(e)) => assert_eq!(e.code, code, "{e}"),
        other => panic!("expected SQLSTATE {code}, got {other:?}"),
    }
}
