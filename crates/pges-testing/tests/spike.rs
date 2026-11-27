//! Spike: runs the migrations and the statements the design relies on against a real PostgreSQL 18, one
//! test per claim. The append and its classification are the statements of pges-postgres's registry; the
//! others are written here.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use anyhow::Result;
use sqlx::postgres::types::PgInterval;
use sqlx::{Connection, Executor, PgConnection, Row};
use uuid::Uuid;

use pges_core::{category, store as core_store};
use pges_postgres::{
    sql::{statement, template},
    store,
};

const STORE: &str = "s1";
const STORE_NAME: core_store::StoreName = pges_core::store_name!("s1");

const NO_STREAM: i64 = -1;
const ANY: i64 = -2;

struct Fixture {
    db: pges_testing::TestDb,
    conn: PgConnection,
    jobs: category::CategoryId,
}

impl Fixture {
    async fn new(outbox: store::Outbox) -> Result<Self> {
        let db = pges_testing::TestDb::new().await?;
        let mut conn = db.connect().await?;
        pges_testing::create_store(&mut conn, &STORE_NAME, &pges_testing::definition(outbox))
            .await?;
        let jobs = pges_testing::register_category(
            &mut conn,
            &STORE_NAME,
            pges_core::category!("jobs"),
            1,
            2,
        )
        .await?;
        pges_testing::create_log_partition(&mut conn, &STORE_NAME, 0).await?;
        conn.execute(sqlx::AssertSqlSafe(format!("SET search_path TO {STORE}")))
            .await?;
        Ok(Self { db, conn, jobs })
    }

    async fn finish(self) -> Result<()> {
        self.conn.close().await?;
        self.db.cleanup().await
    }
}

fn skew() -> PgInterval {
    PgInterval {
        months: 0,
        days: 0,
        microseconds: 5_000_000,
    }
}

/// Renders `statement` for the store.
fn render(statement: &template::Statement) -> String {
    statement.render(
        &template::Values::new()
            .store(&STORE_NAME)
            .stream_table(None),
    )
}

/// Runs the append. Returns `Some(last_version)` or `None` for "no row returned".
async fn append(
    conn: &mut PgConnection,
    outbox: store::Outbox,
    stream: Uuid,
    category: i32,
    expected: i64,
    events: i16,
    commit_id: Uuid,
) -> sqlx::Result<Option<i64>> {
    let sql = render(if outbox == store::Outbox::Enabled {
        &statement::APPEND_OUTBOX
    } else {
        &statement::APPEND
    });
    let row = sqlx::query(sqlx::AssertSqlSafe(&*sql))
        .bind(stream)
        .bind(category)
        .bind(expected)
        .bind(events)
        .bind(1_i16)
        .bind(vec![0_u8; 32])
        .bind(None::<String>)
        .bind(commit_id)
        .bind(skew())
        .fetch_optional(conn)
        .await?;
    Ok(row.map(|r| r.get::<i64, _>("last_version")))
}

#[tokio::test]
async fn migrations_apply_with_every_option() -> Result<()> {
    let f = Fixture::new(store::Outbox::Enabled).await?;
    let mut conn = f.conn;
    let tables: Vec<String> = sqlx::query_scalar(
        "SELECT relname::text FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
         WHERE n.nspname = $1 AND c.relkind IN ('r', 'p') AND NOT c.relispartition ORDER BY 1",
    )
    .bind(STORE)
    .fetch_all(&mut conn)
    .await?;
    for t in [
        "backfills",
        "categories",
        "commits",
        "deleted_commits",
        "event_types",
        "log",
        "pending",
        "snapshots",
        "streams",
        "subscription_members",
        "subscriptions",
    ] {
        assert!(
            tables.iter().any(|x| x == t),
            "missing table {t}: {tables:?}"
        );
    }
    let (outbox, dcb): (bool, bool) =
        sqlx::query_as("SELECT outbox, dcb FROM pges.stores WHERE schema_name = $1")
            .bind(STORE)
            .fetch_one(&mut conn)
            .await?;
    assert!(outbox && !dcb);
    Fixture { conn, ..f }.finish().await
}

#[tokio::test]
async fn virtual_ranges_and_uuidv7_bounds() -> Result<()> {
    let mut f = Fixture::new(store::Outbox::Disabled).await?;
    let stream = Uuid::now_v7();
    let commit = Uuid::now_v7();
    assert_eq!(
        append(
            &mut f.conn,
            store::Outbox::Disabled,
            stream,
            f.jobs.get(),
            NO_STREAM,
            3,
            commit
        )
        .await?,
        Some(3)
    );

    let (versions, recorded_ok): (String, bool) = sqlx::query_as(
        "SELECT versions::text, recorded_at = uuid_extract_timestamp(commit_id) FROM commits WHERE commit_id = $1",
    )
    .bind(commit)
    .fetch_one(&mut f.conn)
    .await?;
    assert_eq!(versions, "[1,4)");
    assert!(recorded_ok);

    // A uuidv7 generated now sorts inside [floor(month start), floor(next month start)).
    let inside: bool = sqlx::query_scalar(
        "SELECT pges.uuidv7_floor(date_trunc('month', now() AT TIME ZONE 'UTC') AT TIME ZONE 'UTC') <= uuidv7()
            AND uuidv7() < pges.uuidv7_floor((date_trunc('month', now() AT TIME ZONE 'UTC') + interval '1 month') AT TIME ZONE 'UTC')",
    )
    .fetch_one(&mut f.conn)
    .await?;
    assert!(inside);
    f.finish().await
}

#[tokio::test]
async fn append_expected_versions_and_idempotent_retry() -> Result<()> {
    let mut f = Fixture::new(store::Outbox::Disabled).await?;
    let orders = pges_testing::register_category(
        &mut f.conn,
        &STORE_NAME,
        pges_core::category!("orders"),
        0,
        1,
    )
    .await?
    .get();
    let s = Uuid::now_v7();
    let c = &mut f.conn;

    assert_eq!(
        append(
            c,
            store::Outbox::Disabled,
            s,
            f.jobs.get(),
            NO_STREAM,
            2,
            Uuid::now_v7()
        )
        .await?,
        Some(2),
        "create"
    );
    assert_eq!(
        append(
            c,
            store::Outbox::Disabled,
            s,
            f.jobs.get(),
            NO_STREAM,
            1,
            Uuid::now_v7()
        )
        .await?,
        None,
        "NoStream on existing"
    );
    assert_eq!(
        append(
            c,
            store::Outbox::Disabled,
            s,
            f.jobs.get(),
            2,
            1,
            Uuid::now_v7()
        )
        .await?,
        Some(3),
        "Exact(2)"
    );
    assert_eq!(
        append(
            c,
            store::Outbox::Disabled,
            s,
            f.jobs.get(),
            2,
            1,
            Uuid::now_v7()
        )
        .await?,
        None,
        "stale Exact(2)"
    );

    let any_id = Uuid::now_v7();
    assert_eq!(
        append(c, store::Outbox::Disabled, s, f.jobs.get(), ANY, 1, any_id).await?,
        Some(4),
        "Any"
    );
    assert_eq!(
        append(c, store::Outbox::Disabled, s, f.jobs.get(), ANY, 1, any_id).await?,
        None,
        "retry of Any with the same id"
    );
    let done: Option<i64> = sqlx::query(sqlx::AssertSqlSafe(&*render(&statement::CLASSIFY)))
        .bind(s)
        .bind(f.jobs.get())
        .bind(any_id)
        .fetch_one(&mut *c)
        .await?
        .get("done");
    assert_eq!(done, Some(4), "classification finds the original result");

    assert_eq!(
        append(
            c,
            store::Outbox::Disabled,
            Uuid::now_v7(),
            f.jobs.get(),
            1,
            1,
            Uuid::now_v7()
        )
        .await?,
        None,
        "Exact on a missing stream"
    );
    assert_eq!(
        append(c, store::Outbox::Disabled, s, orders, 4, 1, Uuid::now_v7()).await?,
        None,
        "wrong category"
    );
    let cat: i32 = sqlx::query(sqlx::AssertSqlSafe(&*render(&statement::CLASSIFY)))
        .bind(s)
        .bind(orders)
        .bind(Uuid::now_v7())
        .fetch_one(&mut *c)
        .await?
        .get("category_id");
    assert_eq!(cat, f.jobs.get());

    // A new commit id outside the skew tolerance raises PGE01.
    let ahead = Uuid::new_v7(uuid::Timestamp::from_unix(
        uuid::NoContext,
        4_000_000_000,
        0,
    ));
    let err = append(c, store::Outbox::Disabled, s, f.jobs.get(), 4, 1, ahead)
        .await
        .unwrap_err();
    assert_eq!(
        err.as_database_error().and_then(|e| e.code()).as_deref(),
        Some("PGE01"),
        "{err}"
    );

    // One at or below the stream's last_commit_id is not checked: it cannot apply, so it returns no row and
    // classification reports a stored one as deduplicated, however late.
    let old = Uuid::new_v7(uuid::Timestamp::from_unix(
        uuid::NoContext,
        1_700_000_000,
        0,
    ));
    assert_eq!(
        append(c, store::Outbox::Disabled, s, f.jobs.get(), 4, 1, old).await?,
        None,
        "an old commit id"
    );

    // An id at or below last_commit_id is rejected (out of order), not silently accepted.
    let early = Uuid::now_v7();
    let late = Uuid::now_v7();
    assert_eq!(
        append(c, store::Outbox::Disabled, s, f.jobs.get(), 4, 1, late).await?,
        Some(5)
    );
    assert_eq!(
        append(c, store::Outbox::Disabled, s, f.jobs.get(), 5, 1, early).await?,
        None,
        "out-of-order commit id"
    );
    f.finish().await
}

#[tokio::test]
async fn read_stream_from_category_partition() -> Result<()> {
    let mut f = Fixture::new(store::Outbox::Disabled).await?;
    let s = Uuid::now_v7();
    append(
        &mut f.conn,
        store::Outbox::Disabled,
        s,
        f.jobs.get(),
        NO_STREAM,
        2,
        Uuid::now_v7(),
    )
    .await?;
    append(
        &mut f.conn,
        store::Outbox::Disabled,
        s,
        f.jobs.get(),
        2,
        1,
        Uuid::now_v7(),
    )
    .await?;
    append(
        &mut f.conn,
        store::Outbox::Disabled,
        s,
        f.jobs.get(),
        3,
        3,
        Uuid::now_v7(),
    )
    .await?;

    let (first_commit,): (Uuid,) =
        sqlx::query_as("SELECT first_commit_id FROM streams WHERE stream_id = $1")
            .bind(s)
            .fetch_one(&mut f.conn)
            .await?;
    let rows: Vec<(i64, i16, String)> = sqlx::query_as(
        "SELECT last_version, event_count, versions::text FROM commits_jobs
         WHERE stream_id = $1 AND last_version >= $2 AND commit_id >= $3
         ORDER BY last_version",
    )
    .bind(s)
    .bind(3_i64)
    .bind(first_commit)
    .fetch_all(&mut f.conn)
    .await?;
    assert_eq!(rows, vec![(3, 1, "[3,4)".into()), (6, 3, "[4,7)".into())]);
    f.finish().await
}

#[tokio::test]
async fn sequencer_lease_batch_and_category_read() -> Result<()> {
    let mut f = Fixture::new(store::Outbox::Disabled).await?;
    let c = &mut f.conn;

    // Lease: acquirable on a fresh database, then exclusive.
    let acquire =
        "UPDATE pges.sequencer SET fence = fence + 1, lease_until = now() + $1, leader = $2
                   WHERE lease_until IS NULL OR lease_until < now() RETURNING fence";
    let ttl = PgInterval {
        months: 0,
        days: 0,
        microseconds: 10_000_000,
    };
    let fence: Option<i64> = sqlx::query_scalar(acquire)
        .bind(ttl)
        .bind("a")
        .fetch_optional(&mut *c)
        .await?;
    assert_eq!(fence, Some(1));
    let second: Option<i64> = sqlx::query_scalar(acquire)
        .bind(ttl)
        .bind("b")
        .fetch_optional(&mut *c)
        .await?;
    assert_eq!(second, None);

    let s = Uuid::now_v7();
    append(
        c,
        store::Outbox::Disabled,
        s,
        f.jobs.get(),
        NO_STREAM,
        2,
        Uuid::now_v7(),
    )
    .await?;
    append(
        c,
        store::Outbox::Disabled,
        s,
        f.jobs.get(),
        2,
        1,
        Uuid::now_v7(),
    )
    .await?;

    // Sequencer batch: fence check with a row lock, then log rows.
    let commits: Vec<(Uuid, i32, i16)> = sqlx::query_as(
        "SELECT commit_id, category_id, event_count FROM commits ORDER BY commit_id",
    )
    .fetch_all(&mut *c)
    .await?;
    let mut position = 0_i64;
    let (mut positions, mut ids, mut cats, mut counts) = (vec![], vec![], vec![], vec![]);
    for (id, cat, n) in &commits {
        position += i64::from(*n);
        positions.push(position);
        ids.push(*id);
        cats.push(*cat);
        counts.push(*n);
    }
    let mut tx = c.begin().await?;
    let fenced: Option<i32> =
        sqlx::query_scalar("SELECT 1 FROM pges.sequencer WHERE fence = $1 FOR SHARE")
            .bind(1_i64)
            .fetch_optional(&mut *tx)
            .await?;
    assert_eq!(fenced, Some(1));
    sqlx::query(
        "INSERT INTO log (last_position, commit_id, category_id, event_count)
         SELECT * FROM unnest($1::int8[], $2::uuid[], $3::int4[], $4::int2[])",
    )
    .bind(&positions)
    .bind(&ids)
    .bind(&cats)
    .bind(&counts)
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;

    // Category read with the horizon check in the same statement.
    let read =
        "SELECT l.last_position, l.event_count, c.stream_id, c.last_version, l.positions::text
                FROM log l
                CROSS JOIN LATERAL (SELECT stream_id, last_version FROM commits_jobs
                                    WHERE commit_id = l.commit_id LIMIT 1) c
                WHERE l.category_id = $1 AND l.last_position > $2 AND l.last_position <= $3
                  AND $2 >= (SELECT horizon_position FROM categories WHERE category_id = $1)
                ORDER BY l.last_position
                LIMIT $4";
    let rows: Vec<(i64, i16, Uuid, i64, String)> = sqlx::query_as(read)
        .bind(f.jobs.get())
        .bind(0_i64)
        .bind(position)
        .bind(100_i64)
        .fetch_all(&mut *c)
        .await?;
    assert_eq!(
        rows.iter().map(|r| (r.0, r.4.clone())).collect::<Vec<_>>(),
        vec![(2, "[1,3)".into()), (3, "[3,4)".into())]
    );

    // A cursor below the horizon returns nothing from the same statement.
    sqlx::query("UPDATE categories SET horizon_position = 2 WHERE category_id = $1")
        .bind(f.jobs.get())
        .execute(&mut *c)
        .await?;
    let below: Vec<(i64, i16, Uuid, i64, String)> = sqlx::query_as(read)
        .bind(f.jobs.get())
        .bind(0_i64)
        .bind(position)
        .bind(100_i64)
        .fetch_all(&mut *c)
        .await?;
    assert!(below.is_empty());
    f.finish().await
}

#[tokio::test]
async fn outbox_drain_follows_seq_not_commit_order() -> Result<()> {
    let mut f = Fixture::new(store::Outbox::Enabled).await?;
    let mut other = f.db.connect().await?;
    other
        .execute(sqlx::AssertSqlSafe(format!("SET search_path TO {STORE}")))
        .await?;

    // A takes seq 1 but commits after B, which takes seq 2.
    let (a, b) = (Uuid::now_v7(), Uuid::now_v7());
    let mut tx_a = other.begin().await?;
    append(
        &mut tx_a,
        store::Outbox::Enabled,
        a,
        f.jobs.get(),
        NO_STREAM,
        1,
        Uuid::now_v7(),
    )
    .await?;
    append(
        &mut f.conn,
        store::Outbox::Enabled,
        b,
        f.jobs.get(),
        NO_STREAM,
        1,
        Uuid::now_v7(),
    )
    .await?;
    tx_a.commit().await?;

    let drained: Vec<(i64, Uuid)> = sqlx::query_as(
        "WITH d AS (
           DELETE FROM pending WHERE seq IN (SELECT seq FROM pending ORDER BY seq LIMIT $1) RETURNING *
         )
         SELECT d.seq, c.stream_id FROM d JOIN commits c ON c.commit_id = d.commit_id ORDER BY d.seq",
    )
    .bind(100_i64)
    .fetch_all(&mut f.conn)
    .await?;
    assert_eq!(drained.iter().map(|r| r.1).collect::<Vec<_>>(), vec![a, b]);
    let left: i64 = sqlx::query_scalar("SELECT count(*) FROM pending")
        .fetch_one(&mut f.conn)
        .await?;
    assert_eq!(left, 0);
    other.close().await?;
    f.finish().await
}

#[tokio::test]
async fn publication_sends_root_table_without_payload() -> Result<()> {
    let mut f = Fixture::new(store::Outbox::Disabled).await?;
    let wal_level: String = sqlx::query_scalar("SHOW wal_level")
        .fetch_one(&mut f.conn)
        .await?;
    if wal_level != "logical" {
        eprintln!("skipped: wal_level is {wal_level}; start the server with wal_level=logical");
        return f.finish().await;
    }
    // Decoding reads only flushed WAL; the test server runs with synchronous_commit=off.
    f.conn.execute("SET synchronous_commit = on").await?;
    // Slot names are cluster-wide, so derive one from the test database.
    let slot = format!("slot_{}", f.db.name());
    sqlx::query("SELECT pg_create_logical_replication_slot($1, 'pgoutput')")
        .bind(&slot)
        .execute(&mut f.conn)
        .await?;

    let marker = [0xAB_u8; 32];
    let row = sqlx::query(sqlx::AssertSqlSafe(&*render(&statement::APPEND)))
        .bind(Uuid::now_v7())
        .bind(f.jobs.get())
        .bind(NO_STREAM)
        .bind(1_i16)
        .bind(1_i16)
        .bind(marker.to_vec())
        .bind(None::<String>)
        .bind(Uuid::now_v7())
        .bind(skew())
        .fetch_one(&mut f.conn)
        .await?;
    assert_eq!(row.get::<i64, _>("last_version"), 1);

    let messages: Vec<Vec<u8>> = sqlx::query_scalar(
        "SELECT data FROM pg_logical_slot_peek_binary_changes($1, NULL, NULL,
           'proto_version', '4', 'publication_names', 'pges')",
    )
    .bind(&slot)
    .fetch_all(&mut f.conn)
    .await?;
    // pgoutput (PG18) also sends a Relation message for the leaf partition, but the Insert is published
    // under the root's relation id. The decoder must route by relation id, not by assuming leaf names never
    // appear. Relation: 'R', oid (u32), namespace\0, name\0, ...  Insert: 'I', oid (u32), 'N', tuple.
    let oid = |m: &[u8]| u32::from_be_bytes([m[1], m[2], m[3], m[4]]);
    let name_of = |m: &[u8]| {
        let rest = &m[5..];
        let ns_end = rest.iter().position(|b| *b == 0).unwrap();
        let name = &rest[ns_end + 1..];
        String::from_utf8_lossy(&name[..name.iter().position(|b| *b == 0).unwrap()]).into_owned()
    };
    let relations: Vec<(u32, String)> = messages
        .iter()
        .filter(|m| m.first() == Some(&b'R'))
        .map(|m| (oid(m), name_of(m)))
        .collect();
    let inserts: Vec<&Vec<u8>> = messages
        .iter()
        .filter(|m| m.first() == Some(&b'I'))
        .collect();
    assert_eq!(inserts.len(), 1, "one insert message");
    let target = relations
        .iter()
        .find(|(o, _)| *o == oid(inserts[0]))
        .map(|(_, n)| n.as_str());
    assert_eq!(
        target,
        Some("commits"),
        "insert is published under the root table; relations: {relations:?}"
    );
    let all: Vec<u8> = messages.concat();
    assert!(
        !all.windows(marker.len()).any(|w| w == marker),
        "payload is not in the output"
    );
    // The insert tuple carries exactly the five published columns.
    assert_eq!(u16::from_be_bytes([inserts[0][6], inserts[0][7]]), 5);

    sqlx::query("SELECT pg_drop_replication_slot($1)")
        .bind(&slot)
        .execute(&mut f.conn)
        .await?;
    f.finish().await
}
