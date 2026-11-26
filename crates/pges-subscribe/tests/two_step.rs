//! A category subscription reads a page of the category's log, then the page's commits by id. It delivers
//! exactly what the earlier one-statement read (each log row's commit looked up in a `LATERAL` subquery,
//! missing commits skipped, the shard filter before the limit) gave: every event of a commit that exists,
//! once, whatever the page size, the shards, and the streams `delete_stream` removed.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

mod common;

use std::{
    collections::BTreeSet,
    num::NonZeroU32,
    sync::{Arc, Mutex},
    time::Duration,
};

use anyhow::{Context, Result, ensure};
use proptest::prelude::*;
use sqlx::PgConnection;

use pges_core::{category, interval, non_empty, stream};
use pges_postgres::delete;
use pges_subscribe::{
    batch, handler,
    subscription::{self, StoreExt},
    waker::recheck,
};

use common::{Cat, Fixture, Running};

const SHARDS: u16 = 3;

/// The earlier one-statement read of a category (`read/category.sql` with its shard filter and order), over
/// the whole log, for one shard of `$2`.
const LATERAL: &str = "
SELECT l.last_position, l.event_count
FROM es.log l
CROSS JOIN LATERAL (SELECT stream_id FROM es.commits_{category}
                    WHERE commit_id = l.commit_id LIMIT 1) c
WHERE l.category_id = $1 AND l.last_position > 0
  AND 0 >= (SELECT horizon_position FROM es.categories WHERE category_id = $1)
  AND ((substring(sha256(uuid_send(c.stream_id)) FROM 1 FOR 8)::int8
     & 9223372036854775807) % $2) = $3
ORDER BY l.last_position";

#[derive(Debug, Clone)]
enum Op {
    Append { cat: u8, stream: u8, events: u32 },
    Sequence(i64),
    Delete { cat: u8, stream: u8 },
}

fn op() -> impl Strategy<Value = Op> {
    prop_oneof![
        5 => (0..3_u8, 0..4_u8, 1..4_u32)
            .prop_map(|(cat, stream, events)| Op::Append { cat, stream, events }),
        2 => (1..5_i64).prop_map(Op::Sequence),
        1 => (0..2_u8, 0..4_u8).prop_map(|(cat, stream)| Op::Delete { cat, stream }),
    ]
}

fn cat(index: u8) -> Cat {
    match index {
        0 => Cat::Jobs,
        1 => Cat::Orders,
        _ => Cat::Parts,
    }
}

/// The positions that the `LATERAL` read gives for jobs and orders, from every shard of `shards`: each event,
/// and each stream deletion's system event.
async fn lateral(f: &Fixture, shards: u16) -> Result<Vec<u64>> {
    let mut events = Vec::new();
    for (name, cat) in [("jobs", Cat::Jobs), ("orders", Cat::Orders)] {
        let sql = LATERAL.replace("{category}", name);
        for shard in 0..shards {
            let rows: Vec<(i64, i16)> = sqlx::query_as(sqlx::AssertSqlSafe(&*sql))
                .bind(f.id(cat).get())
                .bind(i64::from(shards))
                .bind(i64::from(shard))
                .fetch_all(&f.pool)
                .await?;
            for (last, count) in rows {
                events.extend((last - i64::from(count) + 1..=last).map(|p| p as u64));
            }
        }
    }
    events.sort_unstable();

    Ok(events)
}

fn builder(
    f: &Fixture,
    name: &str,
    batch_size: u32,
) -> subscription::Builder<subscription::Listed> {
    f.store
        .subscription(subscription::SubscriptionName::parse(name).unwrap())
        .categories(non_empty![category!("jobs"), category!("orders")])
        .batch_size(NonZeroU32::new(batch_size).unwrap())
        .lease(interval!(600 ms))
        .acquire_interval(interval!(20 ms))
        .retry_interval(interval!(20 ms))
        .waker(recheck::Recheck::every(interval!(5 ms)))
}

/// Waits until every shard row of `name` stores a cursor at `head` or past it.
async fn wait_cursors(f: &Fixture, name: &str, head: u64) -> Result<()> {
    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            let lowest: Option<i64> = sqlx::query_scalar(
                "SELECT min(position) FROM es.subscriptions WHERE name = $1 AND shard < shards",
            )
            .bind(name)
            .fetch_one(&f.pool)
            .await?;
            if lowest.is_some_and(|p| p >= head as i64) {
                return anyhow::Ok(());
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .with_context(|| format!("waiting for {name} to reach {head}"))?
}

/// Every record's position, events and system events, in the order handlers saw them.
#[derive(Debug, Clone, Default)]
struct Positions(Arc<Mutex<Vec<u64>>>);

impl Positions {
    /// The positions sorted, after checking that none came twice.
    fn sorted(&self, subscriber: &str) -> Result<Vec<u64>> {
        let mut seen = self.0.lock().unwrap().clone();
        let distinct: BTreeSet<_> = seen.iter().copied().collect();
        ensure!(
            distinct.len() == seen.len(),
            "{subscriber} delivered a record twice"
        );
        seen.sort_unstable();

        Ok(seen)
    }
}

impl handler::PgHandler for Positions {
    async fn handle(
        &mut self,
        _tx: &mut PgConnection,
        batch: &batch::Batch,
    ) -> Result<(), handler::HandlerError> {
        let mut seen = self.0.lock().unwrap();
        seen.extend(batch.iter().map(|record| record.position().get()));

        Ok(())
    }
}

async fn run_case(ops: Vec<Op>, whole_batch: u32, sharded_batch: u32) -> Result<()> {
    let f = Fixture::new(pges_postgres::store::Outbox::Disabled).await?;
    let streams: Vec<Vec<stream::StreamId>> = (0..3)
        .map(|_| (0..4).map(|_| stream::StreamId::new_v7()).collect())
        .collect();
    let mut created = BTreeSet::new();
    let mut deleted = BTreeSet::new();
    for op in ops {
        match op {
            Op::Append {
                cat: c,
                stream,
                events,
            } => {
                if !deleted.contains(&(c, stream)) {
                    f.append(cat(c), streams[c as usize][stream as usize], events)
                        .await?;
                    created.insert((c, stream));
                }
            }
            Op::Sequence(limit) => {
                f.sequence(limit).await?;
            }
            Op::Delete { cat: c, stream } => {
                if created.contains(&(c, stream)) && deleted.insert((c, stream)) {
                    let id = streams[c as usize][stream as usize];
                    let any = delete::DeleteExpected::Any;
                    match cat(c) {
                        Cat::Jobs => f.store.delete_stream::<common::JobEvent>(id, any).await?,
                        _ => f.store.delete_stream::<common::OrderEvent>(id, any).await?,
                    };
                }
            }
        }
    }
    f.sequence_all().await?;
    let head = common::head(&f.pool).await?;

    let (whole_seen, sharded_seen) = (Positions::default(), Positions::default());
    let whole = Running::spawn(builder(&f, "whole", whole_batch).run_pg(whole_seen.clone()));
    let sharded = Running::spawn(
        builder(&f, "sharded", sharded_batch)
            .sharded(subscription::Shards::new(SHARDS).unwrap())
            .run_pg(sharded_seen.clone()),
    );
    wait_cursors(&f, "whole", head).await?;
    wait_cursors(&f, "sharded", head).await?;
    whole.stop().await?;
    sharded.stop().await?;

    let whole_lateral = lateral(&f, 1).await?;
    ensure!(
        whole_lateral == lateral(&f, SHARDS).await?,
        "the shards of the LATERAL read do not add up to the whole"
    );
    let whole_delivered = whole_seen.sorted("whole")?;
    ensure!(
        whole_delivered == whole_lateral,
        "whole: delivered {whole_delivered:?}, the LATERAL read gives {whole_lateral:?}"
    );
    let sharded_delivered = sharded_seen.sorted("sharded")?;
    ensure!(
        sharded_delivered == whole_lateral,
        "sharded: delivered {sharded_delivered:?}, the LATERAL read gives {whole_lateral:?}"
    );

    f.finish().await
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 10, ..ProptestConfig::default() })]

    /// Any history of appends, sequencer batches and deleted streams, read at any page size, whole or in
    /// shards, delivers what the `LATERAL` read gives.
    #[test]
    fn the_two_step_read_delivers_what_the_lateral_read_gives(
        ops in prop::collection::vec(op(), 1..40),
        whole_batch in prop_oneof![1..4_u32, Just(1024_u32)],
        sharded_batch in prop_oneof![1..6_u32, Just(1024_u32)],
    ) {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(4)
            .enable_all()
            .build()
            .unwrap();
        let result = runtime.block_on(run_case(ops, whole_batch, sharded_batch));
        prop_assert!(result.is_ok(), "{:?}", result);
    }
}
