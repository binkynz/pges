//! The coarse token: a source that replays commits after a restart does not get them sequenced twice.
//! A fake source (behind `Box<dyn DynSource>`) delivers real commits and resumes from its token, rewound
//! by a configurable amount.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

mod common;

use std::{
    num::NonZeroUsize,
    sync::{Arc, Mutex},
    time::Duration,
};

use anyhow::{Result, bail};
use sqlx::{PgConnection, PgPool};

use pges_core::{commit, stream};
use pges_sequencer::source;
use pges_sequencer::source_kind;

use common::{Fixture, Running, Target};

/// Replays from `token - rewind` (from the start without a token). Commits become available as the test
/// appends them.
struct Replaying {
    commits: Arc<Mutex<Vec<source::CommitRef>>>,
    rewind: usize,
    next: usize,
}

impl source::Source for Replaying {
    fn kind(&self) -> source::SourceKind {
        source_kind!("replaying")
    }

    async fn open(&mut self, token: Option<&[u8]>) -> Result<(), source::SourceError> {
        let resume = match token {
            Some(bytes) => usize::from_be_bytes(bytes.try_into().unwrap()),
            None => 0,
        };
        self.next = resume.saturating_sub(self.rewind);
        Ok(())
    }

    async fn ready(&mut self) -> Result<(), source::SourceError> {
        while self.next >= self.commits.lock().unwrap().len() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        Ok(())
    }

    async fn next(
        &mut self,
        _tx: &mut PgConnection,
        limit: NonZeroUsize,
    ) -> Result<source::SourceBatch, source::SourceError> {
        let commits = self.commits.lock().unwrap();
        let end = commits.len().min(self.next + limit.get());
        let batch = commits[self.next..end].to_vec();
        self.next = end;
        Ok(source::SourceBatch::new(batch, end.to_be_bytes().to_vec()))
    }

    async fn ack(&mut self, _token: &[u8]) -> Result<(), source::SourceError> {
        Ok(())
    }
}

struct Harness {
    f: Fixture,
    commits: Arc<Mutex<Vec<source::CommitRef>>>,
}

impl Harness {
    async fn append(&self, n: usize) -> Result<()> {
        for i in 0..n {
            let events = (i % 3 + 1) as u32;
            let result = self
                .f
                .append(Target::Job(stream::StreamId::new_v7()), events)
                .await?;
            self.commits.lock().unwrap().push(source::CommitRef::new(
                self.f.store_id,
                result.commit_id,
                self.f.jobs,
                commit::EventCount::new(events as u16).unwrap(),
            ));
        }
        Ok(())
    }

    async fn start(&self, rewind: usize, dedupe_tail: usize) -> Result<Running> {
        let mut config = common::fast();
        config.batch_size = NonZeroUsize::new(2).unwrap();
        config.token_interval = Duration::from_secs(3600);
        config.dedupe_tail = dedupe_tail;
        let source: Box<dyn source::DynSource> = Box::new(Replaying {
            commits: self.commits.clone(),
            rewind,
            next: 0,
        });
        Running::start(self.f.db.pool().await?, source, config)
    }
}

async fn wait_logged(pool: &PgPool, count: usize) -> Result<()> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let logged: i64 = sqlx::query_scalar("SELECT count(*) FROM es.log")
            .fetch_one(pool)
            .await?;
        if logged as usize == count {
            return Ok(());
        }
        if logged as usize > count || tokio::time::Instant::now() > deadline {
            bail!("log has {logged} rows, expected {count}");
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

#[tokio::test]
async fn replays_after_the_stored_token_are_dropped() -> Result<()> {
    let h = Harness {
        f: Fixture::new().await?,
        commits: Arc::default(),
    };

    // No token is ever stored before the crash, so the next run replays from the start; the log rows
    // above pges.stores.last_position seed the dedupe window.
    h.append(5).await?;
    let running = h.start(0, 0).await?;
    wait_logged(&h.f.pool, 5).await?;
    running.kill().await;

    h.append(3).await?;
    let running = h.start(0, 0).await?;
    wait_logged(&h.f.pool, 8).await?;
    tokio::time::sleep(Duration::from_millis(100)).await;
    wait_logged(&h.f.pool, 8).await?;
    running.stop().await?;
    let token: Vec<u8> = sqlx::query_scalar("SELECT source_token FROM pges.sequencer")
        .fetch_one(&h.f.pool)
        .await?;
    assert_eq!(token, 8_usize.to_be_bytes());

    // A graceful stop stored the token: resuming from it replays nothing.
    h.append(2).await?;
    let running = h.start(0, 0).await?;
    wait_logged(&h.f.pool, 10).await?;
    running.stop().await?;

    // A source that rewinds past its token is covered by the dedupe tail.
    h.append(2).await?;
    let running = h.start(4, 1024).await?;
    wait_logged(&h.f.pool, 12).await?;
    tokio::time::sleep(Duration::from_millis(100)).await;
    wait_logged(&h.f.pool, 12).await?;
    running.stop().await?;

    let log = common::check_log(&h.f.pool).await?;
    let delivered: Vec<_> = h
        .commits
        .lock()
        .unwrap()
        .iter()
        .map(|commit| commit.commit_id)
        .collect();
    assert_eq!(common::commit_order(&log), delivered);

    h.f.finish().await
}
