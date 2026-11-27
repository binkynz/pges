//! Sharded subscriptions ([`Builder::sharded`](super::Builder::sharded)): one instance takes a fair share of
//! the shards and leads each on a task of its own, with the same term loop as an unsharded subscription.
//!
//! Instances find each other through `subscription_members`: each records itself every tick, and takes at
//! most `shards / live instances` (rounded up) of the free shards. An instance above its share releases the
//! highest of its shards. The handler and the waker are shared: shards call the handler one at a time, and
//! one task waits on the waker and wakes every shard.

use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex, MutexGuard, PoisonError},
};

use futures_util::FutureExt;
use tokio::{
    sync::{oneshot, watch},
    task::JoinSet,
    time::Instant,
};

use pges_core::{category, high_water, position};

use crate::{read, sql::statement, subscription, waker};

/// Runs a sharded subscription until `shutdown` resolves or a shard stops with a fatal error.
pub(super) async fn run(
    setup: subscription::Setup,
    delivery: subscription::Delivery,
    waker: Box<dyn waker::DynWaker>,
    status: watch::Sender<subscription::Status>,
    shutdown: &mut subscription::Shutdown,
) -> Result<(), subscription::SubscribeError> {
    let ctx = subscription::Ctx::new(&setup);
    let board = Arc::new(Mutex::new(Board {
        status,
        shards: BTreeMap::new(),
        phase: Phase::Starting,
    }));

    let (reader, requested) = loop {
        match subscription::prepare(&ctx, &setup).await {
            Ok(prepared) => break prepared,
            Err(error) if error.is_retried() => {
                lock(&board).set_phase(Phase::Recovering(Arc::new(error)));
                if subscription::sleep_or_shutdown(setup.retry_interval, shutdown).await
                    == subscription::Wait::Shutdown
                {
                    return Ok(());
                }
            }
            Err(error) => return Err(error),
        }
    };

    let (wakes, woken) = watch::channel(Wake::default());
    let fan_out = fan_out(
        waker,
        reader.known(position::Position::BEGINNING),
        wakes,
        setup.retry_interval,
    );
    tokio::pin!(fan_out);

    let mut coordinator = Coordinator {
        ctx,
        tick: setup.acquire_interval.min(setup.lease / 2),
        setup: Arc::new(setup),
        requested: Arc::new(requested),
        reader,
        delivery: delivery.into_shared(),
        woken,
        board,
        tasks: JoinSet::new(),
        held: BTreeMap::new(),
    };

    let mut next_tick = Instant::now();
    let stopped = loop {
        tokio::select! {
            () = shutdown.as_mut() => break Ok(()),
            Some(joined) = coordinator.tasks.join_next(), if !coordinator.tasks.is_empty() => {
                match joined {
                    Ok((shard, fence, ended)) => {
                        coordinator.ended(shard, fence);
                        if let Err(error) = ended {
                            break Err(error);
                        }
                    }
                    Err(_) => break Err(subscription::SubscribeError::Aborted),
                }
            }
            () = tokio::time::sleep_until(next_tick) => {
                match coordinator.rebalance().await {
                    Ok(()) => {
                        lock(&coordinator.board).set_phase(Phase::Ready);
                        next_tick = Instant::now() + coordinator.tick;
                    }
                    Err(error) => {
                        let error = subscription::SubscribeError::Database(error.into());
                        lock(&coordinator.board).set_phase(Phase::Recovering(Arc::new(error)));
                        next_tick = Instant::now() + coordinator.setup.retry_interval;
                    }
                }
            }
            () = &mut fan_out => {}
        }
    };

    coordinator.stop(stopped).await
}

/// What a shard's task returns: its shard and fence, and how its term ended.
type Joined = (
    i16,
    i64,
    Result<subscription::Ended, subscription::SubscribeError>,
);

struct Coordinator {
    ctx: subscription::Ctx,
    tick: std::time::Duration,
    setup: Arc<subscription::Setup>,
    requested: Arc<Vec<category::CategoryId>>,
    reader: read::Reader,
    delivery: subscription::Shared,
    woken: watch::Receiver<Wake>,
    board: Arc<Mutex<Board>>,
    tasks: JoinSet<Joined>,
    held: BTreeMap<i16, Holding>,
}

/// A shard this instance leads.
struct Holding {
    fence: i64,
    /// Ends the shard's term; `None` once it was asked to.
    stop: Option<oneshot::Sender<()>>,
}

impl Coordinator {
    /// Records this instance as alive, then releases the shards above its share or takes free ones up to
    /// it.
    async fn rebalance(&mut self) -> Result<(), sqlx::Error> {
        let members = self
            .ctx
            .sql
            .heartbeat
            .bind(statement::heartbeat::Params {
                name: &self.ctx.name,
                owner: self.ctx.owner,
                lease: self.ctx.interval,
            })
            .fetch_one(&self.ctx.pool)
            .await?
            .members;
        let members = u16::try_from(members.max(1)).unwrap_or(u16::MAX);
        let share = usize::from(self.setup.shards.get().div_ceil(members));

        let leading: Vec<i16> = self
            .held
            .iter()
            .filter(|(_, holding)| holding.stop.is_some())
            .map(|(shard, _)| *shard)
            .collect();
        for shard in leading
            .iter()
            .rev()
            .take(leading.len().saturating_sub(share))
        {
            if let Some(stop) = self
                .held
                .get_mut(shard)
                .and_then(|holding| holding.stop.take())
            {
                let _ = stop.send(());
            }
        }

        let mut leading = leading.len().min(share);
        while leading < share {
            let Some(term) = self.ctx.acquire().await? else {
                break;
            };
            self.start(term);
            leading += 1;
        }

        Ok(())
    }

    /// Leads `term` on a task of its own.
    fn start(&mut self, term: subscription::Term) {
        let (shard, fence) = (term.shard, term.fence);
        let (stop, stopped) = oneshot::channel::<()>();
        let (status, _) = watch::channel(subscription::Status {
            state: subscription::State::Starting,
            position: term.cursor,
            high_water: position::Position::BEGINNING,
            shards: Vec::new(),
        });
        lock(&self.board).update(shard, status.borrow().clone());

        let runner = subscription::Runner {
            ctx: self.ctx.clone(),
            delivery: subscription::Delivery::Shared(self.delivery.clone()),
            waker: Box::new(Fanned {
                seen: self.woken.borrow().generation,
                woken: self.woken.clone(),
            }),
            status,
            retry: self.setup.retry,
            retry_interval: self.setup.retry_interval,
            max_idle: self.setup.max_idle,
            replica: self.setup.replica(),
        };
        let shutdown: subscription::Shutdown = Box::pin(
            async move {
                let _ = stopped.await;
            }
            .fuse(),
        );
        self.held.insert(
            shard,
            Holding {
                fence,
                stop: Some(stop),
            },
        );
        self.tasks.spawn(lead(
            self.setup.clone(),
            runner,
            term,
            self.reader.for_shard(shard),
            self.requested.clone(),
            shutdown,
            self.board.clone(),
        ));
    }

    /// Forgets a shard whose term ended, unless it was taken again since.
    fn ended(&mut self, shard: i16, fence: i64) {
        if self
            .held
            .get(&shard)
            .is_some_and(|holding| holding.fence == fence)
        {
            self.held.remove(&shard);
            lock(&self.board).remove(shard);
        }
    }

    /// Ends every shard's term (each finishes its batch and releases its lease), waits for them, and leaves.
    /// Returns `stopped`, or the first fatal error a shard stopped with meanwhile.
    async fn stop(
        mut self,
        stopped: Result<(), subscription::SubscribeError>,
    ) -> Result<(), subscription::SubscribeError> {
        for holding in self.held.values_mut() {
            if let Some(stop) = holding.stop.take() {
                let _ = stop.send(());
            }
        }

        let mut result = stopped;
        while let Some(joined) = self.tasks.join_next().await {
            let error = match joined {
                Ok((_, _, Err(error))) => error,
                Ok(_) => continue,
                Err(_) => subscription::SubscribeError::Aborted,
            };
            if result.is_ok() {
                result = Err(error);
            }
        }

        // Best effort: a member that is left behind lapses like a lease.
        let _ = self
            .ctx
            .sql
            .leave
            .bind(statement::leave::Params {
                name: &self.ctx.name,
                owner: self.ctx.owner,
            })
            .execute(&self.ctx.pool)
            .await;
        let mut board = lock(&self.board);
        board.shards.clear();
        board.publish();

        result
    }
}

/// Leads one shard's term, forwarding its status to the board.
async fn lead(
    setup: Arc<subscription::Setup>,
    mut runner: subscription::Runner,
    mut term: subscription::Term,
    mut reader: read::Reader,
    requested: Arc<Vec<category::CategoryId>>,
    mut shutdown: subscription::Shutdown,
    board: Arc<Mutex<Board>>,
) -> Joined {
    let (shard, fence) = (term.shard, term.fence);
    let mut statuses = runner.status.subscribe();

    let lead = runner.lead(&setup, &mut term, &mut reader, &requested, &mut shutdown);
    tokio::pin!(lead);
    let ended = loop {
        tokio::select! {
            biased;
            ended = &mut lead => break ended,
            changed = statuses.changed() => match changed {
                Ok(()) => {
                    let status = statuses.borrow_and_update().clone();
                    lock(&board).update(shard, status);
                }
                Err(_) => break lead.await,
            },
        }
    };

    (shard, fence, ended)
}

/// Waits on the subscription's waker and tells every shard. It never ends; errors are passed on to the
/// shards, which show them as [`subscription::State::Recovering`], and the waker is called again after the
/// retry interval.
async fn fan_out(
    mut waker: Box<dyn waker::DynWaker>,
    mut known: high_water::HighWater,
    wakes: watch::Sender<Wake>,
    retry_interval: std::time::Duration,
) {
    loop {
        match waker::DynWaker::changed(&mut *waker, &known).await {
            Ok(high) => {
                known = raised(&known, &high);
                wakes.send_modify(|wake| {
                    wake.generation += 1;
                    wake.high = high;
                    wake.error = None;
                });
            }
            Err(error) => {
                wakes.send_modify(|wake| {
                    wake.generation += 1;
                    wake.error = Some(Arc::new(error));
                });
                tokio::time::sleep(retry_interval).await;
            }
        }
    }
}

/// `known`, with every position raised to `high`'s where that is higher.
fn raised(known: &high_water::HighWater, high: &high_water::HighWater) -> high_water::HighWater {
    high_water::HighWater::new(
        known.store().max(high.store()),
        known
            .categories()
            .iter()
            .map(|(id, position)| (*id, (*position).max(high.category(*id))))
            .collect(),
        high.lsn().or(known.lsn()),
    )
}

/// The latest call of the subscription's waker.
#[derive(Debug, Clone, Default)]
struct Wake {
    /// Counts the calls that resolved.
    generation: u64,
    high: high_water::HighWater,
    error: Option<Arc<waker::WakeError>>,
}

/// A shard's waker: resolves on every call of the subscription's waker that resolved since its last call,
/// and whenever the latest high-water mark is past `known`.
struct Fanned {
    woken: watch::Receiver<Wake>,
    seen: u64,
}

impl waker::Waker for Fanned {
    async fn changed(
        &mut self,
        known: &high_water::HighWater,
    ) -> Result<high_water::HighWater, waker::WakeError> {
        let seen = self.seen;
        let wake = self
            .woken
            .wait_for(|wake| wake.generation > seen || waker::has_passed(&wake.high, known))
            .await
            .map_err(|_| waker::WakeError::Closed)?
            .clone();
        self.seen = wake.generation;

        match wake.error {
            Some(error) if wake.generation > seen => {
                Err(waker::WakeError::Other(Box::new(FannedError(error))))
            }
            Some(_) | None => Ok(wake.high),
        }
    }
}

/// The subscription's waker failed; every shard sees the same error.
#[derive(Debug, thiserror::Error)]
#[error("{0}")]
struct FannedError(Arc<waker::WakeError>);

/// What the instance shows as its [`Status`]: its own phase and each shard's status.
struct Board {
    status: watch::Sender<subscription::Status>,
    shards: BTreeMap<i16, subscription::Status>,
    phase: Phase,
}

enum Phase {
    Starting,
    Ready,
    Recovering(Arc<subscription::SubscribeError>),
}

impl Board {
    fn set_phase(&mut self, phase: Phase) {
        let unchanged = matches!(
            (&self.phase, &phase),
            (Phase::Ready, Phase::Ready) | (Phase::Starting, Phase::Starting)
        );
        self.phase = phase;
        if !unchanged {
            self.publish();
        }
    }

    fn update(&mut self, shard: i16, status: subscription::Status) {
        self.shards.insert(shard, status);
        self.publish();
    }

    fn remove(&mut self, shard: i16) {
        self.shards.remove(&shard);
        self.publish();
    }

    /// The instance's state: its own error first, then a shard's, then running if it leads any shard.
    fn publish(&self) {
        let state = match &self.phase {
            Phase::Recovering(error) => subscription::State::Recovering(error.clone()),
            Phase::Starting | Phase::Ready => {
                let recovering = self.shards.values().find_map(|status| match &status.state {
                    subscription::State::Recovering(error) => {
                        Some(subscription::State::Recovering(error.clone()))
                    }
                    _ => None,
                });
                match recovering {
                    Some(recovering) => recovering,
                    None if !self.shards.is_empty() => subscription::State::Running,
                    None if matches!(self.phase, Phase::Starting) => subscription::State::Starting,
                    None => subscription::State::Standby,
                }
            }
        };

        self.status.send_modify(|status| {
            status.state = state;
            status.shards = self
                .shards
                .keys()
                .filter_map(|shard| u16::try_from(*shard).ok().map(subscription::Shard))
                .collect();
            if let Some(position) = self.shards.values().map(|s| s.position).min() {
                status.position = position;
            }
            if let Some(high_water) = self.shards.values().map(|s| s.high_water).max() {
                status.high_water = status.high_water.max(high_water);
            }
        });
    }
}

fn lock(board: &Mutex<Board>) -> MutexGuard<'_, Board> {
    board.lock().unwrap_or_else(PoisonError::into_inner)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn at(n: u64) -> position::Position {
        position::Position::new(n).unwrap()
    }

    #[test]
    fn raises_only_known_categories() {
        let known = high_water::HighWater::new(
            at(5),
            vec![
                (category::CategoryId::new(1), at(5)),
                (category::CategoryId::new(2), at(3)),
            ],
            None,
        );
        let high = high_water::HighWater::new(
            at(9),
            vec![
                (category::CategoryId::new(1), at(4)),
                (category::CategoryId::new(3), at(9)),
            ],
            None,
        );

        let raised = raised(&known, &high);

        assert_eq!(raised.store(), at(9));
        assert_eq!(
            raised.categories(),
            vec![
                (category::CategoryId::new(1), at(5)),
                (category::CategoryId::new(2), at(3)),
            ]
        );
    }

    #[tokio::test]
    async fn fanned_wakers_resolve_on_new_wakes_and_on_passed_positions() {
        let (wakes, woken) = watch::channel(Wake::default());
        let mut fanned = Fanned { woken, seen: 0 };
        let known = high_water::HighWater::new(at(5), Vec::new(), None);

        let waiting = tokio::time::timeout(
            std::time::Duration::from_millis(20),
            waker::Waker::changed(&mut fanned, &known),
        )
        .await;
        assert!(waiting.is_err(), "nothing happened yet");

        wakes.send_modify(|wake| wake.generation += 1);
        let high = waker::Waker::changed(&mut fanned, &known).await.unwrap();
        assert_eq!(high.store(), at(0));

        wakes.send_modify(|wake| wake.high = high_water::HighWater::new(at(6), Vec::new(), None));
        let high = waker::Waker::changed(&mut fanned, &known).await.unwrap();
        assert_eq!(high.store(), at(6));

        wakes.send_modify(|wake| {
            wake.generation += 1;
            wake.error = Some(Arc::new(waker::WakeError::Closed));
        });
        assert!(waker::Waker::changed(&mut fanned, &known).await.is_err());
        // The same error is not reported twice.
        assert!(waker::Waker::changed(&mut fanned, &known).await.is_ok());
    }
}
