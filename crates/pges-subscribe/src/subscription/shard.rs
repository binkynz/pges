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

use futures_util::{FutureExt, future};
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
    let (waiting, waiters) = watch::channel(Waiting::new());
    let fan_out = fan_out(waker, waiters, wakes, setup.retry_interval);
    tokio::pin!(fan_out);

    let mut coordinator = Coordinator {
        ctx,
        tick: setup.acquire_interval.min(setup.lease / 2),
        setup: Arc::new(setup),
        requested: Arc::new(requested),
        reader,
        delivery: delivery.into_shared(),
        woken,
        waiting,
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
                    Err(error) if !error.is_retried() => break Err(error),
                    Err(error) => {
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
    waiting: watch::Sender<Waiting>,
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
    /// Fails with [`subscription::SubscribeError::CrateTooOld`] when the database no longer supports this
    /// crate; no shard is taken then.
    async fn rebalance(&mut self) -> Result<(), subscription::SubscribeError> {
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
                waiting: self.waiting.clone(),
                term: (shard, fence),
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
///
/// The waker is called only while a shard waits that has seen every wake so far, with the lowest positions
/// such shards have read up to. So `known` is always what the shards read from the database, never what a
/// waker claimed: a waker's high-water mark is a hint (a `NOTIFY` can come from any role), and carrying it
/// forward would let a forged position hide the real wakes after it. Shards that have not seen the latest
/// wake read again without waiting for the waker.
async fn fan_out(
    mut waker: Box<dyn waker::DynWaker>,
    mut waiters: watch::Receiver<Waiting>,
    wakes: watch::Sender<Wake>,
    retry_interval: std::time::Duration,
) {
    loop {
        let generation = wakes.borrow().generation;
        let mut known = loop {
            if let Some(known) = lowest(&waiters.borrow_and_update(), generation) {
                break known;
            }
            if waiters.changed().await.is_err() {
                return future::pending().await;
            }
        };

        let woken = loop {
            let called = {
                let call = waker::DynWaker::changed(&mut *waker, &known);
                tokio::pin!(call);
                loop {
                    tokio::select! {
                        woken = &mut call => break Called::Woken(woken),
                        changed = waiters.changed() => {
                            if changed.is_err() {
                                return future::pending().await;
                            }
                            // A shard that started waiting with lower positions must not miss a wake that
                            // the current call would take as already read. Higher ones change nothing.
                            let lower = lowest(&waiters.borrow_and_update(), generation)
                                .map(|lower| lowered(&known, &lower))
                                .filter(|lower| *lower != known);
                            if let Some(lower) = lower {
                                break Called::Lowered(lower);
                            }
                        }
                    }
                }
            };
            match called {
                Called::Woken(woken) => break woken,
                Called::Lowered(lower) => known = lower,
            }
        };

        match woken {
            Ok(high) => {
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

/// How a call of the subscription's waker ended.
enum Called {
    Woken(Result<high_water::HighWater, waker::WakeError>),
    /// A shard began waiting with lower positions: call again with these.
    Lowered(high_water::HighWater),
}

/// The lowest positions of the waiting shards that have seen wake `generation`; `None` if no such shard
/// waits.
fn lowest(waiting: &Waiting, generation: u64) -> Option<high_water::HighWater> {
    waiting
        .values()
        .filter(|waiter| waiter.seen == generation)
        .map(|waiter| &waiter.known)
        .fold(None, |lowest, known| {
            Some(match lowest {
                None => known.clone(),
                Some(lowest) => lowered(&lowest, known),
            })
        })
}

/// `known`, with every position lowered to `other`'s where that is lower. Positions only, no LSN.
fn lowered(known: &high_water::HighWater, other: &high_water::HighWater) -> high_water::HighWater {
    high_water::HighWater::new(
        known.store().min(other.store()),
        known
            .categories()
            .iter()
            .map(|(id, position)| (*id, (*position).min(other.category(*id))))
            .collect(),
        None,
    )
}

/// The latest call of the subscription's waker.
#[derive(Debug, Clone, Default)]
struct Wake {
    /// Counts the calls that resolved.
    generation: u64,
    /// What the waker resolved with: a hint, passed to the shards for its LSN.
    high: high_water::HighWater,
    error: Option<Arc<waker::WakeError>>,
}

/// The shards waiting on their [`Fanned`] waker, by shard and fence.
type Waiting = BTreeMap<(i16, i64), Waiter>;

/// A shard waiting on its [`Fanned`] waker.
#[derive(Debug, Clone)]
struct Waiter {
    /// The last wake it saw.
    seen: u64,
    /// What it has read up to.
    known: high_water::HighWater,
}

/// A shard's waker: resolves on every call of the subscription's waker that resolved since its last call.
/// While it waits, the fan-out task knows what the shard has read up to. Once the subscription's task is gone
/// it never resolves.
struct Fanned {
    woken: watch::Receiver<Wake>,
    seen: u64,
    waiting: watch::Sender<Waiting>,
    term: (i16, i64),
}

impl waker::Waker for Fanned {
    async fn changed(
        &mut self,
        known: &high_water::HighWater,
    ) -> Result<high_water::HighWater, waker::WakeError> {
        let seen = self.seen;
        let _waiting = Registered::new(
            &self.waiting,
            self.term,
            Waiter {
                seen,
                known: known.clone(),
            },
        );
        let Ok(wake) = self
            .woken
            .wait_for(|wake| wake.generation > seen)
            .await
            .map(|wake| wake.clone())
        else {
            // `fan_out` never ends, so its sender is gone only once `run` was dropped (its task aborted, as in
            // a crash). Dropping `run` drops the `JoinSet`, which only asks the shards' tasks to stop: one
            // that is being polled meanwhile finishes that poll. Failing here would show the instance as
            // recovering from an error while it goes away; waiting lets the task be cancelled.
            return std::future::pending().await;
        };
        self.seen = wake.generation;

        match wake.error {
            Some(error) => Err(waker::WakeError::Other(Box::new(FannedError(error)))),
            None => Ok(wake.high),
        }
    }
}

/// A shard's entry in [`Waiting`], removed when its wait ends or is dropped.
struct Registered<'a> {
    waiting: &'a watch::Sender<Waiting>,
    term: (i16, i64),
}

impl<'a> Registered<'a> {
    fn new(waiting: &'a watch::Sender<Waiting>, term: (i16, i64), waiter: Waiter) -> Self {
        waiting.send_modify(|waiting| {
            waiting.insert(term, waiter);
        });

        Self { waiting, term }
    }
}

impl Drop for Registered<'_> {
    fn drop(&mut self) {
        self.waiting.send_modify(|waiting| {
            waiting.remove(&self.term);
        });
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

    fn high(store: u64, categories: &[(i32, u64)]) -> high_water::HighWater {
        high_water::HighWater::new(
            at(store),
            categories
                .iter()
                .map(|(id, p)| (category::CategoryId::new(*id), at(*p)))
                .collect(),
            None,
        )
    }

    fn waiter(seen: u64, known: high_water::HighWater) -> Waiter {
        Waiter { seen, known }
    }

    #[test]
    fn the_lowest_positions_of_the_shards_that_saw_the_last_wake() {
        let waiting = Waiting::from([
            ((0, 1), waiter(2, high(9, &[(1, 9), (2, 4)]))),
            ((1, 1), waiter(2, high(7, &[(1, 8), (2, 7)]))),
            // Has not seen wake 2: it reads again before it waits.
            ((2, 1), waiter(1, high(1, &[(1, 1), (2, 1)]))),
        ]);

        assert_eq!(
            lowest(&waiting, 2),
            Some(high(7, &[(1, 8), (2, 4)])),
            "positions only, each the lowest"
        );
        assert_eq!(lowest(&waiting, 3), None);
        assert_eq!(lowest(&Waiting::new(), 0), None);
    }

    #[tokio::test]
    async fn fanned_wakers_resolve_once_per_wake_and_show_while_they_wait() {
        let (wakes, woken) = watch::channel(Wake::default());
        let (waiting, waiters) = watch::channel(Waiting::new());
        let mut fanned = Fanned {
            woken,
            seen: 0,
            waiting,
            term: (3, 7),
        };
        let known = high(5, &[]);

        let pending = tokio::time::timeout(
            std::time::Duration::from_millis(20),
            waker::Waker::changed(&mut fanned, &known),
        )
        .await;
        assert!(pending.is_err(), "nothing happened yet");
        assert!(waiters.borrow().is_empty(), "a dropped wait leaves");

        wakes.send_modify(|wake| {
            wake.generation += 1;
            wake.high = high(u64::MAX >> 1, &[]);
        });
        let woke = waker::Waker::changed(&mut fanned, &known).await.unwrap();
        assert_eq!(woke.store(), at(u64::MAX >> 1), "the hint is passed on");
        // The same wake does not fire twice, however far past `known` it claims to be.
        {
            let wait = waker::Waker::changed(&mut fanned, &known);
            tokio::pin!(wait);
            let pending =
                tokio::time::timeout(std::time::Duration::from_millis(20), &mut wait).await;
            assert!(pending.is_err(), "a wake fires once");
            assert_eq!(
                waiters
                    .borrow()
                    .get(&(3, 7))
                    .map(|w| (w.seen, w.known.clone())),
                Some((1, known.clone())),
                "it shows while it waits"
            );

            wakes.send_modify(|wake| {
                wake.generation += 1;
                wake.error = Some(Arc::new(waker::WakeError::Closed));
            });
            assert!(wait.await.is_err());
        }
        assert!(waiters.borrow().is_empty());
        wakes.send_modify(|wake| {
            wake.generation += 1;
            wake.error = None;
        });
        assert!(waker::Waker::changed(&mut fanned, &known).await.is_ok());
    }

    /// A shard's task can still be polled after the subscription's task was aborted: it must not report the
    /// fan-out's channel closing as an error.
    #[tokio::test]
    async fn fanned_wakers_wait_once_the_fan_out_is_gone() {
        let (wakes, woken) = watch::channel(Wake::default());
        let (waiting, _waiters) = watch::channel(Waiting::new());
        let mut fanned = Fanned {
            woken,
            seen: 0,
            waiting,
            term: (0, 1),
        };
        let known = high_water::HighWater::new(at(5), Vec::new(), None);
        drop(wakes);

        let pending = tokio::time::timeout(
            std::time::Duration::from_millis(20),
            waker::Waker::changed(&mut fanned, &known),
        )
        .await;
        assert!(pending.is_err(), "{pending:?}");
    }

    /// Resolves each call with a forged high-water mark, and reports the `known` it was called with.
    struct Forged {
        calls: tokio::sync::mpsc::UnboundedSender<high_water::HighWater>,
        fire: Arc<tokio::sync::Notify>,
    }

    impl waker::Waker for Forged {
        async fn changed(
            &mut self,
            known: &high_water::HighWater,
        ) -> Result<high_water::HighWater, waker::WakeError> {
            let _ = self.calls.send(known.clone());
            self.fire.notified().await;

            Ok(high(u64::MAX >> 1, &[(1, u64::MAX >> 1)]))
        }
    }

    #[tokio::test]
    async fn the_fan_out_calls_its_waker_with_what_the_shards_read_not_what_it_claimed() {
        let (calls, mut called) = tokio::sync::mpsc::unbounded_channel();
        let fire = Arc::new(tokio::sync::Notify::new());
        let (wakes, mut woken) = watch::channel(Wake::default());
        let (waiting, waiters) = watch::channel(Waiting::new());
        let fan_out = tokio::spawn(fan_out(
            Box::new(Forged {
                calls,
                fire: fire.clone(),
            }),
            waiters,
            wakes,
            std::time::Duration::from_millis(10),
        ));
        let quiet = |called: &mut tokio::sync::mpsc::UnboundedReceiver<_>| {
            matches!(
                called.try_recv(),
                Err(tokio::sync::mpsc::error::TryRecvError::Empty)
            )
        };

        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        assert!(quiet(&mut called), "no shard waits yet");

        waiting.send_modify(|w| {
            w.insert((0, 1), waiter(0, high(5, &[(1, 5)])));
        });
        assert_eq!(called.recv().await, Some(high(5, &[(1, 5)])));
        // A shard with lower positions starts waiting: the call starts again with them.
        waiting.send_modify(|w| {
            w.insert((1, 1), waiter(0, high(3, &[(1, 3)])));
        });
        assert_eq!(called.recv().await, Some(high(3, &[(1, 3)])));
        // Higher ones change nothing.
        waiting.send_modify(|w| {
            w.insert((2, 1), waiter(0, high(8, &[(1, 8)])));
        });
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        assert!(quiet(&mut called));

        fire.notify_one();
        woken.wait_for(|wake| wake.generation == 1).await.unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        assert!(
            quiet(&mut called),
            "no shard has seen the wake yet: the waker is not called again"
        );

        // A shard read up to 6 after the wake: that, not the forged position, is what the waker gets.
        waiting.send_modify(|w| {
            w.clear();
            w.insert((0, 1), waiter(1, high(6, &[(1, 6)])));
        });
        assert_eq!(called.recv().await, Some(high(6, &[(1, 6)])));

        fan_out.abort();
    }
}
