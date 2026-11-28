//! The locks pges waits for, and the error when a wait runs out.
//!
//! Migrations take the migration lock, and every administration operation a store's maintenance lock.
//! Neither waits for longer than its lock wait
//! ([`migrate::MigrateConfig::lock_wait`](crate::migrate::MigrateConfig::lock_wait),
//! [`admin::Admin::with_lock_wait`](crate::admin::Admin::with_lock_wait), [`DEFAULT_WAIT`] by default): it
//! fails with [`LockTimeout`], naming the session that held the lock when PostgreSQL shows it.

use std::{fmt, time::Duration};

use pges_core::{interval, store};

/// How long a migration or administration operation waits for its lock by default: 60 seconds.
pub const DEFAULT_WAIT: interval::Interval = pges_core::interval!(60 s);

/// A lock pges takes.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum Lock {
    /// The migration lock: the `pges` row of `pges.scopes`, or before the shared schema exists the advisory
    /// lock `hashtext('pges.migrate')`.
    Migration,
    /// The maintenance lock of a store: the advisory lock `hashtext('pges.maintain.' || store)`.
    Maintenance(store::StoreName),
}

impl fmt::Display for Lock {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Migration => f.write_str("the migration lock"),
            Self::Maintenance(store) => write!(f, "the maintenance lock of store {store}"),
        }
    }
}

/// A server process id, as `pg_backend_pid()` and `pg_stat_activity.pid` report it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct BackendPid(i32);

impl BackendPid {
    /// The process id.
    pub const fn get(self) -> i32 {
        self.0
    }
}

impl fmt::Display for BackendPid {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

/// The session that held a lock when the wait for it ran out.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct LockHolder {
    /// Its server process: `SELECT pg_terminate_backend(<pid>)` ends it, with the privileges that takes.
    pub pid: BackendPid,
    /// The role it logged in as (`pg_stat_activity.usename`), when PostgreSQL shows it.
    pub role: Option<String>,
}

/// A lock stayed taken by another session for longer than the operation's lock wait. Nothing was done.
///
/// The holder may be a legitimate migration or maintenance run that takes longer than the wait, or any
/// session that holds the lock and does not let go: running the operation again retries.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub struct LockTimeout {
    /// The lock.
    pub lock: Lock,
    /// How long the operation waited.
    pub waited: Duration,
    /// The session that held the lock as the wait ran out, if PostgreSQL showed one: it may have let go by
    /// the time it was looked up.
    pub holder: Option<LockHolder>,
}

impl fmt::Display for LockTimeout {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "waited {:?} for {}", self.waited, self.lock)?;
        match &self.holder {
            Some(LockHolder {
                pid,
                role: Some(role),
            }) => write!(f, ", held by pid {pid} (role {role})"),
            Some(LockHolder { pid, role: None }) => write!(f, ", held by pid {pid}"),
            None => f.write_str(", held by a session that could not be identified"),
        }
    }
}

impl LockHolder {
    /// The holder a lookup found, from its `pid` and `role` columns. None without a process id: no session
    /// held the lock any more, or a prepared transaction does.
    pub(crate) fn from_row(pid: Option<i32>, role: Option<String>) -> Option<Self> {
        pid.map(|pid| Self {
            pid: BackendPid(pid),
            role,
        })
    }
}

/// When a lock wait runs out. Locks are tried, not waited for in the server, so a waiting operation holds no
/// lock queue entry and, between tries, no pooled connection.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Deadline {
    wait: Duration,
    until: tokio::time::Instant,
}

impl Deadline {
    pub(crate) fn after(wait: interval::Interval) -> Self {
        Self {
            wait: wait.get(),
            until: tokio::time::Instant::now() + wait.get(),
        }
    }

    pub(crate) fn wait(&self) -> Duration {
        self.wait
    }

    pub(crate) fn passed(&self) -> bool {
        tokio::time::Instant::now() >= self.until
    }

    /// Sleeps before the next try: `pause`, cut at the deadline, which then doubles up to [`MAX_PAUSE`].
    pub(crate) async fn pause(&self, pause: &mut Duration) {
        tokio::time::sleep_until(self.until.min(tokio::time::Instant::now() + *pause)).await;
        *pause = (*pause * 2).min(MAX_PAUSE);
    }
}

/// The first pause between two tries of a lock.
pub(crate) const FIRST_PAUSE: Duration = Duration::from_millis(10);

/// The longest pause between two tries: how long a lock may stay free before a waiting operation notices.
const MAX_PAUSE: Duration = Duration::from_millis(250);
