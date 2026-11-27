//! The sequencer lease and its fence: the row `pges.sequencer`. One sequencer leads per database; each new
//! leader bumps the fence, and every batch checks it first, so a deposed leader cannot write.

use pges_postgres::sql::typed::Query as _;
use sqlx::{Executor, PgConnection, postgres::types::PgInterval};

use pges_postgres::migrate;

use crate::{sequencer, sql::statement};

/// What the acquire statement returns.
pub(crate) enum Acquire {
    Taken(Acquired),
    /// Another sequencer holds the lease.
    Held,
    /// A migration raised the shared or a store's `min_compatible` above this crate's schema version
    /// ([`migrate::crate_too_old`] tells which); the lease was not taken.
    TooOld,
}

/// A lease taken.
pub(crate) struct Acquired {
    pub(crate) fence: sequencer::Fence,
    pub(crate) halted: Option<String>,
}

/// The stored resume point.
pub(crate) struct StoredToken {
    pub(crate) source: Option<String>,
    pub(crate) token: Option<Vec<u8>>,
}

/// Takes the lease if it is free or expired, bumping the fence, unless the database no longer supports this
/// crate.
pub(crate) async fn acquire(
    conn: &mut PgConnection,
    ttl: PgInterval,
    leader: Option<&str>,
) -> Result<Acquire, sqlx::Error> {
    let row = statement::lease_acquire::Query::fixed()
        .bind(statement::lease_acquire::Params {
            ttl,
            leader,
            shared_version: migrate::latest_version(migrate::SHARED).get(),
            store_version: migrate::latest_version(migrate::STORE).get(),
        })
        .fetch_one(&mut *conn)
        .await?;

    Ok(match (row.too_old, row.fence) {
        (Some(true), _) => Acquire::TooOld,
        (_, Some(fence)) => Acquire::Taken(Acquired {
            fence: sequencer::Fence::new(fence),
            halted: row.halted,
        }),
        (_, None) => Acquire::Held,
    })
}

/// Who may resequence.
#[cfg(feature = "source-logical")]
pub(crate) enum HaltedLease {
    Taken(sequencer::Fence),
    NotHalted,
    Held,
}

/// Takes the lease for resequencing, only if sequencing is halted and no sequencer holds it.
#[cfg(feature = "source-logical")]
pub(crate) async fn acquire_halted(
    conn: &mut PgConnection,
    ttl: PgInterval,
) -> Result<HaltedLease, sqlx::Error> {
    let taken = statement::lease_acquire_halted::Query::fixed()
        .bind(statement::lease_acquire_halted::Params { ttl })
        .fetch_optional(&mut *conn)
        .await?;
    if let Some(taken) = taken {
        return Ok(HaltedLease::Taken(sequencer::Fence::new(taken.fence)));
    }

    let halted = statement::lease_halted::Query::fixed()
        .bind(statement::lease_halted::Params {})
        .fetch_one(&mut *conn)
        .await?
        .halted;

    Ok(if halted {
        HaltedLease::Held
    } else {
        HaltedLease::NotHalted
    })
}

/// Records a halt found outside a term, unless a process holds the lease (it meets the same condition and
/// records it under its fence) or a reason is already recorded.
#[cfg(feature = "source-logical")]
pub(crate) async fn halt_unleased(
    executor: impl sqlx::PgExecutor<'_>,
    reason: &str,
) -> Result<(), sqlx::Error> {
    statement::lease_halt_unleased::Query::fixed()
        .bind(statement::lease_halt_unleased::Params { reason })
        .execute(executor)
        .await?;

    Ok(())
}

/// Extends the lease. `false`: another sequencer took it over, or the database no longer supports this
/// crate ([`migrate::crate_too_old`]); stand down.
pub(crate) async fn renew(
    conn: &mut PgConnection,
    fence: sequencer::Fence,
    ttl: PgInterval,
) -> Result<bool, sqlx::Error> {
    let done = statement::lease_renew::Query::fixed()
        .bind(statement::lease_renew::Params {
            ttl,
            fence: fence.sql(),
            shared_version: migrate::latest_version(migrate::SHARED).get(),
            store_version: migrate::latest_version(migrate::STORE).get(),
        })
        .execute(&mut *conn)
        .await?;

    Ok(done.rows_affected() == 1)
}

/// Gives the lease up, so a standby can take over at once instead of after the lease expires.
pub(crate) async fn release(
    conn: &mut PgConnection,
    fence: sequencer::Fence,
) -> Result<(), sqlx::Error> {
    statement::lease_release::Query::fixed()
        .bind(statement::lease_release::Params { fence: fence.sql() })
        .execute(&mut *conn)
        .await?;

    Ok(())
}

/// Records why sequencing must not resume by itself, under this term's fence. Every later acquire sees it
/// and stops with `RunError::Halted`.
pub(crate) async fn halt(
    conn: &mut PgConnection,
    fence: sequencer::Fence,
    reason: &str,
) -> Result<(), sqlx::Error> {
    statement::lease_halt::Query::fixed()
        .bind(statement::lease_halt::Params {
            fence: fence.sql(),
            reason,
        })
        .execute(&mut *conn)
        .await?;

    Ok(())
}

/// The durable token, read again whenever a term (re)starts: a batch whose COMMIT lost its answer may
/// have persisted a newer one. `None`: the fence moved on.
pub(crate) async fn stored_token(
    conn: &mut PgConnection,
    fence: sequencer::Fence,
) -> Result<Option<StoredToken>, sqlx::Error> {
    let row = statement::lease_stored_token::Query::fixed()
        .bind(statement::lease_stored_token::Params { fence: fence.sql() })
        .fetch_optional(&mut *conn)
        .await?;

    Ok(row.map(|row| StoredToken {
        source: row.source,
        token: row.source_token,
    }))
}

/// The fence check that opens every batch transaction. A row lock, not an update, so batches write no new
/// version of the hot row; a takeover's fence bump waits for the batch holding it. `false`: fenced out.
pub(crate) async fn hold_fence(
    tx: &mut PgConnection,
    fence: sequencer::Fence,
) -> Result<bool, sqlx::Error> {
    let row = statement::lease_hold_fence::Query::fixed()
        .bind(statement::lease_hold_fence::Params { fence: fence.sql() })
        .fetch_optional(&mut *tx)
        .await?;

    Ok(row.is_some())
}

/// Stores the resume token with the fence as guard. `false`: fenced out; roll back.
pub(crate) async fn store_token(
    tx: &mut PgConnection,
    fence: sequencer::Fence,
    source: &str,
    token: &[u8],
) -> Result<bool, sqlx::Error> {
    tx.execute(statement::SYNCHRONOUS_COMMIT.sql()).await?;

    let done = statement::lease_store_token::Query::fixed()
        .bind(statement::lease_store_token::Params {
            fence: fence.sql(),
            source,
            token,
        })
        .execute(&mut *tx)
        .await?;

    Ok(done.rows_affected() == 1)
}
