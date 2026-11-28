-- Locks a shard's row for a run_pg batch, fenced, while the database still supports this crate.
--
-- $1 the name, $2 the shard, $3 the fence, $4 the crate's newest shared (pges) schema version, $5 the store,
-- $6 the crate's newest store schema version, $7 its newest outbox schema version. Returns no row when the
-- fence moved on (another instance took the shard over), or when a migration raised a min_compatible above
-- the crate's version, as subscription/renew.sql.
--
-- The first statement of the batch's transaction, before the handler's. The row lock is the lease while the
-- handler runs: acquire skips locked rows (SKIP LOCKED) and a redefine waits, so the fence cannot move
-- until the transaction ends, and no renewal updates the row meanwhile. A renewal committed after the
-- transaction's snapshot would fail its checkpoint with 40001 under REPEATABLE READ or SERIALIZABLE. FOR NO
-- KEY UPDATE is the lock the checkpoint's UPDATE takes anyway.
SELECT shard FROM {store}.subscriptions
WHERE name = $1 AND shard = $2 AND fence = $3
  AND NOT EXISTS (SELECT FROM pges.scopes
                  WHERE (scope = 'pges' AND min_compatible > $4) OR (scope = $5 AND min_compatible > $6)
                     OR (scope = $5 || '.outbox' AND min_compatible > $7))
FOR NO KEY UPDATE OF subscriptions
