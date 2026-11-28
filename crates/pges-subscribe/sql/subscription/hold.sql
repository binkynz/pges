-- Locks a shard's row for a run_pg batch, fenced, while the database still supports this crate, and returns
-- the id of the batch's transaction.
--
-- $1 the name, $2 the shard, $3 the fence, $4 the crate's newest shared (pges) schema version, $5 the store,
-- $6 the crate's newest store schema version, $7 its newest outbox schema version, $8 to $10 the session's
-- tcp_keepalives_idle (s), tcp_keepalives_interval (s) and tcp_keepalives_count for the batch. Returns no row when the fence moved on (another instance took the shard over), or when a
-- migration raised a min_compatible above the crate's version, as subscription/renew.sql.
--
-- The first statement of the batch's transaction, before the handler's. The row lock is the lease while the
-- handler runs: acquire skips locked rows (SKIP LOCKED) and a redefine waits, so the fence cannot move
-- until the transaction ends, and no renewal updates the row meanwhile. A renewal committed after the
-- transaction's snapshot would fail its checkpoint with 40001 under REPEATABLE READ or SERIALIZABLE. FOR NO
-- KEY UPDATE is the lock the checkpoint's UPDATE takes anyway.
--
-- The lock goes when the backend ends, and a backend whose client host vanished ends only when the server
-- finds the connection dead. The keepalive settings, derived from the lease and local to the transaction
-- (set_config(..., true)), bound that to about one lease over TCP; the server's defaults take hours. A
-- vanished client's backend is idle, with nothing unacknowledged, so keepalives probe it; a live client's
-- kernel answers them, however slowly its handler reads. They do nothing over a Unix socket. Not
-- tcp_user_timeout: on Linux it also bounds a full receive window, so it would drop a healthy handler that
-- reads a large result slower than the lease.
--
-- The id lets subscription/checkpoint_held.sql check that it runs in the same transaction: a raw COMMIT or
-- ROLLBACK in the handler ends it. pg_current_xact_id() assigns one if the transaction has none yet.
WITH keepalive AS MATERIALIZED (
  SELECT set_config('tcp_keepalives_idle', $8::int4::text, true),
         set_config('tcp_keepalives_interval', $9::int4::text, true),
         set_config('tcp_keepalives_count', $10::int4::text, true)
)
SELECT pg_current_xact_id()::text::int8 AS xact FROM keepalive, {store}.subscriptions
WHERE name = $1 AND shard = $2 AND fence = $3
  AND NOT EXISTS (SELECT FROM pges.scopes
                  WHERE (scope = 'pges' AND min_compatible > $4) OR (scope = $5 AND min_compatible > $6)
                     OR (scope = $5 || '.outbox' AND min_compatible > $7))
FOR NO KEY UPDATE OF subscriptions
