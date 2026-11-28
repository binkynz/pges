-- Moves a shard's cursor at the end of a run_pg batch and renews its lease, fenced, if the cursor is still
-- where the instance holds it and this runs in the transaction that locked the row (subscription/hold.sql).
--
-- $1 the name, $2 the shard, $3 the new cursor, $4 the fence, $5 the transaction id hold.sql returned, $6 the
-- lease duration, $7 the cursor the instance holds. Returns in_batch false and moves nothing when this is
-- another transaction: the handler ended the batch's with a raw COMMIT or ROLLBACK, so this statement runs in
-- autocommit (or in a transaction the handler began), and the batch's writes rolled back or committed without
-- the cursor. A transaction without an id yet is another one; in_batch is computed before the UPDATE can
-- assign one. Returns moved false when the fence moved on, or when the stored cursor is elsewhere, as
-- subscription/checkpoint.sql: a COMMIT whose reply was lost moved it, possibly after the instance read it
-- again. The hold waited on that COMMIT's row lock and locked the moved row; applying the batch would apply
-- it twice.
--
-- The lease counts from clock_timestamp(), not now(): the transaction began before the handler ran, maybe
-- longer than the lease ago. No renewal ran meanwhile; the row lock kept the lease.
WITH batch AS (
  SELECT coalesce(pg_current_xact_id_if_assigned()::text::int8 = $5, false) AS in_batch
), moved AS (
  UPDATE {store}.subscriptions SET position = $3, lease_until = clock_timestamp() + $6
  WHERE name = $1 AND shard = $2 AND fence = $4 AND position = $7 AND (SELECT in_batch FROM batch)
  RETURNING shard
)
SELECT batch.in_batch, EXISTS (SELECT FROM moved) AS moved FROM batch
