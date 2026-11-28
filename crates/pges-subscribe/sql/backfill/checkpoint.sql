-- Moves one lane's cursor in the handler's transaction, if it is still where the run left it and this is
-- the transaction that locked the lane (backfill/hold.sql).
--
-- $1 the name, $2 the lane, $3 the new cursor, $4 the cursor the run read, $5 the transaction id hold.sql
-- returned. Returns in_batch false and moves nothing when this is another transaction: the handler ended the
-- batch's with a raw COMMIT or ROLLBACK, and this statement would otherwise move the cursor in autocommit.
-- in_batch is computed before the UPDATE can assign an id. Returns moved false when another run moved the
-- lane meanwhile, so the handler's transaction is rolled back instead of applying events twice.
WITH batch AS (
  SELECT coalesce(pg_current_xact_id_if_assigned()::text::int8 = $5, false) AS in_batch
), moved AS (
  UPDATE {store}.backfills SET position = $3
  WHERE name = $1 AND lane = $2 AND position = $4 AND (SELECT in_batch FROM batch)
  RETURNING lane
)
SELECT batch.in_batch, EXISTS (SELECT FROM moved) AS moved FROM batch
