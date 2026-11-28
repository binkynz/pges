-- Locks one lane's row for a batch, if its cursor is still where the run left it, and returns the id of the
-- batch's transaction.
--
-- $1 the name, $2 the lane, $3 the cursor the run read. Returns no row when another run moved the lane
-- meanwhile: the batch is rolled back before the handler runs. The first statement of the batch's
-- transaction. The id lets backfill/checkpoint.sql check that it runs in the same transaction: a raw COMMIT
-- or ROLLBACK in the handler ends it. pg_current_xact_id() assigns one if the transaction has none yet.
SELECT pg_current_xact_id()::text::int8 AS xact FROM {store}.backfills
WHERE name = $1 AND lane = $2 AND position = $3
FOR NO KEY UPDATE
