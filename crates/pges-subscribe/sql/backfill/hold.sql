-- Locks one lane's row for a batch, if its cursor is still where the run left it, and returns the id of the
-- batch's transaction.
--
-- $1 the name, $2 the lane, $3 the cursor the run read, $4 to $6 the session's tcp_keepalives_idle (s),
-- tcp_keepalives_interval (s) and tcp_keepalives_count for the batch. Returns no row when another run moved
-- the lane meanwhile: the batch is rolled back before the handler runs. The first statement of the batch's
-- transaction. The id lets backfill/checkpoint.sql check that it runs in the same transaction: a raw COMMIT
-- or ROLLBACK in the handler ends it. pg_current_xact_id() assigns one if the transaction has none yet.
--
-- The keepalive settings, local to the transaction, let the server drop the backend of a run whose host
-- vanished, and with it the lane lock a later run waits for, in seconds instead of the hours its defaults
-- take, as in subscription/hold.sql.
WITH keepalive AS MATERIALIZED (
  SELECT set_config('tcp_keepalives_idle', $4::int4::text, true),
         set_config('tcp_keepalives_interval', $5::int4::text, true),
         set_config('tcp_keepalives_count', $6::int4::text, true)
)
SELECT pg_current_xact_id()::text::int8 AS xact FROM keepalive, {store}.backfills
WHERE name = $1 AND lane = $2 AND position = $3
FOR NO KEY UPDATE OF backfills
