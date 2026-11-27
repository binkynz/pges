-- Locks a stream's row like append/lock_stream.sql, without waiting: no row when another transaction holds it
-- (or the stream does not exist).
--
-- $1 stream id.
--
-- Used by append_many in its own transaction. Its `locked` step locked only the streams that existed when the
-- statement started, so waiting now for a stream another transaction created since could deadlock against a
-- batch that locks in stream id order. When the row is busy the batch rolls back and starts over, and the new
-- attempt's `locked` step sees the stream.
--
-- SKIP LOCKED alone still waits in one case. When the updater of the row version the statement found commits
-- while the statement locks it, PostgreSQL 18 goes on to lock the newer versions, and waits for a transaction
-- that holds one whatever the wait policy (heap_lock_updated_tuple). So the lock runs with lock_timeout at 1 ms:
-- a wait fails the statement with SQLSTATE 55P03, which the caller takes as a busy row, and its rollback ends
-- the aborted transaction. `restored` puts the previous lock_timeout back for the rest of the transaction.
--
-- Each step is a one-time filter (an InitPlan) of the next, which fixes their order: `old` reads the setting,
-- `timeout` lowers it, `locked` takes the lock, `restored` puts the setting back.
WITH old AS MATERIALIZED (SELECT current_setting('lock_timeout') AS previous),
timeout AS MATERIALIZED (SELECT set_config('lock_timeout', '1ms', true) AS current FROM old),
locked AS MATERIALIZED (
  SELECT s.last_commit_id FROM {store}.streams s
  WHERE s.stream_id = $1 AND (SELECT current FROM timeout) IS NOT NULL
  FOR UPDATE SKIP LOCKED
),
restored AS MATERIALIZED (
  SELECT set_config('lock_timeout', old.previous, true) AS current FROM old
  WHERE (SELECT count(*) FROM locked) >= 0
)
SELECT last_commit_id FROM locked WHERE (SELECT count(*) FROM restored) >= 0
