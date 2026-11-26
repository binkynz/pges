-- Locks a stream's row like append/lock_stream.sql, without waiting: no row when another transaction holds it
-- (or the stream does not exist).
--
-- $1 stream id.
--
-- Used by append_many in its own transaction. Its `locked` step locked only the streams that existed when the
-- statement started, so waiting now for a stream another transaction created since could deadlock against a
-- batch that locks in stream id order. When the row is busy the batch rolls back and starts over, and the new
-- attempt's `locked` step sees the stream.
SELECT last_commit_id FROM {store}.streams WHERE stream_id = $1 FOR UPDATE SKIP LOCKED
