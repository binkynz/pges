-- Marks a stream deleted and releases its key. Returns its first commit id, where the commits to delete
-- start.
--
-- $1 stream id.
--
-- Runs in delete_stream's transaction, after the stream's row was locked and its `$stream-deleted` commit
-- appended.
UPDATE {store}.streams SET state = 2, stream_key = NULL WHERE stream_id = $1
RETURNING first_commit_id
