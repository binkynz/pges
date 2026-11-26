-- Deletes a stream's snapshot.
--
-- $1 stream id.
DELETE FROM {store}.snapshots WHERE stream_id = $1
