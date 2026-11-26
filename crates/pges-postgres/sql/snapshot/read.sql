-- Reads a stream's snapshot. No row: the stream has none.
--
-- $1 stream id.
--
-- A load ignores a snapshot whose state version, format or category differ from the aggregate's, or whose
-- version is outside the stream's floor and version.
SELECT version, commit_id, category_id, state_version, format, state
FROM {store}.snapshots WHERE stream_id = $1
