-- Reads a stream's bounds before its commits are read: its version, its retention floor, its first and last
-- commit ids, its category and its state (0 active, 1 archived, 2 deleted). No row: the stream does not exist.
--
-- $1 stream id.
SELECT version, floor_version, first_commit_id, last_commit_id, category_id, state
FROM {store}.streams WHERE stream_id = $1
