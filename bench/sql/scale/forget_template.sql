-- Deletes a template commit and its stream, so the store holds only generated rows, every one with a log
-- row.
--
-- $1 the commit id, $2 the stream id.
WITH c AS (DELETE FROM {store}.commits WHERE commit_id = $1)
DELETE FROM {store}.streams WHERE stream_id = $2
