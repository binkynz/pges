-- Appends one commit to one stream, last part for stores without the outbox: after append/head.sql, inserts
-- the commit at the stream's new version.
--
-- Returns the commit id and the stream's version after the commit, or no row when the append did not apply.
INSERT INTO {store}.commits (last_version, commit_id, stream_id, category_id, event_count, format, payload)
SELECT h.last_version, i.commit_id, i.stream_id, i.category_id, i.event_count, i.format, i.payload
FROM head h CROSS JOIN input i
RETURNING commit_id, last_version
