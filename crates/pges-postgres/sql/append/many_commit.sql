-- Appends one commit to each of several streams, last part for stores without the outbox: after
-- append/many_head.sql, inserts each commit at its stream's new version.
--
-- Returns one row per stream whose append applied: stream id, commit id, the stream's version after the
-- commit. A stream missing from the result is classified alone with append/classify.sql.
INSERT INTO {store}.commits (last_version, commit_id, stream_id, category_id, event_count, format, payload)
SELECT h.last_version, i.commit_id, i.stream_id, i.category_id, i.event_count, i.format, i.payload
FROM head h JOIN input i ON i.stream_id = h.stream_id
RETURNING stream_id, commit_id, last_version
