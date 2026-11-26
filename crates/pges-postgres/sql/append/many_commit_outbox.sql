-- Appends one commit to each of several streams, last part for stores with the outbox: after
-- append/many_head.sql, inserts each commit and enqueues it into pending, in commit id order. It continues the
-- WITH list of append/many_head.sql, so it starts with a comma.
--
-- Returns one row per stream whose append applied: stream id, commit id, the stream's version after the
-- commit.
,
inserted AS (
  INSERT INTO {store}.commits (last_version, commit_id, stream_id, category_id, event_count, format, payload)
  SELECT h.last_version, i.commit_id, i.stream_id, i.category_id, i.event_count, i.format, i.payload
  FROM head h JOIN input i ON i.stream_id = h.stream_id
  RETURNING stream_id, commit_id, last_version, category_id, event_count
),
enqueued AS (
  INSERT INTO {store}.pending (commit_id, category_id, event_count)
  SELECT commit_id, category_id, event_count FROM inserted ORDER BY commit_id
)
SELECT stream_id, commit_id, last_version FROM inserted
