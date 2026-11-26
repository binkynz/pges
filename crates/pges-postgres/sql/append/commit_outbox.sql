-- Appends one commit to one stream, last part for stores with the outbox: after append/head.sql, inserts the
-- commit and enqueues it into pending, which the outbox sequencer source drains. It continues the WITH list
-- of append/head.sql, so it starts with a comma.
--
-- Returns the commit id and the stream's version after the commit, or no row when the append did not apply.
-- The pending insert sees only commits that were inserted, so a failed expected-version check enqueues
-- nothing (and the outbox trigger does not notify).
,
inserted AS (
  INSERT INTO {store}.commits (last_version, commit_id, stream_id, category_id, event_count, format, payload)
  SELECT h.last_version, i.commit_id, i.stream_id, i.category_id, i.event_count, i.format, i.payload
  FROM head h CROSS JOIN input i
  RETURNING commit_id, last_version, category_id, event_count
),
enqueued AS (
  INSERT INTO {store}.pending (commit_id, category_id, event_count)
  SELECT commit_id, category_id, event_count FROM inserted
)
SELECT commit_id, last_version FROM inserted
