-- Appends one commit to each of several streams, first part: append/many_commit.sql or
-- append/many_commit_outbox.sql completes the statement. The same checks as append/head.sql, over arrays.
--
-- $1..$8 arrays of one element per stream: stream ids, category ids, expected versions, event counts, payload
-- frame formats, payloads, stream keys, commit ids. $9 the writer's max_clock_skew. The client rejects a batch
-- naming a stream twice, so `head` joins `input` on stream_id without an ordinal.
--
-- `locked` takes the row locks of the existing streams in stream id order (LockRows above Sort) before
-- `input` yields its first row: `input` reads it in a one-time filter (an InitPlan). `bumped` then updates
-- rows the statement already holds, so concurrent batches never wait on each other in opposite orders,
-- whatever join the planner picks. `created` inserts new streams in stream id order after that, and runs
-- before `bumped` (`head` is created UNION ALL bumped), so a batch that already moved a stream on never waits
-- on another batch: ON CONFLICT ignores a lock-only row lock.
--
-- An id at or below its stream's last_commit_id, as `locked` read it, gets an infinite tolerance, as in
-- append/head.sql: the batch's ids are crate-generated, but the retry after a lost connection sends them again,
-- possibly long after the first attempt committed, and it must find that commit and report it deduplicated.
-- `locked` holds the row, so the id it read is the stream's latest. The left join runs after `locked`: the
-- one-time filter gates it.
--
-- `bumped` repeats the `stream_id = ANY($1)` of `locked`: the join alone does not prune the hash partitions of
-- streams, so without it every custom plan expands the update over all of them. Every i.stream_id comes from
-- unnest($1), so the filter changes nothing else. The statement always runs with custom plans
-- (append/begin_many.sql, append/plan_mode.sql): a generic plan locks every partition.
WITH locked AS MATERIALIZED (
  SELECT s.stream_id, s.last_commit_id FROM {store}.streams s
  WHERE s.stream_id = ANY($1::uuid[])
  ORDER BY s.stream_id
  FOR UPDATE
),
input AS MATERIALIZED (
  SELECT u.stream_id, u.category_id, u.expected, u.event_count, u.format, u.payload, u.stream_key,
         pges.check_commit_id(u.commit_id,
           CASE WHEN u.commit_id <= l.last_commit_id THEN 'infinity' ELSE $9::interval END) AS commit_id
  FROM unnest($1::uuid[], $2::int4[], $3::int8[], $4::int2[], $5::int2[], $6::bytea[], $7::text[],
              $8::uuid[])
       AS u(stream_id, category_id, expected, event_count, format, payload, stream_key, commit_id)
  LEFT JOIN locked l ON l.stream_id = u.stream_id
  WHERE (SELECT count(*) FROM locked) >= 0
  ORDER BY u.stream_id
),
created AS (
  INSERT INTO {store}.streams (version, stream_id, first_commit_id, last_commit_id, category_id, stream_key)
  SELECT event_count, stream_id, commit_id, commit_id, category_id, stream_key
  FROM input WHERE expected IN (-1, -2)
  ORDER BY stream_id
  ON CONFLICT (stream_id) DO NOTHING
  RETURNING stream_id, version AS last_version
),
bumped AS (
  UPDATE {store}.streams s SET version         = s.version + i.event_count,
                               first_commit_id = least(s.first_commit_id, i.commit_id),
                               last_commit_id  = i.commit_id
  FROM input i
  WHERE s.stream_id = ANY($1::uuid[])
    AND s.stream_id = i.stream_id AND s.category_id = i.category_id AND s.state = 0
    AND (i.expected = -2 OR s.version = i.expected)
    AND i.commit_id > s.last_commit_id
  RETURNING s.stream_id, s.version AS last_version
),
head AS (SELECT stream_id, last_version FROM created
         UNION ALL SELECT stream_id, last_version FROM bumped)
