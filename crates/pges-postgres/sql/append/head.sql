-- Appends one commit to one stream, first part: append/commit.sql or append/commit_outbox.sql completes the
-- statement. It creates the stream, or moves its version on, under the expected-version and commit id checks.
--
-- $1 stream id, $2 category id, $3 expected version (-1 no stream, -2 any, n >= 1 exactly n), $4 event count,
-- $5 payload frame format, $6 payload, $7 stream key (NULL for none), $8 commit id, $9 the writer's
-- max_clock_skew.
--
-- The client always generates the commit id, so an append whose response was lost can be retried.
-- pges.check_commit_id raises SQLSTATE PGE01 when the id's timestamp is more than $9 from
-- statement_timestamp(); it accepts ids that are not UUIDv7, whose timestamp is NULL.
--
-- `i.commit_id > s.last_commit_id` keeps commit ids increasing within a stream, so first_commit_id is a true
-- minimum and a snapshot's commit id is a safe lower bound for reads. It also makes every retry of an append
-- that already succeeded return no row, whatever the expected version, because its id is at or below
-- last_commit_id. least() keeps the minimum even if that check is relaxed; first_commit_id is not indexed, so
-- the update stays HOT. A created stream starts at version event_count. No row returned means the append did
-- not apply; append/classify.sql finds out why.
--
-- {stream_table} is the hash partition of streams that holds $1 (`streams_p017`), which the store computes
-- with PostgreSQL's partition hash, so no plan has partitions to prune. A plan through streams prunes only
-- when it is custom: PostgreSQL 18 locks every partition of a generic plan before run-time pruning, and the
-- plan cache keeps a generic plan when the partitions were analyzed nearly empty or under
-- `force_generic_plan` (70 locks at 64 partitions, against 7 here). On the partition, the cached generic plan
-- is as good as a custom one, so an append is not planned again after its sixth run on a connection. A store
-- whose partitions it cannot name exactly renders streams here instead.
--
-- `bumped` also filters on `s.stream_id = $1` besides the join: `input` is materialized, so through streams
-- the join alone would not prune its partitions even in a custom plan, and the update would lock every
-- partition and its index (134 locks at 64 partitions, 518 at 256).
-- `created` locks only its partition: an insert routes its row when it runs.
WITH input AS MATERIALIZED (
  SELECT $1::uuid  AS stream_id,   $2::int4 AS category_id,
         $3::int8  AS expected,
         $4::int2  AS event_count, $5::int2 AS format,
         $6::bytea AS payload,     $7::text AS stream_key,
         pges.check_commit_id($8::uuid, $9::interval) AS commit_id
),
created AS (
  INSERT INTO {store}.{stream_table} (version, stream_id, first_commit_id, last_commit_id, category_id, stream_key)
  SELECT event_count, stream_id, commit_id, commit_id, category_id, stream_key
  FROM input WHERE expected IN (-1, -2)
  ON CONFLICT (stream_id) DO NOTHING
  RETURNING version AS last_version
),
bumped AS (
  UPDATE {store}.{stream_table} s SET version         = s.version + i.event_count,
                               first_commit_id = least(s.first_commit_id, i.commit_id),
                               last_commit_id  = i.commit_id
  FROM input i
  WHERE s.stream_id = $1
    AND s.stream_id = i.stream_id AND s.category_id = i.category_id AND s.state = 0
    AND (i.expected = -2 OR s.version = i.expected)
    AND i.commit_id > s.last_commit_id
  RETURNING s.version AS last_version
),
head AS (SELECT last_version FROM created UNION ALL SELECT last_version FROM bumped)
