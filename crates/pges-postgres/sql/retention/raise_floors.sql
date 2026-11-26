-- Raises the retention floor of every stream with commits in a leaf to its newest version in the leaf,
-- before the leaf is dropped. A read below a stream's floor then fails as truncated instead of returning a
-- partial stream.
--
-- Runs after retention/probe_joins.sql, which leaves the planner only nested loops: each of the leaf's
-- streams is then updated through its key, and the cost follows the leaf. A hash or merge join reads every
-- streams partition, whatever the leaf's size.
UPDATE {store}.streams s SET floor_version = d.last_version
FROM (SELECT stream_id, max(last_version) AS last_version FROM {store}.{leaf} GROUP BY stream_id) d
WHERE s.stream_id = d.stream_id AND s.floor_version < d.last_version
