-- Finds streams with commits in a leaf that are not deleted and have no snapshot, of any state version in the
-- category, at or after their last version in the leaf. Any row refuses the leaf under snapshot-and-truncate
-- retention.
--
-- $1 category id, $2 the most streams to report.
--
-- Each of the leaf's streams is looked up on its own, so the cost follows the leaf, not the store, as in
-- retention/live_streams.sql: a join could merge or hash every snapshots or streams partition. The LIMIT 1
-- keeps each lookup a subquery the planner cannot turn into a join. Snapshots are looked up first, and the
-- OFFSET 0 keeps that order: a leaf that can be dropped has a snapshot for every stream, so streams is then
-- never read.
SELECT m.stream_id
FROM (SELECT d.stream_id
      FROM (SELECT stream_id, max(last_version) AS last_version FROM {store}.{leaf} GROUP BY stream_id) d
      LEFT JOIN LATERAL (SELECT 1 AS found FROM {store}.snapshots n
                         WHERE n.stream_id = d.stream_id AND n.category_id = $1
                           AND n.version >= d.last_version
                         LIMIT 1) snapshot ON true
      WHERE snapshot.found IS NULL
      OFFSET 0) m
CROSS JOIN LATERAL (SELECT 1 FROM {store}.streams s
                    WHERE s.stream_id = m.stream_id AND s.state <> 2
                    LIMIT 1) live
ORDER BY m.stream_id
LIMIT $2
