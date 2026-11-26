-- Finds streams with commits in a leaf that are not deleted and had a commit within the retention's keep
-- interval. Any row refuses the leaf under a window retention: such a stream is long-lived and would lose its
-- start.
--
-- $1 the keep interval, $2 the most streams to report.
--
-- Each of the leaf's streams is looked up on its own, so the cost follows the leaf, not the store. A leaf
-- that can be dropped has no such stream, so the limit never ends the query early, and a join could be
-- merged or hashed against every streams partition. The LIMIT 1 keeps the lookup a subquery the planner
-- cannot turn into a join. The keep bound is a subquery so it is computed once, not once per stream.
SELECT d.stream_id
FROM (SELECT DISTINCT stream_id FROM {store}.{leaf}) d
CROSS JOIN LATERAL (SELECT 1 FROM {store}.streams s
                    WHERE s.stream_id = d.stream_id AND s.state <> 2
                      AND s.last_commit_id >= (SELECT pges.uuidv7_floor(now() - $1::interval))
                    LIMIT 1) live
ORDER BY d.stream_id
LIMIT $2
