-- The category read in one statement, each log row's commit looked up in a LATERAL subquery, as
-- pges-subscribe read before it fetched a page's commits by id in a second statement. Measured for
-- comparison: its plan covers every time leaf, so executor startup, locks and plan-cache memory grow with
-- the leaves.
--
-- $1 category id, $2 cursor, $3 the cut H, $4 the page size.
SELECT l.last_position, l.event_count,
       c.stream_id, c.last_version, c.commit_id, c.format, c.payload
FROM {store}.log l
CROSS JOIN LATERAL (SELECT stream_id, last_version, commit_id, format, payload
                    FROM {store}.commits_{category}
                    WHERE commit_id = l.commit_id LIMIT 1) c
WHERE l.category_id = $1 AND l.last_position > $2 AND l.last_position <= $3
  AND $2 >= (SELECT horizon_position FROM {store}.categories WHERE category_id = $1)
ORDER BY l.last_position
LIMIT $4
