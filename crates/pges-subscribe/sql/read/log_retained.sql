-- A page of the log of every category for a reader that skips commits retention dropped, while its cursor
-- is below $4: read/log.sql, without the rows at or below their category's start position.
--
-- $1 the cursor, $2 the cut H, $3 the page size, $4 the position through which the reader skips commits
-- retention dropped (the head at creation, or the highest horizon if that is greater).
--
-- Every commit at or below a category's start was dropped, so its row would be skipped after its commits
-- were fetched. Filtering here returns a full page of rows that may still have a commit, instead of a page
-- per `limit` dead rows. The page is empty when the cursor is below the highest retention horizon above $4,
-- as in read/log.sql. A row whose category is missing keeps start 0, so the reader still reports it.
SELECT l.last_position, l.commit_id, l.category_id, l.event_count FROM {store}.log l
WHERE l.last_position > $1 AND l.last_position <= $2
  AND $1 >= coalesce((SELECT max(horizon_position) FROM {store}.categories WHERE horizon_position > $4), 0)
  AND l.last_position > coalesce((SELECT c.start_position FROM {store}.categories c
                                  WHERE c.category_id = l.category_id), 0)
ORDER BY l.last_position
LIMIT $3
