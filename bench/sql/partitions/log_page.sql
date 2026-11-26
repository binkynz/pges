-- The commit ids of a page of the log after a cursor, for the store-wide fetch to read.
--
-- $1 the cursor, $2 the page size.
SELECT commit_id FROM {store}.log WHERE last_position > $1 ORDER BY last_position LIMIT $2
