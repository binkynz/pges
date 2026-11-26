-- A page of one category's log: rows after the cursor up to the cut. The commits are fetched afterwards by
-- id from the category's own partition (read/commits.sql), so that statement is planned for the leaves the
-- page touches, not for every leaf of the category.
--
-- $1 category id, $2 cursor, $3 the cut H, $4 the page size.
--
-- The retention horizon is checked in the statement: a cursor below it returns an empty page, so retention
-- cannot remove rows between a separate check and the read. log_by_category covers the page.
SELECT last_position, commit_id, event_count FROM {store}.log
WHERE category_id = $1 AND last_position > $2 AND last_position <= $3
  AND $2 >= (SELECT horizon_position FROM {store}.categories WHERE category_id = $1)
ORDER BY last_position
LIMIT $4
