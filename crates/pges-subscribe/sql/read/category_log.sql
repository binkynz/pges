-- A page of one category's log: rows after the cursor up to the cut. The commits are fetched afterwards by
-- id from the category's own partition (read/commits.sql), so that statement is planned for the leaves the
-- page touches, not for every leaf of the category.
--
-- $1 category id, $2 cursor, $3 the cut H, $4 the page size, $5 the position through which the reader skips
-- commits retention dropped (0 for a subscription, the head at creation for one started at Start::Retained,
-- the largest int8 for a backfill).
--
-- The retention check is in the statement: a cursor below it returns an empty page, so retention cannot
-- remove rows between a separate check and the read. A category whose horizon is above $5 is checked against
-- its horizon: the reader must not skip commits it dropped. Otherwise it is checked against its start
-- position, before which every commit was dropped; commits missing between the start and the horizon were
-- dropped too and are skipped. log_by_category covers the page.
SELECT last_position, commit_id, event_count FROM {store}.log
WHERE category_id = $1 AND last_position > $2 AND last_position <= $3
  AND $2 >= (SELECT CASE WHEN horizon_position > $5 THEN horizon_position ELSE start_position END
             FROM {store}.categories WHERE category_id = $1)
ORDER BY last_position
LIMIT $4
