-- A page of the log of every category, for a store-wide subscription: rows after the cursor up to the cut.
--
-- $1 the cursor, $2 the cut H, $3 the page size, $4 the position through which the reader skips commits
-- retention dropped (0, or the head at creation for a subscription started at Start::Retained).
--
-- The page is empty when the cursor is below the highest retention horizon above $4 of any category, checked
-- in the same statement so retention cannot remove rows between a separate check and the read. Rows of
-- categories whose horizon is at or below $4 are returned; their missing commits were dropped and are
-- skipped. The commits are fetched per category afterwards (read/commits.sql), from the category's own
-- partition.
SELECT last_position, commit_id, category_id, event_count FROM {store}.log
WHERE last_position > $1 AND last_position <= $2
  AND $1 >= coalesce((SELECT max(horizon_position) FROM {store}.categories WHERE horizon_position > $4), 0)
ORDER BY last_position
LIMIT $3
