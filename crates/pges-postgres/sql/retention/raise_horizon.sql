-- Raises a category's retention horizon before a leaf is dropped, and returns it. A subscription whose
-- cursor is behind the horizon then fails instead of skipping events.
--
-- $1 category id, $2 the leaf's horizon (retention/scan.sql; NULL leaves it).
UPDATE {store}.categories SET horizon_position = greatest(horizon_position, $2::int8)
WHERE category_id = $1
RETURNING horizon_position
