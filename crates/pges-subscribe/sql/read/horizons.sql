-- The retention horizon and start position of some categories: positions at or below the horizon may have
-- been removed, and every commit at or below the start was.
--
-- $1 the category ids (int4[]).
SELECT category_id, horizon_position, start_position FROM {store}.categories
WHERE category_id = ANY($1)
