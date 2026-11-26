-- The retention horizon of some categories: positions at or below it may have been removed.
--
-- $1 the category ids (int4[]).
SELECT category_id, horizon_position FROM {store}.categories
WHERE category_id = ANY($1)
