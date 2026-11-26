-- Writes the coarse high-water marks of a store's categories that moved, with the stored token, in the same
-- transaction. They are not written per batch: the watch carries the live values.
--
-- $1 category_id of each category that moved
-- $2 its newest position
UPDATE {store}.categories c SET last_position = v.position
FROM unnest($1::int4[], $2::int8[]) AS v(category_id, position)
WHERE c.category_id = v.category_id
