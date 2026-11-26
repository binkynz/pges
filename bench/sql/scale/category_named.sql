-- The id of a category of the store.
--
-- $1 the category's name.
SELECT category_id FROM {store}.categories WHERE name = $1::text
