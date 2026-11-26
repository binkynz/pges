-- Reads a category's retention horizon.
--
-- $1 category id.
SELECT horizon_position FROM {store}.categories WHERE category_id = $1
