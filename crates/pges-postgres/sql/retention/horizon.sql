-- Reads a category's retention horizon and start position.
--
-- $1 category id.
SELECT horizon_position, start_position FROM {store}.categories WHERE category_id = $1
