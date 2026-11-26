-- Registers a category with daily time leaves, returning its id.
--
-- $1 the category's name.
INSERT INTO {store}.categories (name, bucket) VALUES ($1, '1 day')
RETURNING category_id
