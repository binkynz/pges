-- Registers a category with the default bucket and retention, returning its id.
--
-- $1 the category's name.
INSERT INTO {store}.categories (name) VALUES ($1) RETURNING category_id
