-- Reads a category's id and bucket, after category/insert.sql, whether that inserted it or not.
--
-- $1 name.
SELECT category_id, bucket FROM {store}.categories WHERE name = $1
