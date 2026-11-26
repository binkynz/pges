-- Changes a category's bucket. No row: the category is not registered.
--
-- $1 name, $2 the bucket interval ('1 day' or '1 month').
--
-- Existing leaves keep their span; the new bucket applies from the upper bound of the category's newest leaf
-- (leaf/to_create.sql).
UPDATE {store}.categories SET bucket = $2::interval WHERE name = $1 RETURNING category_id
