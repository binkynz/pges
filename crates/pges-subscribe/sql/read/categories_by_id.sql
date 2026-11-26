-- Category names by id, for categories a store-wide read meets in the log that it has not seen before.
--
-- $1 the category ids (int4[]).
SELECT category_id, name FROM {store}.categories WHERE category_id = ANY($1)
