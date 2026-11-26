-- Category ids by name, for the categories a subscription lists.
--
-- $1 the category names (text[]). Names that are not registered are left out; the caller reports the first.
SELECT category_id, name FROM {store}.categories WHERE name = ANY($1)
