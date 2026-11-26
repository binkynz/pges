-- Reads the ids of some event types of a category.
--
-- $1 category id, $2 the names. Ids enter the client's cache only from this query, after
-- event_type/register.sql.
SELECT event_type_id, name FROM {store}.event_types
WHERE category_id = $1 AND name = ANY($2)
