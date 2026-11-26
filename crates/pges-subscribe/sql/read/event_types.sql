-- Every event type of some categories, to name the events a read decodes.
--
-- $1 the category ids (int4[]).
SELECT category_id, event_type_id, name FROM {store}.event_types
WHERE category_id = ANY($1)
