-- Registers event type names of a category; names already registered keep their ids.
--
-- $1 category id, $2 the names.
--
-- Runs in autocommit on a pooled connection, never on a caller's connection, so the ids are committed before
-- any payload refers to them. The append statement never writes event_types.
INSERT INTO {store}.event_types (category_id, name) SELECT $1, unnest($2::text[])
ON CONFLICT (category_id, name) DO NOTHING
