-- Reads every event type of a category.
--
-- $1 category id. Run when a read finds an event type id the client's cache does not know yet, before it
-- fails to decode.
SELECT event_type_id, name FROM {store}.event_types WHERE category_id = $1
