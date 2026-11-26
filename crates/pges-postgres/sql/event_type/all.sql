-- Reads every event type of the store, to fill the client's cache when the store is opened.
SELECT category_id, event_type_id, name FROM {store}.event_types
