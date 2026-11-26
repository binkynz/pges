-- The store's newest log position, 0 when the log is empty.
SELECT coalesce(max(last_position), 0)::int8 AS last_position FROM {store}.log
