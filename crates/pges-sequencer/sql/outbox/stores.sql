-- The stores created with the outbox option, which the outbox source drains. Read on open, on a
-- notification from an unknown store, and every `OutboxConfig::store_refresh`.
SELECT store_id, schema_name FROM pges.stores WHERE outbox ORDER BY store_id
