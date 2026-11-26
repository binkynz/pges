-- A shard's stored cursor, fenced: re-read after an error that may have hidden a committed checkpoint.
--
-- $1 the name, $2 the shard, $3 the fence. No row when the fence moved on.
SELECT position FROM {store}.subscriptions
WHERE name = $1 AND shard = $2 AND fence = $3
