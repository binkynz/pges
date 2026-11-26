-- Moves a shard's cursor, fenced.
--
-- $1 the name, $2 the shard, $3 the new cursor, $4 the fence. Updates no row when the fence moved on: another instance took the shard over.
UPDATE {store}.subscriptions SET position = $3
WHERE name = $1 AND shard = $2 AND fence = $4
