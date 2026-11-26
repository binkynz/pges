-- Gives a shard's lease up, fenced, so a standby instance can take it at once.
--
-- $1 the name, $2 the shard, $3 the fence. Updates no row when the fence moved on: another instance took the shard over.
UPDATE {store}.subscriptions SET lease_owner = NULL, lease_until = NULL
WHERE name = $1 AND shard = $2 AND fence = $3
