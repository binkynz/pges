-- Moves a shard's cursor and renews its lease, fenced.
--
-- $1 the name, $2 the shard, $3 the new cursor, $4 the fence, $5 the lease duration. Updates no row when the fence moved on: another instance took the shard over.
UPDATE {store}.subscriptions SET position = $3, lease_until = now() + $5
WHERE name = $1 AND shard = $2 AND fence = $4
