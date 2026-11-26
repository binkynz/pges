-- Renews a shard's lease, fenced.
--
-- $1 the name, $2 the shard, $3 the fence, $4 the lease duration. Updates no row when the fence moved on: another instance took the shard over.
UPDATE {store}.subscriptions SET lease_until = now() + $4
WHERE name = $1 AND shard = $2 AND fence = $3
