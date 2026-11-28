-- Moves a shard's cursor at the end of a run_pg batch and renews its lease, fenced, in the transaction that
-- locked the row (subscription/hold.sql).
--
-- $1 the name, $2 the shard, $3 the new cursor, $4 the fence, $5 the lease duration. Updates no row when the
-- fence moved on.
--
-- The lease counts from clock_timestamp(), not now(): the transaction began before the handler ran, maybe
-- longer than the lease ago. No renewal ran meanwhile; the row lock kept the lease.
UPDATE {store}.subscriptions SET position = $3, lease_until = clock_timestamp() + $5
WHERE name = $1 AND shard = $2 AND fence = $4
