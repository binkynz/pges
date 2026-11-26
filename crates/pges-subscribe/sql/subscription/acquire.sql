-- Takes over the lapsed lease of the lowest free shard of a subscription and bumps its fence.
--
-- $1 the name, $2 the new holder, $3 the lease duration, $4 the number of shards in use. Returns the shard, its
-- new fence, its cursor, and the stored definition, or no row when every shard is leased.
--
-- FOR UPDATE SKIP LOCKED lets instances racing for a shard each take a different one instead of waiting. The
-- fence makes every later write of the previous holder fail, so two holders never move one cursor.
UPDATE {store}.subscriptions
SET fence = fence + 1, lease_owner = $2, lease_until = now() + $3
WHERE name = $1 AND (lease_until IS NULL OR lease_until < now())
  AND shard = (SELECT shard FROM {store}.subscriptions
               WHERE name = $1 AND shard < $4
                 AND (lease_until IS NULL OR lease_until < now())
               ORDER BY shard LIMIT 1 FOR UPDATE SKIP LOCKED)
RETURNING shard, fence, position, shards, category_ids
