-- Takes over the lapsed lease of the lowest free shard of a subscription and bumps its fence, while the
-- database still supports this crate.
--
-- $1 the name, $2 the new holder, $3 the lease duration, $4 the number of shards in use, $5 the crate's newest
-- shared (pges) schema version, $6 the store, $7 the crate's newest store schema version, $8 its newest outbox
-- schema version. Returns one row: too_old, whether a migration raised the min_compatible of the shared scope,
-- the store's or the store's outbox scope above the crate's version for it (as subscription/renew.sql), and
-- the shard, its new fence, its cursor and the stored definition, all NULL when no shard was taken: every
-- shard is leased, or too_old. A too_old crate takes no shard and locks no row.
--
-- FOR UPDATE SKIP LOCKED lets instances racing for a shard each take a different one instead of waiting. The
-- fence makes every later write of the previous holder fail, so two holders never move one cursor.
WITH checked AS (
  SELECT EXISTS (SELECT FROM pges.scopes
                 WHERE (scope = 'pges' AND min_compatible > $5) OR (scope = $6 AND min_compatible > $7)
                    OR (scope = $6 || '.outbox' AND min_compatible > $8)) AS too_old
), taken AS (
  UPDATE {store}.subscriptions
  SET fence = fence + 1, lease_owner = $2, lease_until = now() + $3
  WHERE name = $1 AND (lease_until IS NULL OR lease_until < now())
    AND shard = (SELECT shard FROM {store}.subscriptions
                 WHERE name = $1 AND shard < $4
                   AND (lease_until IS NULL OR lease_until < now())
                   AND NOT (SELECT too_old FROM checked)
                 ORDER BY shard LIMIT 1 FOR UPDATE SKIP LOCKED)
  RETURNING shard, fence, position, shards, category_ids
)
SELECT checked.too_old, taken.shard, taken.fence, taken.position, taken.shards, taken.category_ids
FROM checked LEFT JOIN taken ON true
