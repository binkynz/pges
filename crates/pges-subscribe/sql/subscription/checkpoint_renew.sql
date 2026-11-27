-- Moves a shard's cursor and renews its lease, fenced, while the database still supports this crate.
--
-- $1 the name, $2 the shard, $3 the new cursor, $4 the fence, $5 the lease duration, $6 the crate's newest
-- shared (pges) schema version, $7 the store, $8 the crate's newest store schema version. Updates no row when
-- the fence moved on (another instance took the shard over), or when a migration raised the shared or the
-- store's min_compatible above the crate's version, as subscription/renew.sql.
UPDATE {store}.subscriptions SET position = $3, lease_until = now() + $5
WHERE name = $1 AND shard = $2 AND fence = $4
  AND NOT EXISTS (SELECT FROM pges.scopes
                  WHERE (scope = 'pges' AND min_compatible > $6) OR (scope = $7 AND min_compatible > $8))
