-- Renews a shard's lease, fenced, while the database still supports this crate.
--
-- $1 the name, $2 the shard, $3 the fence, $4 the lease duration, $5 the crate's newest shared (pges) schema
-- version, $6 the store, $7 the crate's newest store schema version, $8 its newest outbox schema version.
-- Updates no row when the fence moved on (another instance took the shard over), or when a migration raised
-- the min_compatible of the shared scope, the store's or the store's outbox scope (`<store>.outbox`) above
-- the crate's version for it: the holder then reads pges.scopes again and stops.
UPDATE {store}.subscriptions SET lease_until = now() + $4
WHERE name = $1 AND shard = $2 AND fence = $3
  AND NOT EXISTS (SELECT FROM pges.scopes
                  WHERE (scope = 'pges' AND min_compatible > $5) OR (scope = $6 AND min_compatible > $7)
                     OR (scope = $6 || '.outbox' AND min_compatible > $8))
