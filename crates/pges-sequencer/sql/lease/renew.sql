-- Extends the lease held under a fence, while the database still supports this crate.
--
-- $1 how long the lease lasts from now
-- $2 the holder's fence
-- $3 the crate's newest shared (pges) schema version
-- $4 the crate's newest store schema version
-- $5 the crate's newest outbox schema version
--
-- Zero rows: another process bumped the fence and took the lease over, or a migration raised the shared
-- scope's, some store's or some store's outbox scope's (`<store>.outbox`) min_compatible above the crate's
-- version for it (the sequencer writes to every store). The holder stands down, or stops when pges.scopes
-- says the crate is too old.
UPDATE pges.sequencer SET lease_until = now() + $1
WHERE fence = $2
  AND NOT EXISTS (SELECT FROM pges.scopes
                  WHERE (scope = 'pges' AND min_compatible > $3)
                     OR (scope IN (SELECT schema_name FROM pges.stores) AND min_compatible > $4)
                     OR (scope IN (SELECT schema_name || '.outbox' FROM pges.stores) AND min_compatible > $5))
