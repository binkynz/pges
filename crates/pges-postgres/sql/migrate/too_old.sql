-- Finds a scope that no longer supports this crate: its min_compatible is above the crate's newest migration
-- for it. A running process reads it after a lease renewal was refused, to tell a migration that dropped its
-- support from a lease another instance took.
--
-- $1 the crate's newest shared (pges) schema version, $2 its newest store schema version, $3 the store to
-- check, or NULL for every store, $4 its newest outbox schema version. Returns the shared scope first, then
-- stores by name, each store's outbox scope (`<store>.outbox`) right after the store; no row when every
-- checked scope supports the crate. Store names contain no `.`, so a scope ending in `.outbox` is an outbox
-- scope.
SELECT scope, min_compatible FROM pges.scopes
WHERE CASE WHEN scope = 'pges' THEN min_compatible > $1
           WHEN scope LIKE '%.outbox'
             THEN coalesce(scope = $3 || '.outbox', scope IN (SELECT schema_name || '.outbox' FROM pges.stores))
                  AND min_compatible > $4
           ELSE coalesce(scope = $3, scope IN (SELECT schema_name FROM pges.stores)) AND min_compatible > $2 END
ORDER BY scope <> 'pges', scope
LIMIT 1
