-- Finds a scope that no longer supports this crate: its min_compatible is above the crate's newest migration
-- for it. A running process reads it after a lease renewal was refused, to tell a migration that dropped its
-- support from a lease another instance took.
--
-- $1 the crate's newest shared (pges) schema version, $2 its newest store schema version, $3 the store to
-- check, or NULL for every store. Returns the shared scope first, then stores by name; no row when every
-- checked scope supports the crate. A store option's scope (`<store>.outbox`) has versions of its own and is
-- not compared.
SELECT scope, min_compatible FROM pges.scopes
WHERE CASE WHEN scope = 'pges' THEN min_compatible > $1
           ELSE coalesce(scope = $3, scope IN (SELECT schema_name FROM pges.stores)) AND min_compatible > $2 END
ORDER BY scope <> 'pges', scope
LIMIT 1
