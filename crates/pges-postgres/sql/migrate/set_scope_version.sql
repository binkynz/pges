-- Moves a scope's schema version and its compatibility window after a migration.
--
-- $1 scope, $2 schema version, $3 the oldest version crates must still support.
--
-- The first migration of a scope inserts its pges.scopes row itself.
UPDATE pges.scopes SET schema_version = $2, min_compatible = $3 WHERE scope = $1
