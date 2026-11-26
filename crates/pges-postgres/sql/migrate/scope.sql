-- Reads a migration scope's schema version and the oldest version crates must still support. No row: the
-- scope does not exist.
--
-- $1 scope: pges or a store name.
SELECT schema_version, min_compatible FROM pges.scopes WHERE scope = $1
