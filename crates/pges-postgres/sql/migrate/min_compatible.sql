-- Reads the oldest schema version of a scope that crates must still support. No row: the scope does not
-- exist.
--
-- $1 scope: pges or a store name.
SELECT min_compatible FROM pges.scopes WHERE scope = $1
