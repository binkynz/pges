-- Reads the migrations applied to a scope, with the SHA-256 of their text.
--
-- $1 scope: pges or a store name.
--
-- A migration whose checksum differs from the embedded text was edited after it was applied.
SELECT version, checksum FROM pges.migrations WHERE scope = $1
