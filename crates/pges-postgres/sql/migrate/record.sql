-- Records an applied migration and the SHA-256 of its text.
--
-- $1 version, $2 scope, $3 name, $4 checksum.
INSERT INTO pges.migrations (version, scope, name, checksum) VALUES ($1, $2, $3, $4)
