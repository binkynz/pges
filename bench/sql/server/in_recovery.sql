-- Whether the server is a standby, to check that the replica came up as one.
SELECT pg_is_in_recovery()
