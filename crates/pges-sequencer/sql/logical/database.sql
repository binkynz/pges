-- The current database's oid and name. The oid names the replication slot (`pges_<oid>`): slot names are
-- server-wide, so several databases can share a server. It is computed at start, never stored, because a
-- database cloned from a template would inherit a stored one. The name is compared with what the replication
-- connection reaches.
SELECT oid::int8 AS oid, datname::text AS name FROM pg_database
WHERE datname = current_database()
