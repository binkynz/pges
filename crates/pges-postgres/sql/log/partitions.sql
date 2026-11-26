-- Reads the store's log partitions, oldest first: name, number, and whether a concurrent detach of it was
-- interrupted.
--
-- $1 the qualified log table (<store>.log).
SELECT c.relname::text, substring(c.relname FROM '^log_([0-9]+)$')::int8 AS n, i.inhdetachpending
FROM pg_inherits i JOIN pg_class c ON c.oid = i.inhrelid
WHERE i.inhparent = to_regclass($1) AND c.relname ~ '^log_[0-9]+$'
ORDER BY n
