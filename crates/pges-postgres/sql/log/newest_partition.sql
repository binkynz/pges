-- Reads the number of the store's newest log partition: log_<n> holds positions [n * 10^8, (n + 1) * 10^8).
-- NULL: the store has none.
--
-- $1 the qualified log table (<store>.log).
SELECT max(substring(c.relname FROM '^log_([0-9]+)$')::int8)
FROM pg_inherits i JOIN pg_class c ON c.oid = i.inhrelid
WHERE i.inhparent = to_regclass($1)
