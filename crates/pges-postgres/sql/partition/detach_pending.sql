-- Whether a partition of the store is being detached concurrently. No row: it is not a partition any more.
--
-- $1 store name, $2 partition name.
--
-- A concurrent detach interrupted after its first transaction must be finished with FINALIZE; it cannot be
-- started again.
SELECT i.inhdetachpending FROM pg_inherits i
JOIN pg_class c ON c.oid = i.inhrelid
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = $1 AND c.relname = $2
