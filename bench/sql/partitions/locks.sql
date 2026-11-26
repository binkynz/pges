-- The relation locks this backend holds, and how many of them are fast-path locks, which do not take the
-- lock manager's LWLocks.
SELECT count(*), count(*) FILTER (WHERE fastpath) FROM pg_locks
WHERE pid = pg_backend_pid() AND locktype = 'relation'
