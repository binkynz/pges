-- Serialises building the template database across test processes (cargo nextest runs one process per
-- test). The key is fixed; the lock is held by the session until template/unlock.sql.
SELECT pg_advisory_lock(726_637)
