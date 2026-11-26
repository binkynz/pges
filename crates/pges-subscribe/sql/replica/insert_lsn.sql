-- The primary's WAL insert position, as a byte offset: a replica read waits until the replica has replayed
-- at least this far.
SELECT (pg_current_wal_insert_lsn() - '0/0'::pg_lsn)::int8
