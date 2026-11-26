-- The primary's WAL insert position, as text: the replica has caught up once it has replayed this far.
SELECT pg_current_wal_insert_lsn()::text
