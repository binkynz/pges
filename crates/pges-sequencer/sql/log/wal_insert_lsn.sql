-- The WAL position announced with the high-water marks, read after a batch's log rows committed and when a
-- term starts. A replica that has replayed up to it holds those rows.
--
-- The insert position, not `pg_current_wal_lsn()` (the write position): with `synchronous_commit = off` the
-- write position can be behind the batch's own commit record.
SELECT (pg_current_wal_insert_lsn() - '0/0'::pg_lsn)::int8
