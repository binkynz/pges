-- The WAL insert position, as text, and the server's WAL record and full-page image counters: the start of
-- a measurement. The counters are server-wide.
SELECT pg_current_wal_insert_lsn()::text, wal_records, wal_fpi FROM pg_stat_wal
