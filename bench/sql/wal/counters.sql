-- The server's WAL record and full-page image counters.
SELECT wal_records, wal_fpi FROM pg_stat_wal
