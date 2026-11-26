-- Whether the server is a standby, and the WAL position it has replayed, as a byte offset (NULL on a
-- primary).
SELECT pg_is_in_recovery(), (pg_last_wal_replay_lsn() - '0/0'::pg_lsn)::int8
