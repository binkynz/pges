-- Whether the replica has replayed WAL up to a position. NULL on a server that is not a standby.
--
-- $1 the position, as replica/insert_lsn.sql returns it.
SELECT pg_last_wal_replay_lsn() >= $1::pg_lsn
