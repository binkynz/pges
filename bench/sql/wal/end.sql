-- The WAL insert position, as text, and the WAL bytes written since a start position.
--
-- $1 the start position, as wal/mark.sql returns it.
SELECT pg_current_wal_insert_lsn()::text,
       pg_wal_lsn_diff(pg_current_wal_insert_lsn(), $1::pg_lsn)::int8
