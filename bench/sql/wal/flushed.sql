-- Whether the WAL is flushed up to a position: pg_walinspect reads only flushed WAL.
--
-- $1 the position, as text.
SELECT pg_current_wal_flush_lsn() >= $1::pg_lsn
