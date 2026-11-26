-- The bytes of full-page images in the WAL between two positions, through pg_walinspect.
--
-- $1 the start position, $2 the end position, as text.
SELECT coalesce(sum(fpi_size), 0)::int8 FROM pg_get_wal_stats($1::pg_lsn, $2::pg_lsn)
