-- Makes this backend report its pending statistics, WAL counters included, before it answers. A backend
-- otherwise reports them at most once a second, or after ten idle seconds.
SELECT pg_stat_force_next_flush()
