-- Vacuums and analyzes the whole database after a bulk load, so reads see an all-visible map and current
-- statistics, as they would some time after the writes.
VACUUM (ANALYZE)
