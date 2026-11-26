-- Vacuums and analyzes a log partition that lost rows to pruning: it is no longer insert-only.
VACUUM (ANALYZE) {store}.{log_partition}
