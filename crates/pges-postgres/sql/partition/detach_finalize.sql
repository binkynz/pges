-- Finishes a concurrent detach that was interrupted after its first transaction
-- (partition/detach_pending.sql).
--
-- parent is commits_<category> or log.
ALTER TABLE {store}.{parent} DETACH PARTITION {store}.{table} FINALIZE
