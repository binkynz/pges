-- Detaches a leaf or log partition without blocking writers to the parent.
--
-- parent is commits_<category> or log. Runs in autocommit; it cannot run in a transaction block.
ALTER TABLE {store}.{parent} DETACH PARTITION {store}.{table} CONCURRENTLY
