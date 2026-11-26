-- Moves the start of a category's next log scan forward, once a leaf is dropped.
--
-- $1 category id, $2 the new start (retention/scan.sql).
--
-- Only after the drop: a run interrupted before it scans the leaf again from the old start; had the start
-- moved, the leaf's commits would look unsequenced and the leaf would be refused forever, and log pruning
-- (log/prune.sql), which deletes up to it, could delete log rows of commits that still exist.
UPDATE {store}.categories
SET retention_scan_from = greatest(retention_scan_from, $2::int8)
WHERE category_id = $1
