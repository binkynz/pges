-- Whether a log partition still holds a row some category's retention has not passed: a row above its
-- category's retention_scan_from. Such a partition stays.
--
-- Rows at or below retention_scan_from only name commits retention dropped. A category with no rows in the
-- partition does not pin it, but one that keeps its commits and has rows in it does.
SELECT EXISTS (
  SELECT 1 FROM {store}.categories c
  WHERE EXISTS (SELECT 1 FROM {store}.{log_partition} l
                WHERE l.category_id = c.category_id
                  AND l.last_position > c.retention_scan_from))
