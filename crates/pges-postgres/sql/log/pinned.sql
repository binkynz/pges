-- Whether a log partition still holds a row some category's retention has not passed: a row above its
-- category's retention_scan_from. Such a partition stays, and so does one with a row above the store's coarse
-- high-water mark.
--
-- $1 store name.
--
-- Rows at or below retention_scan_from only name commits retention dropped. A category with no rows in the
-- partition does not pin it, but one that keeps its commits and has rows in it does. The sequencer's next term
-- continues from the newest log row, but at least from pges.stores.last_position, which is written with the
-- resume token, not with the rows: dropping rows above it could let that term hand their positions out again.
SELECT EXISTS (
         SELECT 1 FROM {store}.categories c
         WHERE EXISTS (SELECT 1 FROM {store}.{log_partition} l
                       WHERE l.category_id = c.category_id
                         AND l.last_position > c.retention_scan_from))
       OR EXISTS (
         SELECT 1 FROM {store}.{log_partition} l
         WHERE l.last_position > (SELECT s.last_position FROM pges.stores s WHERE s.schema_name = $1))
       AS pinned
