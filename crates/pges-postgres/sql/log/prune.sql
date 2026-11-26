-- Deletes one batch of a category's log rows from one log partition: rows at or below the category's
-- retention_scan_from whose commit no longer exists. Returns nothing; the row count says how many went.
--
-- $1 category id, $2 the category's retention_scan_from, $3 the batch size.
--
-- $2 is retention_scan_from, not horizon_position: rows between the two can belong to commits of the next
-- leaf, which still exists, and deleting them would make that leaf look unsequenced forever. The NOT EXISTS
-- guard keeps a row whose commit still exists even if a writer broke the store's limits: without its log row,
-- the sequencer's resequence would take that commit for an unsequenced one and sequence it again.
WITH batch AS (
  SELECT l.last_position FROM {store}.{log_partition} l
  WHERE l.category_id = $1 AND l.last_position <= $2
    AND NOT EXISTS (SELECT 1 FROM {store}.commits_{category} c WHERE c.commit_id = l.commit_id)
  ORDER BY l.last_position
  LIMIT $3
)
DELETE FROM {store}.{log_partition} l USING batch b WHERE l.last_position = b.last_position
