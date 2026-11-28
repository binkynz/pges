-- Deletes one batch of a category's log rows from one log partition: rows at or below the category's
-- retention_scan_from whose commit no longer exists, and at or below the store's coarse high-water mark.
-- Returns nothing; the row count says how many went.
--
-- $1 category id, $2 the category's retention_scan_from, $3 the batch size, $4 store name.
--
-- $2 is retention_scan_from, not horizon_position: rows between the two can belong to commits of the next
-- leaf, which still exists, and deleting them would make that leaf look unsequenced forever. The NOT EXISTS
-- guard keeps a row whose commit still exists even if a writer broke the store's limits: without its log row,
-- the sequencer's resequence would take that commit for an unsequenced one and sequence it again.
--
-- The sequencer's next term continues from the newest log row, but at least from pges.stores.last_position.
-- That mark is written with the resume token, about once a second, not with the rows: a leader that stops
-- before it writes the mark leaves rows above it, possibly of old commits retention dropped since. Deleting
-- them would let the next term hand their positions out again, so rows above the mark stay.
WITH batch AS (
  SELECT l.last_position FROM {store}.{log_partition} l
  WHERE l.category_id = $1 AND l.last_position <= $2
    AND l.last_position <= (SELECT s.last_position FROM pges.stores s WHERE s.schema_name = $4)
    AND NOT EXISTS (SELECT 1 FROM {store}.commits_{category} c WHERE c.commit_id = l.commit_id)
  ORDER BY l.last_position
  LIMIT $3
)
DELETE FROM {store}.{log_partition} l USING batch b WHERE l.last_position = b.last_position
