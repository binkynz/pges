-- A category's commits without a log row, in commit id order: the commits a lost slot never delivered.
--
-- $1 category_id
-- $2 floor (`resequence/bounds.sql`), or the nil uuid for an empty log
-- $3 the position after which log rows are searched (`resequence/after.sql`)
--
-- Run in the snapshot the new slot exported, which splits commits exactly: everything committed before its
-- consistent point is visible here, and the new slot streams everything after it.
SELECT c.commit_id, c.event_count FROM {store}.commits c
WHERE c.category_id = $1 AND c.commit_id >= $2
  AND NOT EXISTS (SELECT 1 FROM {store}.log l
                  WHERE l.category_id = $1 AND l.last_position > $3
                    AND l.commit_id = c.commit_id)
ORDER BY c.commit_id
