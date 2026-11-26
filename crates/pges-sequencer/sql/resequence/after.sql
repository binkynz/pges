-- The position after which a category's search looks for log rows: its newest log row with an id below
-- `stop` (`resequence/bounds.sql`). No row: 0, the whole log.
--
-- $1 category_id
-- $2 stop
-- $3 first position an earlier resequence wrote in the category
-- $4 last position it wrote
--
-- Rows an earlier resequence wrote are in commit id order, not commit order, so they do not count: a
-- recovered row with an old id could otherwise stand for the newest row, and a commit after it would be
-- sequenced again.
SELECT last_position FROM {store}.log
WHERE category_id = $1 AND commit_id < $2
  AND last_position NOT BETWEEN $3 AND $4
ORDER BY last_position DESC LIMIT 1
