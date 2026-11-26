-- Moves the cursors of the lanes that had no events in a page, so a restart does not read the page again.
--
-- $1 the name, $2 the lanes (int2[]), $3 the new cursor. A cursor never moves back.
UPDATE {store}.backfills SET position = $3
WHERE name = $1 AND lane = ANY($2) AND position < $3
