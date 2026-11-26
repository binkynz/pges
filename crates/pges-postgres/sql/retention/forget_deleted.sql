-- Forgets the deleted commits of a category whose log rows the retention horizon now covers, in the
-- transaction that raises the horizon before a leaf is dropped.
--
-- $1 category id, $2 the leaf's upper bound as a commit id, $3 the category's retention_scan_from, $4 the
-- new horizon.
--
-- A store-wide read never reads a log row at or below the highest horizon, so it no longer needs to know
-- that such a row's commit was deleted. A deleted commit whose log row is not written yet stays recorded:
-- its row may come after the horizon. The log rows are looked up in the range the leaf's scan covered, so
-- the statement reads what the scan read; ids below the leaf's upper bound include leftovers of earlier
-- leaves whose rows arrived late.
DELETE FROM {store}.deleted_commits d
WHERE d.category_id = $1 AND d.commit_id < $2
  AND EXISTS (SELECT 1 FROM {store}.log l
              WHERE l.category_id = $1 AND l.last_position > $3 AND l.last_position <= $4
                AND l.commit_id = d.commit_id)
