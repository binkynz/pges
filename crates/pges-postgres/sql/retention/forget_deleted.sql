-- Forgets the deleted commits of a category whose log rows the retention horizon now covers, in the
-- transaction that raises the horizon before a leaf is dropped.
--
-- $1 category id, $2 the leaf's upper bound as a commit id, $3 the category's retention_scan_from, $4 the
-- new horizon.
--
-- No store-wide read needs to know any more that such a row's commit was deleted. A strict read never reads
-- a log row at or below the highest horizon. A reader that skips dropped commits (Start::Retained, below its
-- skip_through) does read rows at or below the horizon of a category it skips, but takes a missing commit
-- there as dropped and skips it without looking here. A deleted commit whose log row is not written yet
-- stays recorded: its row may come after the horizon. The log rows are looked up in the range the leaf's
-- scan covered, so the statement reads what the scan read; ids below the leaf's upper bound include
-- leftovers of earlier leaves whose rows arrived late.
DELETE FROM {store}.deleted_commits d
WHERE d.category_id = $1 AND d.commit_id < $2
  AND EXISTS (SELECT 1 FROM {store}.log l
              WHERE l.category_id = $1 AND l.last_position > $3 AND l.last_position <= $4
                AND l.commit_id = d.commit_id)
