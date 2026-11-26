-- The lanes of a backfill, with the stored definition and each lane's cursor.
--
-- $1 the name.
SELECT lane, lanes, until, position, category_ids FROM {store}.backfills
WHERE name = $1 ORDER BY lane
