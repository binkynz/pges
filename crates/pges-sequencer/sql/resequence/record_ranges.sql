-- Records the positions a resequence gave each category of a store, so retention scans the recovered rows
-- whole: they are in commit id order, not commit order.
--
-- $1 category_id of each category that got rows
-- $2 the first position it got
-- $3 the last position it got
--
-- A range retention has already passed (`resequenced_through <= retention_scan_from`) is replaced, not
-- widened.
UPDATE {store}.categories c
SET resequenced_from = CASE WHEN c.resequenced_through <= c.retention_scan_from THEN r.first
                            ELSE least(c.resequenced_from, r.first) END,
    resequenced_through = greatest(c.resequenced_through, r.last)
FROM unnest($1::int4[], $2::int8[], $3::int8[]) AS r(category_id, first, last)
WHERE c.category_id = r.category_id
