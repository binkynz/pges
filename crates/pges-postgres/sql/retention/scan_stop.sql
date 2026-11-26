-- Finds where the scan of a category's log rows for one leaf stops: the first row, after the scan start, of a
-- commit that started after every commit of the leaf ended. No row: scan to the end of the log.
--
-- $1 category id, $2 the category's retention_scan_from, $3 the leaf's upper bound as a commit id,
-- $4 the store's max_txn_age, $5 the store's max_clock_skew.
--
-- Every writer transaction is capped by max_txn_age and every commit id is within max_clock_skew of its
-- statement, so a commit whose id is past this bound started after every commit of the leaf committed, and
-- was sequenced after them.
--
-- The bound is a subquery so it is computed once, not for each of the leaf's rows the scan passes.
SELECT last_position FROM {store}.log
WHERE category_id = $1 AND last_position > $2
  AND commit_id >= (SELECT pges.uuidv7_floor(uuid_extract_timestamp($3::uuid) + $4::interval
                                             + 2 * $5::interval))
ORDER BY last_position
LIMIT 1
