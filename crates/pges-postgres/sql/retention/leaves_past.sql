-- Lists the leaves of a category that its retention no longer keeps, oldest first: name, whether a
-- concurrent detach of it was interrupted, and its upper bound as a commit id.
--
-- $1 the qualified category partition (<store>.commits_<category>), $2 the retention's keep interval,
-- $3 the store's max_clock_skew, $4 the store's max_txn_age.
--
-- A leaf qualifies once its upper bound is older than keep plus the skew, and older than max_txn_age plus
-- twice the skew, so no append can still write to it: appends check that their commit id is within the skew
-- of the clock, and no writer transaction lives longer than max_txn_age.
WITH leaves AS (
  SELECT c.relname::text AS leaf, i.inhdetachpending AS detaching,
         substring(pg_get_expr(c.relpartbound, c.oid) FROM 'TO \(''([0-9a-f-]+)''\)')::uuid AS upper
  FROM pg_inherits i JOIN pg_class c ON c.oid = i.inhrelid
  WHERE i.inhparent = to_regclass($1)
)
SELECT leaf, detaching, upper FROM leaves
WHERE uuid_extract_timestamp(upper) < now() - $2::interval - $3::interval
  AND now() > uuid_extract_timestamp(upper) + $4::interval + 2 * $3::interval
ORDER BY upper
