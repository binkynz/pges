-- Lists the time leaves of a store that no append can still write to: those whose upper bound is more than
-- max_txn_age plus twice the clock skew in the past, as for retention (retention/leaves_past.sql). A leaf
-- that retention is detaching is left to it.
--
-- $1 the qualified commits table (<store>.commits), $2 the store's max_txn_age, $3 the store's
-- max_clock_skew.
SELECT c.relname::text AS leaf
FROM pg_inherits p
JOIN pg_inherits i ON i.inhparent = p.inhrelid
JOIN pg_class c ON c.oid = i.inhrelid
WHERE p.inhparent = to_regclass($1) AND NOT i.inhdetachpending
  AND now() > uuid_extract_timestamp(
                substring(pg_get_expr(c.relpartbound, c.oid) FROM 'TO \(''([0-9a-f-]+)''\)')::uuid)
              + $2::interval + 2 * $3::interval
ORDER BY 1
