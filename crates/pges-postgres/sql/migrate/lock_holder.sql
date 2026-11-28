-- The session holding the migration lock, the `pges` row of pges.scopes, looked up when the runner's lock
-- wait runs out. No row: no session holds it any more, or PostgreSQL does not show which.
--
-- A row lock is not in pg_locks; the row's xmax is the locking transaction, whose lock on its own id is.
-- pg_locks, and pid and usename in pg_stat_activity, are readable by every role.
SELECT l.pid, a.usename::text AS role
FROM pges.scopes AS s
JOIN pg_catalog.pg_locks AS l
  ON l.locktype = 'transactionid' AND l.transactionid = s.xmax AND l.mode = 'ExclusiveLock' AND l.granted
LEFT JOIN pg_catalog.pg_stat_activity AS a ON a.pid = l.pid
WHERE s.scope = 'pges'
LIMIT 1
