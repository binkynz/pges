-- The session holding a store's maintenance lock, looked up when an administration operation's lock wait
-- runs out. No row: no session holds it any more.
--
-- $1 store name.
--
-- pg_locks shows an advisory lock on one int8 key as its high half (classid) and low half (objid), with
-- objsubid 1. pg_locks, and pid and usename in pg_stat_activity, are readable by every role.
SELECT l.pid, a.usename::text AS role
FROM pg_catalog.pg_locks AS l
LEFT JOIN pg_catalog.pg_stat_activity AS a ON a.pid = l.pid
WHERE l.locktype = 'advisory' AND l.granted AND l.objsubid = 1
  AND l.database = (SELECT d.oid FROM pg_catalog.pg_database AS d WHERE d.datname = current_database())
  AND ((l.classid::int8 << 32) | l.objid::int8) = hashtext('pges.maintain.' || $1)::int8
LIMIT 1
