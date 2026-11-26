-- Takes a store's maintenance lock for the session. Every administration operation holds it on its own
-- connection, so only one runs per store at a time.
--
-- $1 store name.
--
-- Taken before lock_timeout is set (maintain/session.sql): the timeout also applies to advisory locks, and a
-- second maintainer must wait for the first, not fail after a second.
SELECT pg_advisory_lock(hashtext('pges.maintain.' || $1))
