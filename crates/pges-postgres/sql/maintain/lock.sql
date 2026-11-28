-- Tries to take a store's maintenance lock for the session. True: taken. Every administration operation
-- holds it on its own connection, so only one runs per store at a time.
--
-- $1 store name.
--
-- Tried, not waited for in the server: a waiting operation returns its connection to the pool between tries,
-- so operations waiting for another maintainer cannot drain the pool, and it gives up when its lock wait runs
-- out, naming the holder (maintain/holder.sql). Taken before maintain/session.sql sets lock_timeout.
SELECT pg_try_advisory_lock(hashtext('pges.maintain.' || $1))
