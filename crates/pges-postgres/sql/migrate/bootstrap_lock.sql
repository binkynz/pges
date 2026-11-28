-- Tries to take the advisory lock that stands for the migration lock until the transaction ends, before the
-- shared schema, and with it the `pges` row of pges.scopes (migrate/lock.sql), exists. True: taken.
--
-- Every role that can connect may take an advisory lock, so this one only guards the first shared migration;
-- once pges.scopes exists the runner locks its row instead, also right after taking this lock.
SELECT pg_try_advisory_xact_lock(hashtext('pges.migrate'))
