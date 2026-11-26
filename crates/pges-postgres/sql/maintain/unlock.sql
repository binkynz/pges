-- Releases a store's maintenance lock. Closing the connection releases it too; this only releases it sooner.
--
-- $1 store name.
SELECT pg_advisory_unlock(hashtext('pges.maintain.' || $1))
