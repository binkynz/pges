-- Takes the migration lock until the transaction ends, so concurrent runners apply each migration once.
--
-- The trigger that keeps one source kind per database (pges.check_one_source_kind) takes the same lock, so
-- two transactions adding stores of different kinds serialize and the second sees the first's row.
SELECT pg_advisory_xact_lock(hashtext('pges.migrate'))
