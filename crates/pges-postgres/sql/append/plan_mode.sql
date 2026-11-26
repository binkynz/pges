-- Sets plan_cache_mode until the transaction ends, and returns the value it had.
--
-- $1 the new value. append_many inside a unit of work runs it with force_custom_plan before the batch
-- statement, for the reason in append/begin_many.sql, and with the returned value after it, so the caller's
-- own statements in the transaction keep their plan mode.
--
-- The CTE reads the old value before set_config runs.
WITH old AS MATERIALIZED (SELECT current_setting('plan_cache_mode') AS previous)
SELECT old.previous, set_config('plan_cache_mode', $1, true) AS current FROM old
