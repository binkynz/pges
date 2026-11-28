-- Begins a run_pg or backfill batch's transaction in REPEATABLE READ, for a handler that asks for it.
BEGIN ISOLATION LEVEL REPEATABLE READ
