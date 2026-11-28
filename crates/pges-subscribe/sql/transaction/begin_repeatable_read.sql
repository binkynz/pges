-- Begins a run_pg or backfill batch's transaction in REPEATABLE READ, for a handler that asks for it, and takes
-- the savepoint transaction/rollback_to_batch.sql looks for.
BEGIN ISOLATION LEVEL REPEATABLE READ;
SAVEPOINT pges_batch
