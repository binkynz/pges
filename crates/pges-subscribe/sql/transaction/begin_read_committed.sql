-- Begins a run_pg or backfill batch's transaction in READ COMMITTED, for a handler that asks for it, and takes
-- the savepoint transaction/rollback_to_batch.sql looks for.
BEGIN ISOLATION LEVEL READ COMMITTED;
SAVEPOINT pges_batch
