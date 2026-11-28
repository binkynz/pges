-- Begins a run_pg or backfill batch's transaction in SERIALIZABLE, for a handler that asks for it, and takes the
-- savepoint transaction/rollback_to_batch.sql looks for.
BEGIN ISOLATION LEVEL SERIALIZABLE;
SAVEPOINT pges_batch
