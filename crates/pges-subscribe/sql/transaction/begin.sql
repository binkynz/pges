-- Begins a run_pg or backfill batch's transaction at the session's default_transaction_isolation, for a
-- handler that asks for Isolation::Default, and takes the savepoint transaction/rollback_to_batch.sql looks
-- for. One simple-protocol round trip, as a plain BEGIN.
BEGIN;
SAVEPOINT pges_batch
