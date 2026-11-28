-- Checks, on a failure path only, that a run_pg or backfill batch's transaction is still the one its BEGIN
-- began (transaction/begin*.sql): the savepoint pges_batch exists only there. It fails when the handler ended
-- that transaction with a raw COMMIT or ROLLBACK (no transaction block, or another one without the
-- savepoint). Unlike a query, it also runs in an aborted transaction. The batch is rolled back next, so
-- undoing its work back to the savepoint loses nothing.
ROLLBACK TO SAVEPOINT pges_batch
