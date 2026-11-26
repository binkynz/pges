-- Checks whether the unit of work's transaction is still usable: it fails with SQLSTATE 25P02 when a
-- statement of the caller aborted it. sqlx does not expose the transaction status the server reports.
SELECT 1
