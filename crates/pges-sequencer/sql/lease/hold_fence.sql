-- The fence check that opens every batch transaction, before anything is written.
--
-- $1 the leader's fence
--
-- No row: the leader lost the lease and rolls back. A row lock, not an update, so a batch writes no new
-- version of the hot lease row. A takeover's fence bump waits for the batch that holds the lock, then fences
-- out every later batch.
SELECT 1 FROM pges.sequencer WHERE fence = $1 FOR SHARE
