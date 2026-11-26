-- The fence check that opens a resequence's write transaction.
--
-- $1 the resequence's fence
--
-- No row: another process took the lease over. FOR UPDATE, unlike a batch's FOR SHARE: this transaction
-- updates the lease row at its end (`resequence/finish.sql`), so it takes the row lock that update needs
-- from the start.
SELECT 1 FROM pges.sequencer WHERE fence = $1 FOR UPDATE
