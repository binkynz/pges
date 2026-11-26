-- Ends a resequence in its write transaction, after the log rows and `transaction/synchronous_commit.sql`:
-- stores the new slot's consistent point as the token, clears `halted` and releases the lease.
--
-- $1 the resequence's fence
-- $2 the source's kind, `logical`
-- $3 the consistent point, 8 bytes big-endian
--
-- Zero rows: the lease was lost; roll back. The coarse high-water marks are not written: the next term
-- seeds its dedupe window with every log row above them, which includes every recovered commit.
UPDATE pges.sequencer
SET source = $2, source_token = $3, halted = NULL, lease_until = NULL, leader = NULL
WHERE fence = $1
