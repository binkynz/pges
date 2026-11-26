-- Takes the sequencer lease if it is free or expired, and bumps the fence. One sequencer leads per database
-- and serves every store in it; the row `pges.sequencer` is its lease.
--
-- $1 how long the lease lasts (`SequencerConfig::lease_ttl`)
-- $2 the leader's name, shown to operators (`SequencerConfig::leader`), or NULL
--
-- No row: another sequencer holds the lease. The bumped fence makes every batch of the previous leader fail
-- its fence check, and the update waits for a batch that already holds the row lock. The stored token and
-- `halted` come back so the new leader resumes from the token or stops at once when sequencing is halted.
UPDATE pges.sequencer SET fence = fence + 1, lease_until = now() + $1, leader = $2
WHERE lease_until IS NULL OR lease_until < now()
RETURNING fence, source, source_token, halted
