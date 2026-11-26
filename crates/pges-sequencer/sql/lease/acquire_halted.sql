-- Takes the lease for a resequence: only while sequencing is halted and no process holds the lease.
--
-- $1 how long the lease lasts (`ResequenceConfig::lease_ttl`)
--
-- No row: either sequencing is not halted or the lease is held; `lease/halted.sql` tells which. A refusal
-- changes nothing.
UPDATE pges.sequencer SET fence = fence + 1, lease_until = now() + $1, leader = NULL
WHERE halted IS NOT NULL AND (lease_until IS NULL OR lease_until < now())
RETURNING fence
