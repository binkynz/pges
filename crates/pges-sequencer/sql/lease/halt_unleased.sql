-- Records why sequencing must not resume by itself, found outside a term: by a slot bootstrap that met
-- commits from before the first slot. Every later acquire returns it and stops; only a resequence clears it.
--
-- $1 the reason, as text for operators
--
-- Only while no process holds the lease: a leader meets the same condition when it opens its source, and
-- records it under its fence. A reason already recorded is kept.
UPDATE pges.sequencer SET halted = coalesce(halted, $1)
WHERE lease_until IS NULL OR lease_until < now()
