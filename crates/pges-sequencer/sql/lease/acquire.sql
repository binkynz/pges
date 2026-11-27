-- Takes the sequencer lease if it is free or expired, and bumps the fence, while the database still supports
-- this crate. One sequencer leads per database and serves every store in it; the row `pges.sequencer` is its
-- lease.
--
-- $1 how long the lease lasts (`SequencerConfig::lease_ttl`)
-- $2 the leader's name, shown to operators (`SequencerConfig::leader`), or NULL
-- $3 the crate's newest shared (pges) schema version
-- $4 the crate's newest store schema version
--
-- Returns one row. too_old: a migration raised the shared or some store's min_compatible above the crate's
-- version (the sequencer writes to every store; as lease/renew.sql); the lease is then neither taken nor
-- waited for. The other columns are NULL when the lease was not taken: another sequencer holds it, or
-- too_old. The bumped fence makes every batch of the previous leader fail its fence check, and the update
-- waits for a batch that already holds the row lock. The stored token and `halted` come back so the new
-- leader resumes from the token or stops at once when sequencing is halted.
WITH checked AS (
  SELECT EXISTS (SELECT FROM pges.scopes
                 WHERE (scope = 'pges' AND min_compatible > $3)
                    OR (scope IN (SELECT schema_name FROM pges.stores) AND min_compatible > $4)) AS too_old
), taken AS (
  UPDATE pges.sequencer SET fence = fence + 1, lease_until = now() + $1, leader = $2
  WHERE (lease_until IS NULL OR lease_until < now()) AND NOT (SELECT too_old FROM checked)
  RETURNING fence, source, source_token, halted
)
SELECT checked.too_old, taken.fence, taken.source, taken.source_token, taken.halted
FROM checked LEFT JOIN taken ON true
