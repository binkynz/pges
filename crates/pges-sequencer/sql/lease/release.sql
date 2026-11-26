-- Gives the lease up, so a standby takes over at its next acquire attempt instead of after the lease
-- expires. The fence is kept: the next acquire bumps it.
--
-- $1 the holder's fence; a fence that moved on matches nothing
UPDATE pges.sequencer SET lease_until = NULL, leader = NULL WHERE fence = $1
