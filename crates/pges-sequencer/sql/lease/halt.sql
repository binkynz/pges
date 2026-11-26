-- Records why sequencing must not resume by itself, such as a lost replication slot. Every later acquire
-- returns it and stops; only a resequence clears it.
--
-- $1 the holder's fence; a deposed leader records nothing
-- $2 the reason, as text for operators
UPDATE pges.sequencer SET halted = $2 WHERE fence = $1
