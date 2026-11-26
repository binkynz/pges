-- Whether sequencing is halted. Read after `lease/acquire_halted.sql` returned no row, to tell a database
-- that is not halted from a lease another process holds.
SELECT halted IS NOT NULL FROM pges.sequencer
