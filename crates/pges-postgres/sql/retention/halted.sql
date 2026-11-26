-- Reads why the sequencer is halted (a lost replication slot awaiting resequence). No row, or NULL: it is
-- not. Retention drops nothing while it is halted.
SELECT halted FROM pges.sequencer
