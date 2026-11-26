-- The stored resume token, read without a lease by the slot bootstrap when the slot is missing: a non-empty
-- token of the logical source means the slot was lost after commits were streamed from it.
SELECT source, source_token FROM pges.sequencer
