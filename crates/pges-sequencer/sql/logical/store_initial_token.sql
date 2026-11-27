-- Stores a new slot's consistent point as the logical source's token, right after the source created the
-- slot, so a later start can tell a slot confirmed past the stored token. Run without the lease, by `open`
-- and by the slot bootstrap.
--
-- $1 the source's kind, `logical`
-- $2 the consistent point, 8 bytes big-endian
--
-- Only while no token is stored (none, or the outbox's empty one): a stored logical token is never moved
-- back. The slot was just created, so no sequencer has confirmed anything from it yet.
UPDATE pges.sequencer SET source = $1, source_token = $2
WHERE source_token IS NULL OR octet_length(source_token) = 0
