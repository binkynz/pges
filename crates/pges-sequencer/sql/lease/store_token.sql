-- Stores the source's resume token, guarded by the fence, in the batch transaction (or one of its own when
-- the leader is idle). Run after `transaction/synchronous_commit.sql`.
--
-- $1 the leader's fence
-- $2 the source's kind, such as `logical` or `outbox`
-- $3 the token; the outbox stores an empty one
--
-- Zero rows: fenced out; roll back.
UPDATE pges.sequencer SET source = $2, source_token = $3 WHERE fence = $1
