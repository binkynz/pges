-- The durable resume token, read whenever a leadership term starts or restarts after an error: a batch whose
-- COMMIT lost its answer may have stored a newer token than the leader knows.
--
-- $1 the leader's fence
--
-- No row: the fence moved on and the leader stands down. `source` names the kind of source that stored the
-- token.
SELECT source, source_token FROM pges.sequencer WHERE fence = $1
