-- Sets, for the rest of the transaction, the number of hash partitions the store's first migration creates
-- for streams and snapshots.
--
-- $1 the number, as text.
SELECT set_config('pges.stream_partitions', $1, true)
