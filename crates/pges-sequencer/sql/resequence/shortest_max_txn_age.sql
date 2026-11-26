-- The shortest `max_txn_age` of the stores the logical source sequences, in microseconds. NULL: no such
-- store, and the writer-role check is skipped.
--
-- A resequence refuses to run unless every writer role caps its transactions at this age: the search window
-- holds only if writer transactions are capped. The check comes before the lease, so a refusal has no side
-- effects.
SELECT (extract(epoch FROM min(max_txn_age)) * 1000000)::int8 FROM pges.stores WHERE NOT outbox
