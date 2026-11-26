-- Reads a store's writer limits: max_clock_skew and max_txn_age. No row: the store does not exist.
--
-- $1 store name.
--
-- Appends, retention and resequence all use these recorded values, never configuration, so they cannot
-- disagree.
SELECT max_clock_skew, max_txn_age FROM pges.stores WHERE schema_name = $1
