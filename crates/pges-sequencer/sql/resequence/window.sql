-- A store's search window, from its writer limits, in the exported snapshot.
--
-- $1 the store id
--
-- Every commit after the store's newest sequenced commit has an id at most `max_txn_age + 2 * max_clock_skew`
-- older than that commit's id. With one `max_clock_skew` a commit could be missed.
SELECT max_txn_age + 2 * max_clock_skew FROM pges.stores WHERE store_id = $1
