-- The ids of a store's commits sequenced after its coarse high-water mark, oldest first, to seed the dedupe
-- window when a term starts. A source replays at most from the durable token, and every commit after it lies
-- above the mark that was written with it.
--
-- $1 the store's `pges.stores.last_position`
SELECT commit_id FROM {store}.log WHERE last_position > $1 ORDER BY last_position
