-- The positions and ids of a store's commits sequenced after its coarse high-water mark, oldest first, to seed
-- the dedupe window when a term starts. A source replays at most from the durable token, and every commit
-- after it that is already in the log lies above the mark that was written with it: a term writes the mark
-- below the first of these rows until its source has replayed them.
--
-- $1 the store's `pges.stores.last_position`
SELECT last_position, commit_id FROM {store}.log WHERE last_position > $1 ORDER BY last_position
