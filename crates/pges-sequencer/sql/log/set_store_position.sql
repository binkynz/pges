-- Writes a store's coarse high-water mark with the stored token, in the same transaction. The next term
-- seeds its dedupe window with every log row above it.
--
-- $1 the store's newest position
-- $2 the store id
UPDATE pges.stores SET last_position = $1 WHERE store_id = $2
