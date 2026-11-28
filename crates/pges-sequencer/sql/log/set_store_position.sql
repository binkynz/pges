-- Writes a store's coarse high-water mark with the stored token, in the same transaction. The next term
-- seeds its dedupe window with every log row above it, so it is the newest position, or just below the
-- oldest log row the source may still replay after the token.
--
-- $1 the store's mark
-- $2 the store id
UPDATE pges.stores SET last_position = $1 WHERE store_id = $2
