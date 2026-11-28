-- The category with the highest retention horizon above a position, to tell which category a store-wide
-- cursor fell behind.
--
-- $1 the position through which the reader skips commits retention dropped (read/log.sql).
SELECT category_id, horizon_position FROM {store}.categories
WHERE horizon_position > $1
ORDER BY horizon_position DESC LIMIT 1
