-- The category with the highest retention horizon, to tell which category a store-wide cursor fell behind.
SELECT category_id, horizon_position FROM {store}.categories
ORDER BY horizon_position DESC LIMIT 1
