-- The newest position of each category of a store that has a log row, read when a term starts.
--
-- One index probe per category (LATERAL with LIMIT 1) instead of a scan of the whole log.
SELECT c.category_id, l.last_position FROM {store}.categories c
CROSS JOIN LATERAL (SELECT last_position FROM {store}.log
                    WHERE category_id = c.category_id
                    ORDER BY last_position DESC LIMIT 1) l
