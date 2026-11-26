-- The newest log entry of each category, for the poll waker. Categories without entries are left out.
--
-- $1 the category ids (int4[]). Each category reads one row through the log_by_category index, backwards.
SELECT c.category_id, l.last_position FROM unnest($1::int4[]) AS c(category_id)
CROSS JOIN LATERAL (SELECT last_position FROM {store}.log
                    WHERE category_id = c.category_id
                    ORDER BY last_position DESC LIMIT 1) l
