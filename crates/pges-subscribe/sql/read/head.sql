-- H, the store's newest position: the last_position of the newest log row, 0 for an empty log.
--
-- The subquery reads one row from the log's last_position order, which the log's primary key serves backwards.
SELECT coalesce((SELECT last_position FROM {store}.log
                  ORDER BY last_position DESC LIMIT 1), 0)
