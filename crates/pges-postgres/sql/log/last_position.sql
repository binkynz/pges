-- Reads the store's last position, 0 for an empty log. Positions are gap-free, so a log partition is full
-- once the log reaches its last position.
SELECT coalesce(max(last_position), 0) FROM {store}.log
