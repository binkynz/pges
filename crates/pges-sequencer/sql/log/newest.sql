-- A store's newest position, from its log: positions continue from it, so they stay gap-free and unique
-- across takeovers and restarts. No row: the log is empty and positions start at 1.
--
-- Read from the log, not from the coarse mark in `pges.stores`, which lags behind.
SELECT last_position FROM {store}.log ORDER BY last_position DESC LIMIT 1
