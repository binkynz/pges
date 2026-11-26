-- Attaches a new log partition to the store's log, taking SHARE UPDATE EXCLUSIVE on log.
ALTER TABLE {store}.log ATTACH PARTITION {store}.{log_partition} FOR VALUES FROM ({from}) TO ({to})
