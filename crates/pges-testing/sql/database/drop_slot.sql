-- Drops a replication slot. It fails while a process still streams from it.
--
-- $1 the slot's name.
SELECT pg_drop_replication_slot($1)
