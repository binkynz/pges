-- Ends the walsender still holding the slot (a previous leader's, or this source's previous connection's),
-- before every START_REPLICATION. It would otherwise hold the slot until `wal_sender_timeout`.
--
-- $1 the slot's name
SELECT pg_terminate_backend(active_pid) FROM pg_replication_slots
WHERE slot_name = $1 AND active_pid IS NOT NULL
