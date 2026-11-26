-- Ends the walsender still holding the slot (a previous leader's, or this source's previous connection's),
-- before every START_REPLICATION. It would otherwise hold the slot until `wal_sender_timeout`.
--
-- Only a walsender (`pg_stat_replication`). Any other session holds the slot only while it runs a slot
-- function, such as `pg_drop_replication_slot`; ending it mid-call can drop the slot while its client sees an
-- error.
--
-- $1 the slot's name
SELECT pg_terminate_backend(slot.active_pid)
FROM pg_replication_slots AS slot
JOIN pg_stat_replication AS sender ON sender.pid = slot.active_pid
WHERE slot.slot_name = $1
