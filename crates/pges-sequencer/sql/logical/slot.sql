-- The state of the source's replication slot. A slot of another database or plugin is refused, an
-- invalidated one halts sequencing, and an active one is held by another walsender.
--
-- $1 the slot's name
--
-- No row: the slot does not exist.
SELECT database::text AS database, plugin::text AS plugin, invalidation_reason, failover,
       active
FROM pg_replication_slots WHERE slot_name = $1
