-- The state of the source's replication slot. A slot of another database or plugin is refused, an
-- invalidated one halts sequencing, and an active one is held by another walsender. A slot confirmed past
-- the stored token halts sequencing too: the source confirms only tokens that are stored, so the log rows
-- of the commits in between were lost (a promoted standby that had not received them).
--
-- $1 the slot's name
--
-- `confirmed_flush` is the slot's `confirmed_flush_lsn` as a byte offset.
--
-- No row: the slot does not exist.
SELECT database::text AS database, plugin::text AS plugin, invalidation_reason, failover,
       active, (confirmed_flush_lsn - '0/0'::pg_lsn)::int8 AS confirmed_flush
FROM pg_replication_slots WHERE slot_name = $1
