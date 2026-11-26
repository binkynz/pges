-- The replication slots of a database, with the process streaming from each (NULL when none).
--
-- $1 the database's name.
SELECT slot_name::text, active_pid FROM pg_replication_slots WHERE database = $1
