-- Ends a server process, such as one streaming from a replication slot, so the slot can be dropped.
--
-- $1 the process id.
SELECT pg_terminate_backend($1)
