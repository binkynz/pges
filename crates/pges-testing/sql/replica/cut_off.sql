-- Takes REPLICATION from the failover replica's role, so its WAL receiver cannot reconnect. Its slot
-- synchronisation uses an ordinary connection and goes on. Run on the primary, then end the walsender
-- (replica/end_stream.sql).
ALTER ROLE pges_standby NOREPLICATION
