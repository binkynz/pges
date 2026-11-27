-- Ends the walsender streaming to the failover replica. Run after replica/cut_off.sql, so it does not
-- reconnect.
SELECT pg_terminate_backend(pid) FROM pg_stat_replication WHERE usename = 'pges_standby'
