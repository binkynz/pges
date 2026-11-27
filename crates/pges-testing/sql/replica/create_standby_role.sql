-- Creates the role a failover replica connects as: for streaming WAL (REPLICATION) and, over an ordinary
-- connection, for synchronising replication slots. Taking REPLICATION away later cuts the stream while slot
-- synchronisation goes on.
CREATE ROLE pges_standby LOGIN REPLICATION PASSWORD 'pges_standby'
