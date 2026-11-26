-- Drops a database if it exists, ending the connections still open to it. Its replication slots must be
-- dropped first.
DROP DATABASE IF EXISTS "{database}" WITH (FORCE)
