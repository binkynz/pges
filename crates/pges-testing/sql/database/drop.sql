-- Drops a test database, ending the connections still open to it. A replication slot in the database makes
-- it fail, so a test that leaves one behind notices.
DROP DATABASE "{database}" WITH (FORCE)
