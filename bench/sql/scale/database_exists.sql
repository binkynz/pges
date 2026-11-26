-- Whether a database of this name exists on the server.
--
-- $1 the database's name.
SELECT EXISTS (SELECT 1 FROM pg_database WHERE datname = $1::text) AS exists
