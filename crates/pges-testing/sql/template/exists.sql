-- Whether a database exists.
--
-- $1 the database's name.
SELECT EXISTS (SELECT 1 FROM pg_database WHERE datname = $1)
