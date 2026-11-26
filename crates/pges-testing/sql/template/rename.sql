-- Gives a database that was built under a temporary name its final name, so a crash while building never
-- leaves a half-built template under the name tests look for.
ALTER DATABASE "{database}" RENAME TO "{template}"
