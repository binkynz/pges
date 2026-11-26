-- The schemas whose table of a given name the publication sends.
--
-- $1 the publication's name, `pges`
-- $2 the table's name, `commits`
SELECT schemaname::text FROM pg_publication_tables
WHERE pubname = $1 AND tablename = $2
