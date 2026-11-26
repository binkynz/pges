-- Whether the publication exists. A missing one is created again (`logical/create_publication.sql`).
--
-- $1 the publication's name, `pges`
SELECT 1 FROM pg_publication WHERE pubname = $1
