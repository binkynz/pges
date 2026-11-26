-- The commits of one category named by a page of the log, for a category read or a store-wide read.
--
-- $1 the commit ids (uuid[]). A commit that is missing was removed by retention or deleted.
--
-- A custom plan prunes the category's time leaves to those holding the ids, so its cost and its locks do not
-- grow with the leaves.
SELECT commit_id, stream_id, last_version, format, payload
FROM {store}.commits_{category} WHERE commit_id = ANY($1)
