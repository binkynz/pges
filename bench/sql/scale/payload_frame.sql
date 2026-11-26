-- The payload frame of one real commit, which the fill copies into every generated commit of its category.
--
-- $1 the commit id.
SELECT format, payload FROM {store}.commits WHERE commit_id = $1
