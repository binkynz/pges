-- The log position of some commits, to measure when each was sequenced. Commits not yet sequenced are left
-- out.
--
-- $1 the commit ids (uuid[]).
SELECT commit_id, last_position FROM {store}.log WHERE commit_id = ANY($1)
