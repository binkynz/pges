-- Deletes a stream's commits, except its `$stream-deleted` commit, and records the deleted commit ids.
--
-- $1 stream id, $2 category id, $3 the stream's first commit id, $4 the `$stream-deleted` commit id.
--
-- $4 stays so subscribers still read it. The deleted ids are recorded in deleted_commits because their log
-- rows stay, or arrive later for commits not yet sequenced: the outbox drain and the logical source never
-- consult commits, and store-wide reads skip the recorded ids. deleted_commits is keyed like commits, by
-- category and commit id. The publication holds only commits and publishes inserts, so neither the deletes
-- nor these inserts reach the logical source.
WITH deleted AS (
  DELETE FROM {store}.commits
  WHERE category_id = $2 AND stream_id = $1 AND commit_id >= $3 AND commit_id <> $4
  RETURNING commit_id, category_id)
INSERT INTO {store}.deleted_commits (commit_id, category_id)
SELECT commit_id, category_id FROM deleted
