-- Finds out why an append returned no row: the stream's state, and whether this commit is already stored.
--
-- $1 stream id, $2 category id, $3 commit id. Always returns one row; the stream's columns are NULL when the
-- stream does not exist. `done` is the stored commit's version when the append already succeeded (a retry
-- after a lost response).
--
-- `done` filters on the stream too: without it, a commit id reused on another stream of the same category
-- would be reported as a deduplicated retry of this append. It runs in the append's transaction when there is
-- one.
SELECT s.version, s.state, s.category_id, s.last_commit_id,
       (SELECT c.last_version FROM {store}.commits c
        WHERE c.category_id = $2 AND c.commit_id = $3 AND c.stream_id = $1) AS done
FROM (SELECT) AS one LEFT JOIN {store}.streams s ON s.stream_id = $1
