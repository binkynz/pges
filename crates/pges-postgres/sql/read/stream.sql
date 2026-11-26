-- Reads one page of a stream's commits, from its category's partition of commits, in version order.
--
-- $1 stream id, $2 the first version wanted, $3 a commit id at or below the stream's first wanted commit,
-- $4 the stream's last commit id when the read began, $5 the page size.
--
-- The statement names the category partition (commits_<category>), so the plan holds only that category's
-- leaves. $3 is the snapshot's commit id when loading from a snapshot, otherwise the stream's
-- first_commit_id; $4 is the stream's last_commit_id. Commit ids increase within a stream, so all the
-- commits wanted lie between them. As bind parameters they prune the leaves older than $3 and newer than $4,
-- so a stream probes only the leaves of its own lifetime, not every leaf up to the newest. $4 also ends the
-- read at the stream as its head was read: commits appended after that are not returned. The first commit
-- returned can start below $2: the client skips those events. The next page starts at the last version
-- returned plus one.
SELECT last_version, event_count, commit_id, format, payload
FROM {store}.commits_{category}
WHERE stream_id = $1 AND last_version >= $2 AND commit_id >= $3 AND commit_id <= $4
ORDER BY last_version
LIMIT $5
