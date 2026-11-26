-- Adds a store's `commits` to the publication when it is missing from it, with the column list the
-- migrations use: the source needs only these columns to sequence a commit, and the payload stays out of the
-- replication stream.
ALTER PUBLICATION pges ADD TABLE {store}.commits
  (commit_id, stream_id, category_id, last_version, event_count)
