-- Which of some commits delete_stream deleted. A store-wide read finds a log row whose commit is missing;
-- it skips the row when the commit was deleted, and fails otherwise.
--
-- $1 the category ids (int4[]) and $2 the commit ids (uuid[]), pairwise.
SELECT d.category_id, d.commit_id
FROM unnest($1::int4[], $2::uuid[]) AS m(category_id, commit_id)
JOIN {store}.deleted_commits d
  ON d.category_id = m.category_id AND d.commit_id = m.commit_id
