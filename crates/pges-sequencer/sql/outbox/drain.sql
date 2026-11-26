-- Takes the oldest queued commits of one outbox store, in the sequencer's batch transaction after the fence
-- check.
--
-- $1 how many: what is left of the batch budget
--
-- The rows disappear in the same transaction as their log rows are written, so a rolled-back batch leaves
-- them in place. `DELETE ... RETURNING` has no order guarantee, so the result is sorted by `seq`: appends
-- number their row with an uncached sequence at the end, so a commit that saw another one gets a higher
-- number.
WITH d AS (
  DELETE FROM {store}.pending
  WHERE seq IN (SELECT seq FROM {store}.pending ORDER BY seq LIMIT $1) RETURNING *
)
SELECT commit_id, category_id, event_count FROM d ORDER BY seq
