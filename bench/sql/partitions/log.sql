-- Writes a log row for every commit, positions in commit id order, as the logical source would give
-- them, instead of running a sequencer.
INSERT INTO {store}.log (last_position, commit_id, category_id, event_count)
SELECT row_number() OVER (ORDER BY commit_id), commit_id, category_id, event_count
FROM {store}.commits
