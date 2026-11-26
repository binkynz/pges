-- Writes the log rows of one time window of generated commits, as the sequencer would: positions continue
-- after $8, in commit id order across the categories. The rows are computed from the same ids as the
-- commits (fill_commits.sql), not read from them. Returns the window's last position.
--
-- $1 to $7, one element per category: category id, first g and end g of the window, the first commit's
-- time, the span, the category's commits, events per commit. $8 the position before the window.
WITH generated AS (
  SELECT c.category_id, c.events,
         public.scale_commit_id(c.category_id, g, c.t0, c.span, c.commits) AS commit_id
  FROM unnest($1::int4[], $2::int8[], $3::int8[], $4::int8[], $5::int8[], $6::int8[], $7::int2[])
         AS c(category_id, g_from, g_to, t0, span, commits, events),
       generate_series(c.g_from, c.g_to - 1) AS g
),
written AS (
  INSERT INTO {store}.log (last_position, commit_id, category_id, event_count)
  SELECT $8::int8 + sum(events) OVER (ORDER BY commit_id), commit_id, category_id, events
  FROM generated
  RETURNING last_position
)
SELECT coalesce(max(last_position), $8::int8) AS last_position FROM written
