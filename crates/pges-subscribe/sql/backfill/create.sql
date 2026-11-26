-- Creates the lanes of a backfill, unless they exist. A stream always falls into the same lane.
--
-- $1 the name, $2 the number of lanes, $3 the category ids (int4[]), $4 the position the backfill stops at.
-- Every lane starts at position 0.
INSERT INTO {store}.backfills (position, until, lane, lanes, name, category_ids)
SELECT 0, $4, lane, $2, $1, $3 FROM generate_series(0, $2 - 1) AS lane
ON CONFLICT (name, lane) DO NOTHING
