-- Raises a category's retention horizon and start position before a leaf is dropped, and returns both. A
-- subscription whose cursor is behind the horizon then fails instead of skipping events; a read of what
-- retention kept starts after the start position.
--
-- $1 category id, $2 the leaf's horizon (retention/scan.sql; NULL leaves both), $3 the oldest position of a
-- commit that stays (retention/scan.sql's first_kept; NULL: none below the horizon).
--
-- The start is the position before the oldest commit that stays, or the horizon if that is lower: every
-- commit at or below it is dropped. Commits that stay can lie between the two, so the start is at most the
-- horizon. Both only rise.
UPDATE {store}.categories
SET horizon_position = greatest(horizon_position, $2::int8),
    start_position = CASE WHEN $2::int8 IS NULL THEN start_position
                          ELSE greatest(start_position,
                                        least(greatest(horizon_position, $2::int8),
                                              coalesce($3::int8 - 1, greatest(horizon_position, $2::int8))))
                     END
WHERE category_id = $1
RETURNING horizon_position, start_position
