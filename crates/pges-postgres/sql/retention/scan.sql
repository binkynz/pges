-- Scans a category's log rows for one leaf. Returns the leaf's horizon (the newest position of a commit
-- before the leaf's upper bound), the oldest position of a commit at or past the bound (one that stays;
-- NULL: none in the scan), whether every commit of the leaf has a log row, and where the next scan can start
-- (NULL: it stays).
--
-- $1 category id, $2 the category's retention_scan_from, $3 the stop position (retention/scan_stop.sql),
-- $4 the leaf's upper bound as a commit id, $5 the store's max_txn_age, $6 the store's max_clock_skew,
-- $7 and $8 the range resequence wrote (retention/resequenced.sql).
--
-- Every commit of the next leaf is positioned after the last row below the next_scan_from bound. Positions
-- follow commit order everywhere except inside the range resequence wrote, which is in commit id order and
-- may hold commits older than rows before it. So the scan covers that range whole (the caller extends $3 to
-- it), and next_scan_from ignores rows inside it: a row there with an old id can sit after rows of commits in
-- later leaves.
--
-- A commit past the bound can be positioned below the horizon: it committed before the leaf's last commit.
-- So the horizon is not where the commits that stay begin; first_kept is. Every commit that stays is
-- positioned after the scan start, as every commit of the next leaf is.
--
-- `sequenced` is a set difference, not an anti-join. A fully sequenced leaf has no row the check could stop
-- at, and an anti-join planned for its first row can become a nested loop over the leaf and the scanned rows
-- (seconds to hours per leaf); EXCEPT is only ever hashed or sorted, so it costs one pass over each. The
-- next_scan_from bound is a subquery so it is computed once, not once per row.
WITH seen AS (
  SELECT l.last_position, l.commit_id FROM {store}.log l
  WHERE l.category_id = $1 AND l.last_position > $2 AND l.last_position <= $3
)
SELECT (SELECT max(last_position) FROM seen WHERE commit_id < $4) AS horizon,
       (SELECT min(last_position) FROM seen WHERE commit_id >= $4) AS first_kept,
       NOT EXISTS (SELECT commit_id FROM {store}.{leaf}
                   EXCEPT SELECT commit_id FROM seen) AS sequenced,
       (SELECT max(last_position) FROM seen
        WHERE commit_id < (SELECT pges.uuidv7_floor(uuid_extract_timestamp($4::uuid)
                                                    - $5::interval - 2 * $6::interval))
          AND last_position NOT BETWEEN $7 AND $8)
         AS next_scan_from
