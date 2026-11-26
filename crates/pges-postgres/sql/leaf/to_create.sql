-- Lists the time leaves a category needs: from the later of its newest leaf's upper bound and the bucket
-- holding now() - 1 hour, up to $5 buckets after the current one. Returns each leaf's name suffix, lower
-- bound and upper bound, oldest first; bounds are timestamptz text with an explicit offset.
--
-- $1 the qualified category partition (<store>.commits_<category>), $2 the bucket's date_trunc unit (day,
-- month), $3 its interval, $4 its suffix format (YYYYMMDD, YYYYMM), $5 the number of buckets from the
-- current one.
--
-- The newest upper bound is read from the catalog, so leaves never overlap or leave a gap, and a bucket change
-- applies from there. A first leaf whose lower bound is not a bound of the bucket (after a bucket change)
-- ends at the next bound and takes a YYYYMMDD suffix. Bounds are computed in UTC; the session's time zone is
-- UTC (maintain/session.sql).
WITH newest AS (
  SELECT max(uuid_extract_timestamp(
           substring(pg_get_expr(c.relpartbound, c.oid) FROM 'TO \(''([0-9a-f-]+)''\)')::uuid)) AS upper
  FROM pg_inherits i JOIN pg_class c ON c.oid = i.inhrelid
  WHERE i.inhparent = to_regclass($1)
),
span AS (
  SELECT greatest(n.upper,
                  date_trunc($2, (now() - interval '1 hour') AT TIME ZONE 'UTC') AT TIME ZONE 'UTC') AS first,
         date_trunc($2, now() AT TIME ZONE 'UTC') AT TIME ZONE 'UTC' + $5 * $3::interval AS last
  FROM newest n
),
buckets AS (
  SELECT greatest(b, s.first) AS lower, b + $3::interval AS upper
  FROM span s,
       generate_series(date_trunc($2, s.first AT TIME ZONE 'UTC') AT TIME ZONE 'UTC',
                       s.last - $3::interval, $3::interval) AS b
)
SELECT CASE WHEN date_trunc($2, lower AT TIME ZONE 'UTC') AT TIME ZONE 'UTC' = lower
            THEN to_char(lower, $4) ELSE to_char(lower, 'YYYYMMDD') END,
       lower::text, upper::text
FROM buckets
ORDER BY lower
