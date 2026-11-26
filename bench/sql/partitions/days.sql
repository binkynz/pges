-- The daily leaves of a category from $1 days before today to tomorrow, oldest first: each leaf's name
-- suffix (YYYYMMDD) and its bounds as timestamptz text. The time zone must be UTC (partitions/utc.sql).
--
-- $1 the number of days before today.
SELECT to_char(d.day, 'YYYYMMDD'), d.day::text, (d.day + interval '1 day')::text
FROM generate_series(-$1::int4, 1) AS i,
     LATERAL (SELECT date_trunc('day', now()) + make_interval(days => i) AS day) d
ORDER BY i
