-- The leaves a category needs before those Admin::maintain creates: from the bucket $4 buckets before the
-- current one up to the bucket before the one holding now() - 1 hour, oldest first. Returns each leaf's name
-- suffix and its bounds as timestamptz text. The time zone must be UTC (partitions/utc.sql).
--
-- $1 the bucket's date_trunc unit (day, month), $2 its interval as text, $3 its suffix format (YYYYMMDD,
-- YYYYMM), $4 how many buckets back.
SELECT to_char(b, $3::text) AS suffix, b::text AS lower, (b + $2::text::interval)::text AS upper
FROM generate_series(date_trunc($1::text, now()) - $4::int4 * $2::text::interval,
                     date_trunc($1::text, now() - interval '1 hour') - $2::text::interval,
                     $2::text::interval) AS b
ORDER BY b
