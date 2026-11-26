-- A month leaf's name suffix and bounds: the month $1 months from the current one, in UTC.
--
-- $1 the offset in months. Returns the suffix (YYYYMM), the lower bound and the upper bound, as timestamptz
-- text; the session's time zone must be UTC (fixture/utc.sql), so the text carries the +00 offset.
SELECT to_char(b, 'YYYYMM'), b::text, (b + interval '1 month')::text
FROM (SELECT date_trunc('month', now() AT TIME ZONE 'UTC') AT TIME ZONE 'UTC'
             + make_interval(months => $1) AS b) x
