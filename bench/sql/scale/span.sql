-- When a category's generated commits start, and when they end: from the start of the UTC bucket $3 buckets
-- before the current one to a minute ago, in milliseconds since the epoch.
--
-- $1 the bucket's date_trunc unit (day, month), $2 its interval as text, $3 how many buckets back.
SELECT (extract(epoch FROM date_trunc($1::text, now(), 'UTC') - $3::int4 * $2::text::interval) * 1000)::int8 AS t0,
       (extract(epoch FROM now() - interval '1 minute') * 1000)::int8 AS t1
