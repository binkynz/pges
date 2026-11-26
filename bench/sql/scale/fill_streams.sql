-- Inserts generated streams $2 to $3 - 1 of a category, with the version and first and last commit ids its
-- generated commits give them (fill_commits.sql): stream s holds commits s, s + $4, s + 2 * $4, and so on
-- below $5.
--
-- $1 category id, $2 first stream, $3 end stream, $4 the category's streams, $5 its commits, $6 the first
-- commit's time and $7 the span, in milliseconds since the epoch, $8 events per commit.
INSERT INTO {store}.streams (version, stream_id, first_commit_id, last_commit_id, category_id)
SELECT (last.k + 1) * $8::int2, public.scale_stream_id($1::int4, s),
       public.scale_commit_id($1::int4, s, $6::int8, $7::int8, $5::int8),
       public.scale_commit_id($1::int4, s + last.k * $4::int8, $6::int8, $7::int8, $5::int8), $1::int4
FROM generate_series($2::int8, $3::int8 - 1) AS s,
     LATERAL (SELECT ($5::int8 - 1 - s) / $4::int8 AS k) AS last
