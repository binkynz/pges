-- Inserts generated commits $2 to $3 - 1 of a category. Commit g belongs to stream g % $4 at version
-- (g / $4 + 1) * $8, so every stream gets a commit every $4 commits, and every commit copies one payload.
--
-- $1 category id, $2 first g, $3 end g, $4 the category's streams, $5 its commits, $6 the first commit's
-- time and $7 the span, in milliseconds since the epoch, $8 events per commit, $9 the payload's frame format,
-- $10 the payload.
INSERT INTO {store}.commits (last_version, commit_id, stream_id, category_id, event_count, format, payload)
SELECT (g / $4::int8 + 1) * $8::int2, public.scale_commit_id($1::int4, g, $6::int8, $7::int8, $5::int8),
       public.scale_stream_id($1::int4, g % $4::int8), $1::int4, $8::int2, $9::int2, $10::bytea
FROM generate_series($2::int8, $3::int8 - 1) AS g
