-- Ids of $3 generated streams of a category, drawn uniformly at random with replacement.
--
-- $1 category id, $2 the category's streams, $3 how many.
SELECT public.scale_stream_id($1::int4, floor(random() * $2::int8)::int8) AS stream_id
FROM generate_series(1, $3::int4)
