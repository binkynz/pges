-- Records one category of a fill.
--
-- $1 store, $2 category, $3 category id, $4 streams, $5 commits, $6 events per commit, $7 the first commit's
-- time and $8 the span the commits are spread over, both in milliseconds since the epoch.
INSERT INTO public.scale_dataset (store, category, category_id, streams, commits, events, t0, span)
VALUES ($1::text, $2::text, $3::int4, $4::int8, $5::int8, $6::int2, $7::int8, $8::int8)
