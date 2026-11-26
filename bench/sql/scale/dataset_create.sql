-- Creates the table that records what each fill generated, per store and category, so the measurements can
-- name existing streams.
CREATE TABLE IF NOT EXISTS public.scale_dataset (commits int8 NOT NULL, streams int8 NOT NULL, t0 int8 NOT NULL, span int8 NOT NULL, category_id int4 NOT NULL, events int2 NOT NULL, store text NOT NULL, category text NOT NULL, PRIMARY KEY (store, category))
