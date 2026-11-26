-- What a fill generated in a store, one row per category.
--
-- $1 store.
SELECT category, category_id, streams, commits, events, t0, span
FROM public.scale_dataset WHERE store = $1::text
ORDER BY category_id
