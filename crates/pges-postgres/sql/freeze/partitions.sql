-- Reads, for every partition of a store, how much of it is frozen and how old its oldest unfrozen
-- transaction ids are: name, pages, all-frozen pages, rows deleted or updated that vacuum has not removed
-- yet, and the age of relfrozenxid and of relminmxid. Each figure covers the table and its TOAST table, which
-- vacuum processes with it.
--
-- $1 store name.
--
-- Pages are read from the files, so a table never vacuumed counts in full. All-frozen pages are what the
-- last vacuum or analyze recorded; a page changed since then still counts, which is what the dead rows (from
-- the cumulative statistics, reset by a crash) catch: a delete clears the page's all-frozen bit.
SELECT c.relname::text AS relname,
       (pg_relation_size(c.oid) + coalesce(pg_relation_size(t.oid), 0))
         / current_setting('block_size')::int8 AS pages,
       c.relallfrozen::int8 + coalesce(t.relallfrozen, 0) AS frozen_pages,
       pg_stat_get_dead_tuples(c.oid) + coalesce(pg_stat_get_dead_tuples(t.oid), 0) AS dead_rows,
       greatest(age(c.relfrozenxid), age(t.relfrozenxid)) AS xid_age,
       greatest(mxid_age(c.relminmxid), mxid_age(t.relminmxid)) AS mxid_age
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
LEFT JOIN pg_class t ON t.oid = c.reltoastrelid
WHERE n.nspname = $1 AND c.relkind = 'r' AND c.relispartition
ORDER BY 1
