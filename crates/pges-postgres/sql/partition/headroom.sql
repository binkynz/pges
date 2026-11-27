-- Reads, for every category of the store, its bucket, the upper bound of its newest attached time leaf in
-- microseconds since the Unix epoch, and how far ahead of now() that bound is, in microseconds (negative once
-- it has passed). The bound and the distance are NULL for a category with no attached leaf.
--
-- Appends to a category fail once now() passes the bound: no leaf takes a newer commit id. A category's
-- partition is commits_<category>. A leaf being detached is left out: retention detaches the oldest leaves,
-- never the newest one.
SELECT k.category_id, k.name, k.bucket,
       (extract(epoch FROM l.upper) * 1000000)::int8 AS upper,
       (extract(epoch FROM l.upper - now()) * 1000000)::int8 AS ahead
FROM {store}.categories k
LEFT JOIN LATERAL (
  SELECT max(uuid_extract_timestamp(
           substring(pg_get_expr(c.relpartbound, c.oid) FROM 'TO \(''([0-9a-f-]+)''\)')::uuid)) AS upper
  FROM pg_inherits i JOIN pg_class c ON c.oid = i.inhrelid
  WHERE i.inhparent = to_regclass('{store}.commits_' || k.name) AND NOT i.inhdetachpending
) l ON true
ORDER BY k.category_id
