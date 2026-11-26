-- Lists the hash partitions of streams, so a store can route each append to its stream's partition itself:
-- each plain partition's name, modulus and remainder, and the probes it holds.
--
-- $1 probe stream ids. `probes` are the 1-based positions in $1 of the ids PostgreSQL would route to the
-- partition. The client computes the same with its copy of PostgreSQL's partition hash and routes appends
-- only when every probe is where it expects, every partition has the same modulus, and each remainder below
-- it has one plain partition; otherwise appends go through the parent table. The hash reads native 32-bit
-- words, so a big-endian server routes differently, and the probes catch it.
--
-- A bound that is not a hash bound yields NULL modulus and remainder, and no probes. A partition is left out,
-- so appends go through the parent, when it is in another schema (the client names it in the store's), is
-- being detached, or the current role may not read, insert and update it directly: privileges granted on
-- streams cover only statements through streams.
SELECT c.relname::text AS leaf, b.modulus, b.remainder,
       CASE WHEN b.modulus IS NOT NULL THEN ARRAY(
         SELECT p.ord::int4 FROM unnest($1::uuid[]) WITH ORDINALITY AS p(stream_id, ord)
         WHERE satisfies_hash_partition('{store}.streams'::regclass, b.modulus, b.remainder, p.stream_id)
         ORDER BY p.ord) END AS probes
FROM pg_inherits i
JOIN pg_class c ON c.oid = i.inhrelid
LEFT JOIN LATERAL (
  SELECT m[1]::int4 AS modulus, m[2]::int4 AS remainder
  FROM regexp_match(pg_get_expr(c.relpartbound, c.oid),
                    '^FOR VALUES WITH \(modulus (\d+), remainder (\d+)\)$') AS m
) b ON true
WHERE i.inhparent = '{store}.streams'::regclass AND c.relkind = 'r' AND NOT i.inhdetachpending
  AND c.relnamespace = '{store}'::regnamespace
  AND has_table_privilege(c.oid, 'SELECT') AND has_table_privilege(c.oid, 'INSERT')
  AND has_table_privilege(c.oid, 'UPDATE')
ORDER BY c.relname
