-- Finds the leaves of a category that an interrupted retention run left detached: tables named like the
-- category's leaves that are no partition any more and are older than its oldest attached leaf.
--
-- $1 store name, $2 the leaf name pattern (^commits_<category>__[0-9]{6}([0-9]{2})?$ with the category's name
-- in it), $3 the qualified category partition (<store>.commits_<category>).
--
-- Leaves an interrupted creation left detached are newer than every attached leaf, so they are not found;
-- maintenance drops and creates them again.
SELECT c.relname::text FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = $1 AND c.relkind = 'r' AND NOT c.relispartition AND c.relname ~ $2
  AND to_timestamp(substring(c.relname FROM '([0-9]+)$'),
                   CASE WHEN length(substring(c.relname FROM '([0-9]+)$')) = 6
                        THEN 'YYYYMM' ELSE 'YYYYMMDD' END)
      < (SELECT min(uuid_extract_timestamp(
                substring(pg_get_expr(p.relpartbound, p.oid) FROM 'FROM \(''([0-9a-f-]+)''\)')::uuid))
         FROM pg_inherits i JOIN pg_class p ON p.oid = i.inhrelid
         WHERE i.inhparent = to_regclass($3))
ORDER BY 1
