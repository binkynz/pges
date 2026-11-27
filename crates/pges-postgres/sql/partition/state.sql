-- Reads what a name in the store's schema is: whether it is a table, whether it is a partition, and the name
-- of its parent when that is in the store's schema too. No row: nothing in the store has the name.
--
-- $1 store name, $2 table name.
--
-- The crate's names cannot collide with each other, but one may be taken by a relation the crate did not
-- create. The parent tells the crate's own partition from it, so maintenance never skips, drops or attaches
-- under a relation that is not the one it means.
SELECT c.relkind IN ('r', 'p') AS is_table, c.relispartition, p.relname::text AS parent
FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
LEFT JOIN pg_inherits i ON i.inhrelid = c.oid
LEFT JOIN pg_class p ON p.oid = i.inhparent AND p.relnamespace = c.relnamespace
WHERE n.nspname = $1 AND c.relname = $2
