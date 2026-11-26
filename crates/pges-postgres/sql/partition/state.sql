-- Whether a table of the store is attached as a partition. No row: the table does not exist.
--
-- $1 store name, $2 table name.
SELECT c.relispartition FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = $1 AND c.relname = $2
