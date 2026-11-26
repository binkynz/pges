-- Whether a store exists.
--
-- $1 store name.
SELECT EXISTS (SELECT 1 FROM pges.stores WHERE schema_name = $1)
