-- Sets a store's default retention, for categories registered from now on. No row updated: the store does
-- not exist.
--
-- $1 store name, $2 retention code, $3 retain_for (NULL for forever).
UPDATE pges.stores SET retention = $2, retain_for = $3 WHERE schema_name = $1
