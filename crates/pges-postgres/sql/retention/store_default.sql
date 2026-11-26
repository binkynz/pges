-- Reads a store's default retention: the code and retain_for of categories registered without their own.
-- No row: the store does not exist.
--
-- $1 store name.
SELECT retention, retain_for FROM pges.stores WHERE schema_name = $1
