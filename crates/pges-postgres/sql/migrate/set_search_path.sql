-- Sets the search path to the store for the rest of the transaction, so store migrations use unqualified
-- names. pg_catalog, not listed, is searched first; pg_temp is listed last, so a temporary table of the
-- migrating session never stands in for a store table.
SET LOCAL search_path TO {store}, pg_temp
