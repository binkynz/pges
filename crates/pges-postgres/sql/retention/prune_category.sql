-- Reads a category's id and retention_scan_from, before its log rows are pruned. No row: the category is not
-- registered.
--
-- $1 category name.
SELECT category_id, retention_scan_from FROM {store}.categories WHERE name = $1
