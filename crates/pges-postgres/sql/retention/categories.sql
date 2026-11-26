-- Reads every category's retention: code, retain_for, and where the next scan of its log rows starts.
SELECT category_id, name, retention, retain_for, retention_scan_from
FROM {store}.categories ORDER BY category_id
