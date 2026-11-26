-- Reads every category's id, name and bucket, for maintenance.
SELECT category_id, name, bucket FROM {store}.categories ORDER BY category_id
