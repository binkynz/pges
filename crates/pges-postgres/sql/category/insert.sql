-- Registers a category, unless it exists: an existing category keeps its bucket and retention.
--
-- $1 name, $2 bucket interval ('1 day' or '1 month'), $3 retention code, $4 retain_for (NULL for forever).
INSERT INTO {store}.categories (name, bucket, retention, retain_for)
VALUES ($1, $2::interval, $3, $4)
ON CONFLICT (name) DO NOTHING
