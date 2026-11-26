-- Sets a category's retention. It applies from the next retention run. No row updated: the category is not
-- registered.
--
-- $1 category name, $2 retention code, $3 retain_for (NULL for forever).
UPDATE {store}.categories SET retention = $2, retain_for = $3 WHERE name = $1
