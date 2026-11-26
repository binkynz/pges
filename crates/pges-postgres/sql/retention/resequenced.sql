-- Reads the positions resequence gave a category's commits, which are in commit id order rather than commit
-- order: both 0 when it never did.
--
-- $1 category id.
SELECT resequenced_from, resequenced_through FROM {store}.categories WHERE category_id = $1
