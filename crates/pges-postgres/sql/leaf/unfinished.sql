-- Finds a category's attached leaves that still have their attach constraint: their creation was interrupted
-- after the attach.
--
-- $1 the qualified category partition (<store>.commits_<category>), $2 the constraint name (bound).
--
-- Only those leaves get DDL: ALTER TABLE takes ACCESS EXCLUSIVE on the leaf even when there is nothing to
-- drop.
SELECT c.relname::text FROM pg_inherits i
JOIN pg_class c ON c.oid = i.inhrelid
JOIN pg_constraint k ON k.conrelid = c.oid AND k.conname = $2
WHERE i.inhparent = to_regclass($1)
