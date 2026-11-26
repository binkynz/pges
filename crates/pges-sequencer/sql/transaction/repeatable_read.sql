-- The first statement of a transaction that imports an exported snapshot: `SET TRANSACTION SNAPSHOT` needs
-- REPEATABLE READ (or SERIALIZABLE), set before any query.
SET TRANSACTION ISOLATION LEVEL REPEATABLE READ
