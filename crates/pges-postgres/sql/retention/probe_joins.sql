-- Plans the transaction's next joins as nested loops, so retention/raise_floors.sql looks each of the leaf's
-- streams up by its key instead of hashing or sorting every streams partition: its cost then follows the
-- leaf, not the store. retention/plan_joins.sql gives the planner its choice back.
--
-- Two statements, sent with the simple query protocol. At most until the transaction ends.
SET LOCAL enable_hashjoin = off; SET LOCAL enable_mergejoin = off
