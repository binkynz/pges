-- Gives the planner back its choice of joins after retention/raise_floors.sql (retention/probe_joins.sql), for
-- the rest of the transaction.
--
-- Two statements, sent with the simple query protocol.
SET LOCAL enable_hashjoin = DEFAULT; SET LOCAL enable_mergejoin = DEFAULT
