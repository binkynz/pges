-- Gathers planner statistics for the whole database, as autovacuum would. Without them the plans cached
-- while tables were empty would be used for the measurement.
ANALYZE
