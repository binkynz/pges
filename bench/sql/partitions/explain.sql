-- Runs bench_read and returns its plan with planning and execution times, as JSON. The caller appends the
-- parameter values as a parenthesised list of literals.
EXPLAIN (ANALYZE, SUMMARY, FORMAT JSON) EXECUTE bench_read
