-- Prepares a category read as bench_read, with the types of its four parameters. The caller appends the
-- read's SQL on the next line.
PREPARE bench_read (int4, int8, int8, int8) AS
