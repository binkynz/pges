-- Prepares a store-wide commit fetch as bench_read, with the type of its parameter. The caller appends the
-- fetch's SQL on the next line.
PREPARE bench_read (uuid[]) AS
