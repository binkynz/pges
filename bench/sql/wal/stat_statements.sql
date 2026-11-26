-- Creates pg_stat_statements, which a benchmark server preloads, so its statement statistics can be read.
CREATE EXTENSION IF NOT EXISTS pg_stat_statements
