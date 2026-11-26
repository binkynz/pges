-- Creates a log partition, detached, like log. Created detached and then attached, like leaves
-- (leaf/create.sql), with the same storage parameters.
CREATE TABLE {store}.{log_partition} (LIKE {store}.log INCLUDING DEFAULTS INCLUDING GENERATED INCLUDING CONSTRAINTS) WITH (autovacuum_freeze_min_age = 0, autovacuum_vacuum_insert_scale_factor = 0.05)
