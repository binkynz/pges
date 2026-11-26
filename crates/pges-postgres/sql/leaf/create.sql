-- Creates a time leaf of a category, detached, like commits.
--
-- Freezing early and vacuuming after 5% inserted rows suit a table that is only inserted into and is closed
-- once its time span has passed, so anti-wraparound vacuum does not rewrite it later.
CREATE TABLE {store}.{leaf} (LIKE {store}.commits INCLUDING DEFAULTS INCLUDING GENERATED INCLUDING CONSTRAINTS) WITH (autovacuum_freeze_min_age = 0, autovacuum_vacuum_insert_scale_factor = 0.05)
