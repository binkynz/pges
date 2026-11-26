-- Attaches a new leaf to its category's partition.
--
-- ATTACH PARTITION takes SHARE UPDATE EXCLUSIVE on the parent, which appends do not conflict with, and the
-- leaf's matching CHECK constraint (leaf/add_bound.sql) lets it skip the validation scan. It still
-- invalidates cached plans.
ALTER TABLE {store}.commits_{category} ATTACH PARTITION {store}.{leaf} FOR VALUES FROM (pges.uuidv7_floor('{from}')) TO (pges.uuidv7_floor('{to}'))
