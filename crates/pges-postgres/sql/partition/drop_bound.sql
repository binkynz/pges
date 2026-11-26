-- Drops the attach constraint of a leaf or log partition once it is attached: it only served to skip the
-- attach's validation scan.
ALTER TABLE {store}.{table} DROP CONSTRAINT {bound}
