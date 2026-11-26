-- Drops a leaf or log partition: one never attached (an interrupted creation, so no row was ever routed into
-- it), one retention detached, or one an interrupted retention run left detached.
DROP TABLE {store}.{table}
