-- Freezes a closed partition (a time leaf no append can write to, or a full log partition) and analyzes it,
-- so anti-wraparound vacuum never has to rewrite it. Pages already all frozen are skipped.
VACUUM (FREEZE, ANALYZE) {store}.{table}
