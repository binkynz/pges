-- Saves a stream's snapshot, last part: after snapshot/insert.sql, replaces the stored snapshot under the rules
-- described there.
ON CONFLICT (stream_id) DO UPDATE SET
  version = EXCLUDED.version, commit_id = EXCLUDED.commit_id,
  category_id = EXCLUDED.category_id, state_version = EXCLUDED.state_version,
  format = EXCLUDED.format, state = EXCLUDED.state
WHERE ({store}.snapshots.state_version = EXCLUDED.state_version
       AND {store}.snapshots.version <= EXCLUDED.version - $8)
   OR ({store}.snapshots.state_version < EXCLUDED.state_version
       AND {store}.snapshots.version <= EXCLUDED.version)
