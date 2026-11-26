-- Saves a stream's snapshot, unless the stored one is recent enough or of a higher state version.
--
-- $1 version, $2 stream id, $3 commit id of that version, $4 category id, $5 state version, $6 format,
-- $7 state, $8 the number of versions a new snapshot must be ahead of the stored one (1: any newer one).
--
-- A snapshot of the same state version replaces the stored one when it is at least $8 versions ahead. One of
-- a higher state version replaces it when it is at the same version or newer, so a change of the state's
-- shape takes effect on the next save. One of a lower state version never does: during a rolling deploy the
-- old code cannot overwrite the new code's snapshot. The stored version never goes down, so a snapshot that
-- retention relied on before raising the stream's floor stays at or above that floor.
INSERT INTO {store}.snapshots
  (version, stream_id, commit_id, category_id, state_version, format, state)
SELECT $1, $2, $3, $4, $5, $6, $7 WHERE $1 >= $8
ON CONFLICT (stream_id) DO UPDATE SET
  version = EXCLUDED.version, commit_id = EXCLUDED.commit_id,
  category_id = EXCLUDED.category_id, state_version = EXCLUDED.state_version,
  format = EXCLUDED.format, state = EXCLUDED.state
WHERE ({store}.snapshots.state_version = EXCLUDED.state_version
       AND {store}.snapshots.version <= EXCLUDED.version - $8)
   OR ({store}.snapshots.state_version < EXCLUDED.state_version
       AND {store}.snapshots.version <= EXCLUDED.version)
