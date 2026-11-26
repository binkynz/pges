-- Moves one lane's cursor in the handler's transaction, if it is still where the run left it.
--
-- $1 the name, $2 the lane, $3 the new cursor, $4 the cursor the run read. Updates no row when another run
-- moved the lane meanwhile, so the handler's transaction is rolled back instead of applying events twice.
UPDATE {store}.backfills SET position = $3
WHERE name = $1 AND lane = $2 AND position = $4
