-- Moves a shard's cursor, fenced, if it is still where the instance holds it.
--
-- $1 the name, $2 the shard, $3 the new cursor, $4 the fence, $5 the cursor the instance holds. Updates no
-- row when the fence moved on (another instance took the shard over), or when the stored cursor is elsewhere:
-- a COMMIT whose reply was lost moved it, possibly after the instance read it again, and applying the batch
-- would apply it twice. A transaction that blocks on that COMMIT's row lock sees the moved cursor.
UPDATE {store}.subscriptions SET position = $3
WHERE name = $1 AND shard = $2 AND fence = $4 AND position = $5
