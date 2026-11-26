-- Writes the coarse high-water marks the sequencer would have written after the fill: each category's and
-- the store's newest position, from the log.
--
-- $1 the store's name.
WITH categories AS (
  UPDATE {store}.categories c
  SET last_position = coalesce((SELECT max(l.last_position) FROM {store}.log l
                                WHERE l.category_id = c.category_id), 0)
)
UPDATE pges.stores SET last_position = coalesce((SELECT max(last_position) FROM {store}.log), 0)
WHERE schema_name = $1::text
