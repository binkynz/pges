-- Adds to a new log partition a CHECK constraint matching its position range, so attaching it skips the
-- validation scan.
--
-- from and to are the partition's first position and the next partition's first position.
ALTER TABLE {store}.{log_partition} ADD CONSTRAINT {bound} CHECK (last_position >= {from} AND last_position < {to})
