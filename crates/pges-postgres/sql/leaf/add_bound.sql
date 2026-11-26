-- Adds to a new leaf a CHECK constraint matching its partition bounds, so attaching it skips the validation
-- scan.
--
-- from and to are the leaf's bounds as timestamptz text with an explicit offset (leaf/to_create.sql).
ALTER TABLE {store}.{leaf} ADD CONSTRAINT {bound} CHECK (category_id = {category_id} AND commit_id >= pges.uuidv7_floor('{from}') AND commit_id < pges.uuidv7_floor('{to}'))
