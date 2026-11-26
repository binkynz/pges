-- The number of time leaves attached to a category's partition.
SELECT count(*) FROM pg_inherits WHERE inhparent = '{store}.commits_{category}'::regclass
