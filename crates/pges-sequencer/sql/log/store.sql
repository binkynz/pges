-- One store a source names for the first time during a term: a store created after the term started. A
-- resequence reads its coarse high-water mark too, as the floor of the positions it hands out.
--
-- $1 the store id
--
-- No row: the source named a store that does not exist.
SELECT schema_name, last_position FROM pges.stores WHERE store_id = $1
