-- Every store of the database with its coarse high-water mark, read when a leadership term starts or
-- restarts after an error. Each schema name is checked against the store-name rule before it reaches a
-- statement.
SELECT store_id, schema_name, last_position FROM pges.stores ORDER BY store_id
