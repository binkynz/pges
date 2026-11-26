-- Every store, to route the inserts the publication sends by schema. Outbox stores are skipped: a database is
-- sequenced by one kind of source. Read on every open and when an insert names a schema not seen yet, so a
-- store created while streaming is sequenced.
SELECT store_id, schema_name, outbox FROM pges.stores ORDER BY store_id
