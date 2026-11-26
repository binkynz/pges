-- Records a new store with its final outbox flag and writer limits.
--
-- $1 store name, $2 whether appends enqueue into pending (the outbox), $3 max_clock_skew, $4 max_txn_age.
--
-- The trigger pges.check_one_source_kind raises SQLSTATE PGE02 when the outbox flag differs from the
-- database's other stores': a database is sequenced by one kind of source.
INSERT INTO pges.stores (schema_name, outbox, max_clock_skew, max_txn_age)
VALUES ($1, $2, $3, $4)
