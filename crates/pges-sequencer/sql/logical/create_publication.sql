-- Creates the publication the logical source streams from, when it is missing, as the migrations define it:
-- inserts only, and published under the partitioned table's own name, so the source sees `<store>.commits`
-- whatever partition a row lands in. Adding tables to it later (`logical/publish_store.sql`) needs its
-- ownership.
CREATE PUBLICATION pges WITH (publish = 'insert', publish_via_partition_root = true)
