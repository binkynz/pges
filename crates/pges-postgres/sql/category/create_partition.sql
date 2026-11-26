-- Creates a category's partition of commits, detached, partitioned by commit id into time leaves.
--
-- It is created detached and then attached (category/attach_partition.sql): CREATE TABLE ... PARTITION OF
-- takes ACCESS EXCLUSIVE on commits, and queued behind a long read it would block every append to the store.
CREATE TABLE {store}.commits_{category} (LIKE {store}.commits INCLUDING DEFAULTS INCLUDING GENERATED INCLUDING CONSTRAINTS) PARTITION BY RANGE (commit_id)
