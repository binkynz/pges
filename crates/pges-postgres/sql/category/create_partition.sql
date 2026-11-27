-- Creates a category's partition of commits, detached, partitioned by commit id into time leaves.
--
-- It is created detached and then attached (category/attach_partition.sql): CREATE TABLE ... PARTITION OF
-- takes ACCESS EXCLUSIVE on commits, and queued behind a long read it would block every append to the store.
-- Its primary key, on commit_id alone, is the category's: commits has none, and category_id is constant here.
-- Each leaf attached to the partition gets a copy, <leaf>_pkey. The key is named by the crate
-- (commits_<category>__pkey), not derived by PostgreSQL as commits_<category>_pkey, which is category
-- <category>_pkey's partition.
CREATE TABLE {store}.commits_{category} (LIKE {store}.commits INCLUDING DEFAULTS INCLUDING GENERATED INCLUDING CONSTRAINTS, CONSTRAINT {index} PRIMARY KEY (commit_id)) PARTITION BY RANGE (commit_id)
