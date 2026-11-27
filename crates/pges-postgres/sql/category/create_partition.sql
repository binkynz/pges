-- Creates a category's partition of commits, detached, partitioned by commit id into time leaves.
--
-- It is created detached and then attached (category/attach_partition.sql): CREATE TABLE ... PARTITION OF
-- takes ACCESS EXCLUSIVE on commits, and queued behind a long read it would block every append to the store.
-- Its primary key is created with it, under the name the crate chose (commits_<category>__pkey): the attach
-- adopts it, instead of creating one under a name PostgreSQL derives, commits_<category>_pkey, which is
-- category <category>_pkey's partition.
CREATE TABLE {store}.commits_{category} (LIKE {store}.commits INCLUDING DEFAULTS INCLUDING GENERATED INCLUDING CONSTRAINTS, CONSTRAINT {index} PRIMARY KEY (category_id, commit_id)) PARTITION BY RANGE (commit_id)
