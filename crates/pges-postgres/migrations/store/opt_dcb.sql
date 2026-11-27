-- store/opt_dcb.sql
-- Optional, for a store using Dynamic Consistency Boundaries. Same search_path rules as 0001. No public path
-- applies it (only a test does), it is not recorded in pges.migrations, and it is not frozen: DCB is not
-- released. Sets pges.stores.dcb; Store::open refuses such a store with OpenError::StoreRequiresFeature("dcb"),
-- because DCB appends (the tag_heads bump, commit_tags) are not implemented and a writer that skipped them
-- would break the store's consistency boundaries.
--
-- Tags are hashed to int8 (xxh3 of the tag string). A collision only causes a spurious conflict on append
-- or an extra row on read, which the client filters out by decoding, so correctness does not depend on it.

CREATE TABLE tag_heads (
  version  int8 NOT NULL,                                -- not indexed, so updates are HOT
  tag_hash int8 NOT NULL PRIMARY KEY
) WITH (fillfactor = 70);

-- One row per (tag, commit). tag_version orders commits within a tag without waiting for the sequencer.
CREATE TABLE commit_tags (
  tag_hash    int8 NOT NULL,
  tag_version int8 NOT NULL,
  commit_id   uuid NOT NULL,
  category_id int4 NOT NULL,
  PRIMARY KEY (tag_hash, tag_version, commit_id)
) PARTITION BY RANGE (commit_id);

UPDATE pges.stores SET dcb = true WHERE schema_name = current_schema();
