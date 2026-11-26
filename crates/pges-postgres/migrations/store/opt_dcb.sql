-- store/opt_dcb.sql
-- Optional, applied when a store uses Dynamic Consistency Boundaries. Same search_path rules as 0001.
-- Sets pges.stores.dcb; Store::open reads it and fails with OpenError::StoreRequiresFeature("dcb") when the
-- binary was built without the `dcb` feature, so no writer can skip the tag_heads bump.
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
