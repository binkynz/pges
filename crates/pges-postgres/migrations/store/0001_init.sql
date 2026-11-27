-- store/0001_init.sql
-- Per-store objects. The runner creates the schema, registers it in pges.stores, then runs this inside
-- one transaction after `SET LOCAL search_path TO <store>` and
-- `SET LOCAL pges.stream_partitions = <StoreConfig::stream_partitions>`, so names below are unqualified.
--
-- Column order on every table: NOT NULL fixed-width columns widest first (8-byte, 16-byte uuid, 4, 2),
-- then nullable fixed-width, then variable-width. This avoids alignment padding and lets Postgres cache
-- attribute offsets for the fixed prefix.
--
-- Hot tables carry no foreign keys: categories and event types are cached by the client and never deleted.

INSERT INTO pges.scopes (schema_version, min_compatible, scope)
VALUES (1, 1, current_schema());

CREATE TABLE categories (
  last_position       int8     NOT NULL DEFAULT 0,       -- category high-water mark; written by the
                                                         -- sequencer at most once a second
  horizon_position    int8     NOT NULL DEFAULT 0,       -- retention floor; cursors below it are rejected
  retention_scan_from int8     NOT NULL DEFAULT 0,       -- where the next retention scan of log starts
  resequenced_from    int8     NOT NULL DEFAULT 0,       -- positions resequence gave this category's
  resequenced_through int8     NOT NULL DEFAULT 0,       -- commits, in commit id order; 0, 0: none
  bucket              interval NOT NULL DEFAULT '1 month', -- time span of one commits partition
  category_id         int4     GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  retention           int2     NOT NULL DEFAULT 0        -- 0 forever, 1 window+drop, 2 window+archive,
                      CHECK (retention BETWEEN 0 AND 3), -- 3 snapshot+truncate
  retain_for          interval,
  name                text     NOT NULL UNIQUE      -- no __: it separates the category in partition names
                      CHECK (name ~ '^[a-z][a-z0-9_]{0,39}$' AND name !~ '__'),
  CHECK ((retention = 0) = (retain_for IS NULL))
) WITH (fillfactor = 50);                                -- periodic high-water updates; keep HOT

-- Written by the crate outside the caller's transaction, the first time a store sees an event type, so
-- the ids the client caches never come from a transaction that may roll back.
CREATE TABLE event_types (
  event_type_id int4 GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  category_id   int4 NOT NULL REFERENCES categories,
  name          text NOT NULL,
  UNIQUE (category_id, name)
);

-- Stream heads. Hash-partitioned so vacuum and index maintenance stay bounded at billions of streams.
-- The modulus is fixed for the life of the store.
--
-- Every append updates its stream's row, and retention raises the floors of a dropped leaf's streams. With
-- the default scale factor of 0.2 a partition collects a fifth of its rows as dead tuples before it is
-- vacuumed: about 20 million at 100 million streams, which bloats the primary key. 0.02 is what snapshots
-- partitions use. Inserted streams trigger a vacuum after 5%, as for time leaves and log partitions, so new
-- rows become all-visible and later anti-wraparound vacuums skip their pages.
CREATE TABLE streams (
  version         int8 NOT NULL,                         -- last version; not indexed, so updates are HOT
  floor_version   int8 NOT NULL DEFAULT 0,               -- versions at or below were truncated by retention
  stream_id       uuid NOT NULL,
  first_commit_id uuid NOT NULL,                         -- lowest commit_id of the stream; pruning bound
  last_commit_id  uuid NOT NULL,                         -- newest commit_id; appends must exceed it
  category_id     int4 NOT NULL,
  state           int2 NOT NULL DEFAULT 0                -- 0 active, 1 archived, 2 deleted
                  CHECK (state BETWEEN 0 AND 2),
  stream_key      text,                                  -- original string key, when the id is uuidv5 of one
  PRIMARY KEY (stream_id)
) PARTITION BY HASH (stream_id);

DO $$
DECLARE
  n int := coalesce(nullif(current_setting('pges.stream_partitions', true), ''), '64')::int;
BEGIN
  FOR i IN 0..n - 1 LOOP
    EXECUTE format(
      'CREATE TABLE %I PARTITION OF streams FOR VALUES WITH (MODULUS %s, REMAINDER %s) '
      'WITH (fillfactor = 70, autovacuum_vacuum_scale_factor = 0.02, '
      'autovacuum_vacuum_insert_scale_factor = 0.05)',
      'streams_p' || lpad(i::text, 3, '0'), n, i);
  END LOOP;
END $$;

-- One row per append call. LIST by category; each category partition is RANGE-partitioned by commit_id
-- (uuidv7, so by time). Partitions are created detached and then attached by the maintenance task, which
-- also adds the per-leaf UNIQUE (stream_id, last_version), payload compression and vacuum settings.
CREATE TABLE commits (
  last_version int8 NOT NULL,
  commit_id    uuid NOT NULL,
  stream_id    uuid NOT NULL,
  category_id  int4 NOT NULL,
  event_count  int2 NOT NULL CHECK (event_count > 0),     -- at most 32767 events per commit
  format       int2 NOT NULL,                             -- payload frame format (pges_core::frame)
  payload      bytea NOT NULL,
  versions     int8range   GENERATED ALWAYS AS
               (int8range(last_version - event_count + 1, last_version + 1)) VIRTUAL,
  recorded_at  timestamptz GENERATED ALWAYS AS (uuid_extract_timestamp(commit_id)) VIRTUAL,
  -- Not commits_pkey: that is the partition name of a category named pkey.
  CONSTRAINT commits__pkey PRIMARY KEY (category_id, commit_id)
) PARTITION BY LIST (category_id);

-- Store log, written only by the sequencer. Positions are per store and gap-free.
CREATE TABLE log (
  last_position int8 NOT NULL,
  commit_id     uuid NOT NULL,
  category_id   int4 NOT NULL,
  event_count   int2 NOT NULL,
  positions     int8range GENERATED ALWAYS AS
                (int8range(last_position - event_count + 1, last_position + 1)) VIRTUAL,
  PRIMARY KEY (last_position)
) PARTITION BY RANGE (last_position);

-- Covering, so category reads are index-only once vacuumed; the unvacuumed tail is visited in the heap.
CREATE INDEX log_by_category ON log (category_id, last_position) INCLUDE (commit_id, event_count);

-- Latest snapshot per stream, upserted. Partitioned like streams.
CREATE TABLE snapshots (
  version       int8  NOT NULL,                           -- stream version the state includes
  stream_id     uuid  NOT NULL,
  commit_id     uuid  NOT NULL,                           -- commit holding `version`; read bound for replay
  category_id   int4  NOT NULL,
  state_version int4  NOT NULL,                           -- Snapshotted::SNAPSHOT_VERSION when written
  format        int2  NOT NULL,
  state         bytea NOT NULL,
  PRIMARY KEY (stream_id)
) PARTITION BY HASH (stream_id);

DO $$
DECLARE
  n int := coalesce(nullif(current_setting('pges.stream_partitions', true), ''), '64')::int;
BEGIN
  FOR i IN 0..n - 1 LOOP
    EXECUTE format(
      'CREATE TABLE %I PARTITION OF snapshots FOR VALUES WITH (MODULUS %s, REMAINDER %s) '
      'WITH (fillfactor = 70, autovacuum_vacuum_scale_factor = 0.02)',
      'snapshots_p' || lpad(i::text, 3, '0'), n, i);
  END LOOP;
END $$;

-- Commits removed by delete_stream. Their log rows stay (or are written later, when the commit was not yet
-- sequenced), so a store-wide read tells a deleted commit from a log row whose commit is missing for no
-- recorded reason. Insert-only.
CREATE TABLE deleted_commits (
  commit_id   uuid NOT NULL,
  category_id int4 NOT NULL,
  PRIMARY KEY (category_id, commit_id)
);

-- Subscription checkpoints and leases, one row per shard (a subscription without shards has one, shard 0).
-- Updated on every checkpoint; keep HOT.
CREATE TABLE subscriptions (
  position     int8   NOT NULL DEFAULT 0,
  fence        int8   NOT NULL DEFAULT 0,               -- bumped on every lease takeover and on a redefine
  shard        int2   NOT NULL DEFAULT 0,               -- rows with shard >= shards are retired
  shards       int2   NOT NULL DEFAULT 1 CHECK (shards BETWEEN 1 AND 1024),
  lease_owner  uuid,
  lease_until  timestamptz,                             -- NULL until first taken; take over with
                                                        -- WHERE lease_until IS NULL OR lease_until < now()
  name         text   NOT NULL CHECK (name ~ '^[a-z][a-z0-9_.-]{0,99}$'),
  category_ids int4[] NOT NULL,                         -- empty = all categories; checked on start
  PRIMARY KEY (name, shard),
  CHECK (shard >= 0)
) WITH (
  fillfactor                     = 20,
  autovacuum_vacuum_scale_factor = 0.01,
  autovacuum_vacuum_threshold    = 100,
  autovacuum_vacuum_cost_delay   = 0
);

-- Instances of sharded subscriptions, so each takes a fair share of the shards. Renewed every second or so.
CREATE TABLE subscription_members (
  owner      uuid        NOT NULL,
  seen_until timestamptz NOT NULL,
  name       text        NOT NULL,
  PRIMARY KEY (name, owner)
) WITH (
  fillfactor                     = 50,
  autovacuum_vacuum_scale_factor = 0.01,
  autovacuum_vacuum_threshold    = 100
);

-- Backfill progress: one row per lane of a backfill. A stream belongs to lane stream_id % lanes, and
-- position is the lane's cursor. Updated on every lane checkpoint; keep HOT.
CREATE TABLE backfills (
  position     int8   NOT NULL DEFAULT 0,
  until        int8   NOT NULL,                         -- the backfill ends here; fixed at creation
  lane         int2   NOT NULL CHECK (lane >= 0 AND lane < lanes),
  lanes        int2   NOT NULL CHECK (lanes >= 1),
  name         text   NOT NULL CHECK (name ~ '^[a-z][a-z0-9_.-]{0,99}$'),
  category_ids int4[] NOT NULL,                         -- sorted; checked on start
  PRIMARY KEY (name, lane)
) WITH (
  fillfactor                     = 20,
  autovacuum_vacuum_scale_factor = 0.01,
  autovacuum_vacuum_threshold    = 100
);

ALTER PUBLICATION pges ADD TABLE commits (commit_id, stream_id, category_id, last_version, event_count);
