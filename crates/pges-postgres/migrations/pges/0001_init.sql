-- pges/0001_init.sql
-- Shared objects, once per database. Applied by the crate's migration runner inside one transaction,
-- holding pg_advisory_xact_lock(hashtext('pges.migrate')), which stands for the migration lock until
-- pges.scopes exists; later migrations lock the scope's row in pges.scopes instead.
--
-- Functions pin search_path to pg_catalog, pg_temp and name every pges object with its schema, so neither a
-- caller's search_path nor an object another role creates in public (a function or operator with the same
-- name and a better-matching signature) changes what they call. pg_temp comes last, so a temporary object
-- never shadows a built-in. The migration itself runs with the same search_path, because a function with a
-- SQL-standard body (uuidv7_floor) is bound when it is created, not when it is called.

SET LOCAL search_path = pg_catalog, pg_temp;

CREATE SCHEMA IF NOT EXISTS pges;

-- Applied migrations for every scope: 'pges' itself, each store schema, and a store's option scripts
-- (`<store>.outbox`).
CREATE TABLE pges.migrations (
  applied_at timestamptz NOT NULL DEFAULT now(),
  version    int4        NOT NULL,
  scope      text        NOT NULL,
  name       text        NOT NULL,
  checksum   bytea       NOT NULL,              -- sha256 of the migration text
  PRIMARY KEY (scope, version)
);

-- Compatibility window per scope. A crate whose schema version for the scope is below min_compatible
-- must refuse to start.
CREATE TABLE pges.scopes (
  schema_version int4 NOT NULL,
  min_compatible int4 NOT NULL,
  scope          text PRIMARY KEY,
  CONSTRAINT scopes_window CHECK (min_compatible <= schema_version)
);
INSERT INTO pges.scopes (schema_version, min_compatible, scope) VALUES (1, 1, 'pges');

CREATE TABLE pges.stores (
  max_clock_skew interval NOT NULL DEFAULT '5 seconds' -- furthest a commit id may be from the clock;
                 CHECK (max_clock_skew >= '0'),        -- writers' StoreConfig::max_clock_skew must not
                                                       -- exceed it; retention and resequence use it
  max_txn_age   interval NOT NULL DEFAULT '1 hour'  -- longest writer transaction (the writer roles'
                CHECK (max_txn_age > '0'),          -- transaction_timeout); retention and resequence use it
  last_position int8 NOT NULL DEFAULT 0,        -- store high-water mark, written by the sequencer at most
                                                -- once a second; the exact value is max(log.last_position)
  store_id      int4 GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  retention     int2 NOT NULL DEFAULT 0         -- default retention of new categories; codes as
                CHECK (retention BETWEEN 0 AND 3), -- <store>.categories.retention
  outbox        bool NOT NULL DEFAULT false,    -- appends also enqueue into <store>.pending (opt_outbox.sql)
  dcb           bool NOT NULL DEFAULT false,    -- set by opt_dcb.sql, which no release applies; Store::open
                                                -- refuses such a store (DCB appends are not implemented)
  retain_for    interval,                       -- with retention, as <store>.categories.retain_for
  schema_name   text NOT NULL UNIQUE,
  CONSTRAINT stores_retain_for CHECK ((retention = 0) = (retain_for IS NULL))
);

-- One kind of sequencer source per database: either every store has the outbox or none has. A store row is
-- inserted with its final outbox flag. The check first takes the migration lock, the 'pges' row of
-- pges.scopes (which the runner already holds), so of two READ COMMITTED transactions adding stores of
-- different kinds, the second sees the first's row once it commits. A row lock, not an advisory lock, so
-- only roles with UPDATE on pges.scopes can hold it. SQLSTATE PGE02 maps to MigrateError::OutboxMismatch.
CREATE FUNCTION pges.check_one_source_kind() RETURNS trigger
  LANGUAGE plpgsql
  SET search_path = pg_catalog, pg_temp
AS $$
BEGIN
  PERFORM 1 FROM pges.scopes WHERE scope = 'pges' FOR UPDATE;
  IF EXISTS (SELECT 1 FROM pges.stores WHERE outbox <> NEW.outbox) THEN
    RAISE EXCEPTION 'store % has outbox = %, unlike the other stores of this database', NEW.schema_name,
      NEW.outbox USING ERRCODE = 'PGE02';
  END IF;
  RETURN NULL;
END $$;

CREATE TRIGGER one_source_kind AFTER INSERT OR UPDATE OF outbox ON pges.stores
  FOR EACH ROW EXECUTE FUNCTION pges.check_one_source_kind();

-- Single row: sequencer leadership and progress. Updated on leadership change, lease renewal and token
-- persistence only, not on every batch: a batch checks the fence with a row lock.
CREATE TABLE pges.sequencer (
  fence        int8 NOT NULL DEFAULT 0,         -- bumped on every leadership change
  singleton    bool NOT NULL DEFAULT true PRIMARY KEY CHECK (singleton),
  lease_until  timestamptz,                     -- NULL until the first leader; acquire with
                                                -- WHERE lease_until IS NULL OR lease_until < now()
  source       text,                            -- source kind that wrote source_token, e.g. 'logical'
  source_token bytea,                           -- opaque resume point (LSN, Kafka offset, ...); everything
                                                -- before it is in log
  halted       text,                            -- set when sequencing stopped for a reason that needs an
                                                -- operator (e.g. 'slot invalidated'); cleared by resequence
  leader       text                             -- watch endpoint address of the current leader
);
INSERT INTO pges.sequencer DEFAULT VALUES;

-- Lowest uuidv7 for a timestamp (millisecond floor). Used for commit partition bounds. Pass timestamptz
-- values with an explicit offset ('2026-12-01 00:00:00+00'); a bare literal is read in the session TimeZone.
CREATE FUNCTION pges.uuidv7_floor(ts timestamptz) RETURNS uuid
  LANGUAGE sql IMMUTABLE STRICT PARALLEL SAFE
  SET search_path = pg_catalog, pg_temp
  RETURN (pg_catalog.lpad(pg_catalog.to_hex(pg_catalog.floor(EXTRACT(epoch FROM ts) * 1000)::int8), 12, '0')
          || '70008000000000000000')::uuid;

-- Rejects a client commit id that is not a UUIDv7 (SQLSTATE 22023), or whose timestamp is more than `tol`
-- from the statement's start (SQLSTATE PGE01, which maps to AppendError::CommitIdSkew). Called from the
-- append statements. statement_timestamp(), not now(), so a long UnitOfWork does not drift.
CREATE FUNCTION pges.check_commit_id(id uuid, tol interval) RETURNS uuid
  LANGUAGE plpgsql STABLE PARALLEL SAFE
  SET search_path = pg_catalog, pg_temp
AS $$
BEGIN
  IF pg_catalog.uuid_extract_version(id) IS DISTINCT FROM 7 THEN
    RAISE EXCEPTION 'commit id % is not a UUIDv7', id USING ERRCODE = '22023';
  END IF;
  IF pg_catalog.uuid_extract_timestamp(id)
       NOT BETWEEN pg_catalog.statement_timestamp() - tol AND pg_catalog.statement_timestamp() + tol THEN
    RAISE EXCEPTION 'commit id % is outside the clock skew tolerance %', id, tol USING ERRCODE = 'PGE01';
  END IF;
  RETURN id;
END $$;

-- Store creation adds the store's commits table to this publication (store/0001_init.sql says when it does
-- not), and the logical sequencer source adds the commits table of any store missing from it. Only inserts
-- are published, and the column list (set per table) leaves out the payload from the output. Decoding still
-- reads every WAL record of the database, payload included.
CREATE PUBLICATION pges WITH (publish = 'insert', publish_via_partition_root = true);
