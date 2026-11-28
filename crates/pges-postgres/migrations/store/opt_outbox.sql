-- store/opt_outbox.sql
-- Optional, applied when a store uses the outbox source (no logical replication), in the transaction that
-- creates the store, right after 0001 and with the same search_path. It is version 1 of the store's option
-- scope `<store>.outbox`, recorded and checksummed like a migration. The append statement inserts one row
-- here per commit when pges.stores.outbox is set (read at Store::open; there is no cargo feature for it). The
-- sequencer drains it in the same transaction that writes the log rows, and assigns positions in seq order:
--
--   WITH d AS (
--     DELETE FROM pending WHERE seq IN (SELECT seq FROM pending ORDER BY seq LIMIT $n) RETURNING *
--   )
--   SELECT * FROM d ORDER BY seq;
--
-- DELETE ... RETURNING has no order guarantee, hence the outer ORDER BY.
--
-- seq must come from an uncached sequence (CACHE 1, the default). The row is inserted at the end of the
-- append, after the command's reads, so if one append saw another's commit its seq is higher. A cached
-- sequence hands out per-backend blocks and breaks that.

INSERT INTO pges.scopes (schema_version, min_compatible, scope)
VALUES (1, 1, current_schema() || '.outbox');

CREATE TABLE pending (
  seq         int8 GENERATED ALWAYS AS IDENTITY (CACHE 1) PRIMARY KEY,
  commit_id   uuid NOT NULL,
  category_id int4 NOT NULL,
  event_count int2 NOT NULL
) WITH (
  -- queue churn: every row is inserted once and deleted once
  autovacuum_vacuum_scale_factor        = 0.01,
  autovacuum_vacuum_threshold           = 500,
  autovacuum_vacuum_insert_scale_factor = 0.2,
  autovacuum_vacuum_insert_threshold    = 1000,
  autovacuum_vacuum_cost_limit          = 4000,
  autovacuum_vacuum_cost_delay          = 0
);

-- Wakes the outbox source: every transaction that enqueued a commit sends one notification per store on the
-- database's channel `pges__outbox` when it commits, with the store's schema name as payload. Notifications
-- with the same channel and payload in one transaction are sent once. Two underscores, because no store's own
-- channel (`pges_<store>`, sent by the sequencer) can take this name: store names start with a letter. A
-- statement that enqueued nothing (a failed expected-version check) sends nothing.
--
-- NOTIFY makes every committing transaction that sent one take a server-wide lock, held until its commit is
-- done, so such commits are serialised. That is the cost of a wake-up without polling.
--
-- search_path is pinned as in pges/0001_init.sql: `enqueued` is the trigger's transition table, which needs no
-- schema.
CREATE FUNCTION notify_pending() RETURNS trigger
  LANGUAGE plpgsql
  SET search_path = pg_catalog, pg_temp
AS $$
BEGIN
  IF EXISTS (SELECT 1 FROM enqueued) THEN
    PERFORM pg_catalog.pg_notify('pges__outbox', TG_TABLE_SCHEMA);
  END IF;
  RETURN NULL;
END $$;

CREATE TRIGGER notify_pending AFTER INSERT ON pending
  REFERENCING NEW TABLE AS enqueued
  FOR EACH STATEMENT EXECUTE FUNCTION notify_pending();

UPDATE pges.stores SET outbox = true WHERE schema_name = current_schema();
