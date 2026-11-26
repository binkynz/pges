-- The start of the outbox recheck: one row if any outbox store has a queued commit. The source runs it
-- after `OutboxConfig::recheck_interval` without a notification, in case one was lost, on its listening
-- connection, which also shows that connection still answers.
--
-- Composed in code: this file, then `outbox/probe_store.sql` once per outbox store with
-- `outbox/probe_union.sql` between them, then `outbox/probe_tail.sql`. One statement however many stores,
-- and it stops at the first row.
SELECT 1 FROM (
