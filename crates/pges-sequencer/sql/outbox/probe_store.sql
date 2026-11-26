-- One outbox store's part of the recheck (see `outbox/probe_head.sql`): a row if its queue is not empty.
(SELECT 1 FROM {store}.pending LIMIT 1)
