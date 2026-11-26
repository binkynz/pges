-- Sends one notification per store a batch wrote to, after the batch committed, on a pool connection of its
-- own so a slow or failing notification never holds up the log.
--
-- $1 the channel of each store: `pges_` and its schema name
-- $2 the payload: the store's high-water mark and the categories the batch touched
--
-- One statement, so the notifications commit together in an implicit transaction of their own.
SELECT pg_notify(n.channel, n.payload)
FROM unnest($1::text[], $2::text[]) AS n(channel, payload)
