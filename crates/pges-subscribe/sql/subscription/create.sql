-- Creates every shard row of a subscription, unless the subscription exists.
--
-- $1 the start position, $2 the name, $3 the number of shards, $4 the category ids (int4[], empty for the
-- whole store). An instance creating it at the same moment gets a unique violation on (name, shard), which the
-- caller ignores; the stored definition is compared when a lease is taken.
INSERT INTO {store}.subscriptions (position, shard, shards, name, category_ids)
SELECT $1::int8, s, $3::int2, $2::text, $4::int4[]
FROM generate_series(0, $3::int2 - 1) AS s
WHERE NOT EXISTS (SELECT 1 FROM {store}.subscriptions WHERE name = $2)
