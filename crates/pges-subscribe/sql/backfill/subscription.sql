-- A subscription's cursor: the lowest cursor of its shards in use, or no row when it does not exist.
--
-- $1 the subscription's name.
SELECT min(position) AS position FROM {store}.subscriptions
WHERE name = $1 AND shard < shards HAVING count(*) > 0
