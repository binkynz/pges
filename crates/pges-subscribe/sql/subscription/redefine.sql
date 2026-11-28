-- Replaces a subscription's stored definition and moves the cursor of every shard, when the definition
-- changed: the number of shards or the category ids.
--
-- $1 the name, $2 the new cursor, $3 the number of shards, $4 the category ids (int4[]), $5 the position
-- through which commits retention dropped are skipped (0: none).
--
-- Changed rows get a new fence and lose their lease, so a running instance stops. Shards that did not exist
-- are added; rows of shards beyond the new count stay, unused (acquire takes only shards below the count).
WITH changed AS (
  UPDATE {store}.subscriptions
  SET position = $2, skip_through = $5, shards = $3, category_ids = $4, fence = fence + 1,
      lease_owner = NULL, lease_until = NULL
  WHERE name = $1 AND (shards <> $3 OR category_ids <> $4)
  RETURNING shard)
INSERT INTO {store}.subscriptions (position, skip_through, shard, shards, name, category_ids)
SELECT $2::int8, $5::int8, s, $3::int2, $1::text, $4::int4[]
FROM generate_series(0, $3::int2 - 1) AS s
WHERE EXISTS (SELECT 1 FROM changed) AND s NOT IN (SELECT shard FROM changed)
ON CONFLICT (name, shard) DO NOTHING
