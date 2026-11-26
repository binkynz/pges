-- Records an instance of a sharded subscription as alive and counts the live instances, itself included.
--
-- $1 the name, $2 the instance, $3 how long the record lasts. Records of other instances that lapsed are
-- removed in the same statement. Each instance takes about its share of the shards from the count.
WITH gone AS (
  DELETE FROM {store}.subscription_members
  WHERE name = $1 AND owner <> $2 AND seen_until < now()),
seen AS (
  INSERT INTO {store}.subscription_members (owner, seen_until, name)
  VALUES ($2, now() + $3, $1)
  ON CONFLICT (name, owner) DO UPDATE SET seen_until = excluded.seen_until)
SELECT count(*) + 1 FROM {store}.subscription_members
WHERE name = $1 AND owner <> $2 AND seen_until >= now()
