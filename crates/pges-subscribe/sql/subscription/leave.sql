-- Removes an instance of a sharded subscription, so the others take its shards at once.
--
-- $1 the name, $2 the instance.
DELETE FROM {store}.subscription_members WHERE name = $1 AND owner = $2
