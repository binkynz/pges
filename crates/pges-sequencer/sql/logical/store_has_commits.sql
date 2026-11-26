-- A row if a store holds any commit, run per store in the snapshot a new slot exported at its consistent
-- point (`transaction/snapshot.sql`). Such a commit was made before the slot and would never be delivered,
-- so the new slot is dropped again and sequencing halts.
SELECT 1 FROM {store}.commits LIMIT 1
