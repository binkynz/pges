-- Writes a store's log rows: one per commit, in the order positions were assigned. Used by every batch, per
-- store with new rows, and by a resequence in chunks.
--
-- $1 last_position of each commit: the previous one plus the commit's event count, gap-free
-- $2 commit_id
-- $3 category_id
-- $4 event_count
--
-- Arrays and unnest make a batch one statement per store, whatever its size. A row needs a log partition;
-- without one the insert fails and the batch rolls back until partition maintenance creates it.
INSERT INTO {store}.log (last_position, commit_id, category_id, event_count)
SELECT * FROM unnest($1::int8[], $2::uuid[], $3::int4[], $4::int2[])
