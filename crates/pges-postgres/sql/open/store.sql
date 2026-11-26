-- Reads a store's row when it is opened: its id, its outbox and DCB flags, its recorded max_clock_skew, and
-- whether the opening writer's skew fits within it. No row: the store does not exist.
--
-- $1 store name, $2 the writer's max_clock_skew, NULL for the store's own (which always fits).
--
-- A writer with a wider skew than the store's is refused: it could write a commit into a leaf retention is
-- dropping, or one the sequencer's resequence does not search.
SELECT store_id, outbox, dcb, max_clock_skew, max_clock_skew >= coalesce($2::interval, max_clock_skew)
FROM pges.stores WHERE schema_name = $1
