-- Begins a run_pg or backfill batch's transaction in SERIALIZABLE, for a handler that asks for it.
BEGIN ISOLATION LEVEL SERIALIZABLE
