-- Pauses WAL replay on the replica: it keeps receiving WAL but shows nothing new until replica/resume.sql.
SELECT pg_wal_replay_pause()
