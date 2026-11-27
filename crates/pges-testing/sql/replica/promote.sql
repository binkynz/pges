-- Promotes the replica and waits up to a minute for it to accept writes.
SELECT pg_promote(true, 60)
