-- The databases whose names start with t_, the prefix of test databases. The caller drops those whose name
-- says they were created long ago.
SELECT datname::text FROM pg_database WHERE datname LIKE 't\_%'
