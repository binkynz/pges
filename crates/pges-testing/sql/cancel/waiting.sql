-- Whether another connection to this database ran `$1` (text) as its last statement and now waits inside
-- the transaction: a statement whose reply a proxy holds has reached the server and finished there.
SELECT EXISTS (
    SELECT FROM pg_stat_activity
    WHERE datname = current_database()
      AND pid <> pg_backend_pid()
      AND state = 'idle in transaction'
      AND query = $1
) AS waiting
