-- What the connections of this application in this database hold, besides the one running the query: how
-- many are inside a transaction, and their locks plus this connection's advisory locks. It also returns when
-- this connection's transaction started, in microseconds since the epoch: two runs in autocommit differ, two
-- runs inside one open transaction do not.
--
-- No parameters: the application is the one this connection names (`application_name`).
SELECT
    (extract(epoch FROM transaction_timestamp()) * 1000000)::int8 AS started_micros,
    (SELECT count(*) FROM pg_stat_activity a
     WHERE a.datname = current_database()
       AND a.application_name = current_setting('application_name')
       AND a.pid <> pg_backend_pid()
       AND a.xact_start IS NOT NULL) AS in_transaction,
    (SELECT count(*) FROM pg_locks l JOIN pg_stat_activity a ON a.pid = l.pid
     WHERE a.datname = current_database()
       AND a.application_name = current_setting('application_name')
       AND (a.pid <> pg_backend_pid() OR l.locktype = 'advisory')) AS locks
