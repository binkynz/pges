-- The active client backends of this database other than this one, and how many of them wait on the lock
-- manager's LWLocks.
SELECT count(*) FILTER (WHERE state = 'active'),
       count(*) FILTER (WHERE state = 'active' AND wait_event = 'LockManager')
FROM pg_stat_activity
WHERE datname = current_database() AND backend_type = 'client backend'
  AND pid <> pg_backend_pid()
