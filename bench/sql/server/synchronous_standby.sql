-- Makes the standby named replica (its application_name) synchronous: commits wait for it. It applies
-- after server/reload.sql.
ALTER SYSTEM SET synchronous_standby_names = 'replica'
