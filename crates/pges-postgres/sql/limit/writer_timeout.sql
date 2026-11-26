-- Reads the transaction_timeout that applies to a role in this database: whether the role exists, and the
-- setting's text (NULL: none set).
--
-- $1 role name.
--
-- Settings are read in PostgreSQL's order of precedence: role in database, role, database, every role. A
-- value set only in postgresql.conf, which non-superusers cannot read, or by a session with SET, is not
-- seen. The value is parsed by the client.
SELECT r.oid IS NOT NULL AS found,
       (SELECT substring(cfg FROM '^transaction_timeout=(.*)$')
        FROM pg_db_role_setting s, unnest(s.setconfig) AS cfg
        WHERE s.setrole IN (r.oid, 0)
          AND s.setdatabase IN ((SELECT oid FROM pg_database WHERE datname = current_database()), 0)
          AND cfg LIKE 'transaction\_timeout=%'
        ORDER BY s.setrole <> 0 DESC, s.setdatabase <> 0 DESC
        LIMIT 1) AS setting
FROM (SELECT) AS one LEFT JOIN pg_roles r ON r.rolname = $1
