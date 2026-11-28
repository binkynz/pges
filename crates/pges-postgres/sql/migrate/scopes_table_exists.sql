-- Whether pges.scopes exists, so the migration lock is its `pges` row (migrate/lock.sql) and not the advisory
-- lock that stands for it before the first shared migration (migrate/bootstrap_lock.sql).
SELECT to_regclass('pges.scopes') IS NOT NULL
