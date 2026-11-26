-- Whether pges.migrations exists: it does not before the first shared migration.
SELECT to_regclass('pges.migrations') IS NOT NULL
