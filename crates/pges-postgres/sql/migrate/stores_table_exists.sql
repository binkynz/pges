-- Whether pges.stores exists: a store cannot be created before the shared migrations ran.
SELECT to_regclass('pges.stores') IS NOT NULL
