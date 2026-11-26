-- Whether the shared pges schema exists, checked before its version is read.
SELECT to_regclass('pges.scopes') IS NOT NULL
