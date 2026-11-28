-- The owner of an existing schema pges, read before the first shared migration creates its tables there. No
-- row: the schema does not exist.
--
-- `owned` is whether the current role is the owner itself. Membership in the owner's role is not enough, and
-- neither is being a superuser: a schema's owner may drop any table in it and put its own in its place, with
-- triggers that then run as whoever writes to it.
SELECT n.nspowner::regrole::text AS owner,
       n.nspowner = (SELECT r.oid FROM pg_catalog.pg_roles AS r WHERE r.rolname = current_user) AS owned
FROM pg_catalog.pg_namespace AS n
WHERE n.nspname = 'pges'
