-- Imports the snapshot a new replication slot exported at its consistent point, after
-- `transaction/repeatable_read.sql`. The transaction then sees exactly the commits the slot will not stream.
--
-- The snapshot name is interpolated: utility commands take no bind parameters. It holds only hex digits and
-- dashes, so it cannot end the literal. The export ends with the next command on the replication
-- connection, so that connection runs nothing until this transaction is done.
SET TRANSACTION SNAPSHOT '{snapshot}'
