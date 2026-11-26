-- Makes the transaction read one snapshot: the range resequence wrote, the stop position and the scan of the
-- log must agree.
--
-- A resequence that committed between reading the range and the scan would add rows the scan treats as being
-- in commit order, and the next scan start could land past rows of commits that stay. One that commits after
-- the snapshot only adds rows past the log tail, which the next run covers.
SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY
