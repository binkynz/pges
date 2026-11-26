-- Where a store's search starts, from its newest log row. No row: the log is empty and every commit is
-- searched.
--
-- $1 the store's window (`resequence/window.sql`)
--
-- `floor`: the smallest commit id an unsequenced commit can have, one window older than the newest
-- sequenced commit. `stop`: one more window older. Every log row of an id at or above `floor` lies after the
-- newest row with an id below `stop`, so the search for log rows starts there.
SELECT pges.uuidv7_floor(uuid_extract_timestamp(commit_id) - $1::interval) AS floor,
       pges.uuidv7_floor(uuid_extract_timestamp(commit_id) - 2 * $1::interval) AS stop
FROM {store}.log ORDER BY last_position DESC LIMIT 1
