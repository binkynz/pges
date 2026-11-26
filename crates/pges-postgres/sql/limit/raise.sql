-- Records a store's writer limits, unless either is below the recorded value. No row updated: the store does
-- not exist or a limit would be lowered.
--
-- $1 store name, $2 max_clock_skew, $3 max_txn_age.
--
-- Limits only rise: commits written under a larger value stay in the log out of commit id order by up to that
-- much, and nothing records where. Runs under the maintenance lock, the only other writer of these columns
-- being store creation.
UPDATE pges.stores SET max_clock_skew = $2, max_txn_age = $3
WHERE schema_name = $1 AND max_clock_skew <= $2 AND max_txn_age <= $3
