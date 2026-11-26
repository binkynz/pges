-- The ids of a store's newest commits, newest first, to seed the dedupe window when a term starts: a source
-- may replay commits from before the durable token.
--
-- $1 how many (`SequencerConfig::dedupe_tail`)
SELECT commit_id FROM {store}.log ORDER BY last_position DESC LIMIT $1
