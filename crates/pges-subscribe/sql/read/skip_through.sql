-- The skip_through of a subscription started at Start::Retained: the greater of the store's head (as
-- read/head.sql) and the highest retention horizon of any category, 0 for an empty store.
--
-- The head alone is not enough: Admin::prune_log can delete the newest log row of an idle store, the
-- dropped commit's, so the head falls below that category's horizon, and the category would be read
-- strictly. Every horizon is the position of a commit written before this statement, so skipping through
-- it skips only commits that existed before the subscription. One statement: one snapshot for both reads.
SELECT greatest(
         coalesce((SELECT last_position FROM {store}.log ORDER BY last_position DESC LIMIT 1), 0),
         coalesce((SELECT max(horizon_position) FROM {store}.categories), 0)) AS skip_through
