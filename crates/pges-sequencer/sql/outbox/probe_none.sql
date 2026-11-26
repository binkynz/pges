-- The outbox recheck when there is no outbox store: no row. It still runs on the listening connection, which
-- shows that connection still answers.
SELECT 1 WHERE false
