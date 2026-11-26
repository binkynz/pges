-- Makes the current transaction's commit wait for its WAL flush, whatever the session's setting.
--
-- Run in a transaction that stores a resume token. The source releases everything up to the token once it
-- commits (a replication slot is confirmed past it). An asynchronous commit could be lost in a server crash
-- after that, together with the log rows it covers. This one is flushed, and with it every earlier batch.
SET LOCAL synchronous_commit = on
