-- Creates a new leaf's unique (stream_id, last_version) index, named <leaf>_s.
--
-- It is per leaf because a partitioned unique index must include the partition key. It backs up the streams
-- row lock within a time bucket and serves stream reads.
CREATE UNIQUE INDEX {index} ON {store}.{leaf} (stream_id, last_version)
