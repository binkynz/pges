-- The commits of one category named by a page of the log, for one shard of a category read or a store-wide
-- read.
--
-- $1 the commit ids (uuid[]), $2 the number of shards, $3 the shard. The payload of another shard's commit is
-- NULL rather than the row left out, so a missing row still means that retention removed it.
--
-- The shard of a stream is the first 8 bytes of the SHA-256 of its 16 id bytes, read as a big-endian integer
-- without its sign bit, modulo the number of shards. SHA-256 and the byte order of uuid_send are fixed, so a
-- stream keeps its shard across PostgreSQL versions and platforms.
SELECT commit_id, stream_id, last_version, format,
       CASE WHEN ((substring(sha256(uuid_send(stream_id)) FROM 1 FOR 8)::int8
                   & 9223372036854775807) % $2) = $3
            THEN payload END AS payload
FROM {store}.commits_{category} WHERE commit_id = ANY($1)
