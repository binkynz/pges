-- Fills a category's leaves with generated single-event commits, copying the category, frame format and
-- payload of the store's one real commit.
--
-- $1 the number of commits, $2 the number of streams, $3 the days before today the first leaf starts.
--
-- Commit ids spread evenly from the first leaf's start to a minute ago, so every leaf holds about the same
-- number of commits. Each stream's commits get consecutive versions.
WITH template AS (SELECT category_id, format, payload FROM {store}.commits LIMIT 1),
     span AS (SELECT extract(epoch FROM now() - interval '1 minute'
                            - (date_trunc('day', now() AT TIME ZONE 'UTC') AT TIME ZONE 'UTC'
                               - make_interval(days => $3::int4))) AS seconds)
INSERT INTO {store}.commits (last_version, commit_id, stream_id, category_id, event_count, format, payload)
SELECT g / $2 + 1,
       uuidv7(-make_interval(secs => span.seconds * (1 - g::float8 / $1) + 60)),
       md5((g % $2)::text)::uuid, t.category_id, 1, t.format, t.payload
FROM generate_series(0, $1 - 1) g, template t, span
