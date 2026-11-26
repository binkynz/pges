-- The replica's WAL page and segment sizes in bytes, read once. A target insert position just after a page
-- header is lowered to the page start before it is compared with the replayed position, which is the end of
-- the last replayed record; the header's size depends on whether the page starts a segment.
SELECT current_setting('wal_block_size')::int8,
(SELECT setting::int8 FROM pg_settings WHERE name = 'wal_segment_size')
