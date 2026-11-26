-- Compresses a new leaf's payloads with lz4.
ALTER TABLE {store}.{leaf} ALTER COLUMN payload SET COMPRESSION lz4
