-- Creates pg_walinspect, which reads WAL records, to count the bytes of full-page images.
CREATE EXTENSION IF NOT EXISTS pg_walinspect
