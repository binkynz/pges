-- Creates scale_stream_id(category, n): the id of generated stream n of a category, an md5 of the two, so
-- ids are spread like hashed stream keys and a benchmark can name a random existing stream without a lookup.
CREATE OR REPLACE FUNCTION public.scale_stream_id(category int4, n int8) RETURNS uuid LANGUAGE sql IMMUTABLE PARALLEL SAFE RETURN md5('s' || category::text || ':' || n::text)::uuid
