-- Creates scale_commit_id(category, g, t0, span, n): the id of generated commit g of a category, a UUIDv7
-- whose millisecond timestamp is t0 + floor(span * g / n) and whose random bits are an md5 of the category
-- and g. The commits and their log rows are both computed from it, so they agree without reading each other,
-- and ids grow with g at millisecond resolution.
CREATE OR REPLACE FUNCTION public.scale_commit_id(category int4, g int8, t0 int8, span int8, n int8) RETURNS uuid LANGUAGE sql IMMUTABLE PARALLEL SAFE RETURN (lpad(to_hex(t0 + div(span::numeric * g, n)::int8), 12, '0') || '7' || substr(md5(category::text || ':' || g::text), 1, 3) || '8' || substr(md5(category::text || ':' || g::text), 4, 15))::uuid
