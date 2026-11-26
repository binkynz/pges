-- The bytes of this backend's memory held by cached plans and their sources.
SELECT coalesce(sum(total_bytes), 0)::int8 FROM pg_backend_memory_contexts
WHERE name IN ('CachedPlan', 'CachedPlanSource', 'CachedPlanQuery')
