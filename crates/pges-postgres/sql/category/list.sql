-- Reads the store's categories, to fill the client's cache when the store is opened, and again when an
-- append or read names a category the cache does not know (registered since).
SELECT category_id, name FROM {store}.categories
