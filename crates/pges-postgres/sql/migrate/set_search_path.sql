-- Sets the search path to the store for the rest of the transaction, so store migrations use unqualified
-- names.
SET LOCAL search_path TO {store}
