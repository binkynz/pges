-- Attaches a category's partition to commits under the category's id.
--
-- ATTACH PARTITION takes only SHARE UPDATE EXCLUSIVE on commits. The partition is empty, so there is nothing
-- to validate.
ALTER TABLE {store}.commits ATTACH PARTITION {store}.commits_{category} FOR VALUES IN ({category_id})
