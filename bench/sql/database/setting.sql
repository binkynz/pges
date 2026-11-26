-- A server setting's current value, for the report. NULL if there is no setting of that name.
--
-- $1 the setting's name.
SELECT current_setting($1, true)
