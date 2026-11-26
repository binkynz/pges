-- Sets the session up for maintenance: partition bounds are computed in UTC, and DDL fails after waiting one
-- second for a lock, so it can be retried instead of queueing appends behind it. No JIT: retention's
-- statements look streams up one at a time, and the planner's estimate of that cost would compile them for
-- longer than they run.
--
-- Three statements, sent with the simple query protocol. The connection is closed afterwards, so the settings
-- never reach other users of the pool.
SET TimeZone = 'UTC'; SET lock_timeout = '1s'; SET jit = off
