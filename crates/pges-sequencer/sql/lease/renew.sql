-- Extends the lease held under a fence.
--
-- $1 how long the lease lasts from now
-- $2 the holder's fence
--
-- Zero rows: another process bumped the fence and took the lease over; the holder stands down.
UPDATE pges.sequencer SET lease_until = now() + $1 WHERE fence = $2
