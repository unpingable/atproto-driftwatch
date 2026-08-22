-- NQ saved check: "driftwatch db slack"   (check_mode = non_empty)
--
-- G1, installed 2026-08-20. The EARLY signal of the pair.
--
-- labeler.sqlite runs auto_vacuum=none. Retention frees pages INSIDE the file
-- (freelist) and never returns bytes to the filesystem. Consequence: if
-- retention stops, the DB consumes internal slack for days at ~12 GB/day
-- while `df` reads perfectly flat, and disk-free only moves in the final
-- ~6 hours before the volume wedges.
--
-- Watching the freelist catches that ~1.5-2 days earlier, inside the ~4.5-day
-- budget between "retention stopped" and "hard wedge".
--
-- 5,000,000 pages ~= 19 GiB of remaining internal slack. Projected steady-state
-- freelist under events 3d / edges 7d / claims 30d is ~13M pages, so this sits
-- comfortably below normal and fires only on genuine depletion.
SELECT host, db_path, freelist_count, page_count,
       ROUND(freelist_count * 4096.0 / 1073741824.0, 1) AS slack_gib
FROM monitored_dbs_current
WHERE db_path LIKE '%labeler.sqlite'
  AND freelist_count < 5000000
