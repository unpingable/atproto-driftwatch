-- NQ saved check: "driftwatch volume free space"   (check_mode = non_empty)
--
-- G1, installed 2026-08-20 after the 2026-08-12..08-20 blind period.
--
-- Why this exists: /mnt/zonestorage is invisible to NQ's per-host disk model.
-- hosts_current / v_hosts carry ONE filesystem per host (the root fs), so the
-- built-in "disk critical" check reported PASS while the data volume sat at
-- 100% used with 0 bytes available. This check reads the node_filesystem
-- series directly and does not require changing the host model.
--
-- Threshold note: uses node_filesystem_FREE_bytes (root view), NOT
-- node_filesystem_avail_bytes. avail reads 0 permanently on this volume
-- because ext4 reserves 5% and the production writer runs as root.
--
-- 1.5 GiB floor sits below the ~3.0 GiB steady-state free space, so it does
-- not fire in normal operation. Warning horizon is only ~6h: with
-- auto_vacuum=none the file does not extend until internal slack is gone,
-- so disk-free stays flat through the slow part of the failure. Pair this
-- with driftwatch-db-slack.sql, which warns ~1.5-2 days earlier.
SELECT m.host,
       json_extract(s.labels_json, '$.mountpoint') AS mountpoint,
       ROUND(m.value / 1073741824.0, 2) AS free_gib
FROM series s
JOIN metrics_current m ON m.series_id = s.series_id
WHERE s.metric_name = 'node_filesystem_free_bytes'
  AND json_extract(s.labels_json, '$.mountpoint') = '/mnt/zonestorage'
  AND m.value < 1610612736
